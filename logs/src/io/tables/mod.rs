use crate::io;
use crate::io::writer;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::DEFAULT_PARQUET_EXTENSION;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::DataFusionError;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use log::{debug, error, info};
use std::fs;
use std::path::Path;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub mod logs;

pub struct Table {
    name: String,
    write_opts: Option<DataFrameWriteOptions>,
    bus: mpsc::Receiver<RecordBatch>,
    buffer: Vec<RecordBatch>,
}

impl Table {
    pub fn new(
        name: &str,
        write_opts: DataFrameWriteOptions,
        bus: mpsc::Receiver<RecordBatch>,
    ) -> Self {
        Self {
            name: name.to_string(),
            write_opts: Some(write_opts),
            bus,
            buffer: Vec::new(),
        }
    }

    pub async fn start_writer(&mut self, cancellation_token: CancellationToken) {
        let ctx = io::get_sql_context();
        let mut interval = tokio::time::interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                _ = interval.tick() => self.flush(ctx).await,
                _ = cancellation_token.cancelled() => {
                    self.flush(ctx).await;
                    break;
                }
                bus = self.bus.recv() => match bus {
                    Some(batch) => self.buffer.push(batch),
                    None => {
                        self.flush(ctx).await;
                        break;
                    }
                }
            }
        }
    }

    async fn flush(&mut self, ctx: &SessionContext) {
        debug!("Flushing data...");
        let write_opts = self.get_write_options();
        if !self.buffer.is_empty() {
            match writer::combine_batches(&self.buffer) {
                Ok(batch) => {
                    if let Err(e) =
                        writer::write_batch(ctx, &self.name, write_opts, vec![batch]).await
                    {
                        error!("Failed to write logs: {}", e);
                    }
                    self.buffer.clear();
                }
                Err(e) => {
                    error!("Failed to combine batches: {}", e);
                }
            }
        }
    }

    async fn start_compaction(
        frequency: Duration,
        table_name: &str,
        data_path: &str,
        mut write_opts: Option<DataFrameWriteOptions>,
        schema: Option<&Schema>,
        cancellation_token: CancellationToken,
    ) {
        let ctx = io::get_sql_context();
        let mut interval = tokio::time::interval(frequency);
        loop {
            let write_opts = write_opts.take().unwrap_or_default();
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e)
                        = Table::run_compaction(ctx, table_name, data_path, write_opts, schema).await {
                            error!("Error compacting partitions: {}", e);
                    }
                }
                _ = cancellation_token.cancelled() => {
                    break;
                }
            }
        }
    }

    async fn run_compaction(
        ctx: &SessionContext,
        table_name: &str,
        data_path: &str,
        write_opts: DataFrameWriteOptions,
        schema: Option<&Schema>,
    ) -> Result<(), DataFusionError> {
        let day = chrono::Utc::now().format("%Y-%m-%d").to_string();
        let part_name = format!("day={}", day);
        let part_dir = Path::new(data_path).join(&part_name);

        if !part_dir.exists() {
            return Ok(());
        }

        let rd = fs::read_dir(part_dir).map_err(DataFusionError::IoError)?;
        let mut files = Vec::new();
        for entry_result in rd {
            let entry = entry_result.map_err(DataFusionError::IoError)?;
            if let Some(ext) = entry.path().extension() {
                let fext = format!(".{}", ext.to_str().unwrap_or_default());
                if fext == DEFAULT_PARQUET_EXTENSION {
                    files.push(entry.path());
                }
            }
        }

        if files.is_empty() || files.len() == 1 {
            return Ok(());
        }

        info!("Running table compaction...");
        let df = ctx
            .read_parquet(
                files
                    .iter()
                    .map(|s| s.to_str().unwrap())
                    .collect::<Vec<_>>(),
                ParquetReadOptions {
                    schema,
                    ..Default::default()
                },
            )
            .await?;

        let batches = df.collect().await?;
        writer::write_batch(ctx, table_name, write_opts, batches).await?;
        io::clear_partition_files(files);

        info!("Table compaction completed");
        Ok(())
    }

    fn get_write_options(&mut self) -> DataFrameWriteOptions {
        self.write_opts.take().unwrap_or_default()
    }
}
