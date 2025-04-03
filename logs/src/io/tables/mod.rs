use crate::io;
use crate::io::fs::FileSystem;
use crate::io::writer;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::config::TableParquetOptions;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::listing::ListingOptions;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use log::{error, info};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, RwLock};
use tokio_util::sync::CancellationToken;
use tonic::async_trait;

mod generic;
pub mod log_attributes;
pub mod logs;

#[async_trait]
trait Table<F: FileSystem, T: Clone + Sync + Send> {
    async fn start(&self, cancellation_token: CancellationToken) {
        let table_name = self.name();
        info!("Starting {} table", table_name);
        let fs = self.filesystem();
        let compaction_handler = fs.start_compactor(&table_name, cancellation_token.clone());

        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let mut rx = self.write_rx();
        let ctx = io::get_session_context();
        loop {
            tokio::select! {
                _ = interval.tick() => self.flush(ctx).await,
                _ = cancellation_token.cancelled() => {
                    self.flush(ctx).await;
                    break;
                }
                batch = rx.recv() => match batch {
                    Ok(b) => {
                        self.write_buffer(Self::make_batch(b)).await;
                    }
                    Err(e) => {
                        error!("Failed to receive message in {} table: {}", table_name, e);
                    }
                }
            }
        }

        compaction_handler.await;
        info!("Stopped {} table", table_name);
    }

    async fn flush(&self, ctx: &SessionContext) {
        let buffer_guard = self.buffer();
        let mut buffer = buffer_guard.write().await;
        let df_options = self.data_frame_write_options();
        let writer_options = self.writer_options();

        if !buffer.is_empty() {
            let table_name = self.name();
            info!("Flushing data for {} table", table_name);
            match writer::combine_batches(&buffer) {
                Ok(batch) => {
                    let table_path = self.table_path();
                    if let Err(e) = writer::write_batch(
                        ctx,
                        &table_path,
                        df_options,
                        writer_options,
                        vec![batch],
                    )
                    .await
                    {
                        error!("Failed to write data to {} table: {}", table_name, e);
                    }
                    buffer.clear();
                }
                Err(e) => {
                    error!("Failed to combine batches for {} table: {}", table_name, e);
                }
            }
        }
    }

    async fn write_buffer(&self, record_batch: RecordBatch) {
        let guard = self.buffer();
        let mut buffer = guard.write().await;
        buffer.push(record_batch);
    }

    fn table_path(&self) -> String {
        let url = self.filesystem().url();
        format!("{}/{}", url.as_str(), self.name())
    }

    fn name(&self) -> String;

    fn make_batch(data: T) -> RecordBatch;

    fn filesystem(&self) -> Arc<F>;

    fn write_rx(&self) -> broadcast::Receiver<T>;

    fn buffer(&self) -> Arc<RwLock<Vec<RecordBatch>>>;

    fn data_frame_write_options(&self) -> DataFrameWriteOptions;

    fn writer_options(&self) -> TableParquetOptions;
}

async fn register(
    name: &str,
    url: impl AsRef<str>,
    opts: ListingOptions,
    schema: SchemaRef,
) -> Result<(), DataFusionError> {
    info!("Registering {} table", name);

    let ctx = io::get_session_context();
    ctx.register_listing_table(name, url, opts, Some(schema), None)
        .await?;

    info!("Registered {} table", name);
    Ok(())
}
