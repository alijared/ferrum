use crate::io;
use crate::io::writer;
use chrono::Utc;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::listing::ListingOptions;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use log::{error, info};
use object_store::Filesystem;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, RwLock};
use tokio_util::sync::CancellationToken;
use tonic::async_trait;

mod generic;
pub mod log_attributes;
pub mod logs;

#[async_trait]
trait Table<T: Clone + Sync + Send> {
    async fn start(&self, cancellation_token: CancellationToken) {
        let table_name = self.name();

        info!("Starting {} table", &table_name);

        let compactor_cancellation_token = cancellation_token.clone();
        let compaction_frequency = self.compaction_frequency();
        let fs = self.filesystem();
        let df_opts = self.data_frame_write_options();
        let compactor_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(compaction_frequency);
            let ctx = io::get_session_context();

            let mut df_opts = Some(df_opts);
            let table_name = table_name.clone();
            loop {
                let df_opts = df_opts.take().unwrap_or_default();
                let partition_day = Utc::now();
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = fs.compact(ctx, &table_name, partition_day, df_opts).await {
                            error!("Error compacting {} table files: {}", &table_name, e);
                        }
                    }
                    _ = compactor_cancellation_token.cancelled() => {
                        break;
                    }
                }
            }
        });

        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let mut rx = self.write_rx();
        let ctx = io::get_session_context();
        let table_name = self.name();
        loop {
            tokio::select! {
                _ = interval.tick() => self.flush(ctx).await,
                _ = cancellation_token.cancelled() => {
                    self.flush(ctx).await;
                    break;
                }
                batch = rx.recv() => match batch {
                    Ok(b) => {
                        self.write_buffer(self.make_batch(b)).await;
                    }
                    Err(e) => {
                        error!("Failed to receive message in {} table: {}", &table_name, e);
                    }
                }
            }
        }

        info!("Stopping {} table", &table_name);
        if let Err(e) = compactor_handle.await {
            error!("Error joining compactor handle: {}", e);
        }

        info!("Stopped {} table", &table_name);
    }

    async fn flush(&self, ctx: &SessionContext) {
        let buffer_guard = self.buffer();
        let mut buffer = buffer_guard.write().await;
        let df_options = self.data_frame_write_options();

        if !buffer.is_empty() {
            let table_name = self.name();
            match writer::combine_batches(&buffer) {
                Ok(batch) => {
                    if let Err(e) =
                        writer::write_batch(ctx, &table_name, df_options, vec![batch]).await
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

    fn name(&self) -> String {
        "".to_string()
    }

    fn make_batch(&self, _data: T) -> RecordBatch {
        RecordBatch::new_empty(Arc::new(self.schema()))
    }

    fn data_frame_write_options(&self) -> DataFrameWriteOptions {
        DataFrameWriteOptions::default()
    }

    fn schema(&self) -> Schema {
        Schema::empty()
    }

    fn filesystem(&self) -> Arc<Filesystem>;

    fn compaction_frequency(&self) -> Duration;

    fn write_rx(&self) -> broadcast::Receiver<T>;

    fn buffer(&self) -> Arc<RwLock<Vec<RecordBatch>>>;
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
