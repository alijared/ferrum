use crate::io;
use crate::io::tables::{start_compaction, MakeBatch, Table, TableOptions};
use crate::io::writer;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use datafusion::prelude::SessionContext;
use log::{debug, error};
use std::marker::PhantomData;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tonic::async_trait;

pub struct GenericTable<R: MakeBatch<T>, T: Clone + Send + Sync> {
    _phantom: PhantomData<R>,
    name: String,
    schema: Schema,
    opts: TableOptions,
    bus: broadcast::Receiver<T>,
    buffer: Vec<RecordBatch>,
}

#[async_trait]
impl<R: MakeBatch<T> + Send + Sync, T: Clone + Send + Sync> Table<R, T> for GenericTable<R, T> {
    async fn start(&mut self, cancellation_token: CancellationToken) {
        let ctx = io::get_sql_context();
        let mut interval = tokio::time::interval(Duration::from_secs(1));

        let name = self.name.clone();
        let compaction_handler = start_compaction(
            &name,
            self.opts.clone(),
            self.schema.clone(),
            cancellation_token.clone(),
        ).await;

        let cancellation_token = cancellation_token.clone();
        loop {
            tokio::select! {
                _ = interval.tick() => self.flush(ctx).await,
                _ = cancellation_token.cancelled() => {
                    self.flush(ctx).await;
                    break;
                }
                bus = self.bus.recv() => match bus {
                    Ok(batch) => self.buffer.push(R::make_batch(batch)),
                    Err(e) => {
                        error!("Failed to receive message: {}", e);
                    }
                }
            }
        }

        let _ = compaction_handler.await;
    }
}

impl<R: MakeBatch<T>, T: Clone + Send + Sync> GenericTable<R, T> {
    pub fn new(
        name: &str,
        opts: TableOptions,
        schema: Schema,
        bus: broadcast::Receiver<T>,
    ) -> Self {
        Self {
            _phantom: Default::default(),
            name: name.to_string(),
            opts,
            schema,
            bus,
            buffer: Vec::new(),
        }
    }

    async fn flush(&mut self, ctx: &SessionContext) {
        debug!("Flushing data...");
        if !self.buffer.is_empty() {
            match writer::combine_batches(&self.buffer) {
                Ok(batch) => {
                    let write_opts = self.opts.clone().into();
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
}
