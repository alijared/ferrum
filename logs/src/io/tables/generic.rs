use crate::io::fs::FileSystem;
use crate::io::tables::Table;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use datafusion::config::TableParquetOptions;
use datafusion::dataframe::DataFrameWriteOptions;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

pub struct GenericTable<F: FileSystem, T: Clone + Send + Sync> {
    filesystem: Arc<F>,
    write_rx: broadcast::Receiver<T>,
    buffer: Arc<RwLock<Vec<RecordBatch>>>,
    parquet_opts: TableParquetOptions,
}

impl<F: FileSystem, T: Clone + Send + Sync> GenericTable<F, T> {
    pub fn new(filesystem: Arc<F>, write_rx: broadcast::Receiver<T>) -> Self {
        Self {
            filesystem,
            write_rx,
            buffer: Arc::new(RwLock::new(Vec::new())),
            parquet_opts: TableParquetOptions::default(),
        }
    }

    pub fn with_parquet_options(mut self, opts: TableParquetOptions) -> Self {
        self.parquet_opts = opts;
        self
    }
}

impl<F: FileSystem, T: Clone + Send + Sync> Table<F, T> for GenericTable<F, T> {
    fn name(&self) -> String {
        "generic".to_string()
    }

    fn make_batch(_data: T) -> RecordBatch {
        RecordBatch::new_empty(Arc::new(Schema::empty()))
    }

    fn filesystem(&self) -> Arc<F> {
        self.filesystem.clone()
    }

    fn write_rx(&self) -> broadcast::Receiver<T> {
        self.write_rx.resubscribe()
    }

    fn buffer(&self) -> Arc<RwLock<Vec<RecordBatch>>> {
        self.buffer.clone()
    }

    fn data_frame_write_options(&self) -> DataFrameWriteOptions {
        DataFrameWriteOptions::default()
    }

    fn writer_options(&self) -> TableParquetOptions {
        self.parquet_opts.clone()
    }
}
