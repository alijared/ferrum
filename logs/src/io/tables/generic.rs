use crate::io::tables::Table;
use datafusion::arrow::array::RecordBatch;
use object_store::Filesystem;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, RwLock};

pub struct GenericTable<T: Clone + Send + Sync> {
    write_rx: broadcast::Receiver<T>,
    buffer: Arc<RwLock<Vec<RecordBatch>>>,
    filesystem: Arc<Filesystem>,
    compaction_frequency: Duration,
}

impl<T: Clone + Send + Sync> GenericTable<T> {
    pub fn new(
        write_rx: broadcast::Receiver<T>,
        filesystem: Arc<Filesystem>,
        compaction_frequency: Duration,
    ) -> Self {
        Self {
            write_rx,
            buffer: Arc::new(RwLock::new(Vec::new())),
            filesystem,
            compaction_frequency,
        }
    }
}

impl<T: Clone + Send + Sync> Table<T> for GenericTable<T> {
    fn filesystem(&self) -> Arc<Filesystem> {
        self.filesystem.clone()
    }

    fn compaction_frequency(&self) -> Duration {
        self.compaction_frequency
    }

    fn write_rx(&self) -> broadcast::Receiver<T> {
        self.write_rx.resubscribe()
    }

    fn buffer(&self) -> Arc<RwLock<Vec<RecordBatch>>> {
        self.buffer.clone()
    }
}
