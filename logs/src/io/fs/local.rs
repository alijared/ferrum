use crate::config::LocalFilesystemConfig;
use crate::io::fs;
use crate::io::fs::FileSystem;
use object_store::ObjectStore;
use std::sync::Arc;
use std::time::Duration;
use tonic::async_trait;
use url::Url;

pub struct LocalFileSystem {
    url: Url,
    store: Arc<object_store::local::LocalFileSystem>,
    compact_frequency: Duration,
}

impl LocalFileSystem {
    pub async fn new(
        config: LocalFilesystemConfig,
        compact_frequency: Duration,
    ) -> Result<Self, fs::Error> {
        let data_dir = &config.data_dir;
        let url =
            Url::from_directory_path(data_dir).map_err(|_| object_store::Error::InvalidPath {
                source: object_store::path::Error::InvalidPath {
                    path: data_dir.clone(),
                },
            })?;

        tokio::fs::create_dir_all(data_dir)
            .await
            .map_err(|e| fs::IoError::new(data_dir.clone(), e))?;

        let store = object_store::local::LocalFileSystem::new_with_prefix(data_dir)?;
        Ok(Self {
            url,
            store: Arc::new(store),
            compact_frequency,
        })
    }
}

#[async_trait]
impl FileSystem for LocalFileSystem {
    fn compact_frequency(&self) -> Duration {
        self.compact_frequency
    }

    async fn compact(&self) {
        println!("Running file compaction");
    }

    fn url(&self) -> Url {
        self.url.clone()
    }

    fn object_store(&self) -> Arc<impl ObjectStore> {
        self.store.clone()
    }
}
