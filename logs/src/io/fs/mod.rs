use crate::config;
use crate::io::fs::local::LocalFileSystem;
use datafusion::error::DataFusionError;
use log::info;
use object_store::ObjectStore;
use std::fmt::{Display, Formatter};
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tonic::async_trait;
use url::Url;

mod local;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Io(IoError),

    #[error("{0}")]
    ObjectStore(object_store::Error),
}

impl From<object_store::Error> for Error {
    fn from(error: object_store::Error) -> Self {
        Self::ObjectStore(error)
    }
}

impl From<IoError> for Error {
    fn from(error: IoError) -> Self {
        Self::Io(error)
    }
}

impl From<Error> for DataFusionError {
    fn from(error: Error) -> Self {
        match error {
            Error::Io(e) => {
                let path = e.path;
                let inner = e.inner;
                match inner.kind() {
                    io::ErrorKind::NotFound => object_store::Error::NotFound {
                        path: path.to_string_lossy().to_string(),
                        source: inner.into(),
                    },
                    io::ErrorKind::InvalidInput => object_store::Error::InvalidPath {
                        source: object_store::path::Error::InvalidPath { path },
                    },
                    io::ErrorKind::AlreadyExists => object_store::Error::AlreadyExists {
                        path: path.to_string_lossy().to_string(),
                        source: inner.into(),
                    },
                    io::ErrorKind::PermissionDenied => object_store::Error::PermissionDenied {
                        path: path.to_string_lossy().to_string(),
                        source: inner.into(),
                    },
                    _ => object_store::Error::Generic {
                        store: "",
                        source: inner.into(),
                    },
                }
                .into()
            }
            Error::ObjectStore(e) => e.into(),
        }
    }
}

#[derive(Debug)]
pub struct IoError {
    path: PathBuf,
    inner: io::Error,
}

impl IoError {
    fn new(path: PathBuf, inner: io::Error) -> Self {
        Self { path, inner }
    }
}

impl Display for IoError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

#[async_trait]
pub trait FileSystem: Sized + Send + Sync {
    async fn start_compactor(&self, table_name: &str, cancellation_token: CancellationToken) {
        info!("Starting compactor for {} table", table_name);
        let mut ticker = tokio::time::interval(self.compact_frequency());
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    self.compact().await;
                }
                _ = cancellation_token.cancelled() => {
                    info!("Shutting down compactor for {} table", table_name);
                    self.compact().await;
                    break;
                }
            }
        }
        info!("Compactor stopped for {} table", table_name);
    }

    fn compact_frequency(&self) -> Duration;

    async fn compact(&self);

    fn url(&self) -> Url;

    fn object_store(&self) -> Arc<impl ObjectStore>;
}

pub async fn new(config: &config::FilesystemConfig) -> Result<impl FileSystem, Error> {
    let compact_frequency = Duration::from_secs(config.compaction_frequency_seconds);
    match config.filesystem.clone() {
        config::Filesystem::Local(c) => LocalFileSystem::new(c, compact_frequency).await,
    }
}
