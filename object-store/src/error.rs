use std::io;
use datafusion::common::DataFusionError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Io(io::Error),

    #[error("{0}")]
    ObjectStore(object_store::Error),

    #[error("{0}")]
    DataFusion(DataFusionError),
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<object_store::Error> for Error {
    fn from(error: object_store::Error) -> Self {
        Self::ObjectStore(error)
    }
}

impl From<DataFusionError> for Error {
    fn from(error: DataFusionError) -> Self {
        Self::DataFusion(error)
    }
}
