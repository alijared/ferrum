use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use tonic::async_trait;

pub mod fql;
pub mod sql;

pub use fql::{attribute_keys, attribute_values, logs};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to parse query: {0}")]
    Query(String),

    #[error("Unsupported function: {0}")]
    UnsupportedFunction(String),

    #[error("{0}")]
    DataFusion(DataFusionError),

    #[error("{0}")]
    Arrow(ArrowError),
}

impl From<DataFusionError> for Error {
    fn from(e: DataFusionError) -> Self {
        Self::DataFusion(e)
    }
}

impl From<ArrowError> for Error {
    fn from(e: ArrowError) -> Self {
        Self::Arrow(e)
    }
}

pub trait Log: TryFrom<usize> + FromStreams {}

#[async_trait]
pub trait FromStreams: Sized {
    async fn try_from_streams(
        streams: Vec<SendableRecordBatchStream>,
        should_serialize_message: bool,
    ) -> Result<Self, Error>;
}
