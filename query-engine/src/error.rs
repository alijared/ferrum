use datafusion::error::DataFusionError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    DataFusion(DataFusionError),

    #[error("failed to parse query: {0}")]
    ParseQuery(String),
}

impl From<DataFusionError> for Error {
    fn from(error: DataFusionError) -> Self {
        Self::DataFusion(error)
    }
}

impl From<ferrum_ql::Error<'_>> for Error {
    fn from(error: ferrum_ql::Error) -> Self {
        Self::ParseQuery(error.to_string())
    }
}
