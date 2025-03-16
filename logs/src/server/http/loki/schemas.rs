use crate::io::query;
use crate::server::http::schemas::{AttributeKeys, AttributeValues};
use datafusion::execution::SendableRecordBatchStream;
use serde::Serialize;
use tonic::async_trait;

#[derive(Serialize)]
pub struct LabelResponse {
    status: String,
    data: Vec<String>,
}

impl LabelResponse {
    fn new(data: Vec<String>) -> Self {
        Self {
            status: "success".to_string(),
            data,
        }
    }
}

#[derive(Serialize)]
pub struct AttributeKeysResponse(LabelResponse);

#[async_trait]
impl query::FromStreams for AttributeKeysResponse {
    async fn try_from_streams(
        streams: Vec<SendableRecordBatchStream>,
        should_serialize_message: bool,
    ) -> Result<Self, query::Error> {
        let keys = AttributeKeys::try_from_streams(streams, should_serialize_message).await?;
        Ok(Self(LabelResponse::new(keys.into())))
    }
}

#[derive(Serialize)]
pub struct AttributeValuesResponse(LabelResponse);

#[async_trait]
impl query::FromStreams for AttributeValuesResponse {
    async fn try_from_streams(
        streams: Vec<SendableRecordBatchStream>,
        should_serialize_message: bool,
    ) -> Result<Self, query::Error> {
        let values = AttributeValues::try_from_streams(streams, should_serialize_message).await?;
        Ok(Self(LabelResponse::new(values.into())))
    }
}
