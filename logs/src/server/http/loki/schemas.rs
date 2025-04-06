use crate::io::query;
use crate::server::http::schemas::{AttributeKeys, AttributeValues, LogWithAttributesTuple};
use datafusion::execution::SendableRecordBatchStream;
use serde::Serialize;
use std::collections::HashMap;
use futures::StreamExt;
use tonic::async_trait;

#[derive(Serialize)]
pub struct LogsResponse {
    status: String,
    data: LogsData,
}

impl query::Log for LogsResponse {}

impl TryFrom<usize> for LogsResponse {
    type Error = query::Error;

    fn try_from(_count: usize) -> Result<Self, Self::Error> {
        Err(query::Error::UnsupportedFunction(
            "Loki API does not support the count function".to_string(),
        ))
    }
}

#[async_trait]
impl query::FromStreams for LogsResponse {
    async fn try_from_streams(
        streams: Vec<SendableRecordBatchStream>,
        _should_serialize_message: bool,
    ) -> Result<Self, query::Error> {
        let mut logs = Vec::new();
        for mut stream in streams {
            while let Some(batch_result) = stream.next().await {
                let batch = batch_result?;
                let tuple = LogWithAttributesTuple::from(&batch);
                for i in 0..batch.num_rows() {
                    logs.push(Log {
                        stream: tuple.attributes(i),
                        values: vec![vec![
                            tuple
                                .timestamp(i)
                                .timestamp_nanos_opt()
                                .unwrap()
                                .to_string(),
                            tuple.message(i).to_string(),
                        ]],
                    })
                }
            }
        }

        Ok(LogsResponse {
            status: "success".to_string(),
            data: LogsData {
                result_type: "streams".to_string(),
                result: logs,
            },
        })
    }
}

#[derive(Serialize)]
pub struct LogsData {
    #[serde(rename = "resultType")]
    result_type: String,

    result: Vec<Log>,
}

#[derive(Serialize)]
pub struct Log {
    stream: HashMap<String, String>,
    values: Vec<Vec<String>>,
}

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
