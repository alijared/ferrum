use crate::io::query;
use crate::server::http::schemas::LogWithAttributesTuple;
use chrono::{DateTime, Utc};
use datafusion::execution::SendableRecordBatchStream;
use futures_util::StreamExt;
use serde::Serialize;
use serde_json::json;
use std::collections::HashMap;
use tonic::async_trait;

#[derive(Serialize)]
#[serde(untagged)]
pub enum LogsResponse {
    Count { count: usize },
    Logs(Vec<Log>),
}

impl query::Log for LogsResponse {}

impl From<usize> for LogsResponse {
    fn from(count: usize) -> Self {
        Self::Count { count }
    }
}

#[async_trait]
impl query::FromStreams for LogsResponse {
    async fn try_from_streams(
        streams: Vec<SendableRecordBatchStream>,
        should_serialize_message: bool,
    ) -> Result<Self, query::Error> {
        let mut logs = Vec::new();
        for mut stream in streams {
            while let Some(batch_result) = stream.next().await {
                let batch = batch_result?;
                let tuple = LogWithAttributesTuple::from(&batch);
                for i in 0..batch.num_rows() {
                    let message = tuple.message(i);
                    let json_message = if should_serialize_message {
                        serde_json::from_str(message).unwrap_or(Some(json!({"message": message})))
                    } else {
                        None
                    };

                    let message = if json_message.is_some() {
                        None
                    } else {
                        Some(message.to_string())
                    };

                    logs.push(Log {
                        timestamp: tuple.timestamp(i),
                        level: tuple.level(i).to_string(),
                        message,
                        json_message,
                        attributes: tuple.attributes(i),
                    });
                }
            }
        }
        Ok(Self::Logs(logs))
    }
}

#[derive(Serialize)]
pub struct Log {
    pub timestamp: DateTime<Utc>,
    pub level: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(rename = "message", skip_serializing_if = "Option::is_none")]
    pub json_message: Option<serde_json::Value>,
    pub attributes: HashMap<String, String>,
}
