use chrono::{DateTime, Utc};
use datafusion::arrow::array::AsArray;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Deserialize)]
pub struct FqlQueryParams {
    pub query: Option<String>,
    #[serde(flatten)]
    pub time_range: TimeRangeQueryParams,
    pub limit: Option<usize>,
}

#[derive(Deserialize)]
pub struct TimeRangeQueryParams {
    pub to: DateTime<Utc>,
    pub from: DateTime<Utc>,
}

#[derive(Serialize)]
#[serde(untagged)]
pub enum FqlResponse {
    Count { count: usize },
    Data(Vec<Log>),
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

#[derive(Deserialize)]
pub struct SqlQueryParams {
    pub query: String,
}

#[derive(Deserialize)]
pub struct LabelQueryParams {
    #[serde(flatten)]
    pub time_range: TimeRangeQueryParams,
}

#[derive(Serialize)]
pub struct Labels {
    #[serde(flatten)]
    values: Vec<String>,
}

impl Labels {
    pub async fn from_stream(streams: Vec<SendableRecordBatchStream>) -> Result<Self, DataFusionError> {
        let mut labels = Vec::new();
        for mut stream in streams {
            while let Some(batch_result) = stream.next().await {
                let batch = batch_result?;
                let array = batch.column_by_name("key").unwrap().as_string::<i32>();
                for i in 0..batch.num_rows() {
                    labels.push(array.value(i).to_string());
                }
            }
        }

        Ok(Self { values: labels })
    }
}
