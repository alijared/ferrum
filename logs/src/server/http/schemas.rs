use chrono::{DateTime, Utc};
use datafusion::arrow::array::{Array, AsArray};
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Deserialize)]
pub struct FqlQueryParams {
    pub query: Option<String>,
    #[serde(flatten)]
    pub time_range: TimeRangeQueryParams,
    pub limit: Option<usize>,
}

#[derive(Deserialize)]
pub struct TimeRangeQueryParams {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
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

#[derive(Serialize)]
pub struct AttributeKeys(Vec<String>);

impl AttributeKeys {
    pub async fn try_from_streams(
        streams: Vec<SendableRecordBatchStream>,
    ) -> Result<Self, DataFusionError> {
        let mut attributes = Vec::new();
        let mut attribute_check = HashSet::new();
        for mut stream in streams {
            while let Some(batch_result) = stream.next().await {
                let batch = batch_result?;
                let list = batch
                    .column(batch.schema().index_of("attributes").unwrap())
                    .as_list::<i32>();

                let values = list.values().as_string::<i32>();
                for i in 0..values.len() {
                    let attribute = values.value(i).to_string();
                    if !attribute_check.contains(&attribute) {
                        attributes.push(attribute.clone());
                        attribute_check.insert(attribute);
                    }
                }
            }
        }

        Ok(Self(attributes))
    }
}
