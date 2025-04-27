use crate::server::http::schemas::LogWithAttributesTuple;
use chrono::{DateTime, TimeZone, Utc};
use datafusion::arrow::array::{AsArray, RecordBatch};
use query_engine::FromRecordBatch;
use serde::de::Visitor;
use serde::{de, Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use std::fmt;

#[derive(Deserialize)]
pub struct QuerySqlParams {
    pub query: String,
}

#[derive(Deserialize)]
pub struct QueryLogsParams {
    pub query: Option<String>,
    #[serde(flatten)]
    pub time_range: TimeRangeQueryParams,
    pub sort: Option<SortOrder>,
    pub limit: Option<usize>,
}

#[derive(Deserialize)]
pub struct QueryAttributeValuesParams {
    #[serde(flatten)]
    pub time_range: TimeRangeQueryParams,
    pub limit: Option<usize>,
}

#[derive(Deserialize)]
pub struct TimeRangeQueryParams {
    #[serde(alias = "start", deserialize_with = "deserialize_datetime")]
    pub from: DateTime<Utc>,
    #[serde(alias = "end", deserialize_with = "deserialize_datetime")]
    pub to: DateTime<Utc>,
}

#[derive(Clone, Deserialize)]
pub enum SortOrder {
    #[serde(rename = "asc", alias = "forward")]
    Ascending,
    #[serde(rename = "desc", alias = "backward")]
    Descending,
}

impl From<SortOrder> for bool {
    fn from(order: SortOrder) -> Self {
        match order {
            SortOrder::Ascending => true,
            SortOrder::Descending => false,
        }
    }
}

fn deserialize_datetime<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    struct DateTimeVisitor;

    impl Visitor<'_> for DateTimeVisitor {
        type Value = DateTime<Utc>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("an RFC3339 string or Unix timestamp in nanoseconds")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            if let Ok(ts) = value.parse::<i64>() {
                return Ok(Utc.timestamp_nanos(ts));
            }
            value.parse::<DateTime<Utc>>().map_err(de::Error::custom)
        }
    }

    deserializer.deserialize_any(DateTimeVisitor)
}

#[derive(Serialize)]
#[serde(untagged)]
pub enum LogsResponse {
    Count { count: usize },
    Logs(Vec<Log>),
}

#[derive(Serialize)]
pub struct Log {
    pub timestamp: DateTime<Utc>,
    pub level: String,
    pub message: String,
    pub attributes: HashMap<String, String>,
}

impl FromRecordBatch for Log {
    fn from_batch(batch: &RecordBatch) -> Vec<Self> {
        let tuple = LogWithAttributesTuple::from(batch);
        let mut logs = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            logs.push(Log {
                timestamp: tuple.timestamp(i),
                level: tuple.level(i).to_string(),
                message: tuple.message(i).to_string(),
                attributes: tuple.attributes(i),
            });
        }
        logs
    }
}

#[derive(Serialize)]
pub struct StringValue(String);

impl FromRecordBatch for StringValue {
    fn from_batch(batch: &RecordBatch) -> Vec<Self> {
        let mut values = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            let value = batch.column(0).as_string_view().value(i);
            values.push(StringValue(value.to_string()));
        }
        values
    }
}
