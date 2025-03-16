use crate::io::query;
use chrono::{DateTime, TimeZone, Utc};
use datafusion::arrow::array::{
    Array, AsArray, GenericStringArray, ListArray, MapArray, TimestampNanosecondArray,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::SendableRecordBatchStream;
use futures_util::StreamExt;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use tonic::async_trait;

#[derive(Serialize)]
pub struct AttributeKeys(Vec<String>);

impl From<AttributeKeys> for Vec<String> {
    fn from(values: AttributeKeys) -> Self {
        values.0
    }
}

#[async_trait]
impl query::FromStreams for AttributeKeys {
    async fn try_from_streams(
        streams: Vec<SendableRecordBatchStream>,
        _should_serialize_message: bool,
    ) -> Result<Self, query::Error> {
        let mut attributes = Vec::new();
        let mut attribute_check = HashSet::new();
        for mut stream in streams {
            while let Some(batch_result) = stream.next().await {
                let batch = batch_result?;
                let list = batch
                    .column(batch.schema().index_of("key").unwrap())
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

#[derive(Serialize)]
pub struct AttributeValues(Vec<String>);

impl From<AttributeValues> for Vec<String> {
    fn from(values: AttributeValues) -> Self {
        values.0
    }
}

#[async_trait]
impl query::FromStreams for AttributeValues {
    async fn try_from_streams(
        streams: Vec<SendableRecordBatchStream>,
        _should_serialize_message: bool,
    ) -> Result<Self, query::Error> {
        let mut values = Vec::new();
        for mut stream in streams {
            while let Some(batch_result) = stream.next().await {
                let batch = batch_result?;
                let array = batch.column(0).as_string::<i32>();
                for i in 0..array.len() {
                    values.push(array.value(i).to_string());
                }
            }
        }
        Ok(Self(values))
    }
}

pub struct SqlResponse(Vec<u8>);

impl From<SqlResponse> for Vec<u8> {
    fn from(response: SqlResponse) -> Self {
        response.0
    }
}

#[async_trait]
impl query::FromStreams for SqlResponse {
    async fn try_from_streams(
        streams: Vec<SendableRecordBatchStream>,
        _should_serialize_message: bool,
    ) -> Result<Self, query::Error> {
        let buf = Vec::new();
        let mut writer = arrow_json::ArrayWriter::new(buf);
        for mut stream in streams {
            while let Some(batch_result) = stream.next().await {
                let batch = batch_result?;
                writer.write(&batch)?;
            }
        }

        writer.finish()?;

        Ok(Self(writer.into_inner()))
    }
}

pub struct LogWithAttributesTuple<'a>(
    (
        &'a TimestampNanosecondArray,
        &'a GenericStringArray<i32>,
        &'a GenericStringArray<i32>,
        &'a MapArray,
    ),
);

impl LogWithAttributesTuple<'_> {
    pub fn timestamp(&self, index: usize) -> DateTime<Utc> {
        Utc.timestamp_nanos(self.0 .0.value(index))
    }

    pub fn level(&self, index: usize) -> &str {
        self.0 .1.value(index)
    }

    pub fn message(&self, index: usize) -> &str {
        self.0 .2.value(index)
    }

    pub fn attributes(&self, index: usize) -> HashMap<String, String> {
        let array = self.0 .3.value(index);
        let key_list = array
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let value_list = array
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        let mut attributes = HashMap::new();
        for j in 0..key_list.len() {
            let keys_ref = key_list.value(j);
            let keys = keys_ref.as_string::<i32>();
            let values_ref = value_list.value(j);
            let values = values_ref.as_string::<i32>();

            for k in 0..keys.len() {
                attributes.insert(keys.value(k).to_string(), values.value(k).to_string());
            }
        }
        attributes
    }
}

impl<'a> From<&'a RecordBatch> for LogWithAttributesTuple<'a> {
    fn from(batch: &'a RecordBatch) -> Self {
        let arrays = (
            batch
                .column(batch.schema().index_of("timestamp").unwrap())
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap(),
            batch
                .column(batch.schema().index_of("level").unwrap())
                .as_string::<i32>(),
            batch
                .column(batch.schema().index_of("message").unwrap())
                .as_string::<i32>(),
            batch
                .column(batch.schema().index_of("attributes").unwrap())
                .as_map(),
        );
        Self(arrays)
    }
}
