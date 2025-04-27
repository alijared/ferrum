use chrono::{DateTime, TimeZone, Utc};
use datafusion::arrow::array::{
    Array, AsArray, ListArray, MapArray, StringViewArray, TimestampNanosecondArray,
};
use datafusion::arrow::record_batch::RecordBatch;
use std::collections::HashMap;

pub struct LogWithAttributesTuple<'a>(
    (
        &'a TimestampNanosecondArray,
        &'a StringViewArray,
        &'a StringViewArray,
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
            let keys = keys_ref.as_string_view();
            let values_ref = value_list.value(j);
            let values = values_ref.as_string_view();

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
                .column_by_name("timestamp")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap(),
            batch.column_by_name("level").unwrap().as_string_view(),
            batch.column_by_name("message").unwrap().as_string_view(),
            batch.column_by_name("attributes").unwrap().as_map(),
        );
        Self(arrays)
    }
}
