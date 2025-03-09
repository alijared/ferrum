use crate::io::tables;
use crate::io::tables::generic::GenericTable;
use crate::io::tables::{convert_any_value, schema_with_fields, MakeBatch, Table, TableOptions};
use crate::server::opentelemetry::common::v1::any_value::Value;
use crate::server::opentelemetry::logs::v1::LogRecord;
use datafusion::arrow::array::{
    Date32Array, MapBuilder, RecordBatch, StringArray, StringBuilder, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, TimeUnit};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{col, SortExpr};
use datafusion::prelude::SessionContext;
use log::info;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::async_trait;

pub const NAME: &str = "logs";
static SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("level", DataType::Utf8, false),
        Field::new("message", DataType::Utf8, false),
        Field::new(
            "attributes",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("keys", DataType::Utf8, false),
                        Field::new("values", DataType::Utf8, true),
                    ])),
                    false,
                )),
                false,
            ),
            false,
        ),
    ])
});
static PARTITION_COLUMNS: LazyLock<Vec<FieldRef>> =
    LazyLock::new(|| vec![Arc::new(Field::new("day", DataType::Date32, false))]);

struct LogAttributeTable {
    table: GenericTable<Self, Vec<LogRecord>>,
}

impl LogAttributeTable {
    fn new(table: GenericTable<Self, Vec<LogRecord>>) -> Self {
        Self { table }
    }
}

#[async_trait]
impl Table<Self, Vec<LogRecord>> for LogAttributeTable {
    async fn start(&mut self, cancellation_token: CancellationToken) {
        self.table.start(cancellation_token).await;
    }
}

impl MakeBatch<Vec<LogRecord>> for LogAttributeTable {
    fn make_batch(logs: Vec<LogRecord>) -> RecordBatch {
        let cap = logs.len();
        let mut timestamps = Vec::with_capacity(cap);
        let mut levels = Vec::with_capacity(cap);
        let mut messages = Vec::with_capacity(cap);
        let mut days = Vec::with_capacity(cap);
        let mut attributes = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());

        logs.iter().for_each(|l| {
            let message = l.body.clone().map(convert_any_value).unwrap_or_default();
            let level = l.severity_text.clone();
            let ts = l.time_unix_nano as i64;
            let day = (ts / 86_400_000_000_000) as i32;

            timestamps.push(ts);
            days.push(day);
            levels.push(level);
            messages.push(message);

            l.attributes.iter().for_each(|kv| {
                attributes.keys().append_value(&kv.key);
                let value_str = match &kv.value {
                    Some(v) => match &v.value {
                        Some(value) => match value {
                            Value::StringValue(s) => s.clone(),
                            Value::BoolValue(b) => b.to_string(),
                            Value::IntValue(i) => i.to_string(),
                            Value::DoubleValue(d) => d.to_string(),
                            Value::ArrayValue(av) => format!("{:?}", av),
                            Value::KvlistValue(kv) => format!("{:?}", kv),
                            Value::BytesValue(b) => String::from_utf8_lossy(b).to_string(),
                        },
                        None => String::new(),
                    },
                    None => String::new(),
                };
                attributes.values().append_value(&value_str);
            });
            attributes.append(true).unwrap();
        });

        RecordBatch::try_new(
            Arc::new(schema_with_fields(
                SCHEMA.clone(),
                PARTITION_COLUMNS.clone(),
            )),
            vec![
                Arc::new(TimestampNanosecondArray::from(timestamps).with_timezone("UTC")),
                Arc::new(StringArray::from(levels)),
                Arc::new(StringArray::from(messages)),
                Arc::new(attributes.finish()),
                Arc::new(Date32Array::from(days)),
            ],
        )
        .unwrap()
    }
}

pub async fn initialize(
    ctx: &SessionContext,
    data_path: &str,
    compaction_frequency: Duration,
    bus: broadcast::Receiver<Vec<LogRecord>>,
    cancellation_token: CancellationToken,
) -> Result<JoinHandle<()>, DataFusionError> {
    let partition_by = vec![("day".to_string(), DataType::Date32)];
    let sort_by = vec![SortExpr {
        expr: col("timestamp"),
        asc: false,
        nulls_first: false,
    }];
    let mut opts = TableOptions::new(
        data_path,
        compaction_frequency,
        partition_by,
        sort_by,
        false,
    );
    let data_path = tables::register(ctx, NAME, &opts, Arc::new(SCHEMA.clone())).await?;
    opts.data_path = data_path;

    info!("Starting up {} table", NAME);

    let schema = schema_with_fields(SCHEMA.clone(), PARTITION_COLUMNS.clone());
    let mut table = LogAttributeTable::new(GenericTable::new(NAME, opts, schema, bus));

    Ok(tokio::spawn(async move {
        table.start(cancellation_token).await;
    }))
}
