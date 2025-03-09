use crate::io::tables;
use crate::io::tables::generic::GenericTable;
use crate::io::tables::{schema_with_fields, MakeBatch, Table, TableOptions};
use crate::server::opentelemetry::logs::v1::LogRecord;
use datafusion::arrow::array::{Date32Array, RecordBatch, StringArray, TimestampNanosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema, TimeUnit};
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

pub const NAME: &str = "log_attributes";
static SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
    ])
});
static PARTITION_COLUMNS: LazyLock<Vec<FieldRef>> =
    LazyLock::new(|| vec![Arc::new(Field::new("day", DataType::Date32, false))]);

struct LogTable {
    table: GenericTable<Self, Vec<LogRecord>>,
}

impl LogTable {
    fn new(table: GenericTable<Self, Vec<LogRecord>>) -> Self {
        Self { table }
    }
}

#[async_trait]
impl Table<Self, Vec<LogRecord>> for LogTable {
    async fn start(&mut self, cancellation_token: CancellationToken) {
        self.table.start(cancellation_token).await;
    }
}

impl MakeBatch<Vec<LogRecord>> for LogTable {
    fn make_batch(logs: Vec<LogRecord>) -> RecordBatch {
        let cap = logs.len();
        let mut keys = Vec::with_capacity(cap);
        let mut timestamps = Vec::with_capacity(cap);
        let mut days = Vec::with_capacity(cap);

        logs.iter().for_each(|l| {
            let ts = l.time_unix_nano as i64;
            let day = (ts / 86_400_000_000_000) as i32;

            l.attributes.iter().for_each(|kv| {
                keys.push(kv.key.clone());
                timestamps.push(ts);
                days.push(day);
            });
        });

        RecordBatch::try_new(
            Arc::new(schema_with_fields(
                SCHEMA.clone(),
                PARTITION_COLUMNS.clone(),
            )),
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(TimestampNanosecondArray::from(timestamps).with_timezone("UTC")),
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
    let mut opts = TableOptions::new(data_path, compaction_frequency, partition_by, sort_by, true);
    let data_path = tables::register(ctx, NAME, &opts, Arc::new(SCHEMA.clone())).await?;
    opts.data_path = data_path;

    info!("Starting up {} table", NAME);

    let schema = schema_with_fields(SCHEMA.clone(), PARTITION_COLUMNS.clone());
    let mut table = LogTable::new(GenericTable::new(NAME, opts, schema, bus));

    Ok(tokio::spawn(async move {
        table.start(cancellation_token).await;
    }))
}
