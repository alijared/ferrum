use crate::io::tables;
use crate::io::tables::generic::GenericTable;
use crate::io::tables::{convert_any_value, schema_with_fields, BatchWrite, Table, TableOptions};
use crate::server::opentelemetry::logs::v1::LogRecord;
use chrono::Utc;
use datafusion::arrow::array::{
    Date32Array, RecordBatch, StringArray, TimestampNanosecondArray, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema, TimeUnit};
use datafusion::config::{ParquetColumnOptions, ParquetOptions, TableParquetOptions};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{col, SortExpr};
use datafusion::parquet::basic::Compression;
use datafusion::prelude::SessionContext;
use log::info;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{atomic, Arc, LazyLock};
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::async_trait;

pub const NAME: &str = "logs";
static SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("level", DataType::Utf8, false),
        Field::new("message", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
    ])
});
static PARTITION_COLUMNS: LazyLock<Vec<FieldRef>> =
    LazyLock::new(|| vec![Arc::new(Field::new("day", DataType::Date32, false))]);

struct LogAttributeTable {
    table: GenericTable<Self, Vec<(u64, LogRecord)>>,
}

impl LogAttributeTable {
    fn new(table: GenericTable<Self, Vec<(u64, LogRecord)>>) -> Self {
        Self { table }
    }
}

#[async_trait]
impl Table<Self, Vec<(u64, LogRecord)>> for LogAttributeTable {
    async fn start(&mut self, cancellation_token: CancellationToken) {
        self.table.start(cancellation_token).await;
    }
}

impl BatchWrite<Vec<(u64, LogRecord)>> for LogAttributeTable {
    fn make_batch(logs: Vec<(u64, LogRecord)>) -> RecordBatch {
        let cap = logs.len();
        let mut ids = Vec::with_capacity(cap);
        let mut levels = Vec::with_capacity(cap);
        let mut messages = Vec::with_capacity(cap);
        let mut timestamps = Vec::with_capacity(cap);
        let mut days = Vec::with_capacity(cap);

        logs.iter().for_each(|(id, l)| {
            let level = l.severity_text.clone();
            let message = l.body.clone().map(convert_any_value).unwrap_or_default();
            let ts = l.time_unix_nano as i64;
            let day = (ts / 86_400_000_000_000) as i32;

            ids.push(*id);
            timestamps.push(ts);
            days.push(day);
            levels.push(level);
            messages.push(message);
        });

        RecordBatch::try_new(
            Arc::new(schema_with_fields(
                SCHEMA.clone(),
                PARTITION_COLUMNS.clone(),
            )),
            vec![
                Arc::new(UInt64Array::from(ids)),
                Arc::new(StringArray::from(levels)),
                Arc::new(StringArray::from(messages)),
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
    bus: broadcast::Receiver<Vec<(u64, LogRecord)>>,
    cancellation_token: CancellationToken,
) -> Result<JoinHandle<()>, DataFusionError> {
    let partition_by = vec![("day".to_string(), DataType::Date32)];
    let sort_by = vec![SortExpr {
        expr: col("timestamp"),
        asc: false,
        nulls_first: false,
    }];

    let mut column_opts = HashMap::new();
    column_opts.insert(
        "level".to_string(),
        ParquetColumnOptions {
            bloom_filter_enabled: Some(true),
            bloom_filter_ndv: Some(6),
            dictionary_enabled: Some(true),
            ..Default::default()
        },
    );

    let mut opts = TableOptions::new(
        data_path,
        compaction_frequency,
        partition_by,
        sort_by,
        false,
        TableParquetOptions {
            global: ParquetOptions {
                pushdown_filters: true,
                reorder_filters: true,
                compression: Some(Compression::SNAPPY.to_string()),
                maximum_parallel_row_group_writers: num_cpus::get(),
                bloom_filter_on_write: true,
                ..Default::default()
            },
            column_specific_options: column_opts,
            ..Default::default()
        },
    );
    let data_path = tables::register(ctx, NAME, &opts, Arc::new(SCHEMA.clone())).await?;
    opts.data_path = data_path;

    info!("Starting up {} table", NAME);

    let schema = schema_with_fields(SCHEMA.clone(), PARTITION_COLUMNS.clone());
    let mut table = LogAttributeTable::new(GenericTable::new(opts, schema, bus));

    Ok(tokio::spawn(async move {
        table.start(cancellation_token).await;
    }))
}

//TODO: make this part of config when making this a distributed system
const NODE_ID: u64 = 1;
const SEQUENCE_BITS: u64 = 12;
const NODE_ID_SHIFT: u64 = SEQUENCE_BITS;
const TIMESTAMP_SHIFT: u64 = SEQUENCE_BITS + 10;

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub fn generate_log_id() -> u64 {
    let now = Utc::now().timestamp_millis() as u64;
    let sequence = SEQUENCE.fetch_add(1, atomic::Ordering::SeqCst) & ((1 << SEQUENCE_BITS) - 1);
    (now << TIMESTAMP_SHIFT) | (NODE_ID << NODE_ID_SHIFT) | sequence
}
