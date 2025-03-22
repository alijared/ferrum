use crate::io::tables::generic::GenericTable;
use crate::io::tables::{BatchWrite, Table, TableOptions};
use crate::io::{tables, writer};
use crate::server::grpc::opentelemetry::LogRecord;
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
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::async_trait;

pub const NAME: &str = "log_attributes";
static SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    Schema::new(vec![
        Field::new("log_id", DataType::UInt64, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
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
    table: GenericTable<Self, Vec<(u64, LogRecord)>>,
}

impl LogTable {
    fn new(table: GenericTable<Self, Vec<(u64, LogRecord)>>) -> Self {
        Self { table }
    }
}

#[async_trait]
impl Table<Self, Vec<(u64, LogRecord)>> for LogTable {
    async fn start(&mut self, cancellation_token: CancellationToken) {
        self.table.start(cancellation_token).await;
    }
}

impl BatchWrite<Vec<(u64, LogRecord)>> for LogTable {
    fn make_batch(logs: Vec<(u64, LogRecord)>) -> RecordBatch {
        let mut ids = Vec::new();
        let mut keys = Vec::new();
        let mut values = Vec::new();
        let mut timestamps = Vec::new();
        let mut days = Vec::new();

        for (id, log) in logs {
            for (key, value) in log.attributes {
                ids.push(id);
                keys.push(key);
                values.push(value);
                timestamps.push(log.timestamp);
                days.push(log.day);
            }
        }

        writer::record_batch(
            SCHEMA.clone(),
            PARTITION_COLUMNS.clone(),
            vec![
                Arc::new(UInt64Array::from(ids)),
                Arc::new(StringArray::from(keys)),
                Arc::new(StringArray::from(values)),
                Arc::new(TimestampNanosecondArray::from(timestamps).with_timezone("UTC")),
                Arc::new(Date32Array::from(days)),
            ],
        )
    }
}

pub async fn initialize(
    ctx: &SessionContext,
    data_path: PathBuf,
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
        "key".to_string(),
        ParquetColumnOptions {
            bloom_filter_enabled: Some(true),
            ..Default::default()
        },
    );
    column_opts.insert(
        "value".to_string(),
        ParquetColumnOptions {
            bloom_filter_enabled: Some(true),
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

    let schema = writer::schema_with_fields(SCHEMA.clone(), PARTITION_COLUMNS.clone());
    let mut table = LogTable::new(GenericTable::new(opts, schema, bus));
    Ok(tokio::spawn(async move {
        table.start(cancellation_token).await;
    }))
}
