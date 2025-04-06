use crate::io::tables::generic::GenericTable;
use crate::io::tables::Table;
use crate::io::{tables, writer};
use crate::server::grpc::opentelemetry::LogRecord;
use datafusion::arrow::array::{Date32Array, RecordBatch, StringViewArray, TimestampNanosecondArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema, TimeUnit};
use datafusion::config::{ParquetColumnOptions, ParquetOptions, TableParquetOptions};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{col, SortExpr};
use datafusion::parquet::basic::Compression;
use object_store::Filesystem;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::sync::{broadcast, RwLock};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub const NAME: &str = "logs";
static SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("level", DataType::Utf8View, false),
        Field::new("message", DataType::Utf8View, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
    ])
});
static PARTITION_COLUMNS: LazyLock<Vec<FieldRef>> =
    LazyLock::new(|| vec![Arc::new(Field::new("day", DataType::Date32, false))]);

struct LogTable(GenericTable<Vec<LogRecord>>);

impl LogTable {
    fn new(
        write_rx: broadcast::Receiver<Vec<LogRecord>>,
        filesystem: Arc<Filesystem>,
        compaction_frequency: Duration,
    ) -> Self {
        Self(GenericTable::new(
            write_rx,
            filesystem,
            compaction_frequency,
        ))
    }
}

impl Table<Vec<LogRecord>> for LogTable {
    fn name(&self) -> String {
        NAME.to_string()
    }

    fn make_batch(&self, data: Vec<LogRecord>) -> RecordBatch {
        let cap = data.len();
        let mut ids = Vec::with_capacity(cap);
        let mut levels = Vec::with_capacity(cap);
        let mut messages = Vec::with_capacity(cap);
        let mut timestamps = Vec::with_capacity(cap);
        let mut days = Vec::with_capacity(cap);
        for log in data {
            ids.push(log.id);
            levels.push(log.level);
            messages.push(log.message);
            timestamps.push(log.timestamp);
            days.push(log.day);
        }

        writer::record_batch(
            SCHEMA.clone(),
            PARTITION_COLUMNS.clone(),
            vec![
                Arc::new(UInt64Array::from(ids)),
                Arc::new(StringViewArray::from(levels)),
                Arc::new(StringViewArray::from(messages)),
                Arc::new(TimestampNanosecondArray::from(timestamps).with_timezone("UTC")),
                Arc::new(Date32Array::from(days)),
            ],
        )
    }

    fn data_frame_write_options(&self) -> DataFrameWriteOptions {
        DataFrameWriteOptions::new()
            .with_insert_operation(InsertOp::Append)
            .with_single_file_output(true)
            .with_partition_by(vec!["day".to_string()])
            .with_sort_by(vec![SortExpr {
                expr: col("timestamp"),
                asc: false,
                nulls_first: false,
            }])
    }

    fn schema(&self) -> Schema {
        SCHEMA.clone()
    }

    fn filesystem(&self) -> Arc<Filesystem> {
        self.0.filesystem()
    }

    fn compaction_frequency(&self) -> Duration {
        self.0.compaction_frequency()
    }

    fn write_rx(&self) -> broadcast::Receiver<Vec<LogRecord>> {
        self.0.write_rx()
    }

    fn buffer(&self) -> Arc<RwLock<Vec<RecordBatch>>> {
        self.0.buffer()
    }
}

pub async fn initialize(
    write_rx: broadcast::Receiver<Vec<LogRecord>>,
    filesystem: Arc<Filesystem>,
    compaction_frequency: Duration,
    cancellation_token: CancellationToken,
) -> Result<JoinHandle<()>, DataFusionError> {
    let partition_by = vec![("day".to_string(), DataType::Date32)];
    let sort_by = vec![SortExpr {
        expr: col("timestamp"),
        asc: false,
        nulls_first: false,
    }];

    let format = ParquetFormat::default().with_options(TableParquetOptions {
        global: ParquetOptions {
            pushdown_filters: true,
            reorder_filters: true,
            compression: Some(Compression::SNAPPY.to_string()),
            maximum_parallel_row_group_writers: num_cpus::get(),
            bloom_filter_on_write: true,
            ..Default::default()
        },
        column_specific_options: column_opts(),
        ..Default::default()
    });

    let listing_opts = ListingOptions::new(Arc::new(format))
        .with_collect_stat(false)
        .with_table_partition_cols(partition_by)
        .with_file_sort_order(vec![sort_by]);

    tables::register(
        NAME,
        format!("/{}/", NAME),
        listing_opts,
        Arc::new(SCHEMA.clone()),
    )
    .await?;

    let table = LogTable::new(write_rx, filesystem, compaction_frequency);
    Ok(tokio::spawn(async move {
        table.start(cancellation_token).await;
    }))
}

fn column_opts() -> HashMap<String, ParquetColumnOptions> {
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

    column_opts
}
