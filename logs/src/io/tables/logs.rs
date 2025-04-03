use crate::io::fs::FileSystem;
use crate::io::tables::generic::GenericTable;
use crate::io::tables::Table;
use crate::io::{tables, writer};
use crate::server::grpc::opentelemetry::LogRecord;
use datafusion::arrow::array::{
    Date32Array, RecordBatch, StringArray, TimestampNanosecondArray, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema, TimeUnit};
use datafusion::config::{ParquetColumnOptions, ParquetOptions, TableParquetOptions};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{col, SortExpr};
use datafusion::parquet::basic::Compression;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use tokio::sync::{broadcast, RwLock};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

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

struct LogTable<F: FileSystem>(GenericTable<F, Vec<LogRecord>>);

impl<F: FileSystem> LogTable<F> {
    fn new(filesystem: Arc<F>, write_rx: broadcast::Receiver<Vec<LogRecord>>) -> Self {
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

        let table =
            GenericTable::new(filesystem, write_rx).with_parquet_options(TableParquetOptions {
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
            });
        Self(table)
    }
}

impl<F: FileSystem> Table<F, Vec<LogRecord>> for LogTable<F> {
    fn name(&self) -> String {
        NAME.to_string()
    }

    fn make_batch(data: Vec<LogRecord>) -> RecordBatch {
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
                Arc::new(StringArray::from(levels)),
                Arc::new(StringArray::from(messages)),
                Arc::new(TimestampNanosecondArray::from(timestamps).with_timezone("UTC")),
                Arc::new(Date32Array::from(days)),
            ],
        )
    }

    fn filesystem(&self) -> Arc<F> {
        self.0.filesystem()
    }

    fn write_rx(&self) -> broadcast::Receiver<Vec<LogRecord>> {
        self.0.write_rx()
    }

    fn buffer(&self) -> Arc<RwLock<Vec<RecordBatch>>> {
        self.0.buffer()
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

    fn writer_options(&self) -> TableParquetOptions {
        self.0.writer_options()
    }
}

pub async fn initialize(
    filesystem: Arc<impl FileSystem + 'static>,
    write_rx: broadcast::Receiver<Vec<LogRecord>>,
    cancellation_token: CancellationToken,
) -> Result<JoinHandle<()>, DataFusionError> {
    let partition_by = vec![("day".to_string(), DataType::Date32)];
    let sort_by = vec![SortExpr {
        expr: col("timestamp"),
        asc: false,
        nulls_first: false,
    }];

    let listing_opts = ListingOptions::new(Arc::new(ParquetFormat::new()))
        .with_collect_stat(false)
        .with_table_partition_cols(partition_by)
        .with_file_sort_order(vec![sort_by]);

    tables::register(
        NAME,
        filesystem.url(),
        listing_opts,
        Arc::new(SCHEMA.clone()),
    )
    .await?;

    let table = LogTable::new(filesystem, write_rx);
    Ok(tokio::spawn(async move {
        table.start(cancellation_token).await;
    }))
}
