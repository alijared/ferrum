use std::fs;
use crate::config::LogTableConfig;
use crate::io::tables::Table;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use datafusion::catalog::TableReference;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::SortExpr;
use datafusion::prelude::{col, SessionContext};
use futures_util::join;
use log::info;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub const NAME: &str = "logs";
static SCHEMA_BASE: LazyLock<Schema> = LazyLock::new(|| {
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
        )
    ])
});
pub static SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
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
        Field::new("day", DataType::Date32, false),
    ])
});

//todo: make this table function with parameters
pub async fn register(
    ctx: &SessionContext,
    data_path: &str,
    write_bus: mpsc::Receiver<RecordBatch>,
    config: LogTableConfig,
    cancellation_token: CancellationToken,
) -> Result<JoinHandle<()>, DataFusionError> {
    let table_ref = TableReference::from(NAME);
    let table_opts = listing_options();

    let data_path = format!("{}/{}", data_path, NAME);
    fs::create_dir_all(&data_path)?;
    
    ctx.register_listing_table(
        table_ref,
        data_path.clone(),
        table_opts,
        Some(Arc::new(SCHEMA_BASE.clone())),
        None,
    )
    .await?;

    let mut table = Table::new(NAME, write_options(), write_bus);
    let handle = tokio::spawn(async move {
        info!("Starting up logs table");
        
        let schema = SCHEMA.clone();
        let schema = Some(&schema);
        let writer = table.start_writer(cancellation_token.clone());
        let compactor = Table::start_compaction(
            Duration::from_secs(config.compaction_frequency_seconds),
            NAME,
            &data_path,
            Some(write_options()),
            schema,
            cancellation_token,
        );

        join!(writer, compactor);
    });

    Ok(handle)
}

fn write_options() -> DataFrameWriteOptions {
    DataFrameWriteOptions::new()
        .with_insert_operation(InsertOp::Append)
        .with_partition_by(vec!["day".to_string()])
        .with_sort_by(vec![SortExpr {
            expr: col("timestamp"),
            asc: false,
            nulls_first: false,
        }])
        .with_single_file_output(true)
}

fn listing_options() -> ListingOptions {
    ListingOptions::new(Arc::new(ParquetFormat::new()))
        .with_collect_stat(false)
        .with_file_sort_order(vec![vec![col("timestamp").sort(false, false)]])
        .with_table_partition_cols(vec![("day".to_string(), DataType::Date32)])
}
