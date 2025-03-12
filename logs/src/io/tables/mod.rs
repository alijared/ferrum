use crate::io;
use crate::io::writer;
use crate::server::opentelemetry::common::v1::any_value::Value;
use crate::server::opentelemetry::common::v1::AnyValue;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, FieldRef, Schema, SchemaRef};
use datafusion::catalog::TableReference;
use datafusion::common::DEFAULT_PARQUET_EXTENSION;
use datafusion::config::TableParquetOptions;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::SortExpr;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use log::{error, info};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::async_trait;

mod generic;
pub mod log_attributes;
pub mod logs;

#[async_trait]
trait Table<R: BatchWrite<T>, T: Clone + Send + Sync> {
    async fn start(&mut self, cancellation_token: CancellationToken);
}

trait BatchWrite<T: Clone + Send + Sync> {
    fn make_batch(_data: T) -> RecordBatch {
        RecordBatch::new_empty(Arc::new(Schema::empty()))
    }
}

#[derive(Clone)]
pub struct TableOptions {
    data_path: String,
    compaction_frequency: Duration,
    partition_by: Vec<(String, DataType)>,
    sort_by: Vec<SortExpr>,
    listing_options: ListingOptions,
    parquet: TableParquetOptions,
}

impl TableOptions {
    pub fn new(
        data_path: &str,
        compaction_frequency: Duration,
        partition_by: Vec<(String, DataType)>,
        sort_by: Vec<SortExpr>,
        collect_stat: bool,
        parquet: TableParquetOptions,
    ) -> Self {
        Self {
            data_path: data_path.to_string(),
            compaction_frequency,
            partition_by: partition_by.clone(),
            sort_by: sort_by.clone(),
            listing_options: ListingOptions::new(Arc::new(ParquetFormat::new()))
                .with_collect_stat(collect_stat)
                .with_table_partition_cols(partition_by)
                .with_file_sort_order(vec![sort_by]),
            parquet,
        }
    }

    pub fn data_path(&self) -> &str {
        &self.data_path
    }

    pub fn parquet(&self) -> TableParquetOptions {
        self.parquet.clone()
    }
}

impl From<&TableOptions> for DataFrameWriteOptions {
    fn from(opts: &TableOptions) -> DataFrameWriteOptions {
        let partition_by = opts.partition_by.clone().into_iter().map(|t| t.0).collect();
        DataFrameWriteOptions::new()
            .with_insert_operation(InsertOp::Append)
            .with_partition_by(partition_by)
            .with_sort_by(opts.sort_by.clone())
            .with_single_file_output(true)
    }
}

async fn start_compaction<R: BatchWrite<T>, T: Clone + Send + Sync>(
    opts: Arc<TableOptions>,
    schema: Schema,
    cancellation_token: CancellationToken,
) -> JoinHandle<()> {
    info!("Starting compactor...");
    
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(opts.compaction_frequency);
        let opts = opts.clone();
        loop {
            let schema = schema.clone();
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e)
                        = run_compaction::<R, T>(opts.as_ref(), schema).await {
                            error!("Error compacting partitions: {}", e);
                    }
                }
                _ = cancellation_token.cancelled() => {
                    break;
                }
            }
        }
        info!("Compactor stopped");
    })
}

async fn run_compaction<R: BatchWrite<T>, T: Clone + Send + Sync>(
    opts: &TableOptions,
    schema: Schema,
) -> Result<(), DataFusionError> {
    let day = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let part_name = format!("day={}", day);
    let part_dir = Path::new(&opts.data_path).join(&part_name);

    if !part_dir.exists() {
        return Ok(());
    }

    let rd = fs::read_dir(part_dir).map_err(DataFusionError::IoError)?;
    let mut files = Vec::new();
    for entry_result in rd {
        let entry = entry_result.map_err(DataFusionError::IoError)?;
        if let Some(ext) = entry.path().extension() {
            let fext = format!(".{}", ext.to_str().unwrap_or_default());
            if fext == DEFAULT_PARQUET_EXTENSION {
                files.push(entry.path());
            }
        }
    }

    if files.is_empty() || files.len() == 1 {
        return Ok(());
    }

    info!("Running table compaction...");

    let ctx = io::get_sql_context();
    let df = ctx
        .read_parquet(
            files
                .iter()
                .map(|s| s.to_str().unwrap())
                .collect::<Vec<_>>(),
            ParquetReadOptions {
                schema: Some(&schema),
                ..Default::default()
            },
        )
        .await?;

    let batches = df.collect().await?;
    writer::write_batch(ctx, opts, batches).await?;
    io::clear_partition_files(files);

    info!("Table compaction completed");
    Ok(())
}

async fn register(
    ctx: &SessionContext,
    name: &str,
    opts: &TableOptions,
    schema: SchemaRef,
) -> Result<String, DataFusionError> {
    info!("Registering {} table", name);
    let table_ref = TableReference::from(name);
    let data_path = format!("{}/{}", opts.data_path, name);

    fs::create_dir_all(&data_path)?;
    ctx.register_listing_table(
        table_ref,
        data_path.clone(),
        opts.clone().listing_options,
        Some(schema),
        None,
    )
    .await?;

    info!("Registered {} table", name);
    Ok(data_path)
}

fn schema_with_fields(schema: Schema, mut new_fields: Vec<FieldRef>) -> Schema {
    let mut fields = schema.fields().to_vec();
    fields.append(&mut new_fields);

    Schema::new(fields)
}

fn convert_any_value(value: AnyValue) -> String {
    value.value.map(convert_value).unwrap_or_default()
}

fn convert_value(value: Value) -> String {
    match value {
        Value::StringValue(s) => s,
        Value::BoolValue(b) => b.to_string(),
        Value::IntValue(i) => i.to_string(),
        Value::DoubleValue(d) => d.to_string(),
        Value::ArrayValue(_) => "".to_string(),
        Value::KvlistValue(_) => "".to_string(),
        Value::BytesValue(b) => String::from_utf8(b).unwrap_or_default(),
    }
}
