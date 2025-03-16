use crate::udfs;
use datafusion::config::{ExecutionOptions, ParquetOptions};
use datafusion::logical_expr::ScalarUDF;
use datafusion::parquet::basic::Compression;
use datafusion::prelude::{SessionConfig, SessionContext};
use log::error;
use std::fs;
use std::path::PathBuf;
use tokio::sync::OnceCell;

pub mod query;
pub mod tables;
pub mod writer;

static SQL_CTX: OnceCell<SessionContext> = OnceCell::const_new();

pub async fn set_session_context() -> SessionContext {
    let mut session_config = SessionConfig::new()
        .with_coalesce_batches(true)
        .with_collect_statistics(true)
        .with_target_partitions(num_cpus::get())
        .with_parquet_pruning(true)
        .with_parquet_bloom_filter_pruning(true)
        .with_parquet_page_index_pruning(true)
        .with_information_schema(true);

    let session_opts = session_config.options_mut();
    session_opts.execution = get_execution_options();

    let ctx = SessionContext::new_with_config(session_config);
    ctx.register_udf(ScalarUDF::from(udfs::user::Json::new()));

    let _ = SQL_CTX.set(ctx.clone());
    ctx
}

pub fn get_sql_context<'a>() -> &'a SessionContext {
    SQL_CTX.get().unwrap()
}

pub fn get_execution_options() -> ExecutionOptions {
    ExecutionOptions {
        coalesce_batches: true,
        parquet: ParquetOptions {
            pushdown_filters: true,
            reorder_filters: true,
            compression: Some(Compression::SNAPPY.to_string()),
            maximum_parallel_row_group_writers: num_cpus::get(),
            bloom_filter_on_write: true,
            ..Default::default()
        },
        keep_partition_by_columns: true,
        use_row_number_estimates_to_optimize_partitioning: true,
        ..Default::default()
    }
}

pub fn clear_partition_files(files: Vec<PathBuf>) {
    for file in files {
        if let Err(e) = fs::remove_file(file) {
            error!("Failed to remove old pre-merged partition file: {}", e);
        }
    }
}
