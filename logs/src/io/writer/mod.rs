use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::error::ArrowError;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;

pub fn combine_batches(batches: &[RecordBatch]) -> Result<RecordBatch, ArrowError> {
    if batches.len() == 1 {
        return Ok(batches[0].clone());
    }

    concat_batches(&batches[0].schema(), batches)
}

pub async fn write_batch(
    ctx: &SessionContext,
    table_name: &str,
    write_opts: DataFrameWriteOptions,
    batch: Vec<RecordBatch>,
) -> Result<(), DataFusionError> {
    let df = ctx.read_batches(batch).unwrap();
    match df.write_table(table_name, write_opts).await {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}
