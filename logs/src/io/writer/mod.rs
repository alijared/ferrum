use crate::io::tables;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::error::ArrowError;
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
    opts: &tables::TableOptions,
    batch: Vec<RecordBatch>,
) -> Result<(), DataFusionError> {
    let df = ctx.read_batches(batch).unwrap();
    match df
        .write_parquet(opts.data_path(), opts.into(), Some(opts.parquet()))
        .await
    {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}
