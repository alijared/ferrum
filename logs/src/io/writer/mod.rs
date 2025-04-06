use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{FieldRef, Schema};
use datafusion::arrow::error::ArrowError;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

pub fn record_batch(
    schema: Schema,
    partition_columns: Vec<FieldRef>,
    columns: Vec<ArrayRef>,
) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(schema_with_fields(schema, partition_columns)),
        columns,
    )
    .unwrap()
}

pub fn combine_batches(batches: &[RecordBatch]) -> Result<RecordBatch, ArrowError> {
    if batches.len() == 1 {
        return Ok(batches[0].clone());
    }

    concat_batches(&batches[0].schema(), batches)
}

pub async fn write_batch(
    ctx: &SessionContext,
    table_name: &str,
    df_options: DataFrameWriteOptions,
    batch: Vec<RecordBatch>,
) -> Result<(), DataFusionError> {
    let df = ctx.read_batches(batch)?;
    df.write_table(table_name, df_options).await?;

    Ok(())
}

pub fn schema_with_fields(schema: Schema, mut new_fields: Vec<FieldRef>) -> Schema {
    if new_fields.is_empty() {
        return schema;
    }

    let mut fields = schema.fields().to_vec();
    fields.append(&mut new_fields);
    Schema::new(fields)
}
