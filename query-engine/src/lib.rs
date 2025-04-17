mod error;
pub mod fql;
pub mod sql;

pub use error::Error;

use datafusion::arrow::array::RecordBatch;

pub trait FromRecordBatch: Sized {
    fn from_batch(batch: &RecordBatch) -> Vec<Self>;
}
