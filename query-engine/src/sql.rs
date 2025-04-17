use crate::Error;
use async_trait::async_trait;
use datafusion::dataframe::DataFrame;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use utils::AsyncTryFrom;

pub struct SqlResult(Vec<u8>);

impl From<SqlResult> for Vec<u8> {
    fn from(response: SqlResult) -> Self {
        response.0
    }
}

pub async fn query(ctx: &SessionContext, query: &str) -> Result<SqlResult, Error> {
    let df = ctx.sql(query).await?;
    
    df.clone().show().await?;
    <SqlResult as AsyncTryFrom<DataFrame>>::try_from(df).await
}

#[async_trait]
impl AsyncTryFrom<DataFrame> for SqlResult {
    type Error = Error;

    async fn try_from(value: DataFrame) -> Result<Self, Self::Error> {
        let streams = value.execute_stream_partitioned().await?;
        let mut writer = arrow_json::ArrayWriter::new(Vec::new());
        for mut stream in streams {
            while let Some(batch) = stream.try_next().await? {
                writer
                    .write(&batch)
                    .map_err(|e| Error::DataFusion(e.into()))?;
            }
        }

        Ok(SqlResult(writer.into_inner()))
    }
}
