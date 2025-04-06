use crate::io::query::FromStreams;
use crate::io::{get_session_context, query};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct QueryParams {
    pub query: String,
}

pub async fn query<T: FromStreams>(params: QueryParams) -> Result<T, query::Error> {
    let ctx = get_session_context();
    let df = ctx
        .sql(&params.query)
        .await
        .and_then(|df| df.drop_columns(&["day"]))?;

    let streams = df.execute_stream_partitioned().await?;
    T::try_from_streams(streams, false).await
}
