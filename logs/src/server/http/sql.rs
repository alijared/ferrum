use crate::io::get_sql_context;
use crate::server::http::schemas::SqlQueryParams;
use crate::server::http::ApiError;
use axum::extract::Query;
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use futures_util::StreamExt;

pub async fn query_sql(
    Query(params): Query<SqlQueryParams>,
) -> Result<impl IntoResponse, ApiError> {
    let ctx = get_sql_context();
    let df = ctx
        .sql(&params.query)
        .await
        .and_then(|df| df.drop_columns(&["day"]))?;

    let buf = Vec::new();
    let mut writer = arrow_json::ArrayWriter::new(buf);
    let partition_streams = df.execute_stream_partitioned().await?;

    for mut stream in partition_streams {
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            writer
                .write(&batch)
                .map_err(|e| ApiError::internal_error(&e.to_string()))?;
        }
    }

    writer
        .finish()
        .map_err(|e| ApiError::internal_error(&e.to_string()))?;

    let bytes = writer.into_inner();
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );

    Ok((StatusCode::OK, headers, bytes))
}
