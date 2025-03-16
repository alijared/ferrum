use crate::io::query;
use crate::io::query::{fql, sql};
use crate::server::http::query::schemas::LogsResponse;
use crate::server::http::schemas::{AttributeKeys, AttributeValues, SqlResponse};
use crate::server::http::ApiError;
use axum::extract::{Path, Query};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::Json;

pub async fn query_fql(
    Query(params): Query<fql::QueryParams>,
) -> Result<Json<LogsResponse>, ApiError> {
    let logs = query::logs(params).await?;
    Ok(Json(logs))
}

pub async fn query_attribute_keys(
    Query(params): Query<fql::QueryParams>,
) -> Result<Json<AttributeKeys>, ApiError> {
    let attributes = query::attribute_keys(params).await?;
    Ok(Json(attributes))
}

pub async fn query_attribute_values(
    Path(attribute): Path<String>,
    Query(params): Query<fql::QueryAttributeValuesParams>,
) -> Result<Json<AttributeValues>, ApiError> {
    let values = query::attribute_values(&params.time_range, &attribute, params.limit).await?;
    Ok(Json(values))
}

pub async fn query_sql(
    Query(params): Query<sql::QueryParams>,
) -> Result<impl IntoResponse, ApiError> {
    let bytes = sql::query::<SqlResponse>(params).await?;
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );

    Ok((StatusCode::OK, headers, Vec::from(bytes)))
}
