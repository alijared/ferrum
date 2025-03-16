use crate::io::query;
use crate::io::query::fql;
use crate::server::http::loki::schemas::{
    AttributeKeysResponse, AttributeValuesResponse, LogsResponse,
};
use crate::server::http::ApiError;
use axum::extract::{Path, Query};
use axum::Json;

pub async fn query_logs(
    Query(params): Query<fql::QueryParams>,
) -> Result<Json<LogsResponse>, ApiError> {
    let logs = query::logs(params).await?;
    Ok(Json(logs))
}

pub async fn query_attribute_keys(
    Query(params): Query<fql::QueryParams>,
) -> Result<Json<AttributeKeysResponse>, ApiError> {
    let attributes = query::attribute_keys(params).await?;
    Ok(Json(attributes))
}

pub async fn query_attribute_values(
    Path(attribute): Path<String>,
    Query(params): Query<fql::QueryAttributeValuesParams>,
) -> Result<Json<AttributeValuesResponse>, ApiError> {
    let values = query::attribute_values(&params.time_range, &attribute, params.limit).await?;
    Ok(Json(values))
}
