use crate::io;
use crate::io::tables;
use crate::server::http::query::schemas::{LogsResponse, QueryLogsParams, QuerySqlParams};
use crate::server::http::ApiError;
use axum::extract::Query;
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use datafusion::functions_aggregate::expr_fn::array_agg;
use datafusion::functions_nested;
use datafusion::logical_expr::{exists, JoinType, SortExpr};
use datafusion::prelude::{col, lit};
use std::sync::Arc;

pub async fn query_logs(
    Query(params): Query<QueryLogsParams>,
) -> Result<Json<LogsResponse>, ApiError> {
    let query = query_engine::fql::parse_query(params.query.as_deref().unwrap_or("{}"))?;
    let ctx = io::get_session_context();

    let attrs_table_name = tables::log_attributes::NAME;
    let logs_table_name = tables::logs::NAME;
    // SELECT
    //     l.id,
    //     l.message,
    //     l.timestamp,
    //     map(array_agg(a.key), array_agg(a.value)) AS attributes
    // FROM logs l
    //          LEFT JOIN log_attributes a ON l.id = a.log_id
    // WHERE EXISTS (
    //     SELECT 1
    //     FROM log_attributes
    //     WHERE log_id = l.id AND key = 'level' AND value = 'INFO'
    // )
    //
    // GROUP BY l.id, l.message, l.timestamp
    //     LIMIT 10;

    // let mut attr_df = query_engine::fql::Builder::new(ctx, "timestamp")
    //     .with_timeframe(Some("day"), params.time_range.from, params.time_range.to)
    //     .query(attrs_table_name)
    //     .await?;
    // 
    // for attr in query.selector.attributes {
    //     attr_df = attr_df.filter(
    //         col("key")
    //             .eq(lit(attr.key))
    //             .and(query_engine::fql::lit_expr("value", attr.op, attr.value)),
    //     )?;
    // }
    // 
    // let logs_df = query_engine::fql::Builder::new(ctx, "timestamp")
    //     .with_timeframe(Some("day"), params.time_range.from, params.time_range.to)
    //     .with_sort(vec![("timestamp", false)])
    //     .with_limit(params.limit.unwrap_or(1000))
    //     .query(logs_table_name)
    //     .await
    //     .and_then(|df| match query.selector.message {
    //         Some(filter) => df.filter(query_engine::fql::lit_expr(
    //             "message",
    //             filter.op,
    //             filter.value,
    //         )),
    //         None => Ok(df),
    //     })?;
    //
    // let df = logs_df
    //     .join(
    //         attr_df,
    //         JoinType::Left,
    //         [
    //             col(qualified_name(attrs_table_name, "day"))
    //                 .eq(col(qualified_name(logs_table_name, "day"))),
    //             col("log_id").eq(col("id")),
    //         ],
    //     )?
    //     .aggregate(
    //         vec![
    //             col(qualified_name(logs_table_name, "id")),
    //             col(qualified_name(logs_table_name, "level")),
    //             col(qualified_name(logs_table_name, "message")),
    //             col(qualified_name(logs_table_name, "timestamp")),
    //         ],
    //         vec![array_agg(col("key")), array_agg(col("value"))],
    //     )?;
    //
    // if query.map_functions.contains(&ferrum_ql::Function::Count) {
    //     let count = df.count().await?;
    //     return Ok(Json(LogsResponse::Count { count }));
    // }
    //
    // let array_agg_key_column = format!("array_agg({})", qualified_name(attrs_table_name, "key"));
    // let array_agg_value_column =
    //     format!("array_agg({})", qualified_name(attrs_table_name, "value"));
    //
    // let df = df
    //     .with_column(
    //         "attributes",
    //         functions_nested::map::map(
    //             vec![col(&array_agg_key_column)],
    //             vec![col(&array_agg_value_column)],
    //         ),
    //     )?
    //     .drop_columns(&["id", &array_agg_key_column, &array_agg_value_column])?;
    //
    // let logs = df.collect::<Log>().await?;
  
    
    Ok(Json(LogsResponse::Logs(vec![])))
}

// pub async fn query_attribute_keys(
//     Query(params): Query<fql::QueryParams>,
// ) -> Result<Json<AttributeKeys>, ApiError> {
//     let attributes = query::attribute_keys(params).await?;
//     Ok(Json(attributes))
// }
//
// pub async fn query_attribute_values(
//     Path(attribute): Path<String>,
//     Query(params): Query<fql::QueryAttributeValuesParams>,
// ) -> Result<Json<AttributeValues>, ApiError> {
//     let values = query::attribute_values(&params.time_range, &attribute, params.limit).await?;
//     Ok(Json(values))
// }

pub async fn query_sql(
    Query(params): Query<QuerySqlParams>,
) -> Result<impl IntoResponse, ApiError> {
    let bytes = query_engine::sql::query(io::get_session_context(), &params.query).await?;
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );

    Ok((StatusCode::OK, headers, Vec::from(bytes)))
}

#[inline(always)]
fn qualified_name(table: &str, column: &str) -> String {
    format!("{}.{}", table, column)
}
