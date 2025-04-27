use crate::io;
use crate::io::tables;
use crate::server::http::query::schemas::{
    LogsResponse, QueryAttributeValuesParams, QueryLogsParams, QuerySqlParams, SortOrder,
    StringValue,
};
use crate::server::http::ApiError;
use axum::extract::{Path, Query};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use datafusion::common::JoinType;
use datafusion::functions_aggregate::array_agg::array_agg;
use datafusion::functions_nested;
use datafusion::logical_expr::builder::unnest;
use datafusion::logical_expr::{col, lit};
use query_engine::fql::DataFrame;

pub async fn query_logs(
    Query(params): Query<QueryLogsParams>,
) -> Result<Json<LogsResponse>, ApiError> {
    let sort = params.sort.clone().unwrap_or(SortOrder::Descending);
    let (query, df) = select_logs(params, vec![("timestamp", sort.into())]).await?;
    if query.map_functions.contains(&ferrum_ql::Function::Count) {
        let count = df.count().await?;
        return Ok(Json(LogsResponse::Count { count }));
    }

    let attrs_table_name = tables::log_attributes::NAME;
    let array_agg_key_column = format!("array_agg({})", qualified_name(attrs_table_name, "key"));
    let array_agg_value_column =
        format!("array_agg({})", qualified_name(attrs_table_name, "value"));

    let df = df
        .with_column(
            "attributes",
            functions_nested::map::map(
                vec![col(&array_agg_key_column)],
                vec![col(&array_agg_value_column)],
            ),
        )?
        .drop_columns(&["id", &array_agg_key_column, &array_agg_value_column])?;

    let logs = df.collect().await?;
    Ok(Json(LogsResponse::Logs(logs)))
}

pub async fn query_attribute_keys(
    Query(params): Query<QueryLogsParams>,
) -> Result<Json<Vec<StringValue>>, ApiError> {
    let limit = params.limit.unwrap_or(1000);
    let (_, df) = select_logs(params, Vec::new()).await?;
    let column = format!(
        "array_agg({})",
        qualified_name(tables::log_attributes::NAME, "key")
    );

    let (state, plan) = df.select_columns(&[&column])?.into_parts().clone();
    let plan = unnest(plan, vec![column.into()])?;

    let df = DataFrame::dataframe(state, plan)
        .distinct()?
        .limit(0, Some(limit))?;
    let keys = DataFrame::from(df).collect().await?;
    Ok(Json(keys))
}

pub async fn query_attribute_values(
    Path(attribute): Path<String>,
    Query(params): Query<QueryAttributeValuesParams>,
) -> Result<Json<Vec<StringValue>>, ApiError> {
    let ctx = io::get_session_context();
    let builder = query_engine::fql::Builder::new(ctx, "timestamp")
        .with_timeframe(Some("day"), params.time_range.from, params.time_range.to)
        .with_limit(params.limit.unwrap_or(1000));
    let df = match attribute.as_str() {
        "level" => builder
            .query(tables::logs::NAME)
            .await?
            .select(vec![col("level")])?,
        attr => builder
            .query(tables::log_attributes::NAME)
            .await?
            .filter(col("key").eq(lit(attr)))?
            .select(vec![col("value")])?,
    };

    let values = df.unique()?.collect().await?;
    Ok(Json(values))
}

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

async fn select_logs(
    params: QueryLogsParams,
    sort: Vec<(&str, bool)>,
) -> Result<(ferrum_ql::Query, DataFrame), ApiError> {
    let query = query_engine::fql::parse_query(params.query.as_deref().unwrap_or("{}"))?;
    let ctx = io::get_session_context();

    let attrs_table_name = tables::log_attributes::NAME;
    let logs_table_name = tables::logs::NAME;

    let message = query.selector.message.clone();
    let level = query.selector.level.clone();
    let logs_df = query_engine::fql::Builder::new(ctx, "timestamp")
        .with_timeframe(Some("day"), params.time_range.from, params.time_range.to)
        .with_sort(sort)
        .with_limit(params.limit.unwrap_or(1000))
        .query(logs_table_name)
        .await
        .and_then(|df| {
            let df = match message {
                Some(filter) => df.filter(query_engine::fql::lit_expr(
                    "message",
                    filter.op,
                    filter.value,
                )),
                None => Ok(df),
            }?;

            match level {
                Some(filter) => df.filter(query_engine::fql::lit_expr(
                    "level",
                    filter.op,
                    filter.value,
                )),
                None => Ok(df),
            }
        })?;

    let attr_df = query_engine::fql::Builder::new(ctx, "timestamp")
        .with_timeframe(Some("day"), params.time_range.from, params.time_range.to)
        .query(attrs_table_name)
        .await?;

    let log_ids = match query.selector.attributes.is_empty() {
        false => {
            let mut filter_df = attr_df.clone();
            for attr in &query.selector.attributes {
                filter_df = filter_df.filter(col("key").eq(lit(&attr.key)).and(
                    query_engine::fql::lit_expr("value", attr.op.clone(), &attr.value),
                ))?;
            }

            Some(
                filter_df
                    .select(vec![col("log_id").alias("lid")])?
                    .unique()?,
            )
        }

        true => None,
    };

    let logs_df = match log_ids {
        Some(ids_df) => logs_df.join(ids_df, JoinType::Inner, [col("id").eq(col("lid"))])?,
        None => logs_df,
    };

    let df = logs_df
        .join(
            attr_df,
            JoinType::Left,
            [
                col(qualified_name(attrs_table_name, "day"))
                    .eq(col(qualified_name(logs_table_name, "day"))),
                col(qualified_name(attrs_table_name, "log_id")).eq(col("id")),
            ],
        )?
        .aggregate(
            vec![
                col(qualified_name(logs_table_name, "id")),
                col(qualified_name(logs_table_name, "level")),
                col(qualified_name(logs_table_name, "message")),
                col(qualified_name(logs_table_name, "timestamp")),
            ],
            vec![array_agg(col("key")), array_agg(col("value"))],
        )?;

    Ok((query, df))
}

#[inline(always)]
fn qualified_name(table: &str, column: &str) -> String {
    format!("{}.{}", table, column)
}
