use crate::io::{get_sql_context, tables};
use crate::server::http::schemas::{
    FqlQueryParams, FqlResponse, LabelQueryParams, Labels, Log, SqlQueryParams,
};
use crate::server::http::ApiError;
use axum::extract::Query;
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use chrono::Datelike;
use chrono::{DateTime, TimeZone, Utc};
use datafusion::arrow::array::{Array, AsArray, TimestampNanosecondArray};
use datafusion::common::ScalarValue;
use datafusion::dataframe::DataFrame;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{col, lit, lit_timestamp_nano, Expr};
use datafusion::prelude::{get_field, regexp_match};
use futures_util::StreamExt;
use serde_json::json;
use std::collections::HashMap;

pub async fn query_fql(
    Query(params): Query<FqlQueryParams>,
) -> Result<Json<FqlResponse>, ApiError> {
    let query = ferum_ql::parse(&params.query.unwrap_or("{}".to_string())).map_err(|e| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            format!("Failed to parse query: {}", e).as_str(),
        )
    })?;

    let time_range = params.time_range;
    let df = df_from_timeframe(&time_range.to, &time_range.to)
        .await
        .and_then(|df| df.drop_columns(&["day"]))
        .map_err(ApiError::from_df_error)?;

    let selector = query.selector;
    let df = apply_fql_filter(df, selector.level, None)
        .and_then(|df| apply_fql_filter(df, selector.message, None))
        .and_then(|mut df| {
            for filter in selector.attributes {
                df = apply_fql_filter(df, Some(filter), Some("attributes"))?;
            }
            Ok(df)
        })?;
    let df = df
        .sort(vec![col("timestamp").sort(false, false)])
        .map_err(ApiError::from_df_error)?;

    if query.map_functions.contains(&ferum_ql::Function::Count) {
        let count = df.count().await.map_err(ApiError::from_df_error)?;
        return Ok(Json(FqlResponse::Count { count }));
    }

    let df = df
        .limit(0, Some(params.limit.unwrap_or(1000)))
        .map_err(ApiError::from_df_error)?;
    let partition_streams = df
        .execute_stream_partitioned()
        .await
        .map_err(ApiError::from_df_error)?;

    let should_serialize_message = query.map_functions.contains(&ferum_ql::Function::Json);
    let mut results = Vec::new();
    for mut stream in partition_streams {
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.map_err(ApiError::from_df_error)?;
            let arrays = (
                batch
                    .column(batch.schema().index_of("timestamp").unwrap())
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap(),
                batch
                    .column(batch.schema().index_of("level").unwrap())
                    .as_string::<i32>(),
                batch
                    .column(batch.schema().index_of("message").unwrap())
                    .as_string::<i32>(),
                batch
                    .column(batch.schema().index_of("attributes").unwrap())
                    .as_map(),
            );

            for i in 0..batch.num_rows() {
                let array = arrays.3.value(i);
                let keys = array.column(0).as_string::<i32>();
                let values = array.column(1).as_string::<i32>();

                let attributes_length = keys.len();
                let mut attributes = HashMap::with_capacity(attributes_length);
                for j in 0..attributes_length {
                    attributes.insert(keys.value(j).to_string(), values.value(j).to_string());
                }

                let message = arrays.2.value(i);
                let json_message = if should_serialize_message {
                    serde_json::from_str(message).unwrap_or(Some(json!({"message": message})))
                } else {
                    None
                };

                let message = if json_message.is_some() {
                    None
                } else {
                    Some(message.to_string())
                };

                results.push(Log {
                    timestamp: Utc.timestamp_nanos(arrays.0.value(i)),
                    level: arrays.1.value(i).to_string(),
                    message,
                    json_message,
                    attributes,
                });
            }
        }
    }

    Ok(Json(FqlResponse::Data(results)))
}

fn apply_fql_filter(
    df: DataFrame,
    filter: Option<ferum_ql::Filter>,
    nested: Option<&str>,
) -> Result<DataFrame, ApiError> {
    if let Some(filter) = filter {
        let mut expr = if let Some(nested_column) = nested {
            get_field(col(nested_column), filter.key)
        } else {
            col(filter.key)
        };

        match filter.op {
            ferum_ql::ComparisonOp::Eq => expr = expr.eq(lit(filter.value)),
            ferum_ql::ComparisonOp::Neq => expr = expr.not_eq(lit(filter.value)),
            ferum_ql::ComparisonOp::Regex => {
                expr = regexp_match(expr, lit(filter.value), None).is_not_null()
            }
            ferum_ql::ComparisonOp::Greater => {}
            ferum_ql::ComparisonOp::GreaterEq => {}
            ferum_ql::ComparisonOp::Less => {}
            ferum_ql::ComparisonOp::LessEq => {}
        }
        return df.filter(expr).map_err(ApiError::from_df_error);
    }
    Ok(df)
}

pub async fn query_sql(
    Query(params): Query<SqlQueryParams>,
) -> Result<impl IntoResponse, ApiError> {
    let ctx = get_sql_context();
    let df = ctx
        .sql(&params.query)
        .await
        .and_then(|df| df.drop_columns(&["day"]))
        .map_err(ApiError::from_df_error)?;

    let buf = Vec::new();
    let mut writer = arrow_json::ArrayWriter::new(buf);
    let partition_streams = df
        .execute_stream_partitioned()
        .await
        .map_err(ApiError::from_df_error)?;

    for mut stream in partition_streams {
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.map_err(ApiError::from_df_error)?;
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

pub async fn query_labels(
    Query(params): Query<LabelQueryParams>,
) -> Result<Json<Labels>, ApiError> {
    let time_range = params.time_range;
    let df = df_from_timeframe(&time_range.to, &time_range.from)
        .await
        .and_then(|df| df.distinct())
        .map_err(ApiError::from_df_error)?;

    let stream = df
        .execute_stream_partitioned()
        .await
        .map_err(ApiError::from_df_error)?;
    let labels = Labels::from_stream(stream)
        .await
        .map_err(ApiError::from_df_error)?;
    
    Ok(Json(labels))
}

async fn df_from_timeframe(
    to: &DateTime<Utc>,
    from: &DateTime<Utc>,
) -> Result<DataFrame, DataFusionError> {
    let today_to = num_days_since_epoch(to);
    let today_from = num_days_since_epoch(from);
    let ctx = get_sql_context();
    ctx.table(tables::logs::NAME)
        .await
        .and_then(|df| {
            let from = Expr::Literal(ScalarValue::Date32(Some(today_from)));
            if today_from != today_to {
                return df.filter(
                    col("day").between(from, Expr::Literal(ScalarValue::Date32(Some(today_to)))),
                );
            }
            df.filter(col("day").eq(from))
        })
        .and_then(|df| {
            let from = lit_timestamp_nano(from.timestamp_nanos_opt().unwrap());
            let to = lit_timestamp_nano(to.timestamp_nanos_opt().unwrap());
            df.filter(col("timestamp").between(from, to))
        })
}

const UNIX_EPOCH_DAYS_FROM_CE: i32 = 719163;

fn num_days_since_epoch(dt: &DateTime<Utc>) -> i32 {
    dt.date_naive().num_days_from_ce() - UNIX_EPOCH_DAYS_FROM_CE
}
