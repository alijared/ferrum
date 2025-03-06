use crate::io::{get_sql_context, tables};
use crate::server::ServerError;
use axum::extract::Query;
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use chrono::{Datelike, TimeZone};
use datafusion::arrow::array::{Array, AsArray, TimestampNanosecondArray};
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::{col, lit, lit_timestamp_nano, Expr, Literal};
use datafusion::prelude::{get_field, regexp_match};
use log::{error, info};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tonic::codegen::tokio_stream::StreamExt;

pub async fn run_server(
    port: u32,
    cancellation_token: CancellationToken,
) -> Result<(), ServerError> {
    let listener = TcpListener::bind(format!("0.0.0.0:{}", port))
        .await
        .map_err(ServerError::Http)?;
    info!("API server listening on {}", listener.local_addr().unwrap());

    let app = Router::new()
        .route("/query/fql", get(query_fql))
        .route("/query/sql", get(query_sql));
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            cancellation_token.cancelled().await;
            info!("Shutting down API server")
        })
        .await
        .map_err(ServerError::Http)
}

#[derive(Deserialize)]
struct FqlQueryParams {
    query: Option<String>,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    limit: Option<usize>,
}

#[derive(Serialize)]
#[serde(untagged)]
enum FqlResponse {
    Count { count: usize },
    Data(Vec<Log>),
}

#[derive(Serialize)]
struct Log {
    timestamp: DateTime<Utc>,
    level: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(rename = "message", skip_serializing_if = "Option::is_none")]
    json_message: Option<serde_json::Value>,
    attributes: HashMap<String, String>,
}

async fn query_fql(Query(params): Query<FqlQueryParams>) -> Result<Json<FqlResponse>, ApiError> {
    let query = ferum_ql::parse(&params.query.unwrap_or("{}".to_string())).map_err(|e| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            format!("failed to parse query: {}", e).as_str(),
        )
    })?;

    let today_from = num_days_since_epoch(&params.from);
    let today_to = num_days_since_epoch(&params.to);
    let ctx = get_sql_context();
    let df = ctx
        .table(tables::logs::NAME)
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
            let from = lit_timestamp_nano(params.from.timestamp_nanos_opt().unwrap());
            let to = lit_timestamp_nano(params.to.timestamp_nanos_opt().unwrap());
            df.filter(col("timestamp").between(from, to))
        })
        .and_then(|df| df.drop_columns(&["day"]))
        .map_err(ApiError::from_df_error)?;

    let df = apply_fql_filter(df, col("level"), query.level)
        .and_then(|df| apply_fql_filter(df, col("message"), query.message))
        .and_then(|mut df| {
            for (key, filter) in query.attributes {
                df = apply_fql_filter(df, get_field(col("attributes"), key), Some(filter))?;
            }
            Ok(df)
        })?;
    let df = df
        .sort(vec![col("timestamp").sort(false, false)])
        .map_err(ApiError::from_df_error)?;

    if query.functions.contains_key(&ferum_ql::Function::Count) {
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

    let should_serialize_message = query.functions.contains_key(&ferum_ql::Function::Json);
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

fn apply_fql_filter<T: Literal>(
    df: DataFrame,
    mut expr: Expr,
    filter: Option<ferum_ql::Filter<T>>,
) -> Result<DataFrame, ApiError> {
    if let Some(filter) = filter {
        match filter.op {
            ferum_ql::Operation::Eq => expr = expr.eq(lit(filter.value)),
            ferum_ql::Operation::Neq => expr = expr.not_eq(lit(filter.value)),
            ferum_ql::Operation::Regex => {
                expr = regexp_match(expr, lit(filter.value), None).is_not_null()
            }
        }
        return df.filter(expr).map_err(ApiError::from_df_error);
    }
    Ok(df)
}

#[derive(Deserialize)]
struct SqlQueryParams {
    query: String,
}

async fn query_sql(Query(params): Query<SqlQueryParams>) -> Result<impl IntoResponse, ApiError> {
    let ctx = get_sql_context();
    let df = ctx
        .sql(&params.query)
        .await
        .and_then(|df| df.drop_columns(&["day"]))
        .map_err(ApiError::from_df_error)?;
    make_response(df).await
}

async fn make_response(df: DataFrame) -> Result<impl IntoResponse, ApiError> {
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

pub struct ApiError {
    status_code: StatusCode,
    message: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        if self.status_code == StatusCode::INTERNAL_SERVER_ERROR {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }

        (self.status_code, Json(json!({"error": self.message}))).into_response()
    }
}

impl ApiError {
    pub fn new(code: StatusCode, message: &str) -> Self {
        Self {
            status_code: code,
            message: message.to_string(),
        }
    }

    pub fn from_df_error(e: DataFusionError) -> Self {
        match e {
            DataFusionError::SQL(e, _) => ApiError::new(StatusCode::BAD_REQUEST, &e.to_string()),
            DataFusionError::Plan(message) => ApiError::new(StatusCode::BAD_REQUEST, &message),
            DataFusionError::SchemaError(e, _) => {
                ApiError::new(StatusCode::BAD_REQUEST, &e.to_string())
            }
            _ => ApiError::internal_error(&e.to_string()),
        }
    }

    pub fn internal_error(message: &str) -> Self {
        error!("Generated unexpected internal server error: {}", message);
        Self {
            status_code: StatusCode::INTERNAL_SERVER_ERROR,
            message: "".to_string(),
        }
    }
}

const UNIX_EPOCH_DAYS_FROM_CE: i32 = 719163;

fn num_days_since_epoch(dt: &DateTime<Utc>) -> i32 {
    dt.date_naive().num_days_from_ce() - UNIX_EPOCH_DAYS_FROM_CE
}
