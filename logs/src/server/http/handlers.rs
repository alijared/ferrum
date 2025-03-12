use crate::server::http::schemas::{AttributeKeys, FqlQueryParams, FqlResponse, Log};
use crate::server::http::{fql, ApiError};
use axum::extract::Query;
use axum::Json;
use chrono::TimeZone;
use chrono::Utc;
use datafusion::arrow::array::{Array, AsArray, ListArray, TimestampNanosecondArray};
use datafusion::functions_aggregate::array_agg::array_agg;
use datafusion::functions_nested;
use datafusion::logical_expr::col;
use futures_util::StreamExt;
use serde_json::json;
use std::collections::HashMap;

pub async fn query_fql(
    Query(params): Query<FqlQueryParams>,
) -> Result<Json<FqlResponse>, ApiError> {
    let limit = params.limit;
    let (query, df) =
        fql::select_logs(params, vec![array_agg(col("key")), array_agg(col("value"))])
            .await
            .and_then(|(q, df)| {
                let df = df.with_column(
                    "attributes",
                    functions_nested::map::map(
                        vec![col("array_agg(log_attributes.key)")],
                        vec![col("array_agg(log_attributes.value)")],
                    ),
                )?;
                Ok((q, df))
            })?;
    if query.map_functions.contains(&ferum_ql::Function::Count) {
        let count = df.count().await?;
        return Ok(Json(FqlResponse::Count { count }));
    }

    let partition_streams = fql::partition_streams(
        df,
        &[
            "id",
            "array_agg(log_attributes.key)",
            "array_agg(log_attributes.value)",
        ],
        limit,
    )
    .await?;
    let should_serialize_message = query.map_functions.contains(&ferum_ql::Function::Json);
    let mut results = Vec::new();
    for mut stream in partition_streams {
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
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
                let key_list = array
                    .column(0)
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap();
                let value_list = array
                    .column(1)
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap();

                let mut attributes = HashMap::new();
                for j in 0..key_list.len() {
                    let keys_ref = key_list.value(j);
                    let keys = keys_ref.as_string::<i32>();
                    let values_ref = value_list.value(j);
                    let values = values_ref.as_string::<i32>();

                    for k in 0..keys.len() {
                        attributes.insert(keys.value(k).to_string(), values.value(k).to_string());
                    }
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

pub async fn query_attributes(
    Query(params): Query<FqlQueryParams>,
) -> Result<Json<AttributeKeys>, ApiError> {
    let limit = params.limit;
    let (_, df) = fql::select_logs(params, vec![array_agg(col("key")).alias("attributes")]).await?;
    let streams =
        fql::partition_streams(df, &["id", "level", "message", "timestamp"], limit).await?;

    let attributes = AttributeKeys::try_from_streams(streams).await?;
    Ok(Json(attributes))
}
