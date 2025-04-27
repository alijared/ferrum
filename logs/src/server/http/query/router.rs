use crate::server::http::query::handlers::{
    query_attribute_keys, query_attribute_values, query_logs, query_sql,
};
use axum::routing::get;
use axum::Router;

pub fn new() -> Router {
    Router::new()
        .route("/logs", get(query_logs))
        .route("/sql", get(query_sql))
        .route("/attributes", get(query_attribute_keys))
        .route("/attributes/{value}", get(query_attribute_values))
}
