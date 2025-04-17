use crate::server::http::query::handlers::{query_logs, query_sql};
use axum::routing::get;
use axum::Router;

pub fn new() -> Router {
    Router::new()
        .route("/logs", get(query_logs))
        .route("/sql", get(query_sql))
    // .route("/attributes", get(query_attribute_keys))
    // .route("/attributes/{value}/values", get(query_attribute_values))
}
