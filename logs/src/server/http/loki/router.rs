use crate::server::http::loki::handlers::{query_attribute_keys, query_attribute_values};
use axum::routing::get;
use axum::Router;

pub fn new() -> Router {
    Router::new()
        .route("/api/v1/labels", get(query_attribute_keys))
        .route("/api/v1/label/{value}/values", get(query_attribute_values))
}
