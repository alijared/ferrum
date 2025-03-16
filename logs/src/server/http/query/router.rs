use crate::server::http::query::handlers::{
    query_attribute_keys, query_attribute_values, query_fql, query_sql,
};
use axum::routing::get;
use axum::Router;

pub fn new() -> Router {
    Router::new()
        .route("/fql", get(query_fql))
        .route("/sql", get(query_sql))
        .route("/attributes", get(query_attribute_keys))
        .route("/attributes/{value}/values", get(query_attribute_values))
}
