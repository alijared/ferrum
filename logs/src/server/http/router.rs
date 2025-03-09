use crate::server::http::handlers::{query_fql, query_labels, query_sql};
use axum::routing::get;
use axum::Router;

pub fn new() -> Router {
    Router::new()
        .route("/query/fql", get(query_fql))
        .route("/query/sql", get(query_sql))
        .route("/query/labels", get(query_labels))
}
