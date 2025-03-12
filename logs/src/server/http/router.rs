use crate::server::http::handlers::{query_attribute_values, query_attributes, query_fql};
use crate::server::http::sql::query_sql;
use axum::routing::get;
use axum::Router;

pub fn new() -> Router {
    Router::new()
        .route("/query/fql", get(query_fql))
        .route("/query/sql", get(query_sql))
        .route("/query/attributes", get(query_attributes))
        .route(
            "/query/attributes/{value}/values",
            get(query_attribute_values),
        )
}
