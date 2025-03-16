use crate::server::http::{loki, query};
use axum::Router;

pub fn new() -> Router {
    Router::new()
        .nest("/query", query::router::new())
        .nest("/loki", loki::router::new())
}
