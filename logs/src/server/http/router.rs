use crate::server::http::query;
use axum::Router;

pub fn new() -> Router {
    Router::new().nest("/query", query::router::new())
}
