use crate::io;
use crate::server::ServerError;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use datafusion::arrow::error::ArrowError;
use datafusion::common::DataFusionError;
use log::{error, info};
use serde_json::json;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

mod loki;
mod query;
mod router;
mod schemas;

pub async fn run_server(
    port: u32,
    cancellation_token: CancellationToken,
) -> Result<(), ServerError> {
    let listener = TcpListener::bind(format!("0.0.0.0:{}", port))
        .await
        .map_err(ServerError::Http)?;
    info!("API server listening on {}", listener.local_addr().unwrap());

    let app = router::new();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            cancellation_token.cancelled().await;
            info!("Shutting down API server")
        })
        .await
        .map_err(ServerError::Http)
}

pub struct ApiError {
    status_code: StatusCode,
    message: String,
}

impl From<io::query::Error> for ApiError {
    fn from(error: io::query::Error) -> Self {
        match error {
            io::query::Error::Query(s) => ApiError::new(StatusCode::BAD_REQUEST, &s),
            io::query::Error::DataFusion(e) => ApiError::from(e),
            io::query::Error::Arrow(e) => ApiError::from(e),
        }
    }
}

impl From<DataFusionError> for ApiError {
    fn from(error: DataFusionError) -> Self {
        match error {
            DataFusionError::SQL(e, _) => ApiError::new(StatusCode::BAD_REQUEST, &e.to_string()),
            DataFusionError::Plan(message) => ApiError::new(StatusCode::BAD_REQUEST, &message),
            DataFusionError::SchemaError(e, _) => {
                ApiError::new(StatusCode::BAD_REQUEST, &e.to_string())
            }
            _ => ApiError::internal_error(&error.to_string()),
        }
    }
}

impl From<ArrowError> for ApiError {
    fn from(error: ArrowError) -> Self {
        ApiError::internal_error(&error.to_string())
    }
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

    pub fn internal_error(message: &str) -> Self {
        error!("Generated unexpected internal server error: {}", message);
        Self {
            status_code: StatusCode::INTERNAL_SERVER_ERROR,
            message: "".to_string(),
        }
    }
}
