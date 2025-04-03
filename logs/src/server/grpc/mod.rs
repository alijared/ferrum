use crate::server;
use axum::http::Request;
use log::info;
use std::convert::Infallible;
use tokio_util::sync::CancellationToken;
use tonic::body::Body;
use tonic::codegen::Service;
use tonic::server::NamedService;
use tonic::transport::Server;

pub mod opentelemetry;
pub mod raft;

pub async fn run_server<S>(
    name: &str,
    port: u32,
    service: S,
    cancellation_token: CancellationToken,
) -> Result<(), server::Error>
where
    S: Service<Request<Body>, Error = Infallible> + NamedService + Clone + Send + Sync + 'static,
    S::Response: axum::response::IntoResponse,
    S::Future: Send + 'static,
{
    let addr = format!("0.0.0.0:{}", port)
        .parse()
        .map_err(server::Error::ParseAddr)?;

    info!("{} gRPC server listening on {}", name, addr);
    Server::builder()
        .add_service(service)
        .serve_with_shutdown(addr, async move {
            cancellation_token.cancelled().await;
            info!("Shutting down {} gRPC server", name)
        })
        .await
        .map_err(server::Error::Grpc)
}
