use crate::server;
use axum::http::{Request, Response};
use log::info;
use std::convert::Infallible;
use tokio_util::sync::CancellationToken;
use tonic::body::BoxBody;
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
    S: Service<Request<BoxBody>, Response = Response<BoxBody>, Error = Infallible>
        + NamedService
        + Clone
        + Send
        + 'static,
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
