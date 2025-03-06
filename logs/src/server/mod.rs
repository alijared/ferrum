pub mod grpc;
pub mod http;
pub mod opentelemetry;

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("{0}")]
    ParseAddr(std::net::AddrParseError),

    #[error("{0}")]
    Http(std::io::Error),

    #[error("{0}")]
    Grpc(tonic::transport::Error),
}
