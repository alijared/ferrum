pub mod grpc;
pub mod http;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    ParseAddr(std::net::AddrParseError),

    #[error("{0}")]
    Http(std::io::Error),

    #[error("{0}")]
    Grpc(tonic::transport::Error),
}
