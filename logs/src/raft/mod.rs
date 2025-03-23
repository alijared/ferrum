use crate::config::{ReplicaConfig, ReplicationConfig};
use crate::server::grpc::opentelemetry::LogRecord;
use async_raft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, ClientWriteRequest, ClientWriteResponse,
    InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse,
};
use async_raft::{AppData, AppDataResponse, ClientWriteError, NodeId, RaftError};
use log::error;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tonic::codegen::http::uri::InvalidUri;

mod network;
mod storage;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Config(async_raft::ConfigError),

    #[error("{0}")]
    Initialize(async_raft::InitializeError),

    #[error("{0}")]
    Replica(ReplicaError),

    #[error("{0}")]
    Io(io::Error),

    #[error("{0}")]
    Serde(serde_json::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum ReplicaError {
    #[error("{0}")]
    InvalidUri(InvalidUri),

    #[error("connecting to replica timed out after {0} seconds")]
    Timeout(u64),
}

impl From<InvalidUri> for ReplicaError {
    fn from(err: InvalidUri) -> Self {
        ReplicaError::InvalidUri(err)
    }
}

impl From<async_raft::ConfigError> for Error {
    fn from(error: async_raft::ConfigError) -> Self {
        Error::Config(error)
    }
}

impl From<async_raft::InitializeError> for Error {
    fn from(error: async_raft::InitializeError) -> Self {
        Error::Initialize(error)
    }
}

impl From<ReplicaError> for Error {
    fn from(error: ReplicaError) -> Self {
        Error::Replica(error)
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Error::Io(error)
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Error::Serde(error)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Request {
    id: u64,
    log: LogRecord,
}

impl Request {
    pub fn new(id: u64, log: LogRecord) -> Self {
        Self { id, log }
    }
}

impl AppData for Request {}

#[derive(Clone, Serialize, Deserialize)]
pub struct Response(Option<(u64, LogRecord)>);

impl AppDataResponse for Response {}

#[derive(Clone)]
pub struct Raft {
    network: Arc<network::Server>,
    inner: async_raft::Raft<Request, Response, network::Server, storage::Store>,
}

impl Raft {
    pub async fn new(
        config: ReplicationConfig,
        write_bus: broadcast::Sender<Vec<(u64, LogRecord)>>,
    ) -> Result<Self, Error> {
        let validated_config = config.builder_config.validate()?;

        let network = Arc::new(network::Server::new(config.node_id));
        let storage =
            storage::Store::new(config.node_id, config.replication_log, write_bus).await?;
        storage.initialize().await?;

        let raft = async_raft::Raft::new(
            config.node_id,
            Arc::new(validated_config),
            network.clone(),
            Arc::new(storage),
        );

        let mut members = HashSet::new();
        members.insert(config.node_id);
        for replica in config.replicas {
            members.insert(replica.node_id);
        }

        raft.initialize(members).await?;
        Ok(Self {
            network,
            inner: raft,
        })
    }

    pub fn node_id(&self) -> NodeId {
        self.network.node_id()
    }

    pub async fn leader_id(&self) -> NodeId {
        match self.inner.current_leader().await {
            Some(id) => id,
            None => self.node_id(),
        }
    }

    pub async fn connect_replicas(
        &self,
        timeout: Duration,
        replicas: &[ReplicaConfig],
    ) -> Result<(), ReplicaError> {
        self.network.connect_replicas(timeout, replicas).await
    }

    pub async fn write(
        &self,
        request: Request,
    ) -> Result<ClientWriteResponse<Response>, ClientWriteError<Request>> {
        self.inner
            .client_write(ClientWriteRequest::new(request))
            .await
    }

    pub async fn append_entries(
        &self,
        request: AppendEntriesRequest<Request>,
    ) -> Result<AppendEntriesResponse, RaftError> {
        self.inner.append_entries(request).await
    }

    pub async fn install_snapshot(
        &self,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        self.inner.install_snapshot(request).await
    }

    pub async fn vote(&self, request: VoteRequest) -> Result<VoteResponse, RaftError> {
        self.inner.vote(request).await
    }
}
