use crate::config::ReplicaConfig;
use crate::raft::Raft;
use crate::server::grpc::opentelemetry::collector::logs::v1::logs_service_client::LogsServiceClient;
use crate::server::grpc::opentelemetry::collector::logs::v1::logs_service_server::LogsService;
use crate::server::grpc::opentelemetry::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use crate::server::grpc::opentelemetry::common::v1::any_value::Value;
use crate::server::grpc::opentelemetry::common::v1::AnyValue;
use crate::server::grpc::raft::raft_proto;
use crate::{raft, server, util};
use async_raft::async_trait::async_trait;
use async_raft::{ClientWriteError, NodeId};
use dashmap::DashMap;
use log::{error, info};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tonic::transport::{Channel, Uri};
use tonic::{Request, Response, Status};

#[allow(clippy::all)]
pub mod collector {
    #[allow(clippy::all)]
    pub mod logs {
        pub mod v1 {
            tonic::include_proto!("opentelemetry.proto.collector.logs.v1");
        }
    }
}

#[allow(clippy::all)]
pub mod common {
    #[allow(clippy::all)]
    pub mod v1 {
        tonic::include_proto!("opentelemetry.proto.common.v1");
    }
}

#[allow(clippy::all)]
pub mod resource {
    #[allow(clippy::all)]
    pub mod v1 {
        tonic::include_proto!("opentelemetry.proto.resource.v1");
    }
}

#[allow(clippy::all)]
pub mod logs {
    #[allow(clippy::all)]
    pub mod v1 {
        tonic::include_proto!("opentelemetry.proto.logs.v1");
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogRecord {
    pub id: u64,
    pub level: String,
    pub message: String,
    pub attributes: HashMap<String, String>,
    pub timestamp: i64,
    pub day: i32,
}

impl From<(u64, &logs::v1::LogRecord)> for LogRecord {
    fn from(value: (u64, &logs::v1::LogRecord)) -> Self {
        let log = value.1;
        let timestamp = log.time_unix_nano as i64;
        let mut attributes = HashMap::new();
        attributes.insert("level".to_string(), log.severity_text.to_uppercase());
        for kv in &log.attributes {
            let value = kv.value.clone().map(convert_any_value).unwrap_or_default();
            attributes.insert(kv.key.clone(), value);
        }

        Self {
            id: value.0,
            level: log.severity_text.to_uppercase(),
            message: log.body.clone().map(convert_any_value).unwrap_or_default(),
            attributes,
            timestamp,
            day: (timestamp / 86_400_000_000_000) as i32,
        }
    }
}

impl From<raft_proto::LogRecord> for LogRecord {
    fn from(record: raft_proto::LogRecord) -> Self {
        Self {
            id: record.id,
            level: record.level,
            message: record.message,
            attributes: record.attributes,
            timestamp: record.timestamp,
            day: record.day,
        }
    }
}

pub struct LogService {
    raft: Raft,
    clients: DashMap<NodeId, (String, Option<LogsServiceClient<Channel>>)>,
}

impl LogService {
    pub fn new(replicas: &[ReplicaConfig], raft: Raft) -> Self {
        let clients = DashMap::new();
        for replica in replicas {
            clients.insert(replica.node_id, (replica.otel_address.clone(), None));
        }
        Self { clients, raft }
    }

    async fn forward_to_leader(
        &self,
        request: Request<ExportLogsServiceRequest>,
        node_id: NodeId,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let (address, client) = self
            .clients
            .get(&node_id)
            .map(|client| client.clone())
            .ok_or(Status::not_found("Could not find leader node"))?;

        if let Some(mut c) = client {
            return c.export(request).await;
        }

        info!(
            "Attempting to connect to leader node {} at {} for export",
            node_id, address
        );
        let mut new_client = self
            .connect_replica(&address)
            .await
            .map_err(|e| Status::internal(format!("Could not connect to leader: {}", e)))?;

        self.clients
            .insert(node_id, (address, Some(new_client.clone())));
        new_client.export(request).await
    }

    async fn connect_replica(
        &self,
        address: &str,
    ) -> Result<LogsServiceClient<Channel>, server::Error> {
        let uri = Uri::try_from(address).map_err(server::Error::InvalidUri)?;
        let endpoint = tonic::transport::Endpoint::from(uri.clone());
        match endpoint.connect().await {
            Ok(c) => Ok(LogsServiceClient::new(c)),
            Err(e) => Err(server::Error::Grpc(e)),
        }
    }
}

#[async_trait]
impl LogsService for LogService {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let node_id = self.raft.node_id();
        let leader_id = self.raft.leader_id().await;
        if node_id != leader_id {
            return self.forward_to_leader(request, leader_id).await;
        }

        let mut logs = Vec::new();
        for rl in &request.get_ref().resource_logs {
            for sl in &rl.scope_logs {
                for log in &sl.log_records {
                    let id = util::generate_id(node_id);
                    logs.push(LogRecord::from((id, log)));
                }
            }
        }

        if let Err(e) = self.raft.write(raft::Request::new(logs)).await {
            match e {
                ClientWriteError::RaftError(e) => {
                    error!("Unable to send replication request: {}", e)
                }
                ClientWriteError::ForwardToLeader(_, id) => {
                    if let Some(id) = id {
                        return self.forward_to_leader(request, id).await;
                    } else {
                        error!(
                            "Request needs to be sent to leader, but leader couldn't be determined"
                        );
                    }
                }
            }
        }

        Ok(Response::new(ExportLogsServiceResponse {
            partial_success: None,
        }))
    }
}

fn convert_any_value(value: AnyValue) -> String {
    value.value.map(convert_value).unwrap_or_default()
}

fn convert_value(value: Value) -> String {
    match value {
        Value::StringValue(s) => s,
        Value::BoolValue(b) => b.to_string(),
        Value::IntValue(i) => i.to_string(),
        Value::DoubleValue(d) => d.to_string(),
        Value::ArrayValue(_) => "".to_string(),
        Value::KvlistValue(_) => "".to_string(),
        Value::BytesValue(b) => String::from_utf8(b).unwrap_or_default(),
    }
}
