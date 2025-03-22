use crate::config::ReplicaConfig;
use crate::raft;
use crate::raft::Request;
use crate::server::grpc::raft::raft_proto;
use crate::server::grpc::raft::raft_proto::raft_service_client::RaftServiceClient;
use crate::server::grpc::raft::raft_proto::{
    EntryConfigChangePayload, EntryNormalPayload, EntrySnapshotPointerPayload, LogRecord,
};
use anyhow::anyhow;
use async_raft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, ConflictOpt, Entry, EntryPayload,
    InstallSnapshotRequest, InstallSnapshotResponse, MembershipConfig, VoteRequest, VoteResponse,
};
use async_raft::{NodeId, RaftNetwork};
use dashmap::DashMap;
use log::{info, warn};
use std::collections::HashSet;
use std::time::Duration;
use tokio::time::{interval, Instant};
use tonic::transport::{Channel, Uri};
use tonic::{async_trait, transport};

pub struct Server {
    id: NodeId,
    replicas: DashMap<NodeId, RaftServiceClient<Channel>>,
}

impl Server {
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            replicas: DashMap::new(),
        }
    }
    
    pub fn node_id(&self) -> NodeId {
        self.id
    }

    pub async fn connect_replicas(
        &self,
        connect_timeout: Duration,
        replicas: &[ReplicaConfig],
    ) -> Result<(), raft::ReplicaError> {
        for replica in replicas {
            let client = connect_replica(&replica.address, connect_timeout).await?;
            info!("Connected to replica {}", replica.address);
            self.replicas.insert(replica.node_id, client);
        }
        Ok(())
    }
    
    fn get_client(&self, node_id: NodeId) -> Result<RaftServiceClient<Channel>, anyhow::Error> {
        self.replicas
            .get(&node_id)
            .map(|v| v.value().clone())
            .ok_or(anyhow!("Unknown node ID {}", node_id))
    }
}

#[async_trait]
impl RaftNetwork<Request> for Server {
    async fn append_entries(
        &self,
        target: NodeId,
        rpc: AppendEntriesRequest<Request>,
    ) -> anyhow::Result<AppendEntriesResponse> {
        let mut client = self.get_client(target)?;
        let entries = map_entries(rpc.entries)?;
        let response = client
            .append_entries(raft_proto::AppendEntriesRequest {
                term: rpc.term,
                leader_id: rpc.leader_id,
                prev_log_index: rpc.prev_log_index,
                prev_log_term: rpc.prev_log_term,
                entries,
                leader_commit: rpc.leader_commit,
            })
            .await?
            .into_inner();

        Ok(AppendEntriesResponse {
            term: response.term,
            success: response.success,
            conflict_opt: response.conflict_opt.map(|opt| ConflictOpt {
                term: opt.term,
                index: opt.index,
            }),
        })
    }

    async fn install_snapshot(
        &self,
        target: NodeId,
        rpc: InstallSnapshotRequest,
    ) -> anyhow::Result<InstallSnapshotResponse> {
        let mut client = self.get_client(target)?;
        let response = client
            .install_snapshot(raft_proto::InstallSnapshotRequest {
                term: rpc.term,
                leader_id: rpc.leader_id,
                last_included_index: rpc.last_included_index,
                last_included_term: rpc.last_included_term,
                offset: rpc.offset,
                data: rpc.data,
                done: rpc.done,
            })
            .await?;

        Ok(InstallSnapshotResponse {
            term: response.into_inner().term,
        })
    }

    async fn vote(&self, target: NodeId, rpc: VoteRequest) -> anyhow::Result<VoteResponse> {
        let mut client = self.get_client(target)?;
        let response = client
            .vote(raft_proto::VoteRequest {
                term: rpc.term,
                candidate_id: rpc.candidate_id,
                last_log_index: rpc.last_log_index,
                last_log_term: rpc.last_log_term,
            })
            .await?
            .into_inner();

        Ok(VoteResponse {
            term: response.term,
            vote_granted: response.vote_granted,
        })
    }
}

async fn connect_replica(
    address: &str,
    connect_timeout: Duration,
) -> Result<RaftServiceClient<Channel>, raft::ReplicaError> {
    let uri = Uri::try_from(address)?;
    let endpoint = transport::Endpoint::from(uri.clone());
    if let Ok(channel) = endpoint.connect().await {
        return Ok(RaftServiceClient::new(channel));
    }

    let start_time = Instant::now();
    let mut ticker = interval(Duration::from_secs(1));
    loop {
        ticker.tick().await;
        match endpoint.connect().await {
            Ok(channel) => return Ok(RaftServiceClient::new(channel)),
            Err(_) => {
                if start_time.elapsed() >= connect_timeout {
                    return Err(raft::ReplicaError::Timeout(connect_timeout.as_secs()));
                }
            }
        }
        warn!("Failed to connect to replica {}. Trying again...", uri);
    }
}

fn map_entries(entries: Vec<Entry<Request>>) -> Result<Vec<raft_proto::Entry>, anyhow::Error> {
    let mut mapped = Vec::with_capacity(entries.len());
    for entry in entries {
        let payload = match entry.payload {
            EntryPayload::Blank => raft_proto::entry::Payload::Blank(raft_proto::BlankPayload {}),
            EntryPayload::Normal(e) => {
                let payload = map_payload(e.data);
                raft_proto::entry::Payload::Normal(payload)
            }
            EntryPayload::ConfigChange(c) => {
                let config = map_config_change(c.membership);
                raft_proto::entry::Payload::ConfigChange(EntryConfigChangePayload {
                    membership: Some(config),
                })
            }
            EntryPayload::SnapshotPointer(s) => {
                let membership = map_config_change(s.membership);
                raft_proto::entry::Payload::SnapshotPointer(EntrySnapshotPointerPayload {
                    id: s.id,
                    membership: Some(membership),
                })
            }
        };

        mapped.push(raft_proto::Entry {
            index: entry.index,
            term: entry.term,
            payload: Some(payload.clone()),
        })
    }

    Ok(mapped)
}

fn map_payload(payload: Request) -> EntryNormalPayload {
    let log = payload.log;
    let record = LogRecord {
        level: log.level,
        message: log.message,
        attributes: log.attributes,
        timestamp: log.timestamp,
        day: log.day,
    };
    EntryNormalPayload {
        id: payload.id,
        data: Some(record),
    }
}

fn map_config_change(config: MembershipConfig) -> raft_proto::MembershipConfig {
    let members = convert_node_id_hashset(config.members);
    let members_after_consensus =
        convert_node_id_hashset(config.members_after_consensus.unwrap_or_default());
    raft_proto::MembershipConfig {
        members,
        members_after_consensus,
    }
}

fn convert_node_id_hashset(set: HashSet<NodeId>) -> Vec<u64> {
    let mut vec = Vec::with_capacity(set.len());
    for id in set {
        vec.push(id);
    }
    vec
}
