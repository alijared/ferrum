use crate::raft;
use crate::raft::Raft;
use crate::server::grpc::opentelemetry::LogRecord;
use crate::server::grpc::raft::raft_proto::entry::Payload;
use crate::server::grpc::raft::raft_proto::raft_service_server::RaftService;
use crate::server::grpc::raft::raft_proto::{
    AppendEntriesRequest, AppendEntriesResponse, ConflictOpt, Entry, InstallSnapshotRequest,
    InstallSnapshotResponse, MembershipConfig, VoteRequest, VoteResponse,
};
use anyhow::anyhow;
use async_raft::raft::{EntryConfigChange, EntryNormal, EntryPayload, EntrySnapshotPointer};
use std::collections::HashSet;
use tonic::{async_trait, Request, Response, Status};

pub mod raft_proto {
    tonic::include_proto!("raft");
}

pub struct Service {
    raft: Raft,
}

impl Service {
    pub fn new(raft: Raft) -> Self {
        Self { raft }
    }
}

#[async_trait]
impl RaftService for Service {
    async fn append_entries(
        &self,
        rpc: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let request = rpc.into_inner();
        let entries = map_entries(request.entries)
            .map_err(|e| Status::internal(format!("Error mapping entries: {}", e)))?;

        let raft_request = async_raft::raft::AppendEntriesRequest {
            term: request.term,
            leader_id: request.leader_id,
            prev_log_index: request.prev_log_index,
            prev_log_term: request.prev_log_term,
            entries,
            leader_commit: request.leader_commit,
        };

        match self.raft.append_entries(raft_request).await {
            Ok(r) => Ok(Response::new(AppendEntriesResponse {
                term: r.term,
                success: r.success,
                conflict_opt: r.conflict_opt.map(|opt| ConflictOpt {
                    term: opt.term,
                    index: opt.index,
                }),
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn install_snapshot(
        &self,
        rpc: Request<InstallSnapshotRequest>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        let request = rpc.into_inner();
        let raft_request = async_raft::raft::InstallSnapshotRequest {
            term: request.term,
            leader_id: request.leader_id,
            last_included_index: request.last_included_index,
            last_included_term: request.last_included_term,
            offset: request.offset,
            data: request.data,
            done: request.done,
        };

        match self.raft.install_snapshot(raft_request).await {
            Ok(r) => Ok(Response::new(InstallSnapshotResponse { term: r.term })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn vote(&self, rpc: Request<VoteRequest>) -> Result<Response<VoteResponse>, Status> {
        let request = rpc.into_inner();
        let raft_request = async_raft::raft::VoteRequest::new(
            request.term,
            request.candidate_id,
            request.last_log_index,
            request.last_log_term,
        );

        match self.raft.vote(raft_request).await {
            Ok(r) => Ok(Response::new(VoteResponse {
                term: r.term,
                vote_granted: r.vote_granted,
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }
}

fn map_entries(
    entries: Vec<Entry>,
) -> Result<Vec<async_raft::raft::Entry<raft::Request>>, anyhow::Error> {
    let mut mapped = Vec::with_capacity(entries.len());
    for entry in entries {
        let payload = entry.payload.ok_or(anyhow!("Empty payload"))?;
        let payload = map_entry_payload(payload)?;
        mapped.push(async_raft::raft::Entry {
            term: entry.term,
            index: entry.index,
            payload,
        });
    }
    Ok(mapped)
}

fn map_entry_payload(payload: Payload) -> Result<EntryPayload<raft::Request>, anyhow::Error> {
    match payload {
        Payload::Blank(_) => Ok(EntryPayload::Blank),
        Payload::Normal(payload) => {
            let log = payload.data.ok_or(anyhow!("Empty payload"))?;
            Ok(EntryPayload::Normal(EntryNormal {
                data: raft::Request::new(
                    payload.id,
                    LogRecord {
                        level: log.level,
                        message: log.message,
                        attributes: log.attributes,
                        timestamp: log.timestamp,
                        day: log.day,
                    },
                ),
            }))
        }
        Payload::ConfigChange(c) => {
            let membership = c
                .membership
                .ok_or(anyhow!("Empty membership config"))?;

            let membership = map_membership_config(membership)?;
            Ok(EntryPayload::ConfigChange(EntryConfigChange { membership }))
        }
        Payload::SnapshotPointer(s) => {
            let membership = s
                .membership
                .ok_or(anyhow!("Empty membership config"))?;
            let membership = map_membership_config(membership)?;
            Ok(EntryPayload::SnapshotPointer(EntrySnapshotPointer {
                id: s.id,
                membership,
            }))
        }
    }
}

fn map_membership_config(
    config: MembershipConfig,
) -> Result<async_raft::raft::MembershipConfig, anyhow::Error> {
    let mut members = HashSet::with_capacity(config.members.len());
    for member in config.members {
        members.insert(member);
    }

    let members_after_consensus = if !config.members_after_consensus.is_empty() {
        let mut set = HashSet::with_capacity(config.members_after_consensus.len());
        for member in config.members_after_consensus {
            set.insert(member);
        }
        Some(set)
    } else {
        None
    };

    Ok(async_raft::raft::MembershipConfig {
        members,
        members_after_consensus,
    })
}
