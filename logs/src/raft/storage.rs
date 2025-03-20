use crate::raft;
use crate::raft::{Request, Response};
use crate::server::grpc::opentelemetry::LogRecord;
use async_raft::raft::{Entry, EntryPayload, MembershipConfig};
use async_raft::storage::{CurrentSnapshotData, HardState, InitialState};
use async_raft::{NodeId, RaftStorage};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::io::Cursor;
use std::path::PathBuf;
use tokio::sync::RwLock;
use tonic::async_trait;

pub struct Store {
    id: NodeId,
    base_path: PathBuf,
    log: RwLock<BTreeMap<u64, Entry<Request>>>,
    state_machine: RwLock<StateMachine>,
    hard_state: RwLock<Option<HardState>>,
    current_snapshot: RwLock<Option<Snapshot>>,
}

impl Store {
    pub async fn new(id: NodeId, base_path: PathBuf) -> Result<Self, raft::Error> {
        Ok(Self {
            id,
            base_path,
            log: RwLock::new(BTreeMap::new()),
            state_machine: RwLock::new(StateMachine::default()),
            hard_state: RwLock::new(None),
            current_snapshot: RwLock::new(None),
        })
    }

    async fn membership_config(&self, index: u64) -> MembershipConfig {
        let log = self.log.read().await;
        log.values()
            .rev()
            .skip_while(|entry| entry.index > index)
            .find_map(|entry| match &entry.payload {
                EntryPayload::ConfigChange(c) => Some(c.membership.clone()),
                _ => None,
            })
            .unwrap_or_else(|| MembershipConfig::new_initial(self.id))
    }
}

#[async_trait]
impl RaftStorage<Request, Response> for Store {
    type Snapshot = Cursor<Vec<u8>>;
    type ShutdownError = raft::Error;

    async fn get_membership_config(&self) -> anyhow::Result<MembershipConfig> {
        println!("get membership config");
        let log = self.log.read().await;
        let config = log.values().rev().find_map(|entry| match &entry.payload {
            EntryPayload::ConfigChange(c) => Some(c.membership.clone()),
            EntryPayload::SnapshotPointer(snapshot) => Some(snapshot.membership.clone()),
            _ => None,
        });

        match config {
            Some(cfg) => Ok(cfg),
            None => Ok(MembershipConfig::new_initial(self.id)),
        }
    }

    async fn get_initial_state(&self) -> anyhow::Result<InitialState> {
        println!("get initial state");

        let mut state = self.hard_state.write().await;
        match &mut *state {
            Some(inner) => {
                let log = self.log.read().await;
                let (last_log_index, last_log_term) = match log.values().next_back() {
                    Some(log) => (log.index, log.term),
                    None => (0, 0),
                };
                let last_applied_log = self.state_machine.read().await.last_applied_log;
                let membership = self.get_membership_config().await?;
                Ok(InitialState {
                    last_log_index,
                    last_log_term,
                    last_applied_log,
                    hard_state: inner.clone(),
                    membership,
                })
            }
            None => {
                let initial = InitialState::new_initial(self.id);
                *state = Some(initial.hard_state.clone());
                Ok(initial)
            }
        }
    }

    async fn save_hard_state(&self, state: &HardState) -> anyhow::Result<()> {
        println!("save_hard_state");
        *self.hard_state.write().await = Some(state.clone());
        Ok(())
    }

    async fn get_log_entries(&self, start: u64, stop: u64) -> anyhow::Result<Vec<Entry<Request>>> {
        println!("get log entries");
        let mut entries = Vec::new();
        let log = self.log.read().await;
        for index in start..stop {
            if let Some(entry) = log.get(&index) {
                entries.push(entry.clone());
                continue;
            }
            return Err(anyhow::anyhow!("Missing log entry at index {}", index));
        }

        Ok(entries)
    }

    async fn delete_logs_from(&self, start: u64, stop: Option<u64>) -> anyhow::Result<()> {
        println!("delete_logs_from");
        let mut log = self.log.write().await;
        if let Some(stop) = stop {
            for key in start..stop {
                log.remove(&key);
            }
            return Ok(());
        }
        log.split_off(&start);
        Ok(())
    }

    async fn append_entry_to_log(&self, entry: &Entry<Request>) -> anyhow::Result<()> {
        println!("append_entry_to_log");
        let mut log = self.log.write().await;
        log.insert(entry.index, entry.clone());
        Ok(())
    }

    async fn replicate_to_log(&self, entries: &[Entry<Request>]) -> anyhow::Result<()> {
        println!("replicate_to_log");
        for entry in entries {
            self.append_entry_to_log(entry).await?;
        }
        Ok(())
    }

    async fn apply_entry_to_state_machine(
        &self,
        index: &u64,
        request: &Request,
    ) -> anyhow::Result<Response> {
        println!("apply_entry_to_state_machine");
        let mut state_machine = self.state_machine.write().await;
        state_machine.last_applied_log = *index;
        if let Some((id, log)) = state_machine.log_request.get(&request.id) {
            return Ok(Response(Some((*id, log.clone()))));
        }

        let log = request.log.clone();
        let previous = state_machine
            .log_request
            .insert(request.id, (request.id, log));
        Ok(Response(previous))
    }

    async fn replicate_to_state_machine(&self, entries: &[(&u64, &Request)]) -> anyhow::Result<()> {
        println!("replicate_to_state_machine");

        let mut state_machine = self.state_machine.write().await;
        for (index, request) in entries {
            state_machine.last_applied_log = **index;
            state_machine
                .log_request
                .insert(request.id, (request.id, request.log.clone()));
        }
        Ok(())
    }

    async fn do_log_compaction(&self) -> anyhow::Result<CurrentSnapshotData<Self::Snapshot>> {
        println!("do_log_compaction");
        let (data, last_applied_log);
        {
            let state_machine = self.state_machine.read().await;
            data = serde_json::to_vec(&*state_machine)?;
            last_applied_log = state_machine.last_applied_log;
        } // Release state machine read lock.

        let membership_config = self.membership_config(last_applied_log).await;
        let snapshot_bytes: Vec<u8>;
        let term;
        {
            let mut log = self.log.write().await;
            let mut current_snapshot = self.current_snapshot.write().await;
            term = log
                .get(&last_applied_log)
                .map(|entry| entry.term)
                .ok_or(anyhow::anyhow!("a query was received which was expecting data to be in place which does not exist in the log"))?;
            *log = log.split_off(&last_applied_log);
            log.insert(
                last_applied_log,
                Entry::new_snapshot_pointer(
                    last_applied_log,
                    term,
                    "".into(),
                    membership_config.clone(),
                ),
            );

            let snapshot = Snapshot {
                index: last_applied_log,
                term,
                membership: membership_config.clone(),
                data,
            };
            snapshot_bytes = serde_json::to_vec(&snapshot)?;
            *current_snapshot = Some(snapshot);
        } // Release log & snapshot write locks.

        Ok(CurrentSnapshotData {
            term,
            index: last_applied_log,
            membership: membership_config.clone(),
            snapshot: Box::new(Cursor::new(snapshot_bytes)),
        })
    }

    async fn create_snapshot(&self) -> anyhow::Result<(String, Box<Self::Snapshot>)> {
        println!("create_snapshot");
        Ok((String::from(""), Box::new(Cursor::new(Vec::new()))))
    }

    async fn finalize_snapshot_installation(
        &self,
        index: u64,
        term: u64,
        delete_through: Option<u64>,
        id: String,
        snapshot: Box<Self::Snapshot>,
    ) -> anyhow::Result<()> {
        println!("finalize_snapshot_installation");
        let raw = serde_json::to_string_pretty(snapshot.get_ref().as_slice())?;
        println!("JSON snapshot:\n{}", raw);
        let new_snapshot: Snapshot = serde_json::from_slice(snapshot.get_ref().as_slice())?;

        {
            let mut log = self.log.write().await;
            let membership_config = self.membership_config(index).await;
            match &delete_through {
                Some(through) => {
                    *log = log.split_off(&(through + 1));
                }
                None => log.clear(),
            }
            log.insert(
                index,
                Entry::new_snapshot_pointer(index, term, id, membership_config),
            );
        }

        {
            let new_state_machine: StateMachine = serde_json::from_slice(&new_snapshot.data)?;
            let mut state_machine = self.state_machine.write().await;
            *state_machine = new_state_machine;
        }

        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = Some(new_snapshot);
        Ok(())
    }

    async fn get_current_snapshot(
        &self,
    ) -> anyhow::Result<Option<CurrentSnapshotData<Self::Snapshot>>> {
        println!("get_current_snapshot");

        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let reader = serde_json::to_vec(&snapshot)?;
                Ok(Some(CurrentSnapshotData {
                    index: snapshot.index,
                    term: snapshot.term,
                    membership: snapshot.membership.clone(),
                    snapshot: Box::new(Cursor::new(reader)),
                }))
            }
            None => Ok(None),
        }
    }
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct StateMachine {
    pub last_applied_log: u64,
    pub log_request: HashMap<u64, (u64, LogRecord)>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Snapshot {
    pub index: u64,
    pub term: u64,
    pub membership: MembershipConfig,
    pub data: Vec<u8>,
}
