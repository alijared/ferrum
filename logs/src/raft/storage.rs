use crate::raft;
use crate::raft::{Request, Response};
use crate::redb;
use crate::server::grpc::opentelemetry::LogRecord;
use anyhow::anyhow;
use async_raft::raft::{Entry, EntryPayload, MembershipConfig};
use async_raft::storage::{CurrentSnapshotData, HardState, InitialState};
use async_raft::{NodeId, RaftStorage};
use log::{info, warn};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{broadcast, RwLock};
use tonic::async_trait;

const HARD_STATE_PATH: &str = "hard_state.json";
const LOG_PATH: &str = "logs.db";
const LOG_TABLE: redb::Table<u64, &[u8]> = redb::Table::new("logs");
const SNAPSHOT_PATH: &str = "snapshot.json";
const STATE_MACHINE_PATH: &str = "state_machine.json";

pub struct Store {
    id: NodeId,
    base_path: PathBuf,
    bus: broadcast::Sender<Vec<(u64, LogRecord)>>,
    log_db: redb::Database,
    log: RwLock<BTreeMap<u64, Entry<Request>>>,
    state_machine: RwLock<StateMachine>,
    hard_state: RwLock<Option<HardState>>,
    current_snapshot: RwLock<Option<Snapshot>>,
}

impl Store {
    pub async fn new(
        id: NodeId,
        base_path: PathBuf,
        bus: broadcast::Sender<Vec<(u64, LogRecord)>>,
    ) -> Result<Self, raft::Error> {
        tokio::fs::create_dir_all(&base_path).await?;

        let log_db = redb::Database::new(base_path.join(LOG_PATH))?;
        Ok(Self {
            id,
            base_path,
            bus,
            log_db,
            log: RwLock::new(BTreeMap::new()),
            state_machine: RwLock::new(StateMachine::default()),
            hard_state: RwLock::new(None),
            current_snapshot: RwLock::new(None),
        })
    }

    pub async fn initialize(&self) -> Result<(), raft::Error> {
        self.load_log().await?;

        let state_machine = self.load_state_machine().await?;
        *self.state_machine.write().await = state_machine;

        if let Some(state) = self.load_hard_state().await? {
            self.write_hard_state(&state, false).await?;
        }

        if let Some(snapshot) = self.load_snapshot().await? {
            self.write_snapshot(&snapshot, false).await?;
        }

        Ok(())
    }

    async fn load_log(&self) -> Result<(), raft::Error> {
        let mut log = self.log.write().await;
        self.log_db
            .read_all(LOG_TABLE, |index, entry| {
                match serde_json::from_slice(entry) {
                    Ok(entry) => {
                        log.insert(index, entry);
                    }
                    Err(e) => {
                        warn!("Failed to deserialize log entry: {}", e);
                    }
                };
            })
            .await?;
        Ok(())
    }

    async fn write_log(&self, entry: &Entry<Request>) -> Result<(), raft::Error> {
        let bytes = serde_json::to_vec(entry)?;
        self.log_db
            .insert(LOG_TABLE, entry.index, bytes.as_slice())
            .await?;
        Ok(())
    }

    async fn load_state_machine(&self) -> Result<StateMachine, raft::Error> {
        self.load_persisted_state(STATE_MACHINE_PATH).await
    }

    async fn write_state_machine(&self, state: &StateMachine) -> Result<(), io::Error> {
        let vec = serde_json::to_vec(state)?;
        tokio::fs::write(&self.base_path.join(STATE_MACHINE_PATH), &vec).await
    }

    async fn load_hard_state(&self) -> Result<Option<HardState>, raft::Error> {
        self.load_persisted_state(HARD_STATE_PATH).await
    }

    async fn write_hard_state(&self, state: &HardState, persist: bool) -> Result<(), io::Error> {
        *self.hard_state.write().await = Some(state.clone());
        if persist {
            let vec = serde_json::to_vec(state)?;
            tokio::fs::write(&self.base_path.join(HARD_STATE_PATH), &vec).await?;
        }
        Ok(())
    }

    async fn load_snapshot(&self) -> Result<Option<Snapshot>, raft::Error> {
        self.load_persisted_state(SNAPSHOT_PATH).await
    }

    async fn write_snapshot(
        &self,
        snapshot: &Snapshot,
        persist: bool,
    ) -> Result<tokio::fs::File, io::Error> {
        *self.current_snapshot.write().await = Some(snapshot.clone());
        let path = &self.base_path.join(SNAPSHOT_PATH);
        if persist {
            let mut f = tokio::fs::File::create(path).await?;
            let vec = serde_json::to_vec(snapshot)?;

            f.write_all(&vec).await?;
            f.flush().await?;
            return Ok(f);
        }

        tokio::fs::File::open(path).await
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

    async fn load_persisted_state<T>(&self, path: impl AsRef<Path>) -> Result<T, raft::Error>
    where
        T: Default + for<'de> Deserialize<'de>,
    {
        match tokio::fs::read(&self.base_path.join(path)).await {
            Ok(v) => serde_json::from_slice(&v).map_err(From::from),
            Err(e) => match e.kind() {
                io::ErrorKind::NotFound => Ok(T::default()),
                _ => Err(e.into()),
            },
        }
    }
}

#[async_trait]
impl RaftStorage<Request, Response> for Store {
    type Snapshot = tokio::fs::File;
    type ShutdownError = raft::Error;

    async fn get_membership_config(&self) -> anyhow::Result<MembershipConfig> {
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
        self.write_hard_state(state, true).await?;
        Ok(())
    }

    async fn get_log_entries(&self, start: u64, stop: u64) -> anyhow::Result<Vec<Entry<Request>>> {
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
        let mut log = self.log.write().await;
        let last_key =
            stop.unwrap_or_else(|| log.last_entry().map(|le| *le.key()).unwrap_or(start));

        for key in start..=last_key {
            self.log_db.remove(LOG_TABLE, key).await?;
            log.remove(&key);
        }

        Ok(())
    }

    async fn append_entry_to_log(&self, entry: &Entry<Request>) -> anyhow::Result<()> {
        let mut log = self.log.write().await;
        log.insert(entry.index, entry.clone());
        drop(log);

        self.write_log(entry).await?;
        Ok(())
    }

    async fn replicate_to_log(&self, entries: &[Entry<Request>]) -> anyhow::Result<()> {
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
        let logs = request.clone().0;
        self.bus
            .send(logs.clone())
            .map_err(|e| anyhow!("Failed to send log to broadcast channel: {}", e))?;

        let mut state_machine = self.state_machine.write().await;
        state_machine.last_applied_log = *index;

        let ids = logs.into_iter().map(|(id, _)| id).collect();
        Ok(Response(ids))
    }

    async fn replicate_to_state_machine(&self, entries: &[(&u64, &Request)]) -> anyhow::Result<()> {
        let mut logs = Vec::new();
        let mut last_applied_log = 0;
        for (index, request) in entries {
            for (id, log) in &request.0 {
                logs.push((*id, log.clone()));
            }
            last_applied_log = **index;
        }
        
        self.bus
            .send(logs)
            .map_err(|e| anyhow!("Failed to send log to broadcast channel: {}", e))?;

        let mut state_machine = self.state_machine.write().await;
        self.write_state_machine(&state_machine).await?;

        state_machine.last_applied_log = last_applied_log;
        Ok(())
    }

    async fn do_log_compaction(&self) -> anyhow::Result<CurrentSnapshotData<Self::Snapshot>> {
        info!("Compacting WAL/replication log");

        let state_machine = self.state_machine.read().await;
        let data = serde_json::to_vec(&*state_machine)?;
        let last_applied_log = state_machine.last_applied_log;
        drop(state_machine);

        let membership_config = self.membership_config(last_applied_log).await;
        let mut log = self.log.write().await;
        let first_key = log.first_entry().map(|fe| *fe.key()).unwrap_or(0);
        let term = log
            .get(&last_applied_log)
            .map(|entry| entry.term)
            .ok_or(anyhow::anyhow!("a query was received which was expecting data to be in place which does not exist in the log"))?;

        log.insert(
            last_applied_log,
            Entry::new_snapshot_pointer(
                last_applied_log,
                term,
                "".into(),
                membership_config.clone(),
            ),
        );
        drop(log);

        let snapshot = Snapshot {
            index: last_applied_log,
            term,
            membership: membership_config.clone(),
            data,
        };

        self.delete_logs_from(first_key, Some(last_applied_log - 1))
            .await?;
        let snapshot_file = self.write_snapshot(&snapshot, true).await?;

        Ok(CurrentSnapshotData {
            term,
            index: last_applied_log,
            membership: membership_config.clone(),
            snapshot: Box::new(snapshot_file),
        })
    }

    async fn create_snapshot(&self) -> anyhow::Result<(String, Box<Self::Snapshot>)> {
        let f = tokio::fs::File::create(&self.base_path.join(SNAPSHOT_PATH)).await?;
        Ok((String::from(""), Box::new(f)))
    }

    async fn finalize_snapshot_installation(
        &self,
        index: u64,
        term: u64,
        delete_through: Option<u64>,
        id: String,
        mut snapshot: Box<Self::Snapshot>,
    ) -> anyhow::Result<()> {
        let mut buf = Vec::new();
        let _ = snapshot.read(&mut buf).await?;
        let new_snapshot: Snapshot = serde_json::from_slice(&buf)?;
        {
            let mut log = self.log.write().await;
            let first_index = log.first_entry().map(|fe| *fe.key()).unwrap_or(0);
            drop(log);

            let membership_config = self.membership_config(index).await;
            let last_index = delete_through.map(Some).unwrap_or(None);
            self.delete_logs_from(first_index, last_index).await?;

            let mut log = self.log.write().await;
            let entry = Entry::new_snapshot_pointer(index, term, id, membership_config);
            log.insert(index, entry.clone());
            drop(log);

            self.write_log(&entry).await?;
        }

        {
            let new_state_machine: StateMachine = serde_json::from_slice(&new_snapshot.data)?;
            let mut state_machine = self.state_machine.write().await;
            if self.write_state_machine(&new_state_machine).await.is_ok() {
                *state_machine = new_state_machine;
            }
        }

        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = Some(new_snapshot.clone());
        drop(current_snapshot);

        self.write_snapshot(&new_snapshot, true).await?;
        Ok(())
    }

    async fn get_current_snapshot(
        &self,
    ) -> anyhow::Result<Option<CurrentSnapshotData<Self::Snapshot>>> {
        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let f = self.write_snapshot(snapshot, true).await?;
                Ok(Some(CurrentSnapshotData {
                    index: snapshot.index,
                    term: snapshot.term,
                    membership: snapshot.membership.clone(),
                    snapshot: Box::new(f),
                }))
            }
            None => Ok(None),
        }
    }
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct StateMachine {
    last_applied_log: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Snapshot {
    index: u64,
    term: u64,
    membership: MembershipConfig,
    data: Vec<u8>,
}
