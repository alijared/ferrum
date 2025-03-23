use crate::raft;
use crate::raft::{Request, Response};
use crate::server::grpc::opentelemetry::LogRecord;
use anyhow::anyhow;
use async_raft::raft::{Entry, EntryPayload, MembershipConfig};
use async_raft::storage::{CurrentSnapshotData, HardState, InitialState};
use async_raft::{NodeId, RaftStorage};
use log::warn;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::io;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{broadcast, RwLock};
use tonic::async_trait;

const HARD_STATE_PATH: &str = "hard_state.json";
const LOG_PATH: &str = "logs";
const SNAPSHOT_PATH: &str = "snapshot.json";
const STATE_MACHINE_PATH: &str = "state_machine.json";

pub struct Store {
    id: NodeId,
    base_path: PathBuf,
    bus: broadcast::Sender<Vec<(u64, LogRecord)>>,
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
        let logs_path = &base_path.join(LOG_PATH);
        tokio::fs::create_dir_all(logs_path).await?;

        Ok(Self {
            id,
            base_path,
            bus,
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
        let log_dir = self.base_path.join(LOG_PATH);
        let mut entries = tokio::fs::read_dir(&log_dir).await?;

        let mut log = self.log.write().await;
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_file() {
                continue;
            }

            match tokio::fs::read(entry.path()).await {
                Ok(v) => match serde_json::from_slice::<Entry<Request>>(&v) {
                    Ok(log_entry) => {
                        log.insert(log_entry.index, log_entry);
                    }
                    Err(e) => {
                        warn!("Error parsing log file: {}", e);
                    }
                },
                Err(e) => {
                    warn!("Error reading log file: {}", e);
                }
            }
        }

        Ok(())
    }

    async fn write_log(&self, entry: &Entry<Request>) -> Result<(), io::Error> {
        let vec = serde_json::to_vec(entry)?;
        let path = self
            .base_path
            .join(LOG_PATH)
            .join(format!("{}.json", entry.index));
        tokio::fs::write(path, &vec).await
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
        println!("Deleting logs from {} to {:?}", start, stop);
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
        let mut state_machine = self.state_machine.write().await;
        state_machine.last_applied_log = *index;

        if let Some((id, log)) = state_machine.log_request.get(&request.id) {
            self.write_state_machine(&state_machine).await?;
            return Ok(Response(Some((*id, log.clone()))));
        }

        let log = request.log.clone();
        let previous = state_machine
            .log_request
            .insert(request.id, (request.id, log.clone()));

        if let Err(e) = self.write_state_machine(&state_machine).await {
            state_machine.log_request.remove(&request.id);
            return Err(e.into());
        }

        self.bus
            .send(vec![(request.id, log)])
            .map_err(|e| anyhow!("Failed to send log to broadcast channel: {}", e))?;
        Ok(Response(previous))
    }

    async fn replicate_to_state_machine(&self, entries: &[(&u64, &Request)]) -> anyhow::Result<()> {
        let mut logs = Vec::new();
        let mut state_machine = self.state_machine.write().await;
        let original_last_applied_log = state_machine.last_applied_log;
        for (index, request) in entries {
            logs.push((request.id, request.log.clone()));
            state_machine.last_applied_log = **index;
            state_machine
                .log_request
                .insert(request.id, (request.id, request.log.clone()));
        }

        if let Err(e) = self.write_state_machine(&state_machine).await {
            state_machine.last_applied_log = original_last_applied_log;
            for (_, request) in entries {
                state_machine.log_request.remove(&request.id);
            }
            return Err(e.into());
        }

        self.bus
            .send(logs)
            .map_err(|e| anyhow!("Failed to send log to broadcast channel: {}", e))?;
        Ok(())
    }

    async fn do_log_compaction(&self) -> anyhow::Result<CurrentSnapshotData<Self::Snapshot>> {
        println!("Doing log compaction");
        let (data, last_applied_log);
        {
            let state_machine = self.state_machine.read().await;
            data = serde_json::to_vec(&*state_machine)?;
            last_applied_log = state_machine.last_applied_log;
        } // Release state machine read lock.

        let membership_config = self.membership_config(last_applied_log).await;
        let snapshot_file: tokio::fs::File;
        let term;
        {
            let mut log = self.log.write().await;
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
            snapshot_file = self.write_snapshot(&snapshot, true).await?
        } // Release log & snapshot write locks.

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
            let membership_config = self.membership_config(index).await;
            match &delete_through {
                Some(through) => {
                    *log = log.split_off(&(through + 1));
                }
                None => log.clear(),
            }

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
