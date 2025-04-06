use async_raft::{NodeId, SnapshotPolicy};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::path::PathBuf;

pub const DEFAULT_CONFIG_PATH: &str = "/etc/ferrum/config.yaml";
const DEFAULT_REPLICATION_DATA_DIR: &str = "/var/lib/ferrum/replication";
const DEFAULT_COMPACTION_FREQUENCY: u64 = 60;

#[derive(Default, Serialize, Deserialize)]
pub struct Config {
    #[serde(
        rename = "compaction_frequency",
        default = "default_compaction_frequency"
    )]
    pub compaction_frequency_seconds: u64,

    #[serde(default)]
    pub filesystem: object_store::config::Config,

    #[serde(default)]
    pub server: ServerConfig,

    #[serde(default)]
    pub replication: ReplicationConfig,
}

#[derive(Default, Serialize, Deserialize)]
pub struct ServerConfig {
    pub grpc: GrpcConfig,
    pub http: HttpConfig,
}

#[derive(Serialize, Deserialize)]
pub struct GrpcConfig {
    pub port: u32,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self { port: 4317 }
    }
}

#[derive(Serialize, Deserialize)]
pub struct HttpConfig {
    pub port: u32,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self { port: 8080 }
    }
}

#[derive(Serialize, Deserialize)]
pub struct ReplicationConfig {
    pub node_id: NodeId,
    pub advertise_port: u32,
    pub connect_timeout: u64,
    pub log: ReplicationLogConfig,

    #[serde(flatten)]
    pub builder_config: async_raft::ConfigBuilder,

    #[serde(default)]
    pub replicas: Vec<ReplicaConfig>,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            advertise_port: 9234,
            connect_timeout: 60,
            log: ReplicationLogConfig::default(),
            builder_config: async_raft::config::Config::build("ferrum".to_string()),
            replicas: Vec::new(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct ReplicationLogConfig {
    pub data_dir: PathBuf,
    max_entries: u64,
}

impl Default for ReplicationLogConfig {
    fn default() -> Self {
        Self {
            data_dir: DEFAULT_REPLICATION_DATA_DIR.into(),
            max_entries: 5000,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReplicaConfig {
    pub node_id: NodeId,
    pub otel_address: String,
    pub raft_address: String,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to read config file: {0}")]
    IO(std::io::Error),

    #[error("failed to parse config file: {0}")]
    Parse(serde_yml::Error),
}

pub fn load(filename: &str) -> Result<Config, Error> {
    let file = File::open(filename).map_err(Error::IO)?;
    let mut c: Config = serde_yml::from_reader(file).map_err(Error::Parse)?;

    let max_entries = c.replication.log.max_entries;
    c.replication.builder_config.snapshot_policy = Some(SnapshotPolicy::LogsSinceLast(max_entries));
    Ok(c)
}

fn default_compaction_frequency() -> u64 {
    DEFAULT_COMPACTION_FREQUENCY
}
