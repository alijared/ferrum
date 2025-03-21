use async_raft::NodeId;
use serde::Deserialize;
use std::fs::File;
use std::path::PathBuf;

pub const DEFAULT_CONFIG_PATH: &str = "/etc/ferrum-logs/config.yaml";
const DEFAULT_DATA_DIR: &str = "/var/lib/ferrum-logs/data";
const DEFAULT_REPLICATION_DATA_DIR: &str = "/var/lib/ferrum-logs/replication";

#[derive(Debug, Deserialize)]
pub struct Config {
    pub data_dir: PathBuf,

    #[serde(rename = "log_table")]
    pub log_table_config: TableConfig,

    pub server: ServerConfig,

    #[serde(default)]
    pub replication: ReplicationConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: DEFAULT_DATA_DIR.into(),
            log_table_config: TableConfig::default(),
            server: ServerConfig::default(),
            replication: ReplicationConfig::default(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct TableConfig {
    #[serde(rename = "compaction_frequency")]
    pub compaction_frequency_seconds: u64,
}

impl Default for TableConfig {
    fn default() -> Self {
        Self {
            compaction_frequency_seconds: 60,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct ServerConfig {
    pub grpc: GrpcConfig,
    pub http: HttpConfig,
}

#[derive(Debug, Deserialize)]
pub struct GrpcConfig {
    pub port: u32,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self { port: 4317 }
    }
}

#[derive(Debug, Deserialize)]
pub struct HttpConfig {
    pub port: u32,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self { port: 8080 }
    }
}

#[derive(Debug, Deserialize)]
pub struct ReplicationConfig {
    pub node_id: NodeId,
    pub replication_log: PathBuf,
    pub advertise_port: u32,
    pub connect_timeout: u64,

    #[serde(flatten)]
    pub builder_config: async_raft::ConfigBuilder,

    #[serde(default)]
    pub replicas: Vec<ReplicaConfig>,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            node_id: 0,
            replication_log: DEFAULT_REPLICATION_DATA_DIR.into(),
            advertise_port: 9234,
            connect_timeout: 60,
            builder_config: async_raft::config::Config::build("cluster".to_string()),
            replicas: vec![],
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ReplicaConfig {
    pub node_id: NodeId,
    pub address: String,
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
    serde_yml::from_reader(file).map_err(Error::Parse)
}
