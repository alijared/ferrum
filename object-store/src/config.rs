use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Default, Serialize, Deserialize)]
pub struct Config {
    #[serde(default, flatten)]
    pub object_store: ObjectStore,
}

#[derive(Serialize, Deserialize)]
pub enum ObjectStore {
    #[serde(rename = "local")]
    Local(LocalFilesystemConfig),

    #[serde(alias = "s3")]
    S3(S3Config),
}

const DEFAULT_DATA_DIR: &str = "/var/lib/ferrum/data";

impl Default for ObjectStore {
    fn default() -> Self {
        Self::Local(LocalFilesystemConfig {
            data_dir: DEFAULT_DATA_DIR.into(),
        })
    }
}

#[derive(Serialize, Deserialize)]
pub struct LocalFilesystemConfig {
    pub data_dir: PathBuf,
}

#[derive(Serialize, Deserialize)]
pub struct S3Config {
    pub bucket_name: String,
    pub override_endpoint: Option<String>,
}
