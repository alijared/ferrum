pub mod config;

mod error;

pub use error::Error;
pub use object_store::path::Path;

use crate::config::Config;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dashmap::DashSet;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use log::{error, info};
use object_store::aws::AmazonS3Builder;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult,
};
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;
use url::Url;

#[derive(Debug)]
pub struct Filesystem {
    kind: ObjectStoreKind,
    url: Url,
    object_store: Arc<dyn ObjectStore>,
    marked_for_deletion: DashSet<Path>,
}

impl Filesystem {
    pub async fn new(config: Config) -> Result<Self, Error> {
        let (kind, store, url): (ObjectStoreKind, Arc<dyn ObjectStore>, Url) =
            match config.object_store {
                config::ObjectStore::Local(c) => {
                    let path = c.data_dir.join("tables");
                    tokio::fs::create_dir_all(&path).await?;

                    let path = tokio::fs::canonicalize(std::path::Path::new(&path)).await?;
                    let url = Url::from_directory_path(&path).map_err(|_| {
                        object_store::Error::InvalidPath {
                            source: object_store::path::Error::InvalidPath { path: path.clone() },
                        }
                    })?;

                    let store = object_store::local::LocalFileSystem::new_with_prefix(&path)?;
                    (ObjectStoreKind::Local, Arc::new(store), url)
                }
                config::ObjectStore::S3(c) => {
                    let bucket = c.bucket_name.as_str();
                    let mut store = AmazonS3Builder::from_env().with_bucket_name(bucket);
                    if let Some(endpoint) = c.override_endpoint {
                        store = store
                            .with_endpoint(endpoint)
                            .with_access_key_id(c.access_key)
                            .with_secret_access_key(c.secret_key)
                            .with_allow_http(true);
                    }

                    let store = store.build()?;
                    let url = Url::parse(format!("s3://{}/tables", bucket).as_str()).unwrap();
                    (ObjectStoreKind::S3, Arc::new(store), url)
                }
            };

        Ok(Self {
            kind,
            url,
            object_store: Arc::new(store),
            marked_for_deletion: DashSet::new(),
        })
    }

    pub async fn compact(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        partition_day: DateTime<Utc>,
        df_write_opts: DataFrameWriteOptions,
    ) -> Result<(), Error> {
        let day = partition_day.format("%Y-%m-%d").to_string();
        let base_prefix = format!("/{}/day={}", table_name, day);
        let base_prefix = base_prefix.as_str();
        let prefix = match self.kind {
            ObjectStoreKind::Local => Path::from(base_prefix),
            ObjectStoreKind::S3 => Path::from(format!("/tables{}", base_prefix)),
        };

        let parts: Vec<(Path, String)> = self
            .list(Some(&prefix))
            .map_ok(|m| (m.location.clone(), format!("/{}", m.location)))
            .try_collect()
            .await?;

        let delete_stream =
            futures::stream::iter(parts.clone().into_iter().map(|(l, _)| Ok(l))).boxed();

        let count = parts.len();
        if count < 2 {
            return Ok(());
        }

        info!("Compacting {} files for {} table", table_name, count);
        let df = ctx
            .read_parquet(
                parts
                    .clone()
                    .into_iter()
                    .map(|(p, l)| match self.kind {
                        ObjectStoreKind::Local => l,
                        ObjectStoreKind::S3 => {
                            format!("{}{}/{}", self.url, base_prefix, p.filename().unwrap())
                        }
                    })
                    .collect::<Vec<_>>(),
                ParquetReadOptions::default(),
            )
            .await?;

        df.write_table(table_name, df_write_opts).await?;
        for (l, _) in parts {
            self.marked_for_deletion.insert(l);
        }

        let mut stream = self.delete_stream(delete_stream);
        while let Some(result) = stream.next().await {
            if let Err(e) = result {
                error!("Error deleting file for {} table: {}", table_name, e);
            }
        }
        self.marked_for_deletion.clear();

        Ok(())
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    pub fn table_url(&self, table_name: &str) -> String {
        match self.kind {
            ObjectStoreKind::Local => {
                format!("/{}/", table_name)
            }
            ObjectStoreKind::S3 => {
                format!("{}/{}/", self.url, table_name)
            }
        }
    }
}

#[async_trait]
impl ObjectStore for Filesystem {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        self.object_store.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        self.object_store.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        self.object_store.get_opts(location, options).await
    }

    async fn delete(&self, location: &Path) -> Result<(), object_store::Error> {
        self.object_store.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'_, Result<ObjectMeta, object_store::Error>> {
        self.object_store.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        let mut res = self.object_store.list_with_delimiter(prefix).await?;
        res.objects
            .retain(|m| !self.marked_for_deletion.contains(&m.location));

        Ok(res)
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<(), object_store::Error> {
        self.object_store.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<(), object_store::Error> {
        self.object_store.copy_if_not_exists(from, to).await
    }
}

impl Display for Filesystem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.object_store, f)
    }
}

#[derive(Debug)]
pub enum ObjectStoreKind {
    Local,
    S3,
}
