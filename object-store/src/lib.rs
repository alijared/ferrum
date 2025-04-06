pub mod config;

pub use object_store::path::Path;

use crate::config::Config;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dashmap::DashSet;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::DataFusionError;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use log::{error, info};
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult,
};
use std::fmt::{Debug, Display, Formatter};
use std::io;
use std::sync::Arc;
use url::Url;

#[derive(Debug)]
pub struct Filesystem {
    object_store: Arc<dyn ObjectStore>,
    marked_for_deletion: DashSet<Path>,
}

impl Filesystem {
    pub async fn new(config: Config) -> Result<(Self, Url), Error> {
        let (store, url) = match config.object_store {
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
                (store, url)
            }
            config::ObjectStore::S3(c) => {
                unreachable!()
            }
        };

        Ok((
            Self {
                object_store: Arc::new(store),
                marked_for_deletion: DashSet::new(),
            },
            url,
        ))
    }

    pub async fn compact(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        partition_day: DateTime<Utc>,
        df_write_opts: DataFrameWriteOptions,
    ) -> Result<(), Error> {
        let day = partition_day.format("%Y-%m-%d").to_string();
        let prefix = Path::from(format!("/{}/day={}", table_name, day));
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
                    .map(|(_, l)| l)
                    .collect::<Vec<_>>(),
                ParquetReadOptions::default(),
            )
            .await?;

        // let table_ref = ctx.table(table_name).await?;
        // let table_schema = table_ref.schema();
        // let cast_df = df.into_view().with_schema(table_schema).await?;
        //
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

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Io(io::Error),

    #[error("{0}")]
    ObjectStore(object_store::Error),

    #[error("{0}")]
    DataFusion(DataFusionError),
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<object_store::Error> for Error {
    fn from(error: object_store::Error) -> Self {
        Self::ObjectStore(error)
    }
}

impl From<DataFusionError> for Error {
    fn from(error: DataFusionError) -> Self {
        Self::DataFusion(error)
    }
}
