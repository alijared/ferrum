use redb::ReadableTable;
use std::borrow::Borrow;
use std::path::Path;

pub struct Database(redb::Database);
pub type Table<'a, K, V> = redb::TableDefinition<'a, K, V>;

impl Database {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, Error> {
        let db = redb::Database::create(path)?;
        Ok(Self(db))
    }

    pub async fn read_all<K: redb::Key + 'static, V: redb::Value + 'static, F>(
        &self,
        table: redb::TableDefinition<'_, K, V>,
        mut f: F,
    ) -> Result<(), Error>
    where
        F: FnMut(<K as redb::Value>::SelfType<'_>, <V as redb::Value>::SelfType<'_>),
    {
        tokio::task::block_in_place(|| {
            let tx = self.0.begin_read()?;
            let table = match tx.open_table(table) {
                Ok(table) => table,
                Err(e) => {
                    return match e {
                        redb::TableError::TableDoesNotExist(_) => Ok(()),
                        e => Err(e.into()),
                    }
                }
            };

            for (key, value) in table.iter()?.flatten() {
                f(key.value(), value.value());
            }
            Ok(())
        })
    }

    pub async fn insert<'k, 'v, K: redb::Key + 'static, V: redb::Value + 'static>(
        &self,
        table: redb::TableDefinition<'_, K, V>,
        key: impl Borrow<K::SelfType<'k>>,
        value: impl Borrow<V::SelfType<'v>>,
    ) -> Result<(), Error> {
        tokio::task::block_in_place(|| {
            let tx = self.0.begin_write()?;
            {
                let mut table = tx.open_table(table)?;
                table.insert(key, value)?;
            }
            tx.commit()?;

            Ok(())
        })
    }

    pub async fn remove<'k, K: redb::Key + 'static, V: redb::Value + 'static>(
        &self,
        table: redb::TableDefinition<'_, K, V>,
        key: impl Borrow<K::SelfType<'k>>,
    ) -> Result<(), Error> {
        tokio::task::block_in_place(|| {
            let tx = self.0.begin_write()?;
            {
                let mut table = tx.open_table(table)?;
                table.remove(key)?;
            }
            tx.commit()?;

            Ok(())
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Database(redb::DatabaseError),

    #[error("{0}")]
    Transaction(redb::TransactionError),

    #[error("{0}")]
    Table(redb::TableError),

    #[error("{0}")]
    Storage(redb::StorageError),

    #[error("{0}")]
    Commit(redb::CommitError),
}

impl From<redb::DatabaseError> for Error {
    fn from(error: redb::DatabaseError) -> Self {
        Self::Database(error)
    }
}

impl From<redb::TransactionError> for Error {
    fn from(error: redb::TransactionError) -> Self {
        Self::Transaction(error)
    }
}

impl From<redb::TableError> for Error {
    fn from(error: redb::TableError) -> Self {
        Self::Table(error)
    }
}

impl From<redb::StorageError> for Error {
    fn from(error: redb::StorageError) -> Self {
        Self::Storage(error)
    }
}

impl From<redb::CommitError> for Error {
    fn from(error: redb::CommitError) -> Self {
        Self::Commit(error)
    }
}
