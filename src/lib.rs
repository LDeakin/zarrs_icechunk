//! [`icechunk`] store support for the [`zarrs`](https://docs.rs/zarrs/latest/zarrs/index.html) crate.
//!
//! ```
//! # use std::sync::Arc;
//! # use zarrs_storage::{AsyncWritableStorageTraits, StoreKey};
//! # tokio_test::block_on(async {
//! // Create an icechunk store
//! let storage = Arc::new(icechunk::ObjectStorage::new_in_memory_store(None));
//! let icechunk_store = icechunk::Store::new_from_storage(storage).await?;
//! let store = zarrs_icechunk::AsyncIcechunkStore::new(icechunk_store);
//!
//! // Do some array/metadata manipulation with zarrs, then commit a snapshot
//! # let root_json = StoreKey::new("zarr.json").unwrap();
//! # store.set(&root_json, r#"{"zarr_format":3,"node_type":"group"}"#.into()).await?;
//! let snapshot0 = store.commit("Initial commit").await?;
//!
//! // Do some more array/metadata manipulation, then commit another snapshot
//! # store.set(&root_json, r#"{"zarr_format":3,"node_type":"group","attributes":{"a":"b"}}"#.into()).await?;
//! let snapshot1 = store.commit("Update data").await?;
//!
//! // Checkout the first snapshot
//! store.checkout(icechunk::zarr::VersionInfo::SnapshotId(snapshot0)).await?;
//! # Ok::<_, Box<dyn std::error::Error>>(())
//! # }).unwrap();
//! ```
//!
//! ## Version Compatibility Matrix
//!
#![doc = include_str!("../doc/version_compatibility_matrix.md")]
//!
//! ## Licence
//! `zarrs_icechunk` is licensed under either of
//! - the Apache License, Version 2.0 [LICENSE-APACHE](https://docs.rs/crate/zarrs_icechunk/latest/source/LICENCE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0> or
//! - the MIT license [LICENSE-MIT](https://docs.rs/crate/zarrs_icechunk/latest/source/LICENCE-MIT) or <http://opensource.org/licenses/MIT>, at your option.

use std::sync::Arc;

use futures::{future, StreamExt, TryStreamExt};
pub use icechunk;

use icechunk::{format::SnapshotId, refs::BranchVersion, zarr::VersionInfo};
use tokio::sync::RwLock;
use zarrs_storage::{
    byte_range::ByteRange, AsyncBytes, AsyncListableStorageTraits, AsyncReadableStorageTraits,
    AsyncReadableWritableStorageTraits, AsyncWritableStorageTraits, MaybeAsyncBytes, StorageError,
    StoreKey, StoreKeyStartValue, StoreKeys, StoreKeysPrefixes, StorePrefix,
};

fn handle_err(err: icechunk::zarr::StoreError) -> StorageError {
    StorageError::Other(err.to_string())
}

/// Map [`icechunk::zarr::StoreError::NotFound`] to None, pass through other errors
fn handle_result_notfound<T>(
    result: Result<T, icechunk::zarr::StoreError>,
) -> Result<Option<T>, StorageError> {
    match result {
        Ok(result) => Ok(Some(result)),
        Err(err) => {
            if matches!(err, icechunk::zarr::StoreError::NotFound { .. }) {
                Ok(None)
            } else {
                Err(StorageError::Other(err.to_string()))
            }
        }
    }
}

fn handle_result<T>(result: Result<T, icechunk::zarr::StoreError>) -> Result<T, StorageError> {
    result.map_err(handle_err)
}

/// An asynchronous store backed by an [`icechunk::Store`].
pub struct AsyncIcechunkStore {
    icechunk_store: Arc<RwLock<icechunk::Store>>,
}

impl From<Arc<RwLock<icechunk::Store>>> for AsyncIcechunkStore {
    fn from(icechunk_store: Arc<RwLock<icechunk::Store>>) -> Self {
        Self { icechunk_store }
    }
}

impl AsyncIcechunkStore {
    /// Create a new [`AsyncIcechunkStore`].
    #[must_use]
    pub fn new(icechunk_store: icechunk::Store) -> Self {
        Self {
            icechunk_store: Arc::new(RwLock::new(icechunk_store)),
        }
    }

    /// Return the inner `icechunk::Store`.
    #[must_use]
    pub fn icechunk_store(&self) -> Arc<RwLock<icechunk::Store>> {
        self.icechunk_store.clone()
    }

    pub async fn current_branch(&self) -> Option<String> {
        self.icechunk_store.read().await.current_branch().clone()
    }

    pub async fn snapshot_id(&self) -> SnapshotId {
        self.icechunk_store.read().await.snapshot_id().await
    }

    pub async fn current_version(&self) -> VersionInfo {
        self.icechunk_store.read().await.current_version().await
    }

    pub async fn has_uncommitted_changes(&self) -> bool {
        self.icechunk_store
            .read()
            .await
            .has_uncommitted_changes()
            .await
    }

    pub async fn reset(&self) -> icechunk::zarr::StoreResult<()> {
        self.icechunk_store.write().await.reset().await
    }

    pub async fn checkout(
        &self,
        version: icechunk::zarr::VersionInfo,
    ) -> icechunk::zarr::StoreResult<()> {
        self.icechunk_store.write().await.checkout(version).await
    }

    pub async fn new_branch(
        &self,
        branch: &str,
    ) -> icechunk::zarr::StoreResult<(SnapshotId, BranchVersion)> {
        self.icechunk_store.write().await.new_branch(branch).await
    }

    pub async fn commit(&self, message: &str) -> icechunk::zarr::StoreResult<SnapshotId> {
        self.icechunk_store.write().await.commit(message).await
    }

    pub async fn tag(
        &self,
        tag: &str,
        snapshot_id: &SnapshotId,
    ) -> icechunk::zarr::StoreResult<()> {
        self.icechunk_store
            .write()
            .await
            .tag(tag, snapshot_id)
            .await
    }
}

#[async_trait::async_trait]
impl AsyncReadableStorageTraits for AsyncIcechunkStore {
    async fn get(&self, key: &StoreKey) -> Result<MaybeAsyncBytes, StorageError> {
        let bytes = handle_result_notfound(
            self.icechunk_store
                .read()
                .await
                .get(key.as_str(), &icechunk::format::ByteRange::ALL)
                .await,
        )?;
        if let Some(bytes) = bytes {
            Ok(Some(bytes))
        } else {
            Ok(None)
        }
    }

    async fn get_partial_values_key(
        &self,
        key: &StoreKey,
        byte_ranges: &[ByteRange],
    ) -> Result<Option<Vec<AsyncBytes>>, StorageError> {
        let byte_ranges: Vec<_> = byte_ranges
            .iter()
            .map(|byte_range| {
                let key = key.to_string();
                let byte_range = match byte_range {
                    ByteRange::FromStart(offset, None) => {
                        Ok(icechunk::format::ByteRange::from_offset(*offset))
                    }
                    ByteRange::FromStart(offset, Some(length)) => Ok(
                        icechunk::format::ByteRange::from_offset_with_length(*offset, *length),
                    ),
                    ByteRange::FromEnd(0, Some(length)) => {
                        Ok(icechunk::format::ByteRange::Last(*length))
                    }
                    ByteRange::FromEnd(_offset, _length) => {
                        // TODO: No zarr codecs actually make a request like this, and most stores would not support it anyway
                        // This should be changed in zarrs_storage at some point
                        Err(StorageError::Other(
                            "Byte ranges from the end with an offset are not supported".to_string(),
                        ))
                    }
                }?;
                Ok((key, byte_range))
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        let result = handle_result(
            self.icechunk_store
                .read()
                .await
                .get_partial_values(byte_ranges)
                .await,
        )?;
        result.into_iter().map(handle_result_notfound).collect()
    }

    async fn size_key(&self, _key: &StoreKey) -> Result<Option<u64>, StorageError> {
        // FIXME: upstream icechunk::Store lacks a method to retrieve the size of a key
        Err(StorageError::Unsupported(
            "the store does not support querying the size of a key".to_string(),
        ))
    }
}

#[async_trait::async_trait]
impl AsyncWritableStorageTraits for AsyncIcechunkStore {
    async fn set(&self, key: &StoreKey, value: AsyncBytes) -> Result<(), StorageError> {
        handle_result(
            self.icechunk_store
                .read()
                .await
                .set(key.as_str(), value)
                .await,
        )?;
        Ok(())
    }

    async fn set_partial_values(
        &self,
        _key_start_values: &[StoreKeyStartValue],
    ) -> Result<(), StorageError> {
        if self
            .icechunk_store
            .read()
            .await
            .supports_partial_writes()
            .map_err(handle_err)?
        {
            // FIXME: Upstream: icechunk::Store does not support partial writes
            Err(StorageError::Unsupported(
                "the store does not support partial writes".to_string(),
            ))
        } else {
            Err(StorageError::Unsupported(
                "the store does not support partial writes".to_string(),
            ))
        }
    }

    async fn erase(&self, key: &StoreKey) -> Result<(), StorageError> {
        if self
            .icechunk_store
            .read()
            .await
            .supports_deletes()
            .map_err(handle_err)?
        {
            handle_result_notfound(self.icechunk_store.read().await.delete(key.as_str()).await)?;
            Ok(())
        } else {
            Err(StorageError::Unsupported(
                "the store does not support deletion".to_string(),
            ))
        }
    }

    async fn erase_prefix(&self, prefix: &StorePrefix) -> Result<(), StorageError> {
        if self
            .icechunk_store
            .read()
            .await
            .supports_deletes()
            .map_err(handle_err)?
        {
            let keys = self
                .icechunk_store
                .read()
                .await
                .list_prefix(prefix.as_str())
                .await
                .map_err(handle_err)?
                .try_collect::<Vec<_>>() // TODO: do not collect, use try_for_each
                .await
                .map_err(handle_err)?;
            for key in keys {
                self.icechunk_store
                    .read()
                    .await
                    .delete(&key)
                    .await
                    .map_err(handle_err)?;
            }
            Ok(())
        } else {
            Err(StorageError::Unsupported(
                "the store does not support deletion".to_string(),
            ))
        }
    }
}

#[async_trait::async_trait]
impl AsyncReadableWritableStorageTraits for AsyncIcechunkStore {}

#[async_trait::async_trait]
impl AsyncListableStorageTraits for AsyncIcechunkStore {
    async fn list(&self) -> Result<StoreKeys, StorageError> {
        let keys = self
            .icechunk_store
            .read()
            .await
            .list()
            .await
            .map_err(handle_err)?;
        keys.map(|key| match key {
            Ok(key) => Ok(StoreKey::new(&key)?),
            Err(err) => Err(StorageError::Other(err.to_string())),
        })
        .try_collect::<Vec<_>>()
        .await
    }

    async fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError> {
        let keys = self
            .icechunk_store
            .read()
            .await
            .list_prefix(prefix.as_str())
            .await
            .map_err(handle_err)?;
        keys.map(|key| match key {
            Ok(key) => Ok(StoreKey::new(&key)?),
            Err(err) => Err(StorageError::Other(err.to_string())),
        })
        .try_collect::<Vec<_>>()
        .await
    }

    async fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError> {
        let keys_prefixes = self
            .icechunk_store
            .read()
            .await
            .list_dir_items(prefix.as_str())
            .await
            .map_err(handle_err)?;
        let mut keys = vec![];
        let mut prefixes = vec![];
        keys_prefixes
            .map_err(handle_err)
            .map(|item| {
                match item? {
                    icechunk::zarr::ListDirItem::Key(key) => {
                        keys.push(StoreKey::new(&key)?);
                    }
                    icechunk::zarr::ListDirItem::Prefix(prefix) => {
                        prefixes.push(StorePrefix::new(&prefix)?);
                    }
                }
                Ok::<_, StorageError>(())
            })
            .try_for_each(|_| future::ready(Ok(())))
            .await?;

        Ok(StoreKeysPrefixes::new(keys, prefixes))
    }

    async fn size_prefix(&self, _prefix: &StorePrefix) -> Result<u64, StorageError> {
        // TODO: This can be supported by list -> sum
        Err(StorageError::Unsupported(
            "the store does not support querying the size of a prefix".to_string(),
        ))
    }

    async fn size(&self) -> Result<u64, StorageError> {
        // TODO: This can be supported by list -> sum
        Err(StorageError::Unsupported(
            "the store does not support querying the total size".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{error::Error, sync::Arc};

    fn remove_whitespace(s: &str) -> String {
        s.chars().filter(|c| !c.is_whitespace()).collect()
    }

    // NOTE: The icechunk store is not a run-of-the-mill Zarr store that knows nothing about Zarr.
    // It adds additional requirements on keys/data (like looking for known zarr metadata, c prefix, etc.)
    // Thus it does not support the current zarrs async store test suite.
    // The test suite could be changed to only create a structure that is actually zarr specific (standard keys, actually valid group/array json, c/ prefix etc)
    #[tokio::test]
    #[ignore]
    async fn icechunk() -> Result<(), Box<dyn Error>> {
        let storage = Arc::new(icechunk::ObjectStorage::new_in_memory_store(None));
        let icechunk_store = icechunk::Store::new_from_storage(storage).await?;
        let store = AsyncIcechunkStore::new(icechunk_store);

        zarrs_storage::store_test::async_store_write(&store).await?;
        zarrs_storage::store_test::async_store_read(&store).await?;
        zarrs_storage::store_test::async_store_list(&store).await?;

        Ok(())
    }

    #[tokio::test]
    async fn icechunk_time_travel() -> Result<(), Box<dyn Error>> {
        let storage = Arc::new(icechunk::ObjectStorage::new_in_memory_store(None));
        let icechunk_store = icechunk::Store::new_from_storage(storage).await?;
        let store = AsyncIcechunkStore::new(icechunk_store);

        let json = r#"{
            "zarr_format": 3,
            "node_type": "group"
        }"#;
        let json: String = remove_whitespace(json);

        let json_updated = r#"{
            "zarr_format": 3,
            "node_type": "group",
            "attributes": {
                "icechunk": "x zarrs"
            }
        }"#;
        let json_updated: String = remove_whitespace(json_updated);

        let root_json = StoreKey::new("zarr.json").unwrap();

        assert_eq!(store.get(&root_json).await?, None);
        store.set(&root_json, json.clone().into()).await?;
        assert_eq!(store.get(&root_json).await?, Some(json.clone().into()));
        let snapshot0 = store.commit("intial commit").await?;
        store.set(&root_json, json_updated.clone().into()).await?;
        let _snapshot1 = store.commit("write attributes").await?;
        assert_eq!(store.get(&root_json).await?, Some(json_updated.into()));
        let _snapshot1 = store
            .checkout(icechunk::zarr::VersionInfo::SnapshotId(snapshot0))
            .await?;
        assert_eq!(store.get(&root_json).await?, Some(json.clone().into()));

        Ok(())
    }
}
