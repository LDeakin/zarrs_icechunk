//! [`icechunk`] store support for the [`zarrs`](https://docs.rs/zarrs/latest/zarrs/index.html) crate.
//!
//! ```
//! // TODO
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

use futures::{future, StreamExt, TryStreamExt};
pub use icechunk;

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
    icechunk_store: icechunk::Store,
}

impl AsyncIcechunkStore {
    /// Create a new [`AsyncIcechunkStore`].
    #[must_use]
    pub fn new(icechunk_store: icechunk::Store) -> Self {
        Self { icechunk_store }
    }

    pub fn icechunk_store(&mut self) -> &mut icechunk::Store {
        &mut self.icechunk_store
    }
}

#[async_trait::async_trait]
impl AsyncReadableStorageTraits for AsyncIcechunkStore {
    async fn get(&self, key: &StoreKey) -> Result<MaybeAsyncBytes, StorageError> {
        let bytes = handle_result_notfound(
            self.icechunk_store
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
                        // FIXME: No zarr codecs actually make a request like this, and most stores would not support it anyway
                        // This should be changed in zarrs_storage at some point
                        Err(StorageError::Other(
                            "Byte ranges from the end with an offset are not supported".to_string(),
                        ))
                    }
                }?;
                Ok((key, byte_range))
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        let result = handle_result(self.icechunk_store.get_partial_values(byte_ranges).await)?;
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
        handle_result(self.icechunk_store.set(key.as_str(), value).await)?;
        Ok(())
    }

    async fn set_partial_values(
        &self,
        _key_start_values: &[StoreKeyStartValue],
    ) -> Result<(), StorageError> {
        if self
            .icechunk_store
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
        if self.icechunk_store.supports_deletes().map_err(handle_err)? {
            handle_result_notfound(self.icechunk_store.delete(key.as_str()).await)?;
            Ok(())
        } else {
            Err(StorageError::Unsupported(
                "the store does not support deletion".to_string(),
            ))
        }
    }

    async fn erase_prefix(&self, prefix: &StorePrefix) -> Result<(), StorageError> {
        if self.icechunk_store.supports_deletes().map_err(handle_err)? {
            let keys = self
                .icechunk_store
                .list_prefix(prefix.as_str())
                .await
                .map_err(handle_err)?
                .try_collect::<Vec<_>>() // TODO: do not collect, use try_for_each
                .await
                .map_err(handle_err)?;
            for key in keys {
                self.icechunk_store.delete(&key).await.map_err(handle_err)?;
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
        let keys = self.icechunk_store.list().await.map_err(handle_err)?;
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
    // TODO: add tests
}
