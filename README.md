# zarrs_icechunk

[![Latest Version](https://img.shields.io/crates/v/zarrs_icechunk.svg)](https://crates.io/crates/zarrs_icechunk)
[![icechunk 0.11](https://img.shields.io/badge/icechunk-0.1.0-blue)](https://crates.io/crates/icechunk)
[![zarrs_icechunk documentation](https://docs.rs/zarrs_icechunk/badge.svg)](https://docs.rs/zarrs_icechunk)
![msrv](https://img.shields.io/crates/msrv/zarrs_icechunk)
[![build](https://github.com/LDeakin/zarrs_icechunk/actions/workflows/ci.yml/badge.svg)](https://github.com/LDeakin/zarrs_icechunk/actions/workflows/ci.yml)

[`icechunk`](https://crates.io/crates/icechunk) store support for the [`zarrs`](https://crates.io/crates/zarrs) Rust crate.

```rust
let storage = Arc::new(icechunk::ObjectStorage::new_in_memory_store(None));
let icechunk_store = icechunk::Store::new_from_storage(storage).await?;
let mut store = zarrs_icechunk::AsyncIcechunkStore::new(icechunk_store);

// do some array/metadata manipulation with zarrs, then store a snapshot
let snapshot0 = store.icechunk_store_mut().commit("Initial commit").await?;

// do some more array/metadata manipulation, then store another snapshot
let snapshot1 = store.icechunk_store_mut().commit("Update data").await?;

// checkout the first snapshot
store
    .icechunk_store_mut()
    .checkout(icechunk::zarr::VersionInfo::SnapshotId(snapshot0))
    .await?;
```

## Version Compatibility Matrix
See [doc/version_compatibility_matrix.md](./doc/version_compatibility_matrix.md).

## Licence
`zarrs_icechunk` is licensed under either of
 - the Apache License, Version 2.0 [LICENSE-APACHE](./LICENCE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0> or
 - the MIT license [LICENSE-MIT](./LICENCE-MIT) or <http://opensource.org/licenses/MIT>, at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
