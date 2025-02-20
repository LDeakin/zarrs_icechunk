# zarrs_icechunk

[![Latest Version](https://img.shields.io/crates/v/zarrs_icechunk.svg)](https://crates.io/crates/zarrs_icechunk)
[![icechunk 0.2.0](https://img.shields.io/badge/icechunk-0.2.0+-blue)](https://crates.io/crates/icechunk)
[![zarrs_icechunk documentation](https://docs.rs/zarrs_icechunk/badge.svg)](https://docs.rs/zarrs_icechunk)
![msrv](https://img.shields.io/crates/msrv/zarrs_icechunk)
[![build](https://github.com/LDeakin/zarrs_icechunk/actions/workflows/ci.yml/badge.svg)](https://github.com/LDeakin/zarrs_icechunk/actions/workflows/ci.yml)

[`icechunk`](https://crates.io/crates/icechunk) store support for the [`zarrs`](https://crates.io/crates/zarrs) Rust crate.

```rust
use icechunk::{Repository, RepositoryConfig, repository::VersionInfo};
use zarrs_icechunk::AsyncIcechunkStore;

// Create an icechunk repo
let storage = icechunk::new_in_memory_storage()?;
let config = RepositoryConfig::default();
let repo = Repository::create(Some(config), storage, HashMap::new()).await?;

// Do some array/metadata manipulation with zarrs, then commit a snapshot
let session = repo.writable_session("main").await?;
let store = Arc::new(AsyncIcechunkStore::new(session));
let array: Array<AsyncIcechunkStore> = ...;
let snapshot0 = store.session().write().await.commit("Initial commit", None).await?;

// Do some more array/metadata manipulation, then commit another snapshot
let session = repo.writable_session("main").await?;
let store = Arc::new(AsyncIcechunkStore::new(session));
let array: Array<AsyncIcechunkStore> = ...;
let snapshot1 = store.session().write().await.commit("Update data", None).await?;

// Checkout the first snapshot
let session = repo.readonly_session(&VersionInfo::SnapshotId(snapshot0)).await?;
let store = Arc::new(AsyncIcechunkStore::new(session));
let array: Array<AsyncIcechunkStore> = ...;
```

## Version Compatibility Matrix
See [doc/version_compatibility_matrix.md](./doc/version_compatibility_matrix.md).

## Licence
`zarrs_icechunk` is licensed under either of
 - the Apache License, Version 2.0 [LICENSE-APACHE](./LICENCE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0> or
 - the MIT license [LICENSE-MIT](./LICENCE-MIT) or <http://opensource.org/licenses/MIT>, at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
