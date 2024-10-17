use std::sync::Arc;

use zarrs::{
    array::{ArrayBuilder, DataType, FillValue},
    array_subset::ArraySubset,
};

#[tokio::test]
async fn icechunk_array() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(icechunk::ObjectStorage::new_in_memory_store(None));
    let icechunk_store = icechunk::Store::new_from_storage(storage).await?;
    let store = Arc::new(zarrs_icechunk::AsyncIcechunkStore::new(icechunk_store));

    let array_path = "/array";
    let mut builder = ArrayBuilder::new(
        vec![4, 4], // array shape
        DataType::UInt8,
        vec![2, 2].try_into().unwrap(), // regular chunk shape
        FillValue::from(0u8),
    );
    builder.bytes_to_bytes_codecs(vec![]);
    // builder.storage_transformers(vec![].into());
    builder.array_to_bytes_codec(Arc::new(
        zarrs::array::codec::array_to_bytes::sharding::ShardingCodecBuilder::new(
            vec![1, 1].try_into().unwrap(),
        )
        .bytes_to_bytes_codecs(vec![Arc::new(zarrs::array::codec::GzipCodec::new(5)?)])
        .build(),
    ));
    let array = builder.build(store.clone(), array_path).unwrap();

    array.async_store_metadata().await?;

    assert_eq!(array.data_type(), &DataType::UInt8);
    assert_eq!(array.fill_value().as_ne_bytes(), &[0u8]);
    assert_eq!(array.shape(), &[4, 4]);
    assert_eq!(
        array.chunk_shape(&[0, 0]).unwrap(),
        [2, 2].try_into().unwrap()
    );
    assert_eq!(array.chunk_grid_shape().unwrap(), &[2, 2]);

    // 1  2 | 0  0
    // 0  0 | 0  0
    // -----|-----
    // 0  0 | 0  0
    // 0  0 | 0  0
    array.async_store_chunk(&[0, 0], &[1, 2, 0, 0]).await?;
    let snapshot0 = store
        .icechunk_store()
        .write()
        .await
        .commit("chunk0")
        .await?;

    // 1  2 | 3  4
    // 5  6 | 7  8
    // -----|-----
    // 9 10 | 0  0
    // 0  0 | 0  0
    array.async_store_chunk(&[0, 1], &[3, 4, 7, 8]).await?;
    array
        .async_store_array_subset(&ArraySubset::new_with_ranges(&[1..3, 0..2]), &[5, 6, 9, 10])
        .await?;
    assert!(array.async_retrieve_chunk(&[0, 0, 0]).await.is_err());
    assert_eq!(
        array.async_retrieve_chunk(&[0, 0]).await?,
        vec![1, 2, 5, 6].into()
    );
    let _snapshot1 = store
        .icechunk_store()
        .write()
        .await
        .commit("chunk0")
        .await?;

    store
        .icechunk_store()
        .write()
        .await
        .checkout(icechunk::zarr::VersionInfo::SnapshotId(snapshot0))
        .await?;
    assert_eq!(
        array.async_retrieve_chunk(&[0, 0]).await?,
        vec![1, 2, 0, 0].into()
    );

    Ok(())
}
