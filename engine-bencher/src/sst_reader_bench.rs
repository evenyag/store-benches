//! Parquet SST reader bench.

use std::sync::Arc;
use std::time::Instant;

use common_time::Timestamp;
use mito2::read::BatchReader;
use mito2::sst::file::{FileHandle, FileId, FileMeta};
use mito2::sst::file_purger::{FilePurger, PurgeRequest};
use mito2::sst::parquet::reader::ParquetReaderBuilder;
use object_store::services::Fs;
use object_store::ObjectStore;
use store_api::storage::RegionId;

use crate::target::ScanMetrics;

/// Parquet reader bencher.
pub struct ParquetReaderBench {
    /// Directory of the file.
    file_dir: String,
    /// Id of the file.
    file_id: FileId,
    object_store: ObjectStore,
}

impl ParquetReaderBench {
    /// Creates a new ParquetReaderBench.
    pub fn new(file_dir: &str, file_id: FileId) -> ParquetReaderBench {
        let mut builder = Fs::default();
        builder.root("/");
        let object_store = ObjectStore::new(builder).unwrap().finish();

        ParquetReaderBench {
            file_dir: file_dir.to_string(),
            file_id,
            object_store,
        }
    }

    /// Iter one bench.
    pub async fn bench(&self) -> ScanMetrics {
        let start = Instant::now();

        let mut reader = ParquetReaderBuilder::new(
            self.file_dir.clone(),
            self.new_file_handle(),
            self.object_store.clone(),
        )
        .build()
        .await
        .unwrap();

        let mut num_rows = 0;
        while let Some(batch) = reader.next_batch().await.unwrap() {
            num_rows += batch.num_rows();
        }

        ScanMetrics {
            total_cost: start.elapsed(),
            num_rows,
        }
    }

    fn new_file_handle(&self) -> FileHandle {
        let file_meta = FileMeta {
            region_id: RegionId::new(1, 1),
            file_id: self.file_id,
            time_range: (
                Timestamp::new_millisecond(1000),
                Timestamp::new_millisecond(2000),
            ),
            level: 0,
            file_size: 0,
        };

        FileHandle::new(file_meta, Arc::new(NoopFilePurger))
    }
}

/// Mock file purger.
#[derive(Debug)]
struct NoopFilePurger;

impl FilePurger for NoopFilePurger {
    fn send_request(&self, _request: PurgeRequest) {}
}
