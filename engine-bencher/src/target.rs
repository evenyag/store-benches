// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common_base::readable_size::ReadableSize;
use common_telemetry::logging;
use datatypes::arrow::compute::cast;
use datatypes::arrow::datatypes::{DataType, TimeUnit};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::ConcreteDataType;
use datatypes::vectors::Helper;
use log_store::raft_engine::log_store::RaftEngineLogStore;
use log_store::LogConfig;
use object_store::layers::{LoggingLayer, MetricsLayer, TracingLayer};
use object_store::services::Fs;
use object_store::{util, ObjectStore};
use storage::compaction::{CompactionHandler, CompactionSchedulerRef, SimplePicker};
use storage::config::EngineConfig;
use storage::region::RegionImpl;
use storage::scheduler::{LocalScheduler, SchedulerConfig};
use storage::write_batch::WriteBatch;
use storage::EngineImpl;
use store_api::logstore::LogStore;
use store_api::storage::{
    ChunkReader, CloseOptions, ColumnDescriptorBuilder, ColumnFamilyDescriptorBuilder,
    CreateOptions, EngineContext, OpenOptions, ReadContext, Region, RegionDescriptor,
    RegionDescriptorBuilder, RegionId, RowKeyDescriptorBuilder, ScanRequest, Snapshot,
    StorageEngine, WriteContext, WriteRequest,
};

/// Timestamp column name.
pub const TS_COLUMN_NAME: &str = "ts";

/// Returns a new object store based on local file system.
async fn new_fs_object_store(path: &str) -> ObjectStore {
    let data_dir = util::normalize_dir(path);
    fs::create_dir_all(Path::new(&data_dir)).unwrap();
    logging::info!("The file storage directory is: {}", &data_dir);

    let atomic_write_dir = format!("{data_dir}/.tmp/");

    let mut builder = Fs::default();
    builder.root(&data_dir).atomic_write_dir(&atomic_write_dir);

    ObjectStore::new(builder)
        .unwrap()
        .finish()
        .layer(MetricsLayer)
        .layer(
            LoggingLayer::default()
                .with_error_level(Some("debug"))
                .unwrap(),
        )
        .layer(TracingLayer)
}

/// Returns a new log store.
async fn new_log_store(path: &str) -> RaftEngineLogStore {
    // create WAL directory
    fs::create_dir_all(Path::new(path)).unwrap();
    let log_config = LogConfig {
        file_size: ReadableSize::gb(1).0,
        log_file_dir: path.to_string(),
        purge_interval: Duration::from_secs(600),
        purge_threshold: ReadableSize::gb(10).0,
        read_batch_size: 128,
        sync_write: false,
    };

    RaftEngineLogStore::try_new(log_config).await.unwrap()
}

/// Returns a new compaction scheduler.
fn new_compaction_scheduler<S: LogStore>() -> CompactionSchedulerRef<S> {
    let picker = SimplePicker::default();
    let config = SchedulerConfig {
        max_inflight_tasks: 4,
    };
    let handler = CompactionHandler { picker };
    let scheduler = LocalScheduler::new(config, handler);
    Arc::new(scheduler)
}

/// Returns descriptor for metric `cpu`.
// e.g.
// ```
// cpu,hostname=host_0,region=eu-central-1,datacenter=eu-central-1a,rack=6,os=Ubuntu15.10,arch=x86,team=SF,service=19,service_version=1,service_environment=test usage_user=58i,usage_system=2i,usage_idle=24i,usage_nice=61i,usage_iowait=22i,usage_irq=63i,usage_softirq=6i,usage_steal=44i,usage_guest=80i,usage_guest_nice=38i 1451606400000000000
// ```
pub(crate) fn new_cpu_region_descriptor(
    region_name: &str,
    region_id: RegionId,
) -> RegionDescriptor {
    let mut column_id = 1;
    // Note that the timestamp in the input parquet file has Timestamp(Microsecond, None) type.
    let timestamp = ColumnDescriptorBuilder::new(
        column_id,
        TS_COLUMN_NAME,
        ConcreteDataType::timestamp_millisecond_datatype(),
    )
    .is_time_index(true)
    .build()
    .unwrap();
    column_id += 1;

    let tags = [
        "hostname",
        "region",
        "datacenter",
        "rack",
        "os",
        "arch",
        "team",
        "service",
        "service_version",
        "service_environment",
    ];
    let mut row_key_builder = RowKeyDescriptorBuilder::new(timestamp);
    for tag in tags {
        row_key_builder = row_key_builder.push_column(
            ColumnDescriptorBuilder::new(column_id, tag, ConcreteDataType::string_datatype())
                .build()
                .unwrap(),
        );
        column_id += 1;
    }
    let row_key = row_key_builder.build().unwrap();
    let fields = [
        "usage_user",
        "usage_system",
        "usage_idle",
        "usage_nice",
        "usage_iowait",
        "usage_irq",
        "usage_softirq",
        "usage_steal",
        "usage_guest",
        "usage_guest_nice",
    ];
    let mut cf_builder = ColumnFamilyDescriptorBuilder::default();
    for field in fields {
        cf_builder = cf_builder.push_column(
            ColumnDescriptorBuilder::new(column_id, field, ConcreteDataType::float64_datatype())
                .build()
                .unwrap(),
        );
        column_id += 1;
    }
    let cf = cf_builder.build().unwrap();
    RegionDescriptorBuilder::default()
        .id(region_id)
        .name(region_name)
        .row_key(row_key)
        .default_cf(cf)
        .build()
        .unwrap()
}

/// Returns a new engine.
async fn new_engine(path: &str, config: EngineConfig) -> EngineImpl<RaftEngineLogStore> {
    let path = util::normalize_dir(path);
    let data_dir = format!("{path}data");
    let wal_dir = format!("{path}wal");

    let object_store = new_fs_object_store(&data_dir).await;
    let log_store = Arc::new(new_log_store(&wal_dir).await);
    let compaction_scheduler = new_compaction_scheduler();
    EngineImpl::new(config, log_store, object_store, compaction_scheduler).unwrap()
}

/// Returns the region name.
fn region_name_by_id(region_id: RegionId) -> String {
    format!("cpu-{region_id}")
}

/// Creates or opens the region.
///
/// Returns the region and whether the region is newly created.
async fn init_region(
    engine: &EngineImpl<RaftEngineLogStore>,
    region_name: &str,
    region_id: RegionId,
) -> (RegionImpl<RaftEngineLogStore>, bool) {
    let ctx = EngineContext::default();
    let open_opts = OpenOptions {
        parent_dir: "/".to_string(),
        ..Default::default()
    };
    if let Some(region) = engine
        .open_region(&ctx, &region_name, &open_opts)
        .await
        .unwrap()
    {
        logging::info!("open region {}", region_name);
        return (region, false);
    }

    let create_opts = CreateOptions {
        parent_dir: "/".to_string(),
        ..Default::default()
    };
    let desc = new_cpu_region_descriptor(&region_name, region_id);
    logging::info!("create region {}", region_name);
    (
        engine
            .create_region(&ctx, desc, &create_opts)
            .await
            .unwrap(),
        true,
    )
}

/// Put [RecordBatch] to [WriteBatch].
fn put_record_batch_to_write_batch(batch: RecordBatch, request: &mut WriteBatch) {
    let schema = batch.schema();
    let mut data = HashMap::with_capacity(batch.num_columns());
    for (field, array) in schema.fields().iter().zip(batch.columns()) {
        if let DataType::Timestamp(_unit, zone) = field.data_type() {
            let timestamps = cast(
                array,
                &DataType::Timestamp(TimeUnit::Millisecond, zone.clone()),
            )
            .unwrap();
            let vector = Helper::try_into_vector(timestamps).unwrap();
            data.insert(TS_COLUMN_NAME.to_string(), vector);
        } else {
            let vector = Helper::try_into_vector(array).unwrap();
            data.insert(field.name().clone(), vector);
        }
    }

    request.put(data).unwrap();
}

/// Metrics of scanning a region.
#[derive(Debug)]
pub struct ScanMetrics {
    pub total_cost: Duration,
    pub num_rows: usize,
}

/// Target region to test.
pub struct Target {
    region_id: RegionId,
    engine: EngineImpl<RaftEngineLogStore>,
    region: Mutex<Option<RegionImpl<RaftEngineLogStore>>>,
    is_new_region: bool,
}

impl Target {
    /// Returns a new target.
    pub async fn new(path: &str, config: EngineConfig, region_id: RegionId) -> Target {
        let region_name = region_name_by_id(region_id);
        let engine = new_engine(path, config).await;
        let (region, is_new_region) = init_region(&engine, &region_name, region_id).await;

        Target {
            region_id,
            engine,
            region: Mutex::new(Some(region)),
            is_new_region,
        }
    }

    /// Is the target a newly created region.
    pub fn is_new_region(&self) -> bool {
        self.is_new_region
    }

    /// Stop the target.
    pub async fn shutdown(&self) {
        // Drop the region.
        self.region.lock().unwrap().take();
        let ctx = EngineContext::default();
        let region_name = region_name_by_id(self.region_id);
        self.engine
            .close_region(&ctx, &region_name, &CloseOptions { flush: false })
            .await
            .unwrap();
        self.engine.close(&ctx).await.unwrap();
    }

    /// Write [RecordBatch] to the target.
    pub async fn write(&self, batch: RecordBatch) {
        let region = self.region();
        let mut request = region.write_request();
        put_record_batch_to_write_batch(batch, &mut request);

        let ctx = WriteContext::default();
        region.write(&ctx, request).await.unwrap();
    }

    /// Scan all the data.
    pub async fn full_scan(&self, batch_size: usize) -> ScanMetrics {
        let start = Instant::now();
        let region = self.region();
        let ctx = ReadContext { batch_size };
        let snapshot = region.snapshot(&ctx).unwrap();
        let resp = snapshot.scan(&ctx, ScanRequest::default()).await.unwrap();
        let mut reader = resp.reader;

        let mut num_rows = 0;
        while let Some(chunk) = reader.next_chunk().await.unwrap() {
            let chunk = reader.project_chunk(chunk);
            num_rows += chunk.columns.first().map(|v| v.len()).unwrap_or(0);
        }

        ScanMetrics {
            total_cost: start.elapsed(),
            num_rows,
        }
    }

    /// Returns the region.
    fn region(&self) -> RegionImpl<RaftEngineLogStore> {
        self.region.lock().unwrap().clone().unwrap()
    }
}
