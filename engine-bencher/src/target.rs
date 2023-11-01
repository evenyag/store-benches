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

use api::helper::ColumnDataTypeWrapper;
use api::v1::{Row, Rows, SemanticType};
use common_base::readable_size::ReadableSize;
use common_config::WalConfig;
use common_telemetry::{logging, warn};
use datatypes::arrow::compute::cast;
use datatypes::arrow::datatypes::{DataType, TimeUnit};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use datatypes::vectors::Helper;
use futures_util::TryStreamExt;
use log_store::raft_engine::log_store::RaftEngineLogStore;
use mito2::config::MitoConfig;
use mito2::engine::MitoEngine;
use object_store::layers::LoggingLayer;
use object_store::manager::ObjectStoreManager;
use object_store::services::Fs;
use object_store::{util, ObjectStore};
use storage::compaction::{CompactionHandler, CompactionSchedulerRef};
use storage::config::EngineConfig;
use storage::region::RegionImpl;
use storage::scheduler::{LocalScheduler, SchedulerConfig};
use storage::write_batch::WriteBatch;
use storage::EngineImpl;
use store_api::logstore::LogStore;
use store_api::metadata::ColumnMetadata;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{
    RegionCloseRequest, RegionCreateRequest, RegionOpenRequest, RegionPutRequest, RegionRequest,
};
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

    ObjectStore::new(builder).unwrap().finish().layer(
        LoggingLayer::default()
            .with_error_level(Some("debug"))
            .unwrap(),
    )
}

/// Returns a new log store.
async fn new_log_store(path: &str) -> RaftEngineLogStore {
    // create WAL directory
    fs::create_dir_all(Path::new(path)).unwrap();
    let log_config = WalConfig {
        dir: None,
        file_size: ReadableSize::gb(1),
        purge_threshold: ReadableSize::gb(10),
        purge_interval: Duration::from_secs(600),
        read_batch_size: 128,
        sync_write: false,
    };

    RaftEngineLogStore::try_new(path.to_string(), log_config)
        .await
        .unwrap()
}

/// Returns a new compaction scheduler.
fn new_compaction_scheduler<S: LogStore>() -> CompactionSchedulerRef<S> {
    let config = SchedulerConfig {
        max_inflight_tasks: 4,
    };
    let handler = CompactionHandler::default();
    let scheduler = LocalScheduler::new(config, handler);
    Arc::new(scheduler)
}

const CPU_TAGS: [&'static str; 10] = [
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

const CPU_FIELDS: [&'static str; 10] = [
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

    let mut row_key_builder = RowKeyDescriptorBuilder::new(timestamp);
    for tag in CPU_TAGS {
        row_key_builder = row_key_builder.push_column(
            ColumnDescriptorBuilder::new(column_id, tag, ConcreteDataType::string_datatype())
                .build()
                .unwrap(),
        );
        column_id += 1;
    }
    let row_key = row_key_builder.build().unwrap();
    let mut cf_builder = ColumnFamilyDescriptorBuilder::default();
    for field in CPU_FIELDS {
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

/// Returns create request for metric `cpu`.
fn new_cpu_create_request(region_name: &str) -> RegionCreateRequest {
    let mut column_id = 1;
    let mut column_metadatas = Vec::new();
    // Note that the timestamp in the input parquet file has Timestamp(Microsecond, None) type.
    column_metadatas.push(ColumnMetadata {
        column_schema: ColumnSchema::new(
            TS_COLUMN_NAME,
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        ),
        semantic_type: SemanticType::Timestamp,
        column_id,
    });
    column_id += 1;

    let mut primary_key = Vec::with_capacity(CPU_TAGS.len());
    for tag in CPU_TAGS {
        column_metadatas.push(ColumnMetadata {
            column_schema: ColumnSchema::new(tag, ConcreteDataType::string_datatype(), true),
            semantic_type: SemanticType::Tag,
            column_id,
        });
        primary_key.push(column_id);
        column_id += 1;
    }
    for field in CPU_FIELDS {
        column_metadatas.push(ColumnMetadata {
            column_schema: ColumnSchema::new(field, ConcreteDataType::float64_datatype(), true),
            semantic_type: SemanticType::Field,
            column_id,
        });
        column_id += 1;
    }

    RegionCreateRequest {
        engine: "".to_string(),
        column_metadatas,
        primary_key,
        options: HashMap::new(),
        region_dir: region_name.to_string(),
    }
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

/// Returns a new mito engine.
async fn new_mito_engine(path: &str, config: MitoConfig) -> MitoEngine {
    let path = util::normalize_dir(path);
    let data_dir = format!("{path}data");
    let wal_dir = format!("{path}wal");

    let object_store = new_fs_object_store(&data_dir).await;
    let log_store = Arc::new(new_log_store(&wal_dir).await);
    MitoEngine::new(
        config,
        log_store,
        Arc::new(ObjectStoreManager::new("default", object_store)),
    )
}

/// Returns the region name.
fn region_name_by_id(region_id: RegionId) -> String {
    format!("cpu-{}-{}", region_id.table_id(), region_id.region_number())
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

/// Creates or opens the mito region.
///
/// Returns whether the region is newly created.
async fn init_mito_region(engine: &MitoEngine, region_name: &str, region_id: RegionId) -> bool {
    let opened = engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: "".to_string(),
                region_dir: region_name.to_string(),
                options: HashMap::new(),
            }),
        )
        .await
        .is_ok();

    if opened {
        logging::info!("open region {}", region_name);
        return false;
    }

    engine
        .handle_request(
            region_id,
            RegionRequest::Create(new_cpu_create_request(region_name)),
        )
        .await
        .unwrap();
    logging::info!("create region {}", region_name);

    true
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

/// Convert [RecordBatch] to [RegionPutRequest].
fn record_batch_to_put_request(batch: RecordBatch) -> RegionPutRequest {
    let mut vectors = Vec::with_capacity(batch.num_columns());
    let mut schema = Vec::with_capacity(batch.num_columns());
    for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
        if let DataType::Timestamp(_unit, zone) = field.data_type() {
            let timestamps = cast(
                array,
                &DataType::Timestamp(TimeUnit::Millisecond, zone.clone()),
            )
            .unwrap();
            let vector = Helper::try_into_vector(timestamps).unwrap();
            schema.push(api::v1::ColumnSchema {
                column_name: TS_COLUMN_NAME.to_string(),
                datatype: ColumnDataTypeWrapper::try_from(
                    ConcreteDataType::timestamp_millisecond_datatype(),
                )
                .unwrap()
                .datatype() as i32,
                semantic_type: SemanticType::Timestamp as i32,
            });
            vectors.push(vector);
        } else {
            let vector = Helper::try_into_vector(array).unwrap();
            schema.push(api::v1::ColumnSchema {
                column_name: field.name().to_string(),
                datatype: ColumnDataTypeWrapper::try_from(ConcreteDataType::from_arrow_type(
                    field.data_type(),
                ))
                .unwrap()
                .datatype() as i32,
                semantic_type: if CPU_TAGS.contains(&field.name().as_str()) {
                    SemanticType::Tag as i32
                } else {
                    SemanticType::Field as i32
                },
            });
            vectors.push(vector);
        }
    }
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row_idx in 0..batch.num_rows() {
        let mut values = Vec::with_capacity(batch.num_columns());
        for col_idx in 0..batch.num_columns() {
            let value = vectors[col_idx].get(row_idx);
            let pb_value = api::helper::to_proto_value(value).unwrap();
            values.push(pb_value);
        }
        rows.push(Row { values });
    }
    RegionPutRequest {
        rows: Rows { schema, rows },
    }
}

/// Metrics of scanning a region.
#[derive(Debug)]
pub struct ScanMetrics {
    pub total_cost: Duration,
    pub num_rows: usize,
}

/// Different bench target.
pub enum Target {
    /// Storage target.
    Storage(StorageTarget),
    /// Mito target.
    Mito(MitoTarget),
}

impl Target {
    /// Creates a new target.
    pub async fn new(
        path: &str,
        engine_config: EngineConfig,
        mito_config: MitoConfig,
        region_id: RegionId,
        run_mito: bool,
    ) -> Target {
        if run_mito {
            Target::Mito(MitoTarget::new(path, mito_config, region_id).await)
        } else {
            Target::Storage(StorageTarget::new(path, engine_config, region_id).await)
        }
    }

    /// Is the target a newly created region.
    pub fn is_new_region(&self) -> bool {
        match self {
            Target::Storage(target) => target.is_new_region(),
            Target::Mito(target) => target.is_new_region(),
        }
    }

    /// Stop the target.
    pub async fn shutdown(&self) {
        match self {
            Target::Storage(target) => target.shutdown().await,
            Target::Mito(target) => target.shutdown().await,
        }
    }

    /// Write [RecordBatch] to the target.
    pub async fn write(&self, batch: RecordBatch) {
        match self {
            Target::Storage(target) => target.write(batch).await,
            Target::Mito(target) => target.write(batch).await,
        }
    }

    /// Scan all the data.
    pub async fn full_scan(&self, batch_size: usize) -> ScanMetrics {
        match self {
            Target::Storage(target) => target.full_scan(batch_size).await,
            Target::Mito(target) => target.full_scan(batch_size).await,
        }
    }
}

/// Target storage region to test.
pub struct StorageTarget {
    region_id: RegionId,
    engine: EngineImpl<RaftEngineLogStore>,
    region: Mutex<Option<RegionImpl<RaftEngineLogStore>>>,
    is_new_region: bool,
}

impl StorageTarget {
    /// Returns a new storage target.
    pub async fn new(path: &str, config: EngineConfig, region_id: RegionId) -> StorageTarget {
        let region_name = region_name_by_id(region_id);
        let engine = new_engine(path, config).await;
        let (region, is_new_region) = init_region(&engine, &region_name, region_id).await;

        StorageTarget {
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

/// Target mito region to test.
pub struct MitoTarget {
    region_id: RegionId,
    engine: MitoEngine,
    is_new_region: bool,
}

impl MitoTarget {
    /// Returns a new target.
    pub async fn new(path: &str, config: MitoConfig, region_id: RegionId) -> MitoTarget {
        let region_name = region_name_by_id(region_id);
        let engine = new_mito_engine(path, config).await;
        let is_new_region = init_mito_region(&engine, &region_name, region_id).await;

        MitoTarget {
            region_id,
            engine,
            is_new_region,
        }
    }

    /// Is the target a newly created region.
    pub fn is_new_region(&self) -> bool {
        self.is_new_region
    }

    /// Stops the target.
    pub async fn shutdown(&self) {
        // Drops the region.
        self.engine
            .handle_request(self.region_id, RegionRequest::Close(RegionCloseRequest {}))
            .await
            .unwrap();
        // Stops the engine.
        self.engine.stop().await.unwrap();
    }

    /// Write [RecordBatch] to the target.
    pub async fn write(&self, batch: RecordBatch) {
        let start = Instant::now();
        let request = record_batch_to_put_request(batch);
        self.engine
            .handle_request(self.region_id, RegionRequest::Put(request))
            .await
            .unwrap();
        let cost = start.elapsed();
        if cost > Duration::from_millis(200) {
            warn!("request put cost is too larger {:?} > 200ms", cost);
        }
    }

    /// Scan all the data.
    pub async fn full_scan(&self, _batch_size: usize) -> ScanMetrics {
        let start = Instant::now();
        let mut stream = self
            .engine
            .handle_query(self.region_id, ScanRequest::default())
            .await
            .unwrap();

        let mut num_rows = 0;
        while let Some(batch) = stream.try_next().await.unwrap() {
            num_rows += batch.num_rows();
        }

        ScanMetrics {
            total_cost: start.elapsed(),
            num_rows,
        }
    }
}
