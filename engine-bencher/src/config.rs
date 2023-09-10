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

//! Benchmark configs.

use std::fs::File;
use std::io::Read;
use std::time::Duration;

use mito2::config::MitoConfig;
use serde::Deserialize;
use storage::config::EngineConfig;
use store_api::storage::RegionId;

/// Config for all benchmarks.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct BenchConfig {
    pub runtime_size: usize,
    pub parquet_path: String,
    /// Print metrics every N benches. Never print metrics if N is 0.
    pub print_metrics_every: usize,
    /// Config for scan bench.
    pub scan: ScanConfig,
    /// Config for put bench.
    pub put: PutConfig,
    /// Config for insert memtable bench.
    pub insert_memtable: InsertMemtableConfig,
    /// Config for insert memtable bench.
    pub scan_memtable: ScanMemtableConfig,
}

impl Default for BenchConfig {
    fn default() -> BenchConfig {
        BenchConfig {
            runtime_size: 4,
            parquet_path: "".to_string(),
            print_metrics_every: 0,
            scan: ScanConfig::default(),
            put: PutConfig::default(),
            insert_memtable: InsertMemtableConfig::default(),
            scan_memtable: ScanMemtableConfig::default(),
        }
    }
}

impl BenchConfig {
    pub fn parse_toml(path: &str) -> BenchConfig {
        let mut file = File::open(path).unwrap();
        let mut content = String::new();
        file.read_to_string(&mut content).unwrap();

        toml::from_str(&content).unwrap()
    }
}

/// Scan bench config.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ScanConfig {
    #[serde(with = "humantime_serde")]
    pub measurement_time: Option<Duration>,
    pub sample_size: Option<usize>,
    /// Storage data path.
    pub path: String,
    /// Mito data path.
    pub mito_path: String,
    /// Region to bench.
    pub region_id: RegionId,
    /// Batch size to load data.
    pub load_batch_size: usize,
    /// Batch size to scan.
    pub scan_batch_size: usize,
}

impl Default for ScanConfig {
    fn default() -> Self {
        ScanConfig {
            measurement_time: None,
            sample_size: None,
            path: "/tmp/storage-bencher/".to_string(),
            mito_path: "/tmp/storage-bencher/mito".to_string(),
            region_id: 0.into(),
            load_batch_size: 1024,
            scan_batch_size: 1024,
        }
    }
}

impl ScanConfig {
    /// Returns the engine config for bench.
    pub fn engine_config(&self) -> EngineConfig {
        EngineConfig::default()
    }

    /// Returns the mito config for bench.
    pub fn mito_config(&self) -> MitoConfig {
        MitoConfig::default()
    }
}

/// Put bench config.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct PutConfig {
    #[serde(with = "humantime_serde")]
    pub measurement_time: Option<Duration>,
    pub sample_size: Option<usize>,
    /// Storage data path.
    pub path: String,
    /// Mito data path.
    pub mito_path: String,
    /// Batch size to put.
    pub batch_size: usize,
    /// Put worker num.
    pub put_workers: usize,
}

impl Default for PutConfig {
    fn default() -> Self {
        PutConfig {
            measurement_time: None,
            sample_size: None,
            path: "/tmp/storage-bencher/".to_string(),
            mito_path: "/tmp/storage-bencher/mito".to_string(),
            batch_size: 1024,
            put_workers: 1,
        }
    }
}

impl PutConfig {
    /// Returns the engine config for bench.
    pub fn engine_config(&self) -> EngineConfig {
        EngineConfig::default()
    }

    /// Returns the mito config for bench.
    pub fn mito_config(&self) -> MitoConfig {
        MitoConfig::default()
    }
}

/// Insert memtable bench config.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct InsertMemtableConfig {
    #[serde(with = "humantime_serde")]
    pub measurement_time: Option<Duration>,
    pub sample_size: Option<usize>,
    /// Batch size to load/insert.
    pub batch_size: usize,
    /// Number of rows to insert.
    pub total_rows: usize,
}

impl Default for InsertMemtableConfig {
    fn default() -> Self {
        InsertMemtableConfig {
            measurement_time: None,
            sample_size: None,
            batch_size: 1000,
            total_rows: 500000,
        }
    }
}

/// Scan memtable bench config.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ScanMemtableConfig {
    #[serde(with = "humantime_serde")]
    pub measurement_time: Option<Duration>,
    pub sample_size: Option<usize>,
    /// Number of rows to load and scan.
    pub total_rows: usize,
    /// Batch size to load.
    pub load_batch_size: usize,
    /// Batch size to scan.
    pub scan_batch_size: usize,
}

impl Default for ScanMemtableConfig {
    fn default() -> Self {
        ScanMemtableConfig {
            measurement_time: None,
            sample_size: None,
            total_rows: 500000,
            load_batch_size: 1000,
            scan_batch_size: 1000,
        }
    }
}
