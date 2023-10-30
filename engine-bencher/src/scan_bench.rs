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

use common_telemetry::logging;
use tokio::sync::Mutex;

use crate::loader::ParquetLoader;
use crate::target::{ScanMetrics, Target};

/// Scan benchmark.
pub struct ScanBench {
    loader: ParquetLoader,
    target: Target,
    loaded: Mutex<bool>,
    scan_batch_size: usize,
}

impl ScanBench {
    /// Creates a new scan benchmark.
    pub fn new(loader: ParquetLoader, target: Target, scan_batch_size: usize) -> ScanBench {
        ScanBench {
            loader,
            target,
            loaded: Mutex::new(false),
            scan_batch_size,
        }
    }

    /// Prepare test data if the target contains a new region.
    pub async fn maybe_prepare_data(&self) {
        let mut loaded = self.loaded.lock().await;
        if self.target.is_new_region() && !*loaded {
            logging::info!("Load data to target");

            let metrics = self.loader.load_to_target(&self.target).await;

            logging::info!("Finish loading data, metrics: {:?}", metrics);

            *loaded = true;
        }

        logging::info!("region already exists, don't load again");
    }

    /// Iter one bench.
    pub async fn run(&self) -> ScanMetrics {
        self.target.full_scan(self.scan_batch_size).await
    }

    /// Shutdown the bencher.
    pub async fn shutdown(self) {
        self.target.shutdown().await
    }
}
