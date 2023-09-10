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

use std::fs;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common_telemetry::{info, logging};
use datatypes::arrow::record_batch::{RecordBatch, RecordBatchReader};
use futures_util::future::join_all;
use mito2::config::MitoConfig;
use storage::config::EngineConfig;
use store_api::storage::RegionId;

use crate::loader::ParquetLoader;
use crate::target::{MitoTarget, StorageTarget, Target};

/// Metrics during putting data.
#[derive(Debug)]
pub struct PutMetrics {
    pub num_rows: usize,
    pub num_columns: usize,
    /// Load duration, including time spent on reading file.
    pub load_cost: Duration,
    /// Put duration.
    pub put_cost: Duration,
}

/// Put benchmark.
pub struct PutBench {
    loader: ParquetLoader,
    path: String,
    engine_config: EngineConfig,
    mito_config: MitoConfig,
    put_workers: usize,
    run_mito: bool,
}

impl PutBench {
    /// Creates a new scan benchmark.
    pub fn new(
        loader: ParquetLoader,
        path: String,
        engine_config: EngineConfig,
        mito_config: MitoConfig,
        run_mito: bool,
    ) -> PutBench {
        PutBench {
            loader,
            path,
            engine_config,
            mito_config,
            put_workers: 1,
            run_mito,
        }
    }

    /// Set worker number to put.
    pub fn with_put_workers(mut self, put_workers: usize) -> Self {
        self.put_workers = put_workers;
        self
    }

    /// Iter one bench.
    pub async fn run(&self) -> PutMetrics {
        let run_start = Instant::now();

        let target = self.new_target().await;
        let target = Arc::new(target);
        let reader = self.loader.reader();
        let num_columns = reader.schema().fields().len();

        let put_workers = if self.put_workers <= 1 {
            1
        } else {
            self.put_workers
        };

        info!("worker num is {}", put_workers);

        // Spawn multiple tasks to put.
        let (tx, rx) = async_channel::bounded(64);
        let mut handles = Vec::with_capacity(self.put_workers);
        for worker_id in 0..put_workers {
            let rx: async_channel::Receiver<RecordBatch> = rx.clone();
            let target = target.clone();
            let handle = tokio::spawn(async move {
                let mut put_cost = Duration::ZERO;
                let mut worker_rows = 0;
                // Each worker receive batch from the channel.
                while let Ok(batch) = rx.recv().await {
                    let put_start = Instant::now();
                    worker_rows += batch.num_rows();
                    target.write(batch).await;
                    put_cost += put_start.elapsed();
                }

                info!(
                    "worker {} is finished, totol_rows: {}",
                    worker_id, worker_rows
                );

                put_cost
            });

            handles.push(handle);
        }

        // Send batch to the channel.
        let mut num_rows = 0;
        for batch in reader {
            let batch = batch.unwrap();
            num_rows += batch.num_rows();

            tx.send(batch).await.unwrap();
        }
        tx.close();

        info!("reader finished, totol_rows: {}", num_rows);

        let put_cost = join_all(handles)
            .await
            .into_iter()
            .map(|result| result.unwrap())
            .sum();

        let load_cost = run_start.elapsed();

        info!("Run one put bench finished");

        target.shutdown().await;

        PutMetrics {
            num_rows,
            num_columns,
            load_cost,
            put_cost,
        }
    }

    /// Clean the target path if it isn't empty.
    fn maybe_clean_data(&self) {
        logging::info!("Try to clean dir: {}", self.path);

        let Ok(metadata) = fs::metadata(&self.path) else {
            return;
        };
        if metadata.is_dir() {
            logging::info!("Clean dir: {}", self.path);
            if let Err(e) = fs::remove_dir_all(&self.path) {
                logging::error!("Failed to clean dir {}, err: {}", self.path, e);
            }

            logging::info!("Create dir: {}", self.path);
            if let Err(e) = fs::create_dir_all(&self.path) {
                logging::error!("Failed to create dir {}, err: {}", self.path, e);
            }
        }
    }

    /// Create a new target to write.
    async fn new_target(&self) -> Target {
        self.maybe_clean_data();

        // Use 1 as region id.
        let region_id = RegionId::from(1);
        if self.run_mito {
            let target = MitoTarget::new(&self.path, self.mito_config.clone(), region_id).await;
            Target::Mito(target)
        } else {
            let target =
                StorageTarget::new(&self.path, self.engine_config.clone(), region_id).await;
            Target::Storage(target)
        }
    }
}
