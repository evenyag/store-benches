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

use common_telemetry::logging;
use datatypes::arrow::record_batch::RecordBatchReader;
use futures_util::future::join_all;
use futures_util::StreamExt;
use storage::config::EngineConfig;

use crate::loader::ParquetLoader;
use crate::target::Target;

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
    put_workers: usize,
}

impl PutBench {
    /// Creates a new scan benchmark.
    pub fn new(loader: ParquetLoader, path: String, engine_config: EngineConfig) -> PutBench {
        PutBench {
            loader,
            path,
            engine_config,
            put_workers: 1,
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

        // Spawn multiple tasks to put.
        let (tx, rx) = flume::bounded(64);
        let mut handles = Vec::with_capacity(self.put_workers);
        for _ in 0..put_workers {
            let rx = rx.clone();
            let target = target.clone();
            let handle = tokio::spawn(async move {
                let mut stream = rx.into_stream();
                let mut put_cost = Duration::ZERO;
                // Each worker receive batch from the channel.
                while let Some(batch) = stream.next().await {
                    let put_start = Instant::now();
                    target.write(batch).await;
                    put_cost += put_start.elapsed();
                }

                put_cost
            });

            handles.push(handle);
        }

        // Send batch to the channel.
        let num_rows = tokio::task::block_in_place(|| {
            let mut num_rows = 0;
            for batch in reader {
                let batch = batch.unwrap();
                num_rows += batch.num_rows();

                tx.send(batch).unwrap();
            }
            num_rows
        });

        let put_cost = join_all(handles)
            .await
            .into_iter()
            .map(|result| result.unwrap())
            .sum();

        let load_cost = run_start.elapsed();

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

        let Ok(metadata) = fs::metadata(&self.path) else { return; };
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
        Target::new(&self.path, self.engine_config.clone(), 1).await
    }
}
