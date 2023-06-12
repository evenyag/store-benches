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

//! Data loader.

use std::fs::File;
use std::time::{Duration, Instant};

use datatypes::arrow::record_batch::RecordBatchReader;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};

use crate::target::Target;

/// Metrics during loading data.
#[derive(Debug)]
pub struct LoadMetrics {
    pub total_cost: Duration,
    pub num_rows: usize,
    pub num_columns: usize,
}

/// Parquet data loader.
pub struct ParquetLoader {
    file_path: String,
    batch_size: usize,
}

impl ParquetLoader {
    /// Returns a new loader.
    pub fn new(file_path: String, batch_size: usize) -> ParquetLoader {
        ParquetLoader {
            file_path,
            batch_size,
        }
    }

    /// Load data in the parquet file to the target.
    pub async fn load_to_target(&self, target: &Target) -> LoadMetrics {
        let start = Instant::now();

        let reader = self.reader();
        let num_columns = reader.schema().fields().len();

        let mut num_rows = 0;
        for batch in reader {
            let batch = batch.unwrap();
            num_rows += batch.num_rows();
            target.write(batch).await;
        }

        LoadMetrics {
            total_cost: start.elapsed(),
            num_rows,
            num_columns,
        }
    }

    /// Returns a new [ParquetRecordBatchReader].
    pub fn reader(&self) -> ParquetRecordBatchReader {
        let file = File::open(&self.file_path).unwrap();

        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .with_batch_size(self.batch_size);
        builder.build().unwrap()
    }
}
