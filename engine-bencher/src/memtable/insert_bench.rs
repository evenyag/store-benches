//! Bench inserting memtables.

use std::time::{Duration, Instant};

use common_telemetry::logging;
use datatypes::arrow::record_batch::RecordBatch;

use super::inserter::BTreeMemtableInserter;
use crate::loader::ParquetLoader;
use crate::memtable::inserter::Inserter;

/// Metrics of the insert benchmark.
#[derive(Debug)]
pub struct InsertMetrics {
    pub rows_to_insert: usize,
    pub total_cost: Duration,
}

/// Bencher to bench inserting memtables.
pub struct InsertBench {
    /// Number of rows to insert into the memtable.
    rows_to_insert: usize,
    /// Batches to insert.
    batches: Vec<RecordBatch>,
}

impl InsertBench {
    /// Returns a new [InsertBench].
    pub fn new(rows_to_insert: usize) -> InsertBench {
        InsertBench {
            rows_to_insert,
            batches: Vec::new(),
        }
    }

    /// Load batches from parquet to memory.
    pub fn load_batches(&mut self, loader: &ParquetLoader) {
        let mut rows_loaded = 0;
        let reader = loader.reader();

        for batch in reader {
            let batch = batch.unwrap();
            if batch.num_rows() + rows_loaded > self.rows_to_insert {
                self.batches
                    .push(batch.slice(0, self.rows_to_insert - rows_loaded));
                rows_loaded = self.rows_to_insert;
                break;
            } else {
                rows_loaded += batch.num_rows();
                self.batches.push(batch);
            }
        }

        logging::info!(
            "Insert bench at most load {} rows, load {} rows actually",
            self.rows_to_insert,
            rows_loaded
        );
        self.rows_to_insert = rows_loaded;
    }
}

impl InsertBench {
    /// Run benchmark for BTreeMemtable.
    pub fn bench_btree(&self) -> InsertMetrics {
        let mut inserter = BTreeMemtableInserter::new();
        self.insert(&mut inserter)
    }

    /// Insert data.
    fn insert<T: Inserter>(&self, inserter: &mut T) -> InsertMetrics {
        inserter.reset();

        let start = Instant::now();

        for batch in &self.batches {
            inserter.insert(batch);
        }

        InsertMetrics {
            rows_to_insert: self.rows_to_insert,
            total_cost: start.elapsed(),
        }
    }
}
