//! Bench inserting memtables.

use std::time::{Duration, Instant};

use crate::loader::ParquetLoader;
use crate::memtable::inserter::{BTreeMemtableInserter, Inserter};
use crate::memtable::source::Source;

/// Metrics of the insert benchmark.
#[derive(Debug)]
pub struct InsertMetrics {
    pub rows_to_insert: usize,
    pub total_cost: Duration,
}

/// Bencher to bench inserting memtables.
pub struct InsertMemtableBench {
    source: Source,
}

impl InsertMemtableBench {
    /// Returns a new [InsertMemtableBench].
    pub fn new(rows_to_insert: usize) -> InsertMemtableBench {
        InsertMemtableBench {
            source: Source::new(rows_to_insert),
        }
    }

    /// Load batches from parquet to memory.
    pub fn load_batches(&mut self, loader: &ParquetLoader) {
        self.source.load_batches(loader);
    }
}

impl InsertMemtableBench {
    /// Run benchmark for BTreeMemtable.
    pub fn bench_btree(&self) -> InsertMetrics {
        let mut inserter = BTreeMemtableInserter::new();
        self.insert(&mut inserter)
    }

    /// Insert data.
    fn insert<T: Inserter>(&self, inserter: &mut T) -> InsertMetrics {
        inserter.reset();

        let start = Instant::now();

        self.source.insert(inserter);

        InsertMetrics {
            rows_to_insert: self.source.rows_to_insert(),
            total_cost: start.elapsed(),
        }
    }
}
