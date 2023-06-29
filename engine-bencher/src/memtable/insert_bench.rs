//! Bench inserting memtables.

use std::fmt;
use std::time::{Duration, Instant};

use crate::loader::ParquetLoader;
use crate::memory::{DisplayBytes, MemoryMetrics};
use crate::memtable::source::Source;
use crate::memtable::target::{Inserter, MemtableTarget};

/// Metrics of the insert benchmark.
pub struct InsertMetrics {
    pub rows_to_insert: usize,
    pub total_cost: Duration,
    pub bytes_allocated: usize,
    /// Memory allocated estimated by the memtable.
    pub memtable_estimated_bytes: usize,
}

impl fmt::Debug for InsertMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InsertMetrics")
            .field("rows_to_insert", &self.rows_to_insert)
            .field("total_cost", &self.total_cost)
            .field("bytes_allocated", &DisplayBytes(self.bytes_allocated))
            .field(
                "memtable_estimated_bytes",
                &DisplayBytes(self.memtable_estimated_bytes),
            )
            .finish()
    }
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
    pub fn init(&mut self, loader: &ParquetLoader) {
        self.source.load_batches(loader);
    }

    /// Run benchmark for BTreeMemtable.
    pub fn bench_btree(&self) -> InsertMetrics {
        let mut target = MemtableTarget::new_btree();
        self.insert(&mut target)
    }

    /// Run benchmark for ColumnarMemtable.
    pub fn bench_columnar(&self) -> InsertMetrics {
        let mut target = MemtableTarget::new_columnar();
        self.insert(&mut target)
    }

    /// Insert data.
    fn insert<T: Inserter>(&self, inserter: &mut T) -> InsertMetrics {
        let start = Instant::now();
        let mem_before = MemoryMetrics::read_metrics();

        self.source.insert(inserter);

        let mem_after = MemoryMetrics::read_metrics();
        let bytes_allocated = mem_after.subtract_allocated(&mem_before);

        InsertMetrics {
            rows_to_insert: self.source.rows_to_insert(),
            total_cost: start.elapsed(),
            bytes_allocated,
            memtable_estimated_bytes: inserter.estimated_bytes(),
        }
    }
}
