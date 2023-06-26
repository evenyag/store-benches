//! Scan memtable benchmark.

use std::time::Instant;

use crate::loader::ParquetLoader;
use crate::memtable::source::Source;
use crate::memtable::target::{BTreeMemtableTarget, Scanner};
use crate::target::ScanMetrics;

/// Bencher to scan memtable.
pub struct ScanMemtableBench {
    source: Source,
    target: BTreeMemtableTarget,
}

impl ScanMemtableBench {
    /// Returns a new bencher.
    pub fn new(total_rows: usize) -> ScanMemtableBench {
        ScanMemtableBench {
            source: Source::new(total_rows),
            target: BTreeMemtableTarget::new(),
        }
    }

    /// Init the bencher.
    pub fn init(&mut self, loader: &ParquetLoader) {
        self.source.load_batches(loader);
        self.init_btree();
    }

    /// Bench BTreeMemtable.
    pub fn bench_btree(&self, batch_size: usize) -> ScanMetrics {
        let num_rows = self.source.rows_to_insert();
        let start = Instant::now();

        self.target.scan_all(batch_size);

        ScanMetrics {
            total_cost: start.elapsed(),
            num_rows,
        }
    }

    /// Init btree memtable.
    fn init_btree(&mut self) {
        self.source.insert(&mut self.target);
    }
}
