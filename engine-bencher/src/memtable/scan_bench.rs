//! Scan memtable benchmark.

use std::time::Instant;

use common_telemetry::logging;
use memtable_nursery::columnar::ColumnarConfig;

use crate::loader::ParquetLoader;
use crate::memtable::source::Source;
use crate::memtable::target::{MemtableTarget, Scanner};
use crate::target::ScanMetrics;

/// Bencher to scan memtable.
pub struct ScanMemtableBench {
    source: Source,
    target: Option<MemtableTarget>,
}

impl ScanMemtableBench {
    /// Returns a new bencher.
    pub fn new(total_rows: usize) -> ScanMemtableBench {
        ScanMemtableBench {
            source: Source::new(total_rows),
            target: None,
        }
    }

    /// Load data into the bencher.
    pub fn load_data(&mut self, loader: &ParquetLoader) {
        self.source.load_batches(loader);
    }

    /// Init BTreeMemtable.
    pub fn init_btree(&mut self) {
        self.init_memtable(MemtableTarget::new_btree());
    }

    /// Init ColumnarMemtable.
    pub fn init_columnar(&mut self, config: ColumnarConfig) {
        self.init_memtable(MemtableTarget::new_columnar(config));
    }

    /// Bench memtable.
    pub fn bench(&self, batch_size: usize) -> ScanMetrics {
        let start = Instant::now();

        let num_rows = self.target.as_ref().unwrap().scan_all(batch_size);
        assert_eq!(num_rows, self.source.rows_to_insert());

        ScanMetrics {
            total_cost: start.elapsed(),
            num_rows,
        }
    }

    /// Init memtable.
    fn init_memtable(&mut self, mut target: MemtableTarget) {
        // Clear old target.
        self.target = None;

        logging::info!("Start loading data into memtable");

        self.source.insert(&mut target);

        logging::info!("End loading data into memtable");

        self.target = Some(target);
    }
}
