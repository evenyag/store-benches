//! Data source.

use common_telemetry::logging;
use datatypes::arrow::record_batch::RecordBatch;

use crate::loader::ParquetLoader;
use crate::memtable::inserter::Inserter;

/// Data source for benchmark.
pub(crate) struct Source {
    /// Number of rows to insert into the memtable.
    rows_to_insert: usize,
    /// Batches to insert.
    batches: Vec<RecordBatch>,
}

impl Source {
    /// Returns a new [Source].
    pub(crate) fn new(rows_to_insert: usize) -> Source {
        Source {
            rows_to_insert,
            batches: Vec::new(),
        }
    }

    /// Load batches from parquet to memory.
    pub(crate) fn load_batches(&mut self, loader: &ParquetLoader) {
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

    /// Returns the number of rows to insert.
    pub(crate) fn rows_to_insert(&self) -> usize {
        self.rows_to_insert
    }

    /// Insert all data to the inserter.
    pub(crate) fn insert<T: Inserter>(&self, inserter: &mut T) {
        for batch in &self.batches {
            inserter.insert(batch);
        }
    }
}
