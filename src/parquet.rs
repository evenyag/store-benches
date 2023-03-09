//! Parquet benches.

use std::fs::File;
use std::time::{Duration, Instant};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

#[derive(Debug)]
pub struct Metrics {
    pub open_cost: Duration,
    pub build_cost: Duration,
    pub scan_cost: Duration,
    pub num_rows: usize,
}

pub struct ParquetBench {
    file_path: String,
    batch_size: usize,
}

impl ParquetBench {
    pub fn new(file_path: String, batch_size: usize) -> ParquetBench {
        ParquetBench {
            file_path,
            batch_size,
        }
    }

    pub fn run(&self) -> Metrics {
        let start = Instant::now();

        let file = File::open(&self.file_path).unwrap();
        let open_cost = start.elapsed();

        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .with_batch_size(self.batch_size);

        let reader = builder.build().unwrap();
        let build_cost = start.elapsed();

        let mut num_rows = 0;
        for batch in reader {
            let batch = batch.unwrap();
            num_rows += batch.num_rows();
        }
        let scan_cost = start.elapsed();

        Metrics {
            open_cost,
            build_cost,
            scan_cost,
            num_rows,
        }
    }
}
