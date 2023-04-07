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
    pub num_columns: usize,
}

pub struct ParquetBench {
    file_path: String,
    batch_size: usize,
    columns: Vec<usize>,
}

impl ParquetBench {
    pub fn new(file_path: String, batch_size: usize) -> ParquetBench {
        ParquetBench {
            file_path,
            batch_size,
            columns: Vec::new(),
        }
    }

    pub fn with_columns(mut self, columns: Vec<usize>) -> Self {
        self.columns = columns;
        self
    }

    pub fn run(&self) -> Metrics {
        let start = Instant::now();

        let file = File::open(&self.file_path).unwrap();
        let open_cost = start.elapsed();

        let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .with_batch_size(self.batch_size);
        let parquet_schema_desc = builder.metadata().file_metadata().schema_descr_ptr();
        let mut num_columns = parquet_schema_desc.num_columns();
        if !self.columns.is_empty() {
            num_columns = self.columns.len();
            builder = builder.with_projection(parquet::arrow::ProjectionMask::roots(
                &parquet_schema_desc,
                self.columns.clone(),
            ));
        }

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
            num_columns,
        }
    }
}
