//! Parquet benchmark.

use std::fs::File;
use std::time::{Duration, Instant};

use parquet::arrow::arrow_reader::{ParquetRecordBatchReaderBuilder, RowSelection};

#[derive(Debug)]
pub struct Metrics {
    /// Cost to open the file.
    pub open_cost: Duration,
    /// Cost to build the reader.
    pub build_cost: Duration,
    /// Total cost of reading the file (including other costs).
    pub scan_cost: Duration,
    pub num_rows: usize,
    pub num_columns: usize,
    pub num_row_groups: usize,
}

pub struct ParquetBench {
    file_path: String,
    batch_size: usize,
    columns: Vec<usize>,
    row_groups: Vec<usize>,
    selection: Option<RowSelection>,
}

impl ParquetBench {
    pub fn new(file_path: String, batch_size: usize) -> ParquetBench {
        ParquetBench {
            file_path,
            batch_size,
            columns: Vec::new(),
            row_groups: Vec::new(),
            selection: None,
        }
    }

    pub fn with_columns(mut self, columns: Vec<usize>) -> Self {
        self.columns = columns;
        self
    }

    pub fn with_row_groups(mut self, row_groups: Vec<usize>) -> Self {
        self.row_groups = row_groups;
        self
    }

    pub fn with_selection(mut self, selection: RowSelection) -> Self {
        self.selection = Some(selection);
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
        let mut num_row_groups = builder.metadata().num_row_groups();
        if !self.row_groups.is_empty() {
            num_row_groups = self.row_groups.len();
            builder = builder.with_row_groups(self.row_groups.clone());
        }
        if let Some(selection) = &self.selection {
            builder = builder.with_row_selection(selection.clone());
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
            num_row_groups,
        }
    }
}
