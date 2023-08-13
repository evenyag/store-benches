//! Bench parquet async.

//! Parquet benchmark.

use std::time::Instant;

use async_compat::CompatExt;
use futures_util::TryStreamExt;
use opendal::Operator;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use tokio::io::BufReader;

use crate::parquet_bench::Metrics;

pub struct ParquetAsyncBench {
    operator: Operator,
    file_path: String,
    batch_size: usize,
    columns: Vec<usize>,
}

impl ParquetAsyncBench {
    pub fn new(operator: Operator, file_path: String, batch_size: usize) -> ParquetAsyncBench {
        ParquetAsyncBench {
            operator,
            file_path,
            batch_size,
            columns: Vec::new(),
        }
    }

    pub fn with_columns(mut self, columns: Vec<usize>) -> Self {
        self.columns = columns;
        self
    }

    pub async fn run(&self) -> Metrics {
        let start = Instant::now();

        let reader = self
            .operator
            .reader(&self.file_path)
            .await
            .unwrap()
            .compat();
        let buf_reader = BufReader::new(reader);

        let open_cost = start.elapsed();

        let mut builder = ParquetRecordBatchStreamBuilder::new(buf_reader)
            .await
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

        let mut stream = builder.build().unwrap();
        let build_cost = start.elapsed();

        let mut num_rows = 0;
        while let Some(batch) = stream.try_next().await.unwrap() {
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
