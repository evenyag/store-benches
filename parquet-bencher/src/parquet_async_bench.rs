//! Bench parquet async.

use std::time::Instant;

use arrow_array::RecordBatch;
use async_compat::CompatExt;
use futures_util::stream::BoxStream;
use futures_util::TryStreamExt;
use opendal::Operator;
use parquet::arrow::arrow_reader::RowSelection;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::errors::ParquetError;
use tokio::io::BufReader;

use crate::parquet_bench::Metrics;

pub struct ParquetAsyncBench {
    operator: Operator,
    file_path: String,
    batch_size: usize,
    columns: Vec<usize>,
    use_async_trait: bool,
    row_groups: Vec<usize>,
    selection: Option<RowSelection>,
}

impl ParquetAsyncBench {
    pub fn new(operator: Operator, file_path: String, batch_size: usize) -> ParquetAsyncBench {
        ParquetAsyncBench {
            operator,
            file_path,
            batch_size,
            columns: Vec::new(),
            use_async_trait: false,
            row_groups: Vec::new(),
            selection: None,
        }
    }

    pub fn with_columns(mut self, columns: Vec<usize>) -> Self {
        self.columns = columns;
        self
    }

    pub fn with_async_trait(mut self, use_async_trait: bool) -> Self {
        self.use_async_trait = use_async_trait;
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
        let new_builder_cost = start.elapsed();
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

        let mut stream = builder.build().unwrap();
        let build_cost = start.elapsed();

        let mut num_rows = 0;
        if self.use_async_trait {
            let mut batch_reader = StreamReader {
                stream: Box::pin(stream),
            };
            while let Some(batch) = batch_reader.next_batch().await.unwrap() {
                num_rows += batch.num_rows();
            }
        } else {
            while let Some(batch) = stream.try_next().await.unwrap() {
                num_rows += batch.num_rows();
            }
        }
        let scan_cost = start.elapsed();

        Metrics {
            open_cost,
            new_builder_cost,
            build_cost,
            scan_cost,
            num_rows,
            num_columns,
            num_row_groups,
        }
    }
}

type BoxedRecordBatchStream = BoxStream<'static, std::result::Result<RecordBatch, ParquetError>>;

#[async_trait::async_trait]
trait BatchReader {
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, ParquetError>;
}

struct StreamReader {
    stream: BoxedRecordBatchStream,
}

#[async_trait::async_trait]
impl BatchReader for StreamReader {
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, ParquetError> {
        self.stream.try_next().await
    }
}
