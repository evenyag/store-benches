//! Parquet row group benchmark.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_schema::Fields;
use bytes::{Buf, Bytes};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, RowGroups, RowSelection};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{
    parquet_to_arrow_field_levels, ParquetRecordBatchStreamBuilder, ProjectionMask,
};
use parquet::column::page::{Page, PageIterator, PageMetadata, PageReader};
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::reader::{ChunkReader, Length};
use parquet::file::serialized_reader::SerializedPageReader;
use tokio::fs::File;

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
    /// Total cost to fetch data.
    pub fetch_cost: Duration,
    /// Total cost to build readers.
    pub build_reader_cost: Duration,
    /// Total cost to get pages.
    pub get_page_cost: Duration,
    pub page_total_size: usize,
    pub read_batch_cost: Duration,
}

/// Based on this [example](https://github.com/apache/arrow-rs/blob/master/parquet/examples/read_with_rowgroup.rs).
pub struct ParquetRowGroupBench {
    file_path: String,
    batch_size: usize,
    columns: Vec<usize>,
    row_groups: Vec<usize>,
}

impl ParquetRowGroupBench {
    pub fn new(file_path: String, batch_size: usize) -> ParquetRowGroupBench {
        ParquetRowGroupBench {
            file_path,
            batch_size,
            columns: Vec::new(),
            row_groups: Vec::new(),
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

    pub async fn run(&self) -> Metrics {
        let start = Instant::now();

        let file = File::open(&self.file_path).await.unwrap();
        let builder = ParquetRecordBatchStreamBuilder::new(file).await.unwrap();
        let metadata = builder.metadata();
        let mut file = File::open(&self.file_path).await.unwrap();

        let open_cost = start.elapsed();

        let parquet_schema_desc = metadata.file_metadata().schema_descr_ptr();
        let num_columns = parquet_schema_desc.num_columns();
        let num_row_groups = metadata.num_row_groups();

        let build_cost = start.elapsed();

        let mut num_rows = 0;
        let mut fetch_cost = Duration::ZERO;
        let mut build_reader_cost = Duration::ZERO;
        let mut read_batch_cost = Duration::ZERO;
        let reader_metrics = Arc::new(PageReaderMetrics::default());
        let mask = if self.columns.is_empty() {
            ProjectionMask::all()
        } else {
            ProjectionMask::roots(&parquet_schema_desc, self.columns.iter().copied())
        };
        for rg in metadata.row_groups() {
            let mut rowgroup =
                InMemoryRowGroup::create(rg.clone(), mask.clone(), reader_metrics.clone());
            let start = Instant::now();
            rowgroup.async_fetch_data(&mut file, None).await.unwrap();
            fetch_cost += start.elapsed();
            let start = Instant::now();
            let reader = rowgroup
                .build_reader(self.batch_size, None, Some(builder.schema().fields()))
                .unwrap();
            build_reader_cost += start.elapsed();

            let start = Instant::now();
            for batch in reader {
                let batch = batch.unwrap();
                num_rows += batch.num_rows();
            }
            read_batch_cost += start.elapsed();
        }
        let scan_cost = start.elapsed();
        let get_page_cost =
            Duration::from_nanos(reader_metrics.get_page_cost.load(Ordering::Relaxed));
        let page_total_size = reader_metrics.page_total_size.load(Ordering::Relaxed);

        Metrics {
            open_cost,
            build_cost,
            scan_cost,
            num_rows,
            num_columns,
            num_row_groups,
            fetch_cost,
            build_reader_cost,
            get_page_cost,
            page_total_size,
            read_batch_cost,
        }
    }
}

/// Implements [`PageIterator`] for a single column chunk, yielding a single [`PageReader`]
struct ColumnChunkIterator {
    reader: Option<Result<Box<dyn PageReader>>>,
}

impl Iterator for ColumnChunkIterator {
    type Item = Result<Box<dyn PageReader>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.take()
    }
}

impl PageIterator for ColumnChunkIterator {}

/// An in-memory column chunk
#[derive(Clone)]
pub struct ColumnChunkData {
    offset: usize,
    data: Bytes,
}

impl ColumnChunkData {
    fn get(&self, start: u64) -> Result<Bytes> {
        let start = start as usize - self.offset;
        Ok(self.data.slice(start..))
    }
}

impl Length for ColumnChunkData {
    fn len(&self) -> u64 {
        self.data.len() as u64
    }
}

impl ChunkReader for ColumnChunkData {
    type T = bytes::buf::Reader<Bytes>;

    fn get_read(&self, start: u64) -> Result<Self::T> {
        Ok(self.get(start)?.reader())
    }

    fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes> {
        Ok(self.get(start)?.slice(..length))
    }
}

#[derive(Default)]
struct PageReaderMetrics {
    get_page_cost: AtomicU64,
    page_total_size: AtomicUsize,
}

struct PageReaderImpl<R: ChunkReader> {
    page_reader: SerializedPageReader<R>,
    metrics: Arc<PageReaderMetrics>,
    get_page_cost: Duration,
    page_total_size: usize,
}

impl<R: ChunkReader> PageReaderImpl<R> {
    fn new(page_reader: SerializedPageReader<R>, metrics: Arc<PageReaderMetrics>) -> Self {
        Self {
            page_reader,
            metrics,
            get_page_cost: Duration::ZERO,
            page_total_size: 0,
        }
    }
}

impl<R: ChunkReader> Iterator for PageReaderImpl<R> {
    type Item = Result<Page>;

    fn next(&mut self) -> Option<Self::Item> {
        self.get_next_page().transpose()
    }
}

impl<R: ChunkReader> PageReader for PageReaderImpl<R> {
    fn get_next_page(&mut self) -> Result<Option<Page>> {
        let start = Instant::now();
        let ret = self.page_reader.get_next_page();
        self.get_page_cost += start.elapsed();
        if let Ok(Some(page)) = &ret {
            self.page_total_size += page.buffer().len();
        }
        ret
    }

    fn peek_next_page(&mut self) -> Result<Option<PageMetadata>> {
        self.page_reader.peek_next_page()
    }

    fn skip_next_page(&mut self) -> Result<()> {
        self.page_reader.skip_next_page()
    }
}

impl<R: ChunkReader> Drop for PageReaderImpl<R> {
    fn drop(&mut self) {
        self.metrics
            .get_page_cost
            .fetch_add(self.get_page_cost.as_nanos() as u64, Ordering::Relaxed);
        self.metrics
            .page_total_size
            .fetch_add(self.page_total_size, Ordering::Relaxed);
    }
}

#[derive(Clone)]
pub struct InMemoryRowGroup {
    pub metadata: RowGroupMetaData,
    mask: ProjectionMask,
    column_chunks: Vec<Option<Arc<ColumnChunkData>>>,
    reader_metrics: Arc<PageReaderMetrics>,
}

impl RowGroups for InMemoryRowGroup {
    fn num_rows(&self) -> usize {
        self.metadata.num_rows() as usize
    }

    fn column_chunks(&self, i: usize) -> Result<Box<dyn PageIterator>> {
        match &self.column_chunks[i] {
            None => Err(ParquetError::General(format!(
                "Invalid column index {i}, column was not fetched"
            ))),
            Some(data) => {
                let page_reader: Box<dyn PageReader> = Box::new(PageReaderImpl::new(
                    SerializedPageReader::new(
                        data.clone(),
                        self.metadata.column(i),
                        self.num_rows(),
                        None,
                    )?,
                    self.reader_metrics.clone(),
                ));

                Ok(Box::new(ColumnChunkIterator {
                    reader: Some(Ok(page_reader)),
                }))
            }
        }
    }
}

impl InMemoryRowGroup {
    fn create(
        metadata: RowGroupMetaData,
        mask: ProjectionMask,
        reader_metrics: Arc<PageReaderMetrics>,
    ) -> Self {
        let column_chunks = metadata.columns().iter().map(|_| None).collect::<Vec<_>>();

        Self {
            metadata,
            mask,
            column_chunks,
            reader_metrics,
        }
    }

    fn build_reader(
        &self,
        batch_size: usize,
        selection: Option<RowSelection>,
        hint: Option<&Fields>,
    ) -> Result<ParquetRecordBatchReader> {
        let levels = parquet_to_arrow_field_levels(
            &self.metadata.schema_descr_ptr(),
            self.mask.clone(),
            hint,
        )?;

        ParquetRecordBatchReader::try_new_with_row_groups(&levels, self, batch_size, selection)
    }

    /// fetch data from a reader in sync mode
    async fn async_fetch_data<R: AsyncFileReader>(
        &mut self,
        reader: &mut R,
        _selection: Option<&RowSelection>,
    ) -> Result<()> {
        let mut vs = std::mem::take(&mut self.column_chunks);
        for (leaf_idx, meta) in self.metadata.columns().iter().enumerate() {
            if self.mask.leaf_included(leaf_idx) {
                let (start, len) = meta.byte_range();
                let data = reader
                    .get_bytes(start as usize..(start + len) as usize)
                    .await?;

                vs[leaf_idx] = Some(Arc::new(ColumnChunkData {
                    offset: start as usize,
                    data,
                }));
            }
        }
        self.column_chunks = std::mem::take(&mut vs);
        Ok(())
    }
}
