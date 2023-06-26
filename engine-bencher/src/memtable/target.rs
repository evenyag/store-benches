//! Memtable inserter.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use datatypes::arrow::datatypes::{DataType, TimeUnit};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::vectors::{Float64Vector, StringVector, TimestampMillisecondVector, VectorRef};
use storage::memtable::btree::BTreeMemtable;
use storage::memtable::{IterContext, KeyValues, Memtable};
use storage::metadata::RegionMetadata;
use storage::schema::RegionSchemaRef;
use store_api::storage::OpType;

use crate::target::{new_cpu_region_descriptor, TS_COLUMN_NAME};

const PARQUET_TIMESTAMP_NAME: &str = "timestamp";

/// Memtable inserter.
pub trait Inserter {
    /// Insert `record_batch` into the memtable.
    fn insert(&self, record_batch: &RecordBatch);

    /// Returns memtable's estimated bytes.
    fn estimated_bytes(&self) -> usize;
}

/// Memtable scanner.
pub trait Scanner {
    /// Scan all rows in the memtable.
    fn scan_all(&self, batch_size: usize);
}

/// BTreeMemtable Target.
pub struct BTreeMemtableTarget {
    schema: RegionSchemaRef,
    memtable: BTreeMemtable,
    sequence: AtomicU64,
}

impl BTreeMemtableTarget {
    pub fn new() -> BTreeMemtableTarget {
        let schema = cpu_region_schema();
        BTreeMemtableTarget {
            schema: schema.clone(),
            memtable: new_btree_memtable(schema),
            sequence: AtomicU64::new(0),
        }
    }

    fn record_batch_to_key_values(&self, batch: &RecordBatch) -> KeyValues {
        let mut keys = Vec::with_capacity(batch.num_columns());
        for key_column in self.schema.row_key_columns() {
            if key_column.name() == TS_COLUMN_NAME {
                continue;
            }

            let array = batch.column_by_name(key_column.name()).unwrap();
            let vector: VectorRef = match array.data_type() {
                DataType::Utf8 => Arc::new(StringVector::try_from_arrow_array(array).unwrap()),
                DataType::Float64 => Arc::new(Float64Vector::try_from_arrow_array(array).unwrap()),
                other => panic!("Unsupported data type {:?}", other),
            };
            keys.push(vector);
        }
        let mut values = Vec::with_capacity(batch.num_columns());
        for value_column in self.schema.field_columns() {
            let array = batch.column_by_name(value_column.name()).unwrap();
            let vector: VectorRef = match array.data_type() {
                DataType::Utf8 => Arc::new(StringVector::try_from_arrow_array(array).unwrap()),
                DataType::Float64 => Arc::new(Float64Vector::try_from_arrow_array(array).unwrap()),
                other => panic!("Unsupported data type {:?}", other),
            };
            values.push(vector);
        }
        let array = batch.column_by_name(PARQUET_TIMESTAMP_NAME).unwrap();
        // Cast to millisecond.
        let array = datatypes::arrow::compute::cast(
            array,
            &DataType::Timestamp(TimeUnit::Millisecond, None),
        )
        .unwrap();
        let timestamp = Arc::new(TimestampMillisecondVector::try_from_arrow_array(array).unwrap());

        KeyValues {
            sequence: self.sequence.fetch_add(1, Ordering::Relaxed),
            op_type: OpType::Put,
            start_index_in_batch: 0,
            keys,
            values,
            timestamp: Some(timestamp),
        }
    }
}

impl Inserter for BTreeMemtableTarget {
    fn insert(&self, record_batch: &RecordBatch) {
        let kvs = self.record_batch_to_key_values(record_batch);

        self.memtable.write(&kvs).unwrap();
    }

    fn estimated_bytes(&self) -> usize {
        self.memtable.stats().estimated_bytes
    }
}

impl Scanner for BTreeMemtableTarget {
    fn scan_all(&self, batch_size: usize) {
        let ctx = IterContext {
            batch_size,
            for_flush: false,
            ..Default::default()
        };
        let iter = self.memtable.iter(ctx).unwrap();
        for batch in iter {
            batch.unwrap();
        }
    }
}

fn cpu_region_schema() -> RegionSchemaRef {
    let desc = new_cpu_region_descriptor("cpu", 0);
    let metadata = RegionMetadata::try_from(desc).unwrap();
    metadata.schema().clone()
}

fn new_btree_memtable(schema: RegionSchemaRef) -> BTreeMemtable {
    BTreeMemtable::new(1, schema, None)
}