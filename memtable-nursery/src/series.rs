//! Time series memtable.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use datatypes::arrow;

use datatypes::arrow::row::{RowConverter, Rows, SortField};
use datatypes::prelude::DataType;
use datatypes::scalars::ScalarVector;
use datatypes::types::{TimestampMillisecondType, TimestampType};
use datatypes::vectors::{UInt32Vector, UInt64Vector, UInt8Vector, VectorRef};
use storage::error::Result;
use storage::memtable::{
    BoxedBatchIterator, IterContext, KeyValues, Memtable, MemtableId, MemtableStats,
};
use storage::schema::RegionSchemaRef;

#[derive(Debug, Default)]
pub struct SeriesConfig {}

/// Memtable that organizes data in series.
#[derive(Debug)]
pub struct SeriesMemtable {
    /// Schema of the memtable.
    schema: RegionSchemaRef,
    row_groups: Arc<RowGroupList>,
    series_interner: RwLock<ByteInterner>,
    allocated: AtomicUsize,
}

impl Memtable for SeriesMemtable {
    fn id(&self) -> MemtableId {
        0
    }

    fn schema(&self) -> RegionSchemaRef {
        self.schema.clone()
    }

    fn write(&self, kvs: &KeyValues) -> Result<()> {
        self.write_key_values(kvs);

        self.allocated
            .fetch_add(kvs.estimated_memory_size(), Ordering::Relaxed);

        Ok(())
    }

    fn iter(&self, ctx: IterContext) -> Result<BoxedBatchIterator> {
        assert!(ctx.batch_size > 0);

        unimplemented!()
    }

    fn num_rows(&self) -> usize {
        unimplemented!()
    }

    fn stats(&self) -> MemtableStats {
        MemtableStats {
            estimated_bytes: self.allocated.load(Ordering::Relaxed),
            max_timestamp: TimestampType::Millisecond(TimestampMillisecondType).create_timestamp(0),
            min_timestamp: TimestampType::Millisecond(TimestampMillisecondType).create_timestamp(0),
        }
    }

    fn mark_immutable(&self) {}
}

impl SeriesMemtable {
    /// Returns a new [SeriesMemtable] with specific `schema`.
    pub fn new(schema: RegionSchemaRef, _config: SeriesConfig) -> SeriesMemtable {
        SeriesMemtable {
            schema,
            row_groups: Arc::new(RowGroupList::default()),
            series_interner: RwLock::new(ByteInterner::default()),
            allocated: AtomicUsize::new(0),
        }
    }

    /// Write the `kvs` to the memtable.
    fn write_key_values(&self, kvs: &KeyValues) {
        let row_group = self.sort_key_values(kvs);

        self.row_groups.push(row_group);
    }

    /// Sort elements from key values by key into indices.
    ///
    /// Returns the indices and rows.
    fn lexsort_to_indices(&self, kvs: &KeyValues) -> (UInt32Vector, Rows) {
        let mut fields = kvs
            .keys
            .iter()
            .map(|v| {
                let arrow_type = v.data_type().as_arrow_type();
                SortField::new(arrow_type)
            })
            .collect::<Vec<_>>();

        let mut columns = kvs.keys.iter().map(|v| v.to_arrow_array()).collect::<Vec<_>>();

        if let Some(v) = kvs.timestamp.as_ref() {
            fields.push(SortField::new(v.data_type().as_arrow_type()));
            columns.push(v.to_arrow_array());
        }

        let mut converter = RowConverter::new(fields).unwrap();
        let rows = converter.convert_columns(&columns).unwrap();
        let mut to_sort: Vec<_> = rows.iter().enumerate().collect();
        to_sort.sort_unstable_by(|(_, a), (_, b)| a.cmp(b));

        let indices = UInt32Vector::from_iter_values(to_sort.iter().map(|(i, _)| *i as u32));

        (indices, rows)
    }

    /// Sort key values and returns a sorted row group.
    fn sort_key_values(&self, kvs: &KeyValues) -> RowGroup {
        let (indices, rows) = self.lexsort_to_indices(kvs);

        let timestamp = kvs
            .timestamp
            .as_ref()
            .map(|v| v.take(&indices).unwrap())
            .expect("Require timestamp column");
        let sequence = Arc::new(UInt64Vector::from_values(
            std::iter::repeat(kvs.sequence).take(timestamp.len()),
        ));
        let op_type = Arc::new(UInt8Vector::from_values(
            std::iter::repeat(kvs.op_type.as_u8()).take(timestamp.len()),
        ));
        let values = kvs
            .values
            .iter()
            .map(|v| v.take(&indices).unwrap())
            .collect();

        let key_id = {
            let mut interner = self.series_interner.write().unwrap();
            Arc::new(UInt32Vector::from_iter_values(indices.iter_data().map(
                |v| {
                    let row_idx = v.unwrap();
                    let row = rows.row(row_idx as usize);
                    interner.get_or_intern(row.as_ref())
                },
            )))
        };

        RowGroup {
            key_id,
            sequence,
            op_type,
            timestamp,
            values,
        }
    }
}

#[derive(Debug, Default)]
struct ByteInterner {
    map: HashMap<Vec<u8>, u32>,
    next_id: u32,
}

impl ByteInterner {
    fn get_or_intern(&mut self, value: &[u8]) -> u32 {
        if let Some(id) = self.map.get(value) {
            return *id;
        }

        let id = self.next_id;
        self.next_id += 1;
        self.map.insert(value.to_vec(), id);
        id
    }
}

/// A group of rows in the memtable.
#[derive(Debug, Clone)]
struct RowGroup {
    key_id: VectorRef,
    sequence: VectorRef,
    op_type: VectorRef,
    timestamp: VectorRef,
    values: Vec<VectorRef>,
}

impl RowGroup {
    /// Returns the number of rows in the row group.
    fn num_rows(&self) -> usize {
        self.timestamp.len()
    }
}

/// List of inserted row groups.
#[derive(Debug, Default)]
struct RowGroupList {
    row_groups: RwLock<Vec<RowGroup>>,
    total_rows: AtomicUsize,
}

impl RowGroupList {
    /// Push back the row group.
    fn push(&self, row_group: RowGroup) {
        let mut row_groups = self.row_groups.write().unwrap();
        let num = row_group.num_rows();
        row_groups.push(row_group);
        self.total_rows.fetch_add(num, Ordering::Relaxed);
    }

    /// List all row groups.
    fn list(&self) -> Vec<RowGroup> {
        self.row_groups.read().unwrap().clone()
    }

    /// Returns the number of rows.
    fn num_rows(&self) -> usize {
        self.total_rows.load(Ordering::Relaxed)
    }
}
