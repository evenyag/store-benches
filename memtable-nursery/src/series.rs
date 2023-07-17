//! Time series memtable.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, RwLock};

use common_base::bytes::Bytes;
use datatypes::arrow::array::{Array, ArrayRef};
use datatypes::arrow::compute::interleave;
use datatypes::arrow::row::{RowConverter, Rows, SortField};
use datatypes::prelude::DataType;
use datatypes::scalars::ScalarVector;
use datatypes::types::{TimestampMillisecondType, TimestampType};
use datatypes::value::Value;
use datatypes::vectors::{Helper, UInt32Vector, UInt64Vector, UInt8Vector, VectorRef};
use storage::error::Result;
use storage::memtable::{
    BatchIterator, BoxedBatchIterator, IterContext, KeyValues, Memtable, MemtableId, MemtableStats,
    RowOrdering,
};
use storage::read::Batch;
use storage::schema::compat::ReadAdapter;
use storage::schema::{ProjectedSchema, ProjectedSchemaRef, RegionSchemaRef};

#[derive(Debug, Default)]
pub struct SeriesConfig {}

/// Memtable that organizes data in series.
#[derive(Debug)]
pub struct SeriesMemtable {
    /// Schema of the memtable.
    schema: RegionSchemaRef,
    row_groups: Arc<RowGroupList>,
    series_interner: Arc<RwLock<ByteInterner>>,
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
            .fetch_add(kvs.estimated_memory_size(), AtomicOrdering::Relaxed);

        Ok(())
    }

    fn iter(&self, ctx: IterContext) -> Result<BoxedBatchIterator> {
        assert!(ctx.batch_size > 0);

        let iter = IterImpl::new(
            self.row_groups.list(),
            ctx,
            self.series_interner.clone(),
            self.schema.clone(),
        )?;

        Ok(Box::new(iter))
    }

    fn num_rows(&self) -> usize {
        self.row_groups.num_rows()
    }

    fn stats(&self) -> MemtableStats {
        MemtableStats {
            estimated_bytes: self.allocated.load(AtomicOrdering::Relaxed),
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
            series_interner: Arc::new(RwLock::new(ByteInterner::default())),
            allocated: AtomicUsize::new(0),
        }
    }

    /// Write the `kvs` to the memtable.
    fn write_key_values(&self, kvs: &KeyValues) {
        let row_group = self.sort_key_values(kvs);

        assert!(row_group.num_rows() > 0);

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

        let mut columns = kvs
            .keys
            .iter()
            .map(|v| v.to_arrow_array())
            .collect::<Vec<_>>();

        if let Some(v) = kvs.timestamp.as_ref() {
            fields.push(SortField::new(v.data_type().as_arrow_type()));
            columns.push(v.to_arrow_array());
        }

        let mut converter = RowConverter::new(fields).unwrap();
        let rows = converter.convert_columns(&columns).unwrap();
        let mut to_sort: Vec<_> = rows
            .iter()
            .enumerate()
            .map(|(index, key)| {
                (
                    index,
                    (key, u64::MAX - kvs.sequence, u8::MAX - kvs.op_type.as_u8()),
                )
            })
            .collect();
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
                    // Skip the last timestamp.
                    let series_key = &row.as_ref()[..row.as_ref().len() - 9];
                    interner.get_or_intern(series_key)
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
    map: HashMap<Bytes, u32>,
    data: HashMap<u32, Bytes>,
    next_id: u32,
}

impl ByteInterner {
    fn get_or_intern(&mut self, value: &[u8]) -> u32 {
        if let Some(id) = self.map.get(value) {
            return *id;
        }

        let id = self.next_id;
        self.next_id += 1;
        let bytes: Bytes = value.into();
        self.map.insert(bytes.clone(), id);
        self.data.insert(id, bytes);
        id
    }

    fn get(&self, symbol: u32) -> Option<Bytes> {
        self.data.get(&symbol).cloned()
    }
}

/// A group of rows in the memtable.
#[derive(Debug, Clone)]
struct RowGroup {
    key_id: Arc<UInt32Vector>,
    sequence: Arc<UInt64Vector>,
    op_type: Arc<UInt8Vector>,
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
        self.total_rows.fetch_add(num, AtomicOrdering::Relaxed);
    }

    /// List all row groups.
    fn list(&self) -> Vec<RowGroup> {
        self.row_groups.read().unwrap().clone()
    }

    /// Returns the number of rows.
    fn num_rows(&self) -> usize {
        self.total_rows.load(AtomicOrdering::Relaxed)
    }
}

/// A `Node` represent an individual input row group to be merged.
struct Node {
    /// Current batch to be read.
    ///
    /// `None` means the `source` has reached EOF.
    cursor: Option<BatchCursor>,
}

impl Node {
    fn new(id: usize, row_group: RowGroup, interner: Arc<RwLock<ByteInterner>>) -> Node {
        let byte_interner = interner.read().unwrap();
        let cursor = Some(BatchCursor::new(id, row_group, &byte_interner));
        Node { cursor }
    }
}

impl Node {
    /// Returns true if no more batch could be fetched from this node.
    fn is_eof(&self) -> bool {
        self.cursor.is_none()
    }

    /// Compare first row of two nodes.
    ///
    /// # Panics
    /// Panics if
    /// - either `self` or `other` is EOF.
    fn compare_first_row(&self, other: &Node) -> Ordering {
        self.cursor
            .as_ref()
            .unwrap()
            .first_row()
            .cmp(other.cursor.as_ref().unwrap().first_row())
    }

    /// Push next row from `self` to the `builder`.
    ///
    /// # Panics
    /// Panics if `self` is EOF.
    fn push_next_row_to(&mut self, builder: &mut BatchBuilder) {
        let cursor = self.cursor.as_mut().unwrap();
        cursor.push_next_row_to(builder);

        if cursor.pos == cursor.batch.num_rows() {
            self.cursor = None;
        }
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Node) -> bool {
        self.compare_first_row(other) == Ordering::Equal
    }
}

impl Eq for Node {}

impl PartialOrd for Node {
    fn partial_cmp(&self, other: &Node) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Node {
    fn cmp(&self, other: &Node) -> Ordering {
        // The std binary heap is a max heap, but we want the nodes are ordered in
        // ascend order, so we compare the nodes in reverse order.
        other.compare_first_row(self)
    }
}

// Order by key asc, ts asc, seq desc, op type desc
#[derive(Debug, Clone, PartialEq, Eq)]
struct RowKey(Bytes, Value, u64, u8);

impl PartialOrd for RowKey {
    fn partial_cmp(&self, other: &RowKey) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RowKey {
    fn cmp(&self, other: &RowKey) -> Ordering {
        self.0
            .cmp(&other.0)
            .then_with(|| self.1.cmp(&other.1))
            .then_with(|| other.2.cmp(&self.2))
            .then_with(|| other.3.cmp(&self.3))
    }
}

/// A `BatchCursor` wraps the `RowGroup` and allows reading the `RowGroup` by row.
#[derive(Debug)]
struct BatchCursor {
    id: usize,
    /// Current buffered `RowGroup`.
    ///
    /// `RowGroup` must contains at least one row.
    batch: RowGroup,
    /// Index of current row.
    ///
    /// `pos == batch.num_rows()` indicates no more rows to read.
    pos: usize,
    keys: Vec<RowKey>,
}

impl BatchCursor {
    /// Create a new `BatchCursor`.
    ///
    /// # Panics
    /// Panics if `batch` is empty.
    fn new(id: usize, batch: RowGroup, interner: &ByteInterner) -> BatchCursor {
        assert!(!batch.timestamp.is_empty());

        let mut keys = Vec::with_capacity(batch.num_rows());
        for pos in 0..batch.num_rows() {
            let key_id = batch.key_id.get_data(pos).unwrap();
            let timestamp = batch.timestamp.get(pos);
            let seq = batch.sequence.get_data(pos).unwrap();
            let op_type = batch.op_type.get_data(pos).unwrap();
            let key = interner.get(key_id).unwrap();

            keys.push(RowKey(key, timestamp, seq, op_type));
        }

        BatchCursor {
            id,
            batch,
            pos: 0,
            keys,
        }
    }

    /// Returns true if there are remaining rows to read.
    #[inline]
    fn is_valid(&self) -> bool {
        !self.is_empty()
    }

    /// Returns first row of current batch.
    ///
    /// # Panics
    /// Panics if `self` is invalid.
    fn first_row(&self) -> &RowKey {
        assert!(self.is_valid());

        &self.keys[self.pos]
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.pos >= self.batch.num_rows()
    }

    /// Push next row from `self` to the `builder` and advance the cursor.
    ///
    /// # Panics
    /// Panics if `self` is invalid.
    fn push_next_row_to(&mut self, builder: &mut BatchBuilder) {
        builder.0.push(self.keys[self.pos].clone());
        builder.1.push((self.id, self.pos));
        self.pos += 1;
    }
}

struct IterImpl {
    reader: MergeReader,
    adapter: ReadAdapter,
    projected_schema: ProjectedSchemaRef,
}

impl IterImpl {
    fn new(
        row_groups: Vec<RowGroup>,
        ctx: IterContext,
        interner: Arc<RwLock<ByteInterner>>,
        schema: RegionSchemaRef,
    ) -> Result<IterImpl> {
        let projected_schema = ctx
            .projected_schema
            .clone()
            .unwrap_or_else(|| Arc::new(ProjectedSchema::no_projection(schema.clone())));
        let adapter = ReadAdapter::new(schema.store_schema().clone(), projected_schema.clone())?;
        let reader = MergeReader::new(row_groups, ctx.batch_size, interner, schema);

        Ok(IterImpl {
            reader,
            adapter,
            projected_schema,
        })
    }

    fn next_batch(&mut self) -> Option<Batch> {
        self.reader.fetch_next_batch(&self.adapter)
    }
}

impl BatchIterator for IterImpl {
    fn schema(&self) -> ProjectedSchemaRef {
        self.projected_schema.clone()
    }

    fn ordering(&self) -> RowOrdering {
        RowOrdering::Unordered
    }
}

impl Iterator for IterImpl {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Result<Batch>> {
        self.next_batch().map(Ok)
    }
}

type BatchBuilder = (Vec<RowKey>, Vec<(usize, usize)>);

/// A reader that would sort and merge `Batch` from multiple sources by key.
///
/// `Batch` from each `Source` **must** be sorted.
struct MergeReader {
    /// Nodes to merge.
    nodes: BinaryHeap<Node>,
    /// Suggested row number of each batch.
    ///
    /// The size of the batch yield from this reader may not always equal to this suggested size.
    batch_size: usize,
    /// Buffered batch.
    batch_builder: BatchBuilder,

    converter: RowConverter,
    timestamps: Vec<ArrayRef>,
    values: Vec<Vec<ArrayRef>>,
}

impl MergeReader {
    fn new(
        row_groups: Vec<RowGroup>,
        batch_size: usize,
        interner: Arc<RwLock<ByteInterner>>,
        schema: RegionSchemaRef,
    ) -> MergeReader {
        let mut timestamps = Vec::with_capacity(row_groups.len());
        let mut values = vec![Vec::with_capacity(row_groups.len()); row_groups[0].values.len()];

        for row_group in &row_groups {
            timestamps.push(row_group.timestamp.to_arrow_array());

            for i in 0..values.len() {
                values[i].push(row_group.values[i].to_arrow_array());
            }
        }

        let mut fields: Vec<_> = schema
            .row_key_columns()
            .map(|col| {
                let arrow_type = col.desc.data_type.as_arrow_type();
                SortField::new(arrow_type)
            })
            .collect();
        // Remove the last timestamp column.
        fields.pop();

        let nodes = row_groups
            .into_iter()
            .enumerate()
            .map(|(id, row_group)| Node::new(id, row_group, interner.clone()))
            .collect();

        MergeReader {
            nodes,
            batch_size,
            batch_builder: (
                Vec::with_capacity(batch_size),
                Vec::with_capacity(batch_size),
            ),
            converter: RowConverter::new(fields).unwrap(),
            timestamps,
            values,
        }
    }

    fn fetch_next_batch(&mut self, adapter: &ReadAdapter) -> Option<Batch> {
        while !self.nodes.is_empty() && self.batch_builder.0.len() < self.batch_size {
            self.fetch_next_row();
        }

        if self.batch_builder.0.is_empty() {
            None
        } else {
            self.build_batch(adapter)
        }
    }

    fn build_batch(&mut self, adapter: &ReadAdapter) -> Option<Batch> {
        let parser = self.converter.parser();
        let rows: Vec<_> = self
            .batch_builder
            .0
            .iter()
            .map(|v| parser.parse(&v.0))
            .collect();
        let keys = self.converter.convert_rows(rows).unwrap();
        let mut keys: Vec<_> = keys
            .iter()
            .map(|k| Helper::try_into_vector(k).unwrap())
            .collect();

        let timestamps = {
            let column_array = Self::vec_array_ref_to_vec_dyn(&self.timestamps);
            let new_array = interleave(&column_array, &self.batch_builder.1).unwrap();
            Helper::try_into_vector(new_array).unwrap()
        };
        keys.push(timestamps);

        let values: Vec<_> = self
            .values
            .iter()
            .map(|column| {
                let column_array = Self::vec_array_ref_to_vec_dyn(column);
                let new_array = interleave(&column_array, &self.batch_builder.1).unwrap();
                Helper::try_into_vector(new_array).unwrap()
            })
            .collect();

        let sequences = Arc::new(UInt64Vector::from_values(
            self.batch_builder.0.iter().map(|row_key| row_key.2),
        ));
        let op_types = Arc::new(UInt8Vector::from_values(
            self.batch_builder.0.iter().map(|row_key| row_key.3),
        ));

        self.batch_builder.0.clear();
        self.batch_builder.1.clear();

        Some(
            adapter
                .batch_from_parts(keys, values, sequences, op_types)
                .unwrap(),
        )
    }

    fn vec_array_ref_to_vec_dyn(input: &Vec<ArrayRef>) -> Vec<&dyn Array> {
        input.iter().map(|v| &**v).collect()
    }

    fn fetch_next_row(&mut self) {
        let mut node = self.nodes.pop().unwrap();
        node.push_next_row_to(&mut self.batch_builder);

        if !node.is_eof() {
            self.nodes.push(node)
        }
    }
}
