// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use datatypes::arrow;
use datatypes::arrow::array::{Array, PrimitiveArray, UInt32Array};
use datatypes::arrow::datatypes::Int32Type;
use datatypes::arrow::row::{RowConverter, RowParser, SortField};
use datatypes::data_type::DataType;
use datatypes::prelude::{MutableVector, ScalarVectorBuilder, Value, ValueRef, Vector, VectorRef};
use datatypes::types::{TimestampMillisecondType, TimestampType};
use datatypes::vectors::{
    Helper, UInt32Vector, UInt64Vector, UInt64VectorBuilder, UInt8Vector, UInt8VectorBuilder,
};
use storage::memtable::{
    BatchIterator, BoxedBatchIterator, IterContext, KeyValues, Memtable, MemtableId, MemtableStats,
    RowOrdering,
};
use storage::read::Batch;
use storage::schema::compat::ReadAdapter;
use storage::schema::{ProjectedSchema, ProjectedSchemaRef, RegionSchemaRef};

#[derive(Debug, Default)]
pub struct PlainVectorConfig {}

pub struct PlainVectorMemtable {
    schema: RegionSchemaRef,
    inner: Arc<Series>,
    allocated: AtomicUsize,
    rows: AtomicUsize,
}

impl PlainVectorMemtable {
    pub fn new(schema: RegionSchemaRef) -> Self {
        let series = Arc::new(Series::new(schema.clone()));
        Self {
            schema,
            inner: series,
            allocated: Default::default(),
            rows: Default::default(),
        }
    }
}

impl Memtable for PlainVectorMemtable {
    fn id(&self) -> MemtableId {
        0
    }

    fn schema(&self) -> RegionSchemaRef {
        self.schema.clone()
    }

    fn write(&self, kvs: &KeyValues) -> storage::error::Result<()> {
        let fields = kvs
            .keys
            .iter()
            .map(|v| {
                let arrow_type = v.data_type().as_arrow_type();
                SortField::new(arrow_type)
            })
            .collect::<Vec<_>>();

        let key_cols = kvs
            .keys
            .iter()
            .map(|v| v.to_arrow_array())
            .collect::<Vec<_>>();

        let converter = RowConverter::new(fields).unwrap();
        let rows = converter.convert_columns(&key_cols).unwrap();

        rows.iter().enumerate().for_each(|(idx, row)| {
            let key_bytes = row.as_ref().to_vec();
            let values: Vec<_> = kvs.values.iter().map(|v| v.get(idx)).collect();
            let ts = kvs.timestamp.as_ref().unwrap().get(idx);
            self.inner
                .push(key_bytes, ts, kvs.sequence, kvs.op_type as u8, values);
        });

        self.allocated
            .fetch_add(kvs.estimated_memory_size(), Ordering::Relaxed);
        self.rows.fetch_add(kvs.len(), Ordering::Relaxed);
        Ok(())
    }

    fn iter(&self, ctx: IterContext) -> storage::error::Result<BoxedBatchIterator> {
        self.inner.freeze();
        let iter = IterImpl::new(ctx, self.schema.clone(), self.inner.snapshot());
        Ok(Box::new(iter))
    }

    fn num_rows(&self) -> usize {
        self.rows.load(Ordering::Relaxed)
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

impl Debug for PlainVectorMemtable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "PlainVectorMemtable")
    }
}

struct Series {
    schema: RegionSchemaRef,
    active: RwLock<Option<BTreeMap<Vec<u8>, ValueBuilder>>>,
    frozen: Arc<RwLock<BTreeMap<Vec<u8>, Vec<Values>>>>,
}

struct ValueBuilder {
    ts: Box<dyn MutableVector>,
    sequence: UInt64VectorBuilder,
    op_type: UInt8VectorBuilder,
    fields: Vec<Box<dyn MutableVector>>,
}

#[derive(Clone)]
struct Values {
    ts: VectorRef,
    sequence: Arc<UInt64Vector>,
    op_type: Arc<UInt8Vector>,
    fields: Vec<VectorRef>,
}

impl From<ValueBuilder> for Values {
    fn from(mut value: ValueBuilder) -> Self {
        let fields = value.fields.iter_mut().map(|v| v.to_vector()).collect();

        Self {
            ts: value.ts.to_vector(),
            sequence: Arc::new(value.sequence.finish()),
            op_type: Arc::new(value.op_type.finish()),
            fields,
        }
    }
}

impl Series {
    pub(crate) fn new(schema: RegionSchemaRef) -> Self {
        Self {
            schema,
            active: Default::default(),
            frozen: Default::default(),
        }
    }

    fn build_value_builders(&self) -> ValueBuilder {
        let init_vector_cap = 128;
        let value_vectors: Vec<_> = self
            .schema
            .field_columns()
            .map(|col| col.desc.data_type.create_mutable_vector(init_vector_cap))
            .collect();
        let ts_vector = self
            .schema
            .user_schema()
            .timestamp_column()
            .unwrap()
            .data_type
            .create_mutable_vector(init_vector_cap);
        let sequence = UInt64VectorBuilder::with_capacity(init_vector_cap);
        let op_type = UInt8VectorBuilder::with_capacity(init_vector_cap);
        ValueBuilder {
            ts: ts_vector,
            sequence,
            op_type,
            fields: value_vectors,
        }
    }

    pub(crate) fn push(
        &self,
        key_bytes: Vec<u8>,
        ts: Value,
        sequence: u64,
        op_type: u8,
        values: Vec<Value>,
    ) {
        let mut map = self.active.write().unwrap();

        let builders = map
            .get_or_insert_with(|| Default::default())
            .entry(key_bytes)
            .or_insert_with(|| self.build_value_builders());
        builders.sequence.push_value_ref(ValueRef::UInt64(sequence));
        builders.op_type.push_value_ref(ValueRef::UInt8(op_type));
        builders.ts.push_value_ref(ts.as_value_ref());

        for (builder, value) in builders.fields.iter_mut().zip(values.into_iter()) {
            builder.push_value_ref(value.as_value_ref());
        }
    }

    pub(crate) fn freeze(&self) {
        let mut inner = self.active.write().unwrap();

        if let Some(existing) = inner.take() {
            let mut frozen = self.frozen.write().unwrap();
            for (key, value_builder) in existing.into_iter() {
                frozen
                    .entry(key)
                    .or_default()
                    .push(crate::plain_vector::Values::from(value_builder));
            }
        }
    }

    pub(crate) fn snapshot(&self) -> BTreeMap<Vec<u8>, Vec<Values>> {
        self.freeze();
        let guard = self.frozen.read().unwrap();
        guard.clone()
    }
}

struct IterImpl {
    frozen: BTreeMap<Vec<u8>, Vec<Values>>,
    schema: ProjectedSchemaRef,
    row_converter: RowConverter,
    parser: RowParser,
    read_adaptor: ReadAdapter,
}

impl IterImpl {
    fn new(
        ctx: IterContext,
        schema: RegionSchemaRef,
        frozen: BTreeMap<Vec<u8>, Vec<Values>>,
    ) -> Self {
        let projected_schema = ctx
            .projected_schema
            .clone()
            .unwrap_or_else(|| Arc::new(ProjectedSchema::no_projection(schema.clone())));
        let store_schema = schema.store_schema();
        let adaptor = ReadAdapter::new(store_schema.clone(), projected_schema.clone()).unwrap();

        let columns = store_schema.schema().column_schemas();

        let key_sort_fields = (0..store_schema.row_key_end() - 1)
            .map(|idx| {
                let arrow_type = columns[idx].data_type.as_arrow_type();
                SortField::new(arrow_type)
            })
            .collect();

        let row_converter = RowConverter::new(key_sort_fields).unwrap();
        let parser = row_converter.parser();

        Self {
            frozen,
            schema: projected_schema,
            row_converter,
            parser,
            read_adaptor: adaptor,
        }
    }
}

fn memtable_to_batch(
    row_key: &Vec<u8>,
    values: Values,
    time_series_row_converter: &RowConverter,
    parser: &RowParser,
    read_adaptor: &ReadAdapter,
) -> Option<Batch> {
    // we expected a vec of array with len 1
    let keys = time_series_row_converter
        .convert_rows(vec![parser.parse(row_key)])
        .unwrap();

    let Values {
        ts,
        sequence,
        op_type,
        fields,
    } = values;

    let indices = sort_in_time_series(ts.clone(), sequence.clone(), op_type.clone());

    for key_vector in &keys {
        assert_eq!(1, key_vector.len());
    }

    let col_len = ts.len();
    let repeated_idx: PrimitiveArray<Int32Type> = PrimitiveArray::from_value(0, col_len);

    let mut key_arrs = keys
        .iter()
        .map(|v| {
            let expanded_key = arrow::compute::take(v, &repeated_idx, None).unwrap();
            Helper::try_into_vector(expanded_key).unwrap()
        })
        .collect::<Vec<_>>();

    key_arrs.push(ts.take(&indices).unwrap());

    let fields_sorted = fields
        .into_iter()
        .map(|f| f.take(&indices).unwrap())
        .collect();

    let indices_arr = indices.to_arrow_array();
    let indices_arr = indices_arr.as_any().downcast_ref::<UInt32Array>().unwrap();

    Some(
        read_adaptor
            .batch_from_parts(
                key_arrs,
                fields_sorted,
                Helper::try_into_vector(
                    arrow::compute::take(&sequence.to_arrow_array(), indices_arr, None).unwrap(),
                )
                .unwrap(),
                Helper::try_into_vector(
                    arrow::compute::take(&op_type.to_arrow_array(), indices_arr, None).unwrap(),
                )
                .unwrap(),
            )
            .unwrap(),
    )
}

fn sort_in_time_series(
    ts_vector: VectorRef,
    sequence_vector: Arc<UInt64Vector>,
    op_type_vector: Arc<UInt8Vector>,
) -> UInt32Vector {
    let row_converter = RowConverter::new(vec![
        SortField::new(ts_vector.data_type().as_arrow_type()),
        SortField::new(arrow::datatypes::DataType::UInt64),
        SortField::new(arrow::datatypes::DataType::UInt8),
    ])
    .unwrap();
    let rows = row_converter
        .convert_columns(&[
            ts_vector.to_arrow_array(),
            sequence_vector.to_arrow_array(),
            op_type_vector.to_arrow_array(),
        ])
        .unwrap();

    let mut rows_with_index: Vec<_> = rows.iter().enumerate().collect();
    rows_with_index.sort_unstable_by(|(_, a), (_, b)| a.cmp(b));

    UInt32Vector::from_iter_values(rows_with_index.iter().map(|(idx, _)| *idx as u32))
}

impl Iterator for IterImpl {
    type Item = storage::error::Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        let Some(mut first_key) = self.frozen.first_entry() else {
            return None;
        };
        let key = first_key.key().clone();
        let values = first_key.get_mut();
        if let Some(values) = values.pop() {
            Ok(memtable_to_batch(
                &key,
                values,
                &self.row_converter,
                &self.parser,
                &self.read_adaptor,
            ))
            .transpose()
        } else {
            first_key.remove_entry();
            self.next()
        }
    }
}

impl BatchIterator for IterImpl {
    fn schema(&self) -> ProjectedSchemaRef {
        self.schema.clone()
    }

    fn ordering(&self) -> RowOrdering {
        RowOrdering::Unordered
    }
}
