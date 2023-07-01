//! Columnar memtable.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use datatypes::arrow::array::{ArrayRef, DictionaryArray, StringArray, UInt32Array};
use datatypes::arrow::datatypes::UInt32Type;
use datatypes::prelude::ConcreteDataType;
use datatypes::scalars::ScalarVector;
use datatypes::types::{TimestampMillisecondType, TimestampType};
use datatypes::vectors::{Helper, StringVector, UInt64Vector, UInt8Vector, VectorRef};
use storage::error::Result;
use storage::memtable::{
    BatchIterator, BoxedBatchIterator, IterContext, KeyValues, Memtable, MemtableId, MemtableStats,
    RowOrdering,
};
use storage::read::Batch;
use storage::schema::compat::ReadAdapter;
use storage::schema::{ProjectedSchema, ProjectedSchemaRef, RegionSchemaRef};
use string_interner::{DefaultSymbol, StringInterner, Symbol};

/// Config for columnar memtable.
#[derive(Debug, Default, Clone)]
pub struct ColumnarConfig {
    /// Use dictionary array to scan.
    pub use_dict: bool,
}

/// Memtable that organizes data in columnar form.
#[derive(Debug)]
pub struct ColumnarMemtable {
    /// Schema of the memtable.
    schema: RegionSchemaRef,
    interners: Arc<ColumnInterners>,
    row_groups: Arc<RowGroupList>,
    allocated: AtomicUsize,
    config: ColumnarConfig,
}

impl ColumnarMemtable {
    /// Returns a new [ColumnarMemtable] with specific `schema`.
    pub fn new(schema: RegionSchemaRef, config: ColumnarConfig) -> ColumnarMemtable {
        let interners = Arc::new(ColumnInterners::new(&schema));

        ColumnarMemtable {
            schema,
            interners,
            row_groups: Arc::new(RowGroupList::default()),
            allocated: AtomicUsize::new(0),
            config,
        }
    }

    /// Write the `kvs` to the memtable.
    fn write_key_values(&self, kvs: &KeyValues) {
        let row_group = self.interners.intern(kvs);

        self.row_groups.push(row_group);
    }
}

impl Memtable for ColumnarMemtable {
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

        let iter = IterImpl::new(
            self.config.clone(),
            ctx,
            self.schema.clone(),
            self.interners.clone(),
            self.row_groups.clone(),
        )?;

        Ok(Box::new(iter))
    }

    fn num_rows(&self) -> usize {
        self.row_groups.num_rows()
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

/// Structure to intern columns.
#[derive(Debug)]
struct ColumnInterners {
    /// Optional interner for each key column.
    key_interners: Vec<Option<KeyInterner>>,
}

impl ColumnInterners {
    /// Returns a new [ColumnInterners] with specific `schema`.
    fn new(schema: &RegionSchemaRef) -> ColumnInterners {
        let key_interners = schema
            .row_key_columns()
            .map(|column_schema| {
                if let ConcreteDataType::String(_) = column_schema.desc.data_type {
                    Some(KeyInterner::default())
                } else {
                    None
                }
            })
            .collect();

        ColumnInterners { key_interners }
    }

    /// Intern the key columns and returns the [RowGroup].
    fn intern(&self, kvs: &KeyValues) -> RowGroup {
        // Number of keys and timestamp should equals to number of interners.
        assert_eq!(kvs.keys.len() + 1, self.key_interners.len());
        let keys = kvs
            .keys
            .iter()
            .zip(self.key_interners.iter())
            .map(|(key_column, interner_opt)| {
                if let Some(interner) = interner_opt {
                    MaybeInternedVector::Interned(interner.intern(key_column))
                } else {
                    MaybeInternedVector::Raw(key_column.clone())
                }
            })
            .collect();

        let timestamp = kvs.timestamp.clone().expect("Require timestamp column");
        let sequence = Arc::new(UInt64Vector::from_values(
            std::iter::repeat(kvs.sequence).take(timestamp.len()),
        ));
        let op_type = Arc::new(UInt8Vector::from_values(
            std::iter::repeat(kvs.op_type.as_u8()).take(timestamp.len()),
        ));

        RowGroup {
            keys,
            sequence,
            op_type,
            timestamp,
            values: kvs.values.clone(),
        }
    }

    /// Resolve list of [MaybeInternedVector] to vectors.
    fn resolve_keys(&self, keys: &[MaybeInternedVector], use_dict: bool) -> Vec<VectorRef> {
        assert_eq!(keys.len() + 1, self.key_interners.len());
        keys.iter()
            .zip(self.key_interners.iter())
            .map(|(may_interned, interner_opt)| {
                match may_interned {
                    MaybeInternedVector::Raw(vector) => vector.clone(),
                    MaybeInternedVector::Interned(symbols) => {
                        // Safety: this column is interned so the key interner must exists.
                        let interner = interner_opt.as_ref().unwrap();
                        if use_dict {
                            interner.resolve_as_dict(symbols)
                        } else {
                            interner.resolve(symbols)
                        }
                    }
                }
            })
            .collect()
    }

    /// Resolve row group to batch.
    fn resolve(
        &self,
        row_group: &RowGroup,
        adapter: &ReadAdapter,
        use_dict: bool,
    ) -> Result<Batch> {
        let mut keys = self.resolve_keys(&row_group.keys, use_dict);
        keys.push(row_group.timestamp.clone());

        let keys = keys
            .iter()
            .zip(adapter.source_key_needed().iter())
            .filter_map(|(key, is_needed)| if *is_needed { Some(key) } else { None })
            .cloned()
            .collect();
        let values = row_group
            .values
            .iter()
            .zip(adapter.source_value_needed().iter())
            .filter_map(
                |(value, is_needed)| {
                    if *is_needed {
                        Some(value)
                    } else {
                        None
                    }
                },
            )
            .cloned()
            .collect();
        adapter.batch_from_parts(
            keys,
            values,
            row_group.sequence.clone(),
            row_group.op_type.clone(),
        )
    }
}

/// Key column interner.
///
/// Currently, this interner only supports string type.
#[derive(Debug, Default)]
struct KeyInterner {
    interner: RwLock<StringInterner>,
    cache: RwLock<Option<StringArray>>,
}

impl KeyInterner {
    /// Intern the vector. The vector must have String type.
    fn intern(&self, vector: &VectorRef) -> Vec<Option<DefaultSymbol>> {
        let string_vector = vector.as_any().downcast_ref::<StringVector>().unwrap();
        let mut interner = self.interner.write().unwrap();
        string_vector
            .iter_data()
            .map(|key_opt| key_opt.map(|key| interner.get_or_intern(key)))
            .collect()
    }

    /// Resolve symbols to vector.
    fn resolve(&self, symbols: &[Option<DefaultSymbol>]) -> VectorRef {
        let interner = self.interner.read().unwrap();
        let values: Vec<_> = symbols
            .iter()
            .map(|symbol_opt| {
                symbol_opt.map(|symbol| {
                    // Safety: the symbol should be interned.
                    interner.resolve(symbol).unwrap()
                })
            })
            .collect();

        Arc::new(StringVector::from(values))
    }

    /// Resolve symbols to vector by dict.
    fn resolve_as_dict(&self, symbols: &[Option<DefaultSymbol>]) -> VectorRef {
        // Currently, we don't support dictionary vector, so we create a dictionary
        // array but returns its keys only.
        let dict_array = self.resolve_to_dict_array(symbols);
        let keys: ArrayRef = Arc::new(dict_array.keys().clone());
        Helper::try_into_vector(keys).unwrap()
    }

    /// Resolve symbols to dictionary array.
    fn resolve_to_dict_array(
        &self,
        symbols: &[Option<DefaultSymbol>],
    ) -> DictionaryArray<UInt32Type> {
        let values = self.interned_values();
        let keys = UInt32Array::from_iter(symbols.iter().map(|symbol_opt| {
            symbol_opt.map(|symbol| {
                // Safety: DefaultSymbol is SymbolU32.
                symbol.to_usize() as u32
            })
        }));

        DictionaryArray::new(keys, Arc::new(values))
    }

    /// Return array of values.
    fn interned_values(&self) -> StringArray {
        {
            if let Some(cache) = self.cache.read().unwrap().as_ref() {
                return cache.clone();
            }
        }

        let interner = self.interner.read().unwrap();
        let array =
            StringArray::from_iter_values(interner.into_iter().map(|(_symbol, value)| value));
        *self.cache.write().unwrap() = Some(array.clone());
        array
    }
}

/// A vector that may be interned.
#[derive(Debug, Clone)]
enum MaybeInternedVector {
    /// A vector that has not been interned.
    Raw(VectorRef),
    /// An interned vector.
    Interned(Vec<Option<DefaultSymbol>>),
}

/// A group of rows in the memtable.
#[derive(Debug, Clone)]
struct RowGroup {
    keys: Vec<MaybeInternedVector>,
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

/// Iterator implementation.
struct IterImpl {
    config: ColumnarConfig,
    ctx: IterContext,
    /// Projected schema that user expect to read.
    projected_schema: ProjectedSchemaRef,
    adapter: ReadAdapter,

    interners: Arc<ColumnInterners>,
    row_groups: Vec<RowGroup>,

    // Buffered batch.
    batch: Option<Batch>,
}

impl IterImpl {
    /// Returns a new [IterImpl].
    fn new(
        config: ColumnarConfig,
        ctx: IterContext,
        schema: RegionSchemaRef,
        interners: Arc<ColumnInterners>,
        row_groups: Arc<RowGroupList>,
    ) -> Result<IterImpl> {
        let projected_schema = ctx
            .projected_schema
            .clone()
            .unwrap_or_else(|| Arc::new(ProjectedSchema::no_projection(schema.clone())));
        let adapter = ReadAdapter::new(schema.store_schema().clone(), projected_schema.clone())?;

        Ok(IterImpl {
            config,
            ctx,
            projected_schema,
            adapter,
            interners,
            row_groups: row_groups.list(),
            batch: None,
        })
    }

    /// Returns next batch of rows.
    fn next_batch(&mut self) -> Result<Option<Batch>> {
        while !self.is_batch_enough() {
            if let Some(row_group) = self.row_groups.pop() {
                let batch =
                    self.interners
                        .resolve(&row_group, &self.adapter, self.config.use_dict)?;
                if let Some(buffered) = self.batch.take() {
                    self.batch = Some(concat_batch(&buffered, &batch));
                } else {
                    self.batch = Some(batch);
                }
            } else {
                // No row group any more.
                break;
            }
        }

        let Some(batch) = self.batch.take() else {
            return Ok(None);
        };
        if batch.num_rows() > self.ctx.batch_size {
            let sliced = batch.slice(0, self.ctx.batch_size);
            self.batch =
                Some(batch.slice(self.ctx.batch_size, batch.num_rows() - self.ctx.batch_size));
            Ok(Some(sliced))
        } else {
            self.batch = None;
            Ok(Some(batch))
        }
    }

    /// Returns true if the row number of the batch is large enough.
    fn is_batch_enough(&self) -> bool {
        if let Some(batch) = &self.batch {
            batch.num_rows() >= self.ctx.batch_size
        } else {
            false
        }
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
        self.next_batch().transpose()
    }
}

/// Concat two batches.
fn concat_batch(left: &Batch, right: &Batch) -> Batch {
    let columns = left
        .columns
        .iter()
        .zip(right.columns.iter())
        .map(|(vl, vr)| concat_vector(vl, vr))
        .collect();
    Batch::new(columns)
}

/// Concat two vectors.
fn concat_vector(left: &VectorRef, right: &VectorRef) -> VectorRef {
    let left_array = left.to_arrow_array();
    let right_array = right.to_arrow_array();

    let array = datatypes::arrow::compute::concat(&[&left_array, &right_array]).unwrap();
    Helper::try_into_vector(array).unwrap()
}

#[cfg(test)]
mod tests {
    use string_interner::Symbol;

    use super::*;

    #[test]
    fn test_string_interner() {
        let mut interner = StringInterner::default();
        let a = interner.get_or_intern("a");
        assert_eq!(0, a.to_usize());
        let b = interner.get_or_intern("b");
        assert_eq!(1, b.to_usize());

        let keys: Vec<_> = interner.into_iter().collect();
        assert_eq!(2, keys.len());
        assert_eq!((0, "a"), (keys[0].0.to_usize(), keys[0].1));
        assert_eq!((1, "b"), (keys[1].0.to_usize(), keys[1].1));
    }
}
