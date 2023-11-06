use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BinaryArray, DictionaryArray, RecordBatch, RecordBatchReader, UInt32Array,
};
use arrow_row::{RowConverter, SortField};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use clap::Parser;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::ZstdLevel;
use parquet::file::properties::WriterProperties;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The input parquet file.
    #[arg(short, long)]
    input: String,
    /// The output parquet file.
    #[arg(short, long)]
    output: String,
    /// Output row group size.
    #[arg(long)]
    row_group_size: Option<usize>,
    /// Convert tags to primary key.
    #[arg(long, default_value_t = false)]
    to_pk: bool,
    /// Use dictionary for primary key.
    #[arg(long, default_value_t = false)]
    enable_pk_dict: bool,
    /// Does nothing to each batch.
    #[arg(long, default_value_t = false)]
    raw: bool,
    /// Output data page size.
    #[arg(long)]
    data_page_size: Option<usize>,
    /// Enable Zstd compression.
    #[arg(long, default_value_t = false)]
    zstd: bool,
}

fn main() {
    let args = Args::parse();
    println!("args: {:?}\n", args);

    convert_to_dict(&args);
}

fn convert_to_dict(args: &Args) {
    let input_path = &args.input;
    let output_path = &args.output;
    let row_group_size = args.row_group_size;
    let data_page_size = args.data_page_size;

    let input_file = std::fs::OpenOptions::new()
        .read(true)
        .open(input_path)
        .unwrap();
    let input_builder = ParquetRecordBatchReaderBuilder::try_new(input_file).unwrap();
    let key_value_metadata = input_builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .cloned();
    let mut reader = input_builder.build().unwrap();
    let input_schema = reader.schema();

    let output_file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(output_path)
        .unwrap();
    let output_schema = if args.to_pk {
        pk_schema(&input_schema, args.enable_pk_dict)
    } else if args.raw {
        input_schema.clone()
    } else {
        dict_schema(&input_schema)
    };
    println!(
        "input_schema is {:?}, output schema is {:?}",
        input_schema, output_schema
    );
    let mut builder = WriterProperties::builder().set_key_value_metadata(key_value_metadata);
    if let Some(value) = row_group_size {
        println!("output row group size: {}", value);
        builder = builder.set_max_row_group_size(value);
    }
    if let Some(value) = data_page_size {
        println!("output data page size: {}", value);
        builder = builder.set_data_page_size_limit(value);
    }
    if args.zstd {
        println!("Use zstd compression");
        builder = builder.set_compression(parquet::basic::Compression::ZSTD(ZstdLevel::default()));
    }
    let write_props = builder.build();
    let mut writer =
        ArrowWriter::try_new(output_file, output_schema.clone(), Some(write_props)).unwrap();

    while let Some(rb) = reader.next() {
        let batch = rb.unwrap();
        let batch = if args.to_pk {
            pk_batch(batch, output_schema.clone(), args.enable_pk_dict)
        } else if args.raw {
            batch
        } else {
            dict_batch(batch, output_schema.clone())
        };

        writer.write(&batch).unwrap();
    }

    writer.close().unwrap();
}

fn dict_schema(input_schema: &SchemaRef) -> SchemaRef {
    let output_fields: Vec<_> = input_schema
        .fields
        .iter()
        .map(|field| {
            if *field.data_type() == DataType::Utf8 {
                Arc::new(Field::new_dictionary(
                    field.name(),
                    DataType::UInt32,
                    field.data_type().clone(),
                    field.is_nullable(),
                ))
            } else {
                field.clone()
            }
        })
        .collect();
    Arc::new(Schema::new(output_fields))
}

fn pk_schema(input_schema: &SchemaRef, enable_pk_dict: bool) -> SchemaRef {
    let mut output_fields = vec![];
    if enable_pk_dict {
        output_fields.push(Arc::new(Field::new_dictionary(
            "pk",
            DataType::UInt32,
            DataType::Binary,
            false,
        )));
    } else {
        output_fields.push(Arc::new(Field::new("pk", DataType::Binary, false)));
    }

    for field in &input_schema.fields {
        if *field.data_type() == DataType::Utf8 {
            continue;
        }
        output_fields.push(field.clone());
    }

    Arc::new(Schema::new(output_fields))
}

fn dict_batch(batch: RecordBatch, output_schema: SchemaRef) -> RecordBatch {
    let columns = batch
        .columns()
        .into_iter()
        .map(|array| {
            if *array.data_type() == DataType::Utf8 {
                let keys_array = UInt32Array::from_iter_values((0..array.len() as u32).into_iter());
                let dict_array = DictionaryArray::new(keys_array, array.clone());
                Arc::new(dict_array)
            } else {
                array.clone()
            }
        })
        .collect();
    RecordBatch::try_new(output_schema, columns).unwrap()
}

fn pk_batch(batch: RecordBatch, output_schema: SchemaRef, enable_pk_dict: bool) -> RecordBatch {
    let key_columns: Vec<_> = batch
        .columns()
        .iter()
        .filter(|array| *array.data_type() == DataType::Utf8)
        .cloned()
        .collect();
    let sort_field = SortField::new(DataType::Utf8);

    let converter = RowConverter::new(vec![sort_field; key_columns.len()]).unwrap();
    let rows = converter.convert_columns(&key_columns).unwrap();

    let mut pk_array: ArrayRef = Arc::new(BinaryArray::from_iter_values(rows.iter()));
    if enable_pk_dict {
        let keys_array = UInt32Array::from_iter_values((0..pk_array.len() as u32).into_iter());
        let dict_array = DictionaryArray::new(keys_array, pk_array.clone());
        pk_array = Arc::new(dict_array);
    }

    let columns = [pk_array]
        .into_iter()
        .chain(
            batch
                .columns()
                .iter()
                .filter(|array| *array.data_type() != DataType::Utf8)
                .cloned(),
        )
        .collect();

    RecordBatch::try_new(output_schema, columns).unwrap()
}
