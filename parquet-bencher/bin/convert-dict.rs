use std::sync::Arc;

use arrow_array::{Array, DictionaryArray, RecordBatch, RecordBatchReader, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use clap::Parser;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
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
    row_group_size: usize,
}

fn main() {
    let args = Args::parse();
    println!("args: {:?}\n", args);

    convert_to_dict(&args.input, &args.output, args.row_group_size);
}

fn convert_to_dict(input_path: &str, output_path: &str, row_group_size: usize) {
    let input_file = std::fs::OpenOptions::new()
        .read(true)
        .open(input_path)
        .unwrap();
    let input_builder = ParquetRecordBatchReaderBuilder::try_new(input_file).unwrap();
    let mut reader = input_builder.build().unwrap();
    let input_schema = reader.schema();

    let output_file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(output_path)
        .unwrap();
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
    let output_schema = Arc::new(Schema::new(output_fields));
    println!("output schema is {:?}", output_schema);
    let write_props = WriterProperties::builder()
        .set_max_row_group_size(row_group_size)
        .build();
    let mut writer =
        ArrowWriter::try_new(output_file, output_schema.clone(), Some(write_props)).unwrap();

    while let Some(rb) = reader.next() {
        let batch = rb.unwrap();
        let batch = dict_batch(batch, output_schema.clone());

        writer.write(&batch).unwrap();
    }

    writer.close().unwrap();
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
