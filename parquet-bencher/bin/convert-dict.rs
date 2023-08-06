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

    // let src = read_src(&args.input);
    // println!(
    //     "Read src from {} finished, record batches: {}",
    //     args.input,
    //     src.len()
    // );

    // let config_files = read_config_dir(&args.config_dir);
    // for f in config_files {
    //     println!("Config file: {}", f);
    //     let parameter = read_parameters(&f);
    //     let metrics = run_bench_suite_inner(&src, parameter);
    //     println!("metrics: {:?}\n", metrics);
    // }

    convert_to_dict(&args.input, &args.output, args.row_group_size);
}

// fn read_config_dir(config_dir: &str) -> Vec<String> {
//     let mut res = vec![];
//     for file in std::fs::read_dir(config_dir).unwrap() {
//         let file = file.unwrap();
//         if file.file_type().unwrap().is_file() {
//             res.push(file.path().to_str().unwrap().to_string());
//         }
//     }
//     res
// }

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

// fn run_bench_suite_inner(src: &[RecordBatch], parameter: Parameter) -> Metrics {
//     let properties = write_props_from_parameter(&parameter);
//     let temp_file = tempfile::tempfile().unwrap();
//     let file_cloned = temp_file.try_clone().unwrap();
//     let start = std::time::SystemTime::now();

//     let schema = src.get(0).unwrap().schema().clone();
//     let mut writer = ArrowWriter::try_new(temp_file, schema, Some(properties)).unwrap();

//     for rb in src {
//         writer.write(rb).unwrap();
//     }

//     let _file_metadata = writer.close().unwrap();
//     let elapsed = std::time::SystemTime::now().duration_since(start).unwrap();
//     let file_size = file_cloned.metadata().unwrap().len();

//     Metrics {
//         output_size: file_size as usize,
//         elapsed_time: elapsed,
//     }
// }

// fn read_src(src_path: &str) -> Vec<RecordBatch> {
//     let src_file = std::fs::OpenOptions::new()
//         .read(true)
//         .open(src_path)
//         .unwrap();
//     let source_builder = ParquetRecordBatchReaderBuilder::try_new(src_file).unwrap();
//     let mut reader = source_builder.build().unwrap();
//     let mut res = vec![];
//     while let Some(rb) = reader.next() {
//         let batch = rb.unwrap();
//         res.push(batch)
//     }
//     res
// }

// fn read_parameters(path: &str) -> Parameter {
//     let s = std::fs::read_to_string(path).unwrap();
//     toml::from_str(&s).unwrap()
// }

// fn write_props_from_parameter(parameter: &Parameter) -> WriterProperties {
//     let mut builder = WriterProperties::builder();
//     builder = builder.set_write_batch_size(1024 * 32);
//     builder = builder.set_max_row_group_size(parameter.max_row_group_size);

//     for (column, config) in &parameter.column_configs {
//         if let Some(v) = config.encoding {
//             builder = builder.set_column_encoding(ColumnPath::new(vec![column.clone()]), v)
//         }
//         if let Some(v) = config.compression {
//             builder = builder.set_column_compression(ColumnPath::new(vec![column.clone()]), v)
//         }
//     }

//     builder.build()
// }
