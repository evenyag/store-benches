use arrow_array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;

use crate::bench_result::Metrics;
use crate::parameter::Parameter;
use clap::Parser;


mod bench_result;
mod errors;
mod parameter;

fn main() {
    let args = Args::parse();
    println!("args: {:?}\n", args);

    let src = read_src(&args.source);
    println!(
        "Read src from {} finished, record batches: {}",
        args.source,
        src.len()
    );

    let config_files = read_config_dir(&args.config_dir);
    for f in config_files {
        println!("Config file: {}", f);
        let parameter = read_parameters(&f);
        let metrics = run_bench_suite_inner(&src, parameter);
        println!("metrics: {:?}\n", metrics);
    }
}

fn read_config_dir(config_dir: &str) -> Vec<String> {
    let mut res = vec![];
    for file in std::fs::read_dir(config_dir).unwrap() {
        let file = file.unwrap();
        if file.file_type().unwrap().is_file() {
            res.push(file.path().to_str().unwrap().to_string());
        }
    }
    res
}

fn run_bench_suite_inner(src: &[RecordBatch], parameter: Parameter) -> Metrics {
    let properties = write_props_from_parameter(&parameter);
    let temp_file = tempfile::tempfile().unwrap();
    let file_cloned = temp_file.try_clone().unwrap();
    let start = std::time::SystemTime::now();

    let schema = src.get(0).unwrap().schema().clone();
    let mut writer = ArrowWriter::try_new(temp_file, schema, Some(properties)).unwrap();

    for rb in src {
        writer.write(rb).unwrap();
    }

    let _file_metadata = writer.close().unwrap();
    let elapsed = std::time::SystemTime::now().duration_since(start).unwrap();
    let file_size = file_cloned.metadata().unwrap().len();

    Metrics {
        output_size: file_size as usize,
        elapsed_time: elapsed,
    }
}

fn read_src(src_path: &str) -> Vec<RecordBatch> {
    let src_file = std::fs::OpenOptions::new()
        .read(true)
        .open(src_path)
        .unwrap();
    let source_builder = ParquetRecordBatchReaderBuilder::try_new(src_file).unwrap();
    let mut reader = source_builder.build().unwrap();
    let mut res = vec![];
    while let Some(rb) = reader.next() {
        let batch = rb.unwrap();
        res.push(batch)
    }
    res
}

fn read_parameters(path: &str) -> Parameter {
    let s = std::fs::read_to_string(path).unwrap();
    toml::from_str(&s).unwrap()
}

fn write_props_from_parameter(parameter: &Parameter) -> WriterProperties {
    let mut builder = WriterProperties::builder();
    builder = builder.set_write_batch_size(1024 * 32);
    builder = builder.set_max_row_group_size(parameter.max_row_group_size);

    for (column, config) in &parameter.column_configs {
        if let Some(v) = config.encoding {
            builder = builder.set_column_encoding(ColumnPath::new(vec![column.clone()]), v)
        }
        if let Some(v) = config.compression {
            builder = builder.set_column_compression(ColumnPath::new(vec![column.clone()]), v)
        }
    }

    builder.build()
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The directory where parquet write config files reside.
    #[arg(short, long)]
    config_dir: String,
    /// The source parquet file.
    #[arg(short, long)]
    source: String,
}
