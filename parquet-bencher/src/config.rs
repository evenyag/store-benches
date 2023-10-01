//! Benchmark configs.

use std::fs::File;
use std::io::Read;
use std::time::Duration;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct BenchConfig {
    pub parquet_path: String,
    #[serde(with = "humantime_serde")]
    pub measurement_time: Option<Duration>,
    pub sample_size: Option<usize>,
    pub scan_batch_size: usize,
    pub print_metrics: bool,
    /// Index of columns to read, empty for all columns.
    pub columns: Vec<usize>,
    /// Index of row groups to read, empty for all row groups.
    pub row_groups: Vec<usize>,
}

impl Default for BenchConfig {
    fn default() -> BenchConfig {
        BenchConfig {
            parquet_path: "".to_string(),
            measurement_time: None,
            sample_size: None,
            scan_batch_size: 1024,
            print_metrics: false,
            columns: Vec::new(),
            row_groups: Vec::new(),
        }
    }
}

impl BenchConfig {
    pub fn parse_toml(path: &str) -> BenchConfig {
        let mut file = File::open(path).unwrap();
        let mut content = String::new();
        file.read_to_string(&mut content).unwrap();

        toml::from_str(&content).unwrap()
    }
}
