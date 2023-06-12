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

use std::env;
use std::fmt::Debug;
use std::sync::{Mutex, Once};

use common_runtime::{create_runtime, Runtime};
use common_telemetry::logging;
use criterion::*;
use engine_bencher::config::BenchConfig;
use engine_bencher::loader::ParquetLoader;
use engine_bencher::put_bench::PutBench;
use engine_bencher::scan_bench::ScanBench;
use engine_bencher::target::Target;
use once_cell::sync::Lazy;

const BENCH_CONFIG_KEY: &str = "BENCH_CONFIG";
const BENCH_ENABLE_LOG_KEY: &str = "BENCH_ENABLE_LOG";
const DEFAULT_CONFIG_PATH: &str = "./bench-config.toml";
static GLOBAL_CONFIG: Lazy<Mutex<BenchConfig>> = Lazy::new(|| Mutex::new(BenchConfig::default()));

struct BenchContext {
    config: BenchConfig,
    runtime: Runtime,
}

impl BenchContext {
    fn new(config: BenchConfig) -> BenchContext {
        let runtime = create_runtime("bench", "bench-worker", config.runtime_size);
        BenchContext { config, runtime }
    }

    fn maybe_print_log<T: Debug>(&self, times: usize, metrics: &T) {
        if self.config.print_metrics_every > 0 {
            if times % self.config.print_metrics_every == 0 {
                logging::info!("Metrics at times {} is: {:?}", times, metrics);
            }
        }
    }

    async fn new_scan_bench(&self) -> ScanBench {
        let loader = ParquetLoader::new(
            self.config.parquet_path.clone(),
            self.config.scan.load_batch_size,
        );
        let target = Target::new(
            &self.config.scan.path,
            self.config.scan.engine_config(),
            self.config.scan.region_id,
        )
        .await;

        ScanBench::new(loader, target, self.config.scan.scan_batch_size)
    }

    fn new_put_bench(&self) -> PutBench {
        let loader =
            ParquetLoader::new(self.config.parquet_path.clone(), self.config.put.batch_size);

        PutBench::new(
            loader,
            self.config.put.path.clone(),
            self.config.put.engine_config(),
        )
    }
}

fn init_bench() -> BenchConfig {
    let enable_log: bool = env::var(BENCH_ENABLE_LOG_KEY)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(false);
    if enable_log {
        common_telemetry::init_default_ut_logging();
    }

    static START: Once = Once::new();

    START.call_once(|| {
        let cwd = env::current_dir().unwrap();
        logging::info!("Init bench, current dir: {}", cwd.display());
        let config_path =
            env::var(BENCH_CONFIG_KEY).unwrap_or_else(|_| DEFAULT_CONFIG_PATH.to_string());

        logging::info!("Loading config from path: {}", config_path);

        let mut config = GLOBAL_CONFIG.lock().unwrap();
        *config = BenchConfig::parse_toml(&config_path);

        logging::info!("config is {:?}", config);
    });

    let config = GLOBAL_CONFIG.lock().unwrap();
    (*config).clone()
}

fn scan_storage_iter(times: usize, b: &mut Bencher<'_>, input: &(BenchContext, ScanBench)) {
    b.iter(|| {
        let metrics = input.0.runtime.block_on(async { input.1.run().await });

        input.0.maybe_print_log(times, &metrics);
    })
}

fn bench_full_scan(c: &mut Criterion) {
    let config = init_bench();

    let mut group = c.benchmark_group("full_scan");

    group.measurement_time(config.measurement_time);
    group.sample_size(config.sample_size);

    let parquet_path = config.parquet_path.clone();
    let ctx = BenchContext::new(config);
    let scan_bench = ctx.runtime.block_on(async {
        let mut scan_bench = ctx.new_scan_bench().await;
        scan_bench.maybe_prepare_data().await;

        scan_bench
    });

    logging::info!("Start full scan bench");

    let mut times = 0;
    let input = (ctx, scan_bench);
    group.bench_with_input(
        BenchmarkId::new("scan", parquet_path),
        &input,
        |b, input| {
            times += 1;
            scan_storage_iter(times, b, input)
        },
    );

    input.0.runtime.block_on(async {
        input.1.shutdown().await;
    });

    group.finish();
}

fn put_storage_iter(times: usize, b: &mut Bencher<'_>, input: &(BenchContext, PutBench)) {
    b.iter(|| {
        let metrics = input.0.runtime.block_on(async { input.1.run().await });

        input.0.maybe_print_log(times, &metrics);
    })
}

fn bench_put(c: &mut Criterion) {
    let config = init_bench();

    let mut group = c.benchmark_group("full_scan");

    group.measurement_time(config.measurement_time);
    group.sample_size(config.sample_size);

    let parquet_path = config.parquet_path.clone();
    let ctx = BenchContext::new(config);
    let put_bench = ctx.new_put_bench();

    logging::info!("Start put bench");

    let mut times = 0;
    let input = (ctx, put_bench);
    group.bench_with_input(BenchmarkId::new("put", parquet_path), &input, |b, input| {
        times += 1;
        put_storage_iter(times, b, input)
    });

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default();
    targets = bench_full_scan, bench_put,
);

criterion_main!(benches);
