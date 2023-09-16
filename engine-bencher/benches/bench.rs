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

use std::cell::Cell;
use std::env;
use std::fmt::Debug;
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;

use common_base::readable_size::ReadableSize;
use common_runtime::{create_runtime, Runtime};
use common_telemetry::logging;
use criterion::*;
use engine_bencher::config::BenchConfig;
use engine_bencher::loader::ParquetLoader;
use engine_bencher::memory::MemoryMetrics;
use engine_bencher::memtable::insert_bench::InsertMemtableBench;
use engine_bencher::memtable::scan_bench::ScanMemtableBench;
use engine_bencher::put_bench::PutBench;
use engine_bencher::scan_bench::ScanBench;
use engine_bencher::sst_reader_bench::ParquetReaderBench;
use engine_bencher::target::Target;
use memtable_nursery::columnar::ColumnarConfig;
use memtable_nursery::plain_vector::PlainVectorConfig;
use memtable_nursery::series::SeriesConfig;
use once_cell::sync::Lazy;
use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const BENCH_CONFIG_KEY: &str = "BENCH_CONFIG";
const BENCH_ENABLE_LOG_KEY: &str = "BENCH_ENABLE_LOG";
const DEFAULT_CONFIG_PATH: &str = "./bench-config.toml";
static GLOBAL_CONFIG: Lazy<Mutex<BenchConfig>> = Lazy::new(|| Mutex::new(BenchConfig::default()));

struct BenchContext {
    config: BenchConfig,
    runtime: Runtime,
    times: Cell<usize>,
}

impl BenchContext {
    fn new(config: BenchConfig) -> BenchContext {
        let runtime = create_runtime("bench", "bench-worker", config.runtime_size);
        BenchContext {
            config,
            runtime,
            times: Cell::new(0),
        }
    }

    fn maybe_print_log<T: Debug>(&self, metrics: &T) {
        let mut times = self.times.take();
        times += 1;
        self.times.set(times);

        if self.config.print_metrics_every > 0 {
            if times % self.config.print_metrics_every == 0 {
                logging::info!("Metrics at times {} is: {:?}", times, metrics);
            }
        }
    }

    async fn new_scan_bench(&self, run_mito: bool) -> ScanBench {
        let loader = ParquetLoader::new(
            self.config.parquet_path.clone(),
            self.config.scan.load_batch_size,
        );
        let path = if run_mito {
            &self.config.scan.mito_path
        } else {
            &self.config.scan.path
        };
        let target = Target::new(
            path,
            self.config.scan.engine_config(),
            self.config.scan.mito_config(),
            self.config.scan.region_id,
            run_mito,
        )
        .await;

        ScanBench::new(loader, target, self.config.scan.scan_batch_size)
    }

    fn new_put_bench(&self, run_mito: bool) -> PutBench {
        let loader =
            ParquetLoader::new(self.config.parquet_path.clone(), self.config.put.batch_size);

        let path = if run_mito {
            &self.config.put.mito_path
        } else {
            &self.config.put.path
        };
        PutBench::new(
            loader,
            path.to_string(),
            self.config.put.engine_config(),
            self.config.put.mito_config(),
            run_mito,
        )
    }

    fn new_insert_memtable_bench(&self) -> InsertMemtableBench {
        let loader = ParquetLoader::new(
            self.config.parquet_path.clone(),
            self.config.insert_memtable.batch_size,
        );

        let mut bench = InsertMemtableBench::new(self.config.insert_memtable.total_rows);

        let mem_before = MemoryMetrics::read_metrics();
        logging::info!(
            "Start loading {} rows from parquet, memory: {:?}",
            self.config.insert_memtable.total_rows,
            mem_before,
        );

        bench.init(&loader);

        let mem_after = MemoryMetrics::read_metrics();
        logging::info!(
            "End loading rows from parquet, allocated: {}, memory: {:?}",
            ReadableSize(mem_after.subtract_allocated(&mem_before) as u64),
            mem_after
        );

        bench
    }

    fn new_scan_memtable_bench(&self) -> ScanMemtableBench {
        let loader = ParquetLoader::new(
            self.config.parquet_path.clone(),
            self.config.scan_memtable.load_batch_size,
        );

        let mut bench = ScanMemtableBench::new(self.config.scan_memtable.total_rows);

        logging::info!(
            "Start loading {} rows from parquet to bencher",
            self.config.insert_memtable.total_rows
        );

        bench.load_data(&loader);

        logging::info!("End loading rows from parquet to bencher");

        bench
    }

    fn new_sst_reader_bench(&self) -> Option<ParquetReaderBench> {
        self.config
            .sst_reader
            .file_id
            .map(|file_id| ParquetReaderBench::new(&self.config.sst_reader.file_dir, file_id))
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

fn scan_storage_iter(b: &mut Bencher<'_>, ctx: &BenchContext, run_mito: bool) {
    logging::info!("Prepare full scan bench, run_mito: {}", run_mito);

    let scan_bench = ctx.runtime.block_on(async {
        let mut scan_bench = ctx.new_scan_bench(run_mito).await;
        scan_bench.maybe_prepare_data().await;

        scan_bench
    });

    logging::info!("Start full scan bench, run_mito: {}", run_mito);

    let input = Arc::new((ctx, scan_bench));
    b.iter_batched(
        || input.clone(),
        |input| {
            let metrics = input.0.runtime.block_on(async { input.1.run().await });

            input.0.maybe_print_log(&metrics);
        },
        BatchSize::SmallInput,
    );
}

fn bench_full_scan(c: &mut Criterion) {
    let config = init_bench();

    let mut group = c.benchmark_group("full_scan");

    if let Some(v) = config.scan.measurement_time {
        group.measurement_time(v);
    }
    if let Some(v) = config.scan.sample_size {
        group.sample_size(v);
    }

    let parquet_path = config.scan.path.clone();
    let ctx = BenchContext::new(config);
    group.bench_with_input(
        BenchmarkId::new("storage_scan", parquet_path),
        &ctx,
        |b, input| scan_storage_iter(b, input, false),
    );

    group.finish();
}

fn bench_mito_full_scan(c: &mut Criterion) {
    let config = init_bench();

    let mut group = c.benchmark_group("mito_full_scan");

    if let Some(v) = config.scan.measurement_time {
        group.measurement_time(v);
    }
    if let Some(v) = config.scan.sample_size {
        group.sample_size(v);
    }

    let parquet_path = config.scan.path.clone();
    let ctx = BenchContext::new(config);

    group.bench_with_input(
        BenchmarkId::new("mito_scan", parquet_path),
        &ctx,
        |b, input| scan_storage_iter(b, input, true),
    );

    group.finish();
}

fn put_storage_iter(b: &mut Bencher<'_>, input: &(BenchContext, PutBench)) {
    b.iter(|| {
        let metrics = input.0.runtime.block_on(async { input.1.run().await });

        input.0.maybe_print_log(&metrics);
    })
}

fn bench_put(c: &mut Criterion) {
    let config = init_bench();

    let mut group = c.benchmark_group("put");

    if let Some(v) = config.put.measurement_time {
        group.measurement_time(v);
    }
    if let Some(v) = config.put.sample_size {
        group.sample_size(v);
    }

    let parquet_path = config.parquet_path.clone();
    let ctx = BenchContext::new(config);
    let put_bench = ctx.new_put_bench(false);

    logging::info!("Start put bench");

    let input = (ctx, put_bench);
    group.bench_with_input(
        BenchmarkId::new("storage_put", parquet_path),
        &input,
        |b, input| put_storage_iter(b, input),
    );

    group.finish();
}

fn bench_mito_put(c: &mut Criterion) {
    let config = init_bench();

    let mut group = c.benchmark_group("mito_put");

    if let Some(v) = config.put.measurement_time {
        group.measurement_time(v);
    }
    if let Some(v) = config.put.sample_size {
        group.sample_size(v);
    }

    let parquet_path = config.parquet_path.clone();
    let ctx = BenchContext::new(config);
    let put_bench = ctx.new_put_bench(true);

    logging::info!("Start mito put bench");

    let input = (ctx, put_bench);
    group.bench_with_input(
        BenchmarkId::new("mito_put", parquet_path),
        &input,
        |b, input| put_storage_iter(b, input),
    );

    group.finish();
}

fn insert_btree_iter(b: &mut Bencher<'_>, ctx: &BenchContext) {
    logging::info!("Prepare insert btree memtable bench");

    let insert_bench = ctx.new_insert_memtable_bench();

    logging::info!("Start insert btree memtable bench");

    let input = (ctx, insert_bench);
    b.iter(|| {
        let metrics = input.1.bench_btree();

        input.0.maybe_print_log(&metrics);
    })
}

fn insert_btree_only_iter(b: &mut Bencher<'_>, ctx: &BenchContext) {
    logging::info!("Prepare insert btree memtable only bench");

    let insert_bench = ctx.new_insert_memtable_bench();

    logging::info!("Start insert btree memtable only bench");

    let input = (ctx, insert_bench);

    b.iter_custom(|iters| {
        let mut insert_cost = Duration::ZERO;
        for _i in 0..iters {
            let metrics = input.1.bench_btree();
            insert_cost += metrics.total_cost;

            input.0.maybe_print_log(&metrics);
        }

        insert_cost
    })
}

fn insert_columnar_iter(b: &mut Bencher<'_>, input: &(BenchContext, InsertMemtableBench)) {
    b.iter(|| {
        let metrics = input.1.bench_columnar();

        input.0.maybe_print_log(&metrics);
    })
}

fn insert_columnar_only_iter(b: &mut Bencher<'_>, input: &(BenchContext, InsertMemtableBench)) {
    b.iter_custom(|iters| {
        let mut insert_cost = Duration::ZERO;
        for _i in 0..iters {
            let metrics = input.1.bench_columnar();
            insert_cost += metrics.total_cost;

            input.0.maybe_print_log(&metrics);
        }

        insert_cost
    })
}

fn bench_insert_btree_memtable(c: &mut Criterion) {
    let config = init_bench();

    let mut group = c.benchmark_group("insert_btree_memtable");

    if let Some(v) = config.insert_memtable.measurement_time {
        group.measurement_time(v);
    }
    if let Some(v) = config.insert_memtable.sample_size {
        group.sample_size(v);
    }

    let parquet_path = config.parquet_path.clone();
    let ctx = BenchContext::new(config);
    // let insert_bench = ctx.new_insert_memtable_bench();

    // logging::info!("Start insert btree memtable bench");

    // let input = (ctx, insert_bench);
    group.bench_with_input(
        BenchmarkId::new("btree-insert", parquet_path.clone()),
        &ctx,
        |b, input| insert_btree_iter(b, input),
    );
    group.bench_with_input(
        BenchmarkId::new("btree-insert-only", parquet_path.clone()),
        &ctx,
        |b, input| insert_btree_only_iter(b, input),
    );

    group.finish();
}

fn bench_insert_columnar_memtable(c: &mut Criterion) {
    let config = init_bench();

    let mut group = c.benchmark_group("insert_columnar_memtable");

    if let Some(v) = config.insert_memtable.measurement_time {
        group.measurement_time(v);
    }
    if let Some(v) = config.insert_memtable.sample_size {
        group.sample_size(v);
    }

    let parquet_path = config.parquet_path.clone();
    let ctx = BenchContext::new(config);
    let insert_bench = ctx.new_insert_memtable_bench();

    logging::info!("Start insert columnar memtable bench");

    let input = (ctx, insert_bench);
    // columnar
    group.bench_with_input(
        BenchmarkId::new("columnar-insert", parquet_path.clone()),
        &input,
        |b, input| insert_columnar_iter(b, input),
    );
    group.bench_with_input(
        BenchmarkId::new("columnar-insert-only", parquet_path),
        &input,
        |b, input| insert_columnar_only_iter(b, input),
    );

    group.finish();
}

fn insert_series_iter(b: &mut Bencher<'_>, input: &(BenchContext, InsertMemtableBench)) {
    b.iter(|| {
        let metrics = input.1.bench_series();

        input.0.maybe_print_log(&metrics);
    })
}

fn insert_vector_iter(b: &mut Bencher<'_>, input: &(BenchContext, InsertMemtableBench)) {
    b.iter(|| {
        let metrics = input.1.bench_plain_vector();
        input.0.maybe_print_log(&metrics);
    })
}

fn bench_insert_series_memtable(c: &mut Criterion) {
    let config = init_bench();

    let mut group = c.benchmark_group("insert_series_memtable");

    if let Some(v) = config.insert_memtable.measurement_time {
        group.measurement_time(v);
    }
    if let Some(v) = config.insert_memtable.sample_size {
        group.sample_size(v);
    }

    let parquet_path = config.parquet_path.clone();
    let ctx = BenchContext::new(config);
    let insert_bench = ctx.new_insert_memtable_bench();

    logging::info!("Start insert series memtable bench");

    let input = (ctx, insert_bench);
    // series
    group.bench_with_input(
        BenchmarkId::new("series-insert", parquet_path.clone()),
        &input,
        |b, input| insert_series_iter(b, input),
    );

    group.finish();
}

fn bench_insert_plain_vector_memtable(c: &mut Criterion) {
    let config = init_bench();

    let mut group = c.benchmark_group("insert_plain_vector_memtable");

    if let Some(v) = config.insert_memtable.measurement_time {
        group.measurement_time(v);
    }
    if let Some(v) = config.insert_memtable.sample_size {
        group.sample_size(v);
    }

    let parquet_path = config.parquet_path.clone();
    let ctx = BenchContext::new(config);
    let insert_bench = ctx.new_insert_memtable_bench();

    logging::info!("Start insert plain_vector memtable bench");

    let input = (ctx, insert_bench);
    // series
    group.bench_with_input(
        BenchmarkId::new("plain_vector-insert", parquet_path.clone()),
        &input,
        |b, input| insert_vector_iter(b, input),
    );

    group.finish();
}

fn scan_memtable_iter(b: &mut Bencher<'_>, input: &(BenchContext, ScanMemtableBench)) {
    b.iter(|| {
        let metrics = input.1.bench(input.0.config.scan_memtable.scan_batch_size);

        input.0.maybe_print_log(&metrics);
    })
}

fn bench_scan_btree_memtable(c: &mut Criterion) {
    let config = init_bench();

    let mut group = c.benchmark_group("scan_btree_memtable");

    if let Some(v) = config.scan_memtable.measurement_time {
        group.measurement_time(v);
    }
    if let Some(v) = config.scan_memtable.sample_size {
        group.sample_size(v);
    }

    let parquet_path = config.parquet_path.clone();
    let ctx = BenchContext::new(config);
    let scan_bench = ctx.new_scan_memtable_bench();

    logging::info!("Start scan btree memtable bench");

    let mut input = (ctx, scan_bench);
    input.1.init_btree();
    group.bench_with_input(
        BenchmarkId::new("btree-scan", parquet_path.clone()),
        &input,
        |b, input| scan_memtable_iter(b, input),
    );

    group.finish();
}

fn bench_scan_columnar_memtable(c: &mut Criterion) {
    let config = init_bench();

    let mut group = c.benchmark_group("scan_columnar_memtable");

    if let Some(v) = config.scan_memtable.measurement_time {
        group.measurement_time(v);
    }
    if let Some(v) = config.scan_memtable.sample_size {
        group.sample_size(v);
    }

    let parquet_path = config.parquet_path.clone();
    let ctx = BenchContext::new(config);
    let scan_bench = ctx.new_scan_memtable_bench();

    logging::info!("Start scan columnar memtable bench");

    let mut input = (ctx, scan_bench);
    input.1.init_columnar(ColumnarConfig { use_dict: false });
    group.bench_with_input(
        BenchmarkId::new("columnar-scan", parquet_path.clone()),
        &input,
        |b, input| scan_memtable_iter(b, input),
    );

    input.1.init_columnar(ColumnarConfig { use_dict: true });
    group.bench_with_input(
        BenchmarkId::new("columnar-dict-scan", parquet_path),
        &input,
        |b, input| scan_memtable_iter(b, input),
    );

    group.finish();
}

fn bench_scan_series_memtable(c: &mut Criterion) {
    let config = init_bench();

    let mut group = c.benchmark_group("scan_series_memtable");

    if let Some(v) = config.scan_memtable.measurement_time {
        group.measurement_time(v);
    }
    if let Some(v) = config.scan_memtable.sample_size {
        group.sample_size(v);
    }

    let parquet_path = config.parquet_path.clone();
    let ctx = BenchContext::new(config);
    let scan_bench = ctx.new_scan_memtable_bench();

    logging::info!("Start scan series memtable bench");

    let mut input = (ctx, scan_bench);
    input.1.init_series(SeriesConfig::default());
    group.bench_with_input(
        BenchmarkId::new("series-scan", parquet_path.clone()),
        &input,
        |b, input| scan_memtable_iter(b, input),
    );

    group.finish();
}

fn bench_scan_plain_vector_memtable(c: &mut Criterion) {
    let config = init_bench();

    let mut group = c.benchmark_group("scan_plain_vector_memtable");

    if let Some(v) = config.scan_memtable.measurement_time {
        group.measurement_time(v);
    }
    if let Some(v) = config.scan_memtable.sample_size {
        group.sample_size(v);
    }

    let parquet_path = config.parquet_path.clone();
    let ctx = BenchContext::new(config);
    let scan_bench = ctx.new_scan_memtable_bench();

    logging::info!("Start scan series memtable bench");

    let mut input = (ctx, scan_bench);
    input.1.init_plain_vector(PlainVectorConfig::default());
    group.bench_with_input(
        BenchmarkId::new("plain_vector-scan", parquet_path.clone()),
        &input,
        |b, input| scan_memtable_iter(b, input),
    );

    group.finish();
}

fn sst_reader_iter(b: &mut Bencher<'_>, input: &(BenchContext, ParquetReaderBench)) {
    b.iter(|| {
        let metrics = input.0.runtime.block_on(async { input.1.bench().await });

        input.0.maybe_print_log(&metrics);
    })
}

fn bench_sst_reader(c: &mut Criterion) {
    let config = init_bench();

    let Some(file_id) = config.sst_reader.file_id else {
        return;
    };

    let mut group = c.benchmark_group("sst_reader");

    if let Some(v) = config.scan_memtable.measurement_time {
        group.measurement_time(v);
    }
    if let Some(v) = config.scan_memtable.sample_size {
        group.sample_size(v);
    }

    let ctx = BenchContext::new(config);
    let reader_bench = ctx.new_sst_reader_bench().unwrap();

    logging::info!("Start parquet sst reader bench");

    let input = (ctx, reader_bench);
    group.bench_with_input(
        BenchmarkId::new("parquet_reader", file_id),
        &input,
        |b, input| sst_reader_iter(b, input),
    );

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default();
    targets = bench_mito_put,
              bench_put,
              bench_mito_full_scan,
              bench_full_scan,
              bench_insert_btree_memtable,
              bench_insert_plain_vector_memtable,
              bench_scan_plain_vector_memtable,
              bench_insert_columnar_memtable,
              bench_scan_btree_memtable,
              bench_scan_columnar_memtable,
              bench_insert_series_memtable,
              bench_scan_series_memtable,
              bench_sst_reader,
);

criterion_main!(benches);
