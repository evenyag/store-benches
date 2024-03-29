use std::cell::Cell;
use std::fmt::Debug;
use std::sync::Arc;

use criterion::*;
use opendal::services::Fs;
use opendal::Operator;
use parquet_bencher::config::BenchConfig;
use parquet_bencher::parquet_async_bench::ParquetAsyncBench;
use parquet_bencher::parquet_bench::ParquetBench;
use parquet_bencher::row_group_bench::ParquetRowGroupBench;
use tokio::runtime::Runtime;

const BENCH_CONFIG_KEY: &str = "BENCH_CONFIG";

#[derive(Debug, Clone)]
struct BenchContext {
    print_metrics_every: usize,
    times: Cell<usize>,
}

impl BenchContext {
    fn new(print_metrics_every: usize) -> Self {
        Self {
            print_metrics_every,
            times: Cell::new(0),
        }
    }

    fn maybe_print_metrics<T: Debug>(&self, metrics: &T) {
        let mut times = self.times.take();
        times += 1;
        self.times.set(times);

        if self.print_metrics_every > 0 {
            if times % self.print_metrics_every == 0 {
                println!("Metrics at times {} is: {:?}", times, metrics);
            }
        }
    }
}

#[derive(Clone)]
struct AsyncBenchContext {
    runtime: Arc<Runtime>,
    print_metrics_every: usize,
    times: Cell<usize>,
}

impl AsyncBenchContext {
    fn new(runtime: Arc<Runtime>, print_metrics_every: usize) -> Self {
        Self {
            runtime,
            print_metrics_every,
            times: Cell::new(0),
        }
    }

    fn maybe_print_metrics<T: Debug>(&self, metrics: &T) {
        let mut times = self.times.take();
        times += 1;
        self.times.set(times);

        if self.print_metrics_every > 0 {
            if times % self.print_metrics_every == 0 {
                println!("Metrics at times {} is: {:?}", times, metrics);
            }
        }
    }
}

fn init_bench() -> BenchConfig {
    let config_path = std::env::var(BENCH_CONFIG_KEY)
        .expect("Please specify the path to the config file in env 'BENCH_CONFIG'");
    BenchConfig::parse_toml(&config_path)
}

fn bench_parquet_iter(b: &mut Bencher<'_>, bench: &(ParquetBench, BenchContext)) {
    b.iter(|| {
        let metrics = bench.0.run();
        bench.1.maybe_print_metrics(&metrics);
    })
}

fn bench_file_name(path: &str) -> String {
    path.split('/').last().unwrap_or(path).to_string()
}

fn bench_parquet(c: &mut Criterion) {
    let config = init_bench();

    println!("config is {:?}", config);

    let mut group = c.benchmark_group("scan_parquet");

    if let Some(value) = config.measurement_time {
        group.measurement_time(value);
    }
    if let Some(value) = config.sample_size {
        group.sample_size(value);
    }

    let ctx = BenchContext::new(config.print_metrics_every);
    let parquet_name = bench_file_name(&config.parquet_path);
    let bench = ParquetBench::new(config.parquet_path.clone(), config.scan_batch_size)
        .with_columns(config.columns.clone());
    group.bench_with_input(
        BenchmarkId::new("scan_cols", parquet_name.clone()),
        &(bench, ctx.clone()),
        bench_parquet_iter,
    );

    if !config.row_groups.is_empty() {
        let bench = ParquetBench::new(config.parquet_path.clone(), config.scan_batch_size)
            .with_columns(config.columns.clone())
            .with_row_groups(config.row_groups.clone());
        group.bench_with_input(
            BenchmarkId::new(
                "scan_row_groups",
                format!("{}/{:?}", parquet_name, config.row_groups),
            ),
            &(bench, ctx.clone()),
            bench_parquet_iter,
        );
    }

    if let Some(selection) = &config.selection {
        let selection = selection.to_selection();
        let bench = ParquetBench::new(config.parquet_path.clone(), config.scan_batch_size)
            .with_columns(config.columns.clone())
            .with_row_groups(config.row_groups.clone())
            .with_selection(selection.clone());
        group.bench_with_input(
            BenchmarkId::new(
                "scan_row_selection",
                format!(
                    "{}/{:?}/{}",
                    parquet_name,
                    config.row_groups,
                    selection.row_count()
                ),
            ),
            &(bench, ctx),
            bench_parquet_iter,
        );
    }

    group.finish();
}

fn bench_parquet_async_iter(b: &mut Bencher<'_>, bench: &(ParquetAsyncBench, AsyncBenchContext)) {
    b.iter(|| {
        let metrics = bench.1.runtime.block_on(async { bench.0.run().await });
        bench.1.maybe_print_metrics(&metrics);
    })
}

fn bench_async_row_group_iter(
    b: &mut Bencher<'_>,
    bench: &(ParquetRowGroupBench, AsyncBenchContext),
) {
    b.iter(|| {
        let metrics = bench.1.runtime.block_on(async { bench.0.run().await });
        bench.1.maybe_print_metrics(&metrics);
    })
}

fn bench_parquet_async(c: &mut Criterion) {
    let config = init_bench();

    println!("config is {:?}", config);

    let mut group = c.benchmark_group("scan_parquet_async");

    if let Some(value) = config.measurement_time {
        group.measurement_time(value);
    }
    if let Some(value) = config.sample_size {
        group.sample_size(value);
    }

    let mut builder = Fs::default();
    builder.root("/");
    let operator = Operator::new(builder).unwrap().finish();

    let runtime = Arc::new(Runtime::new().unwrap());
    let ctx = AsyncBenchContext::new(runtime, config.print_metrics_every);
    let parquet_name = bench_file_name(&config.parquet_path);
    let bench = ParquetAsyncBench::new(
        operator.clone(),
        config.parquet_path.clone(),
        config.scan_batch_size,
    )
    .with_columns(config.columns.clone());
    group.bench_with_input(
        BenchmarkId::new("scan_stream_async", parquet_name.clone()),
        &(bench, ctx.clone()),
        bench_parquet_async_iter,
    );

    let bench = ParquetAsyncBench::new(
        operator.clone(),
        config.parquet_path.clone(),
        config.scan_batch_size,
    )
    .with_columns(config.columns.clone())
    .with_async_trait(true);
    group.bench_with_input(
        BenchmarkId::new("scan_reader_async", parquet_name.clone()),
        &(bench, ctx.clone()),
        bench_parquet_async_iter,
    );

    if !config.row_groups.is_empty() {
        let bench = ParquetAsyncBench::new(
            operator.clone(),
            config.parquet_path.clone(),
            config.scan_batch_size,
        )
        .with_columns(config.columns.clone())
        .with_row_groups(config.row_groups.clone());
        group.bench_with_input(
            BenchmarkId::new(
                "scan_row_group_async",
                format!("{}/{:?}", parquet_name.clone(), config.row_groups),
            ),
            &(bench, ctx.clone()),
            bench_parquet_async_iter,
        );
    }

    if let Some(selection) = &config.selection {
        let selection = selection.to_selection();
        let bench = ParquetAsyncBench::new(
            operator.clone(),
            config.parquet_path.clone(),
            config.scan_batch_size,
        )
        .with_columns(config.columns.clone())
        .with_row_groups(config.row_groups.clone())
        .with_selection(selection.clone());
        group.bench_with_input(
            BenchmarkId::new(
                "scan_row_selection_async",
                format!(
                    "{}/{:?}/{}",
                    parquet_name,
                    config.row_groups,
                    selection.row_count()
                ),
            ),
            &(bench, ctx.clone()),
            bench_parquet_async_iter,
        );
    }

    // Row group bench.
    let bench = ParquetRowGroupBench::new(config.parquet_path.clone(), config.scan_batch_size)
        .with_columns(config.columns.clone())
        .with_row_groups(config.row_groups.clone());
    group.bench_with_input(
        BenchmarkId::new("scan_async_row_group", parquet_name),
        &(bench, ctx),
        bench_async_row_group_iter,
    );

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default();
    targets = bench_parquet, bench_parquet_async,
);

criterion_main!(benches);
