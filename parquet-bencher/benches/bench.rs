use criterion::*;
use opendal::services::Fs;
use opendal::Operator;
use parquet_bencher::config::BenchConfig;
use parquet_bencher::parquet_async_bench::ParquetAsyncBench;
use parquet_bencher::parquet_bench::ParquetBench;
use tokio::runtime::Runtime;

const BENCH_CONFIG_KEY: &str = "BENCH_CONFIG";

struct BenchContext {
    print_metrics: bool,
}

struct AsyncBenchContext {
    runtime: Runtime,
    print_metrics: bool,
}

fn init_bench() -> BenchConfig {
    let config_path = std::env::var(BENCH_CONFIG_KEY)
        .expect("Please specify the path to the config file in env 'BENCH_CONFIG'");
    BenchConfig::parse_toml(&config_path)
}

fn bench_parquet_iter(b: &mut Bencher<'_>, bench: &(ParquetBench, BenchContext)) {
    b.iter(|| {
        let metrics = bench.0.run();
        if bench.1.print_metrics {
            println!("metrics is {:?}", metrics);
        }
    })
}

fn bench_parquet(c: &mut Criterion) {
    let config = init_bench();

    println!("config is {:?}", config);

    let mut group = c.benchmark_group("scan_parquet");

    group.measurement_time(config.measurement_time);
    group.sample_size(config.sample_size);

    let bench = ParquetBench::new(config.parquet_path.clone(), config.scan_batch_size)
        .with_columns(config.columns.clone());
    let ctx = BenchContext {
        print_metrics: config.print_metrics,
    };
    group.bench_with_input(
        BenchmarkId::new("scan", config.parquet_path),
        &(bench, ctx),
        bench_parquet_iter,
    );

    group.finish();
}

fn bench_parquet_async_iter(b: &mut Bencher<'_>, bench: &(ParquetAsyncBench, AsyncBenchContext)) {
    b.iter(|| {
        let metrics = bench.1.runtime.block_on(async { bench.0.run().await });
        if bench.1.print_metrics {
            println!("metrics is {:?}", metrics);
        }
    })
}

fn bench_parquet_async(c: &mut Criterion) {
    let config = init_bench();

    println!("config is {:?}", config);

    let mut group = c.benchmark_group("scan_parquet_async");

    group.measurement_time(config.measurement_time);
    group.sample_size(config.sample_size);

    let mut builder = Fs::default();
    builder.root("/");
    let operator = Operator::new(builder).unwrap().finish();

    let bench = ParquetAsyncBench::new(
        operator,
        config.parquet_path.clone(),
        config.scan_batch_size,
    )
    .with_columns(config.columns.clone());
    let runtime = Runtime::new().unwrap();
    let ctx = AsyncBenchContext {
        runtime,
        print_metrics: config.print_metrics,
    };
    group.bench_with_input(
        BenchmarkId::new("scan_async", config.parquet_path),
        &(bench, ctx),
        bench_parquet_async_iter,
    );

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default();
    targets = bench_parquet, bench_parquet_async,
);

criterion_main!(benches);
