use criterion::*;
use parquet_bencher::config::BenchConfig;
use parquet_bencher::parquet_bench::ParquetBench;

const BENCH_CONFIG_KEY: &str = "BENCH_CONFIG";

struct BenchContext {
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

criterion_group!(
    name = benches;
    config = Criterion::default();
    targets = bench_parquet,
);

criterion_main!(benches);
