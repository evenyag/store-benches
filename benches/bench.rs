use criterion::*;
use store_benches::config::BenchConfig;
use store_benches::parquet::ParquetBench;

const CONFIG_PATH: &str = "bench-config.toml";

struct BenchContext {
    print_metrics: bool,
}

fn init_bench() -> BenchConfig {
    BenchConfig::parse_toml(CONFIG_PATH)
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

    let bench = ParquetBench::new(config.parquet_path.clone(), config.scan_batch_size);
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
