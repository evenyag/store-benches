# Storage Benchmark Suite

Benchmark suite for reading/write data from/to different storage format.

# Bench Parquet

Run:
```bash
BENCH_CONFIG=/path/to/parquet/bencher/bench-config.toml cargo bench -p parquet-bencher
```

# Bench Storage Engine

Run:
```bash
BENCH_CONFIG=/path/to/engine/bencher/bench-config.toml cargo bench -p engine-bencher
```
