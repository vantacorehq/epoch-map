//! High-contention throughput and latency benchmarks.
//!
//! Measures ops/sec and latency percentiles (p50 / p99 / p99.9) under
//! different read/write ratios and thread counts.
//!
//! Run with:
//!   cargo bench --bench contention

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use epoch_map::EpochMap;
use rand::Rng;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const CAPACITY: usize = 1 << 16; // 65536
const KEY_SPACE: u64 = 1 << 14; // 16384 keys → moderate load factor

type Map = EpochMap<u64, u64, CAPACITY>;

fn prefill(map: &Map, n: u64) {
    map.enter(0);
    for i in 0..n {
        let _ = map.insert(i, i);
    }
    map.exit(0);
}

/// Run a fixed number of operations per thread and collect latencies.
fn run_workload(
    threads: usize,
    read_pct: u8,
    ops_per_thread: u64,
) -> (u64, Vec<Duration>) {
    let map = Arc::new(Map::new());
    prefill(&map, KEY_SPACE / 2);

    let total_ops = AtomicU64::new(0);
    let mut handles = Vec::with_capacity(threads);

    for tid in 0..threads {
        let map = map.clone();
        let total_ops = &total_ops;
        handles.push(thread::spawn(move || {
            let mut rng = rand::thread_rng();
            let mut latencies = Vec::with_capacity(ops_per_thread as usize);
            map.enter(tid);

            for _ in 0..ops_per_thread {
                let key = rng.gen_range(0..KEY_SPACE);
                let start = Instant::now();
                if rng.gen_range(0..100) < read_pct {
                    let _ = black_box(map.get(&key));
                } else {
                    // 50/50 insert vs remove among writes
                    if rng.gen_bool(0.5) {
                        let _ = map.insert(key, key);
                    } else {
                        let _ = map.remove(&key);
                    }
                }
                latencies.push(start.elapsed());
            }

            map.exit(tid);
            total_ops.fetch_add(ops_per_thread, Ordering::Relaxed);
            latencies
        }));
    }

    let mut all_latencies = Vec::new();
    for h in handles {
        all_latencies.extend(h.join().unwrap());
    }

    // Opportunistic reclaim
    map.enter(0);
    map.exit(0);
    let _ = map.try_advance_and_reclaim();

    (total_ops.load(Ordering::Relaxed), all_latencies)
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn bench_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("contention");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(15));

    // (threads, read_pct)
    let configs = [
        (4, 95u8),
        (4, 50),
        (8, 95),
        (8, 50),
        (16, 95),
        (16, 50),
    ];

    for &(threads, read_pct) in &configs {
        let ops_per_thread = 50_000u64;
        let id = BenchmarkId::new(format!("t{}_r{}", threads, read_pct), "");

        group.throughput(Throughput::Elements(threads as u64 * ops_per_thread));
        group.bench_function(id, |b| {
            b.iter_custom(|_iters| {
                let (ops, mut lats) = run_workload(threads, read_pct, ops_per_thread);
                lats.sort_unstable();
                let p50 = percentile(&lats, 0.50);
                let p99 = percentile(&lats, 0.99);
                let p999 = percentile(&lats, 0.999);
                // Print once per sample so the user sees the numbers
                eprintln!(
                    "  threads={} read%={}  ops={}  p50={:?}  p99={:?}  p99.9={:?}",
                    threads, read_pct, ops, p50, p99, p999
                );
                // Return total wall time of the whole run
                lats.iter().sum::<Duration>()
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_contention);
criterion_main!(benches);
