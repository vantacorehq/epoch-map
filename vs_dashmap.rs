use criterion::{criterion_group, criterion_main, Criterion};
use dashmap::DashMap;
use epoch_map::EpochMap;
use std::hint::black_box;
use std::sync::Arc;
use std::thread;

const CAP: usize = 1 << 17; // 131072 slots
const OPS: usize = 50_000; // load factor ~0.38, the table never fills
const THREADS: usize = 4;

fn bench_concurrent(c: &mut Criterion) {
    let mut group = c.benchmark_group("u64_u64_concurrent");

    group.bench_function("epoch-map", |b| {
        b.iter(|| {
            let map = Arc::new(EpochMap::<u64, u64, CAP>::new());
            let mut handles = vec![];
            for tid in 0..THREADS {
                let m = map.clone();
                handles.push(thread::spawn(move || {
                    m.enter(tid);
                    for i in 0..OPS / THREADS {
                        let k = (tid as u64 * 1_000_000) + i as u64;
                        m.insert(k, k);
                        black_box(m.get(&k));
                    }
                    m.exit(tid);
                }));
            }
            for h in handles {
                h.join().unwrap();
            }
        });
    });

    group.bench_function("dashmap", |b| {
        b.iter(|| {
            let map = Arc::new(DashMap::<u64, u64>::with_capacity(CAP));
            let mut handles = vec![];
            for tid in 0..THREADS {
                let m = map.clone();
                handles.push(thread::spawn(move || {
                    for i in 0..OPS / THREADS {
                        let k = (tid as u64 * 1_000_000) + i as u64;
                        m.insert(k, k);
                        black_box(m.get(&k));
                    }
                }));
            }
            for h in handles {
                h.join().unwrap();
            }
        });
    });

    group.finish();
}

criterion_group!(benches, bench_concurrent);
criterion_main!(benches);
