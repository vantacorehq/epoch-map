use epoch_map::EpochMap;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn main() {
    const CAP: usize = 1 << 14;
    const THREADS: usize = 4;
    const OPS_PER_THREAD: usize = 50_000;

    let map = Arc::new(EpochMap::<u64, u64, CAP>::new());
    let mut handles = vec![];
    let mut all_latencies = vec![];

    let start = Instant::now();

    for tid in 0..THREADS {
        let m = map.clone();
        handles.push(thread::spawn(move || {
            let mut local = Vec::with_capacity(OPS_PER_THREAD);
            m.enter(tid);
            for i in 0..OPS_PER_THREAD {
                let t0 = Instant::now();
                let k = (tid as u64) * 1_000_000 + i as u64;
                m.insert(k, k);
                let _ = m.get(&k);
                local.push(t0.elapsed().as_nanos() as u64);
            }
            m.exit(tid);
            local
        }));
    }

    for h in handles {
        all_latencies.extend(h.join().unwrap());
    }

    let total = start.elapsed();
    all_latencies.sort_unstable();

    let p50 = percentile(&all_latencies, 0.50);
    let p99 = percentile(&all_latencies, 0.99);
    let p999 = percentile(&all_latencies, 0.999);

    println!("epoch-map concurrent u64/u64");
    println!("threads: {}", THREADS);
    println!("ops:     {}", THREADS * OPS_PER_THREAD);
    println!("time:    {:.2?}", total);
    println!("p50:     {} ns", p50);
    println!("p99:     {} ns", p99);
    println!("p99.9:   {} ns", p999);
    println!("retired: {}", map.retired_approx());
}
