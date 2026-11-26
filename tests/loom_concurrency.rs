//! Loom-based formal concurrency tests.
//!
//! These tests explore all possible interleavings of a small number of
//! threads. They are expensive and are therefore behind the `loom` feature
//! (or run only under `cargo test --features loom` / CI with LOOM=1).
//!
//! Run with:
//!   RUSTFLAGS="--cfg loom" cargo test --test loom_concurrency --features loom -- --nocapture
//!
//! or simply:
//!   cargo test --test loom_concurrency --features loom

#![cfg(feature = "loom")]

use epoch_map::EpochMap;
use loom::sync::Arc;
use loom::thread;

#[test]
fn concurrent_insert_get_remove() {
    loom::model(|| {
        let map = Arc::new(EpochMap::<u64, u64, 16>::new());

        let m1 = map.clone();
        let t1 = thread::spawn(move || {
            m1.enter(0);
            assert_eq!(m1.insert(1, 10), None);
            assert_eq!(m1.get(&1), Some(10));
            m1.exit(0);
        });

        let m2 = map.clone();
        let t2 = thread::spawn(move || {
            m2.enter(1);
            // May or may not see the key depending on interleaving.
            let _ = m2.get(&1);
            let _ = m2.insert(2, 20);
            m2.exit(1);
        });

        t1.join().unwrap();
        t2.join().unwrap();

        // After both threads have exited, reclaim must be able to run.
        map.enter(0);
        map.exit(0);
        let _ = map.try_advance_and_reclaim();
    });
}

#[test]
fn reclaim_under_quiescence() {
    loom::model(|| {
        let map = Arc::new(EpochMap::<u64, u64, 8>::new());

        map.enter(0);
        map.insert(1, 100);
        map.remove(&1);
        map.exit(0);

        // No other thread is active → reclaim should free the tombstone.
        let freed = map.try_advance_and_reclaim();
        assert!(freed >= 1 || map.retired() == 0);
    });
}

#[test]
fn no_data_race_on_same_key() {
    loom::model(|| {
        let map = Arc::new(EpochMap::<u64, u64, 16>::new());

        let m1 = map.clone();
        let t1 = thread::spawn(move || {
            m1.enter(0);
            let _ = m1.insert(42, 1);
            let _ = m1.update(&42, |v| v + 1);
            m1.exit(0);
        });

        let m2 = map.clone();
        let t2 = thread::spawn(move || {
            m2.enter(1);
            let _ = m2.insert(42, 10);
            let _ = m2.get(&42);
            m2.exit(1);
        });

        t1.join().unwrap();
        t2.join().unwrap();
    });
}
