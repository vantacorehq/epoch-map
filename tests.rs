use super::{EpochMap, MapFull};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

#[test]
fn basic_insert_get_remove() {
    let map = EpochMap::<u64, u64, 1024>::new();
    map.enter(0);
    assert_eq!(map.insert(1, 100), None);
    assert_eq!(map.get(&1), Some(100));
    assert_eq!(map.remove(&1), Some(100));
    assert_eq!(map.get(&1), None);
    assert_eq!(map.remove(&1), None);
    map.exit(0);
}

#[test]
fn insert_existing_returns_old_value() {
    let map = EpochMap::<u64, u64, 64>::new();
    assert_eq!(map.insert(7, 1), None);
    assert_eq!(map.insert(7, 2), Some(1));
    assert_eq!(map.get(&7), Some(2));
    assert_eq!(map.len_approx(), 1);
}

#[test]
fn key_zero_and_other_types_work() {
    let map = EpochMap::<u32, i64, 64>::new();
    assert_eq!(map.insert(0, -5), None);
    assert_eq!(map.get(&0), Some(-5));
}

#[test]
fn removed_key_can_be_reinserted() {
    let map = EpochMap::<u64, u64, 64>::new();
    map.insert(5, 1);
    map.remove(&5);
    assert_eq!(map.retired_approx(), 1);
    assert_eq!(map.insert(5, 2), None);
    assert_eq!(map.retired_approx(), 0);
    assert_eq!(map.get(&5), Some(2));
}

#[test]
fn full_table_reports_error() {
    let map = EpochMap::<u64, u64, 8>::new();
    for i in 0..8u64 {
        assert_eq!(map.try_insert(i, i), Ok(None));
    }
    assert_eq!(map.try_insert(100, 1), Err(MapFull));
    // existing keys still update when full
    assert_eq!(map.try_insert(3, 33), Ok(Some(3)));
}

#[test]
fn concurrent_inserts() {
    let map = Arc::new(EpochMap::<u64, u64, 8192>::new());
    let mut handles = vec![];

    for tid in 0..8usize {
        let m = map.clone();
        handles.push(thread::spawn(move || {
            m.enter(tid);
            for i in 0..500u64 {
                let k = (tid as u64) * 1000 + i;
                m.insert(k, k * 2);
            }
            m.exit(tid);
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    for tid in 0..8u64 {
        for i in 0..500u64 {
            let k = tid * 1000 + i;
            assert_eq!(map.get(&k), Some(k * 2), "key {} lost", k);
        }
    }
    assert_eq!(map.len_approx(), 4000);
}

#[test]
fn same_key_from_many_threads_is_stored_once() {
    let map = Arc::new(EpochMap::<u64, u64, 1024>::new());
    let mut handles = vec![];
    for tid in 0..8usize {
        let m = map.clone();
        handles.push(thread::spawn(move || {
            for k in 0..200u64 {
                m.insert(k, tid as u64);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(map.len_approx(), 200);
}

#[test]
fn reclamation_reduces_tombstones() {
    let map = EpochMap::<u64, u64, 2048>::new();
    map.enter(0);
    for i in 0..1000u64 {
        map.insert(i, i);
    }
    for i in 0..800u64 {
        map.remove(&i);
    }
    let before = map.retired_approx();
    assert_eq!(before, 800);
    map.exit(0);

    map.try_advance_and_reclaim();
    let after = map.retired_approx();
    assert!(after < before, "{} -> {}", before, after);
    for i in 800..1000u64 {
        assert_eq!(map.get(&i), Some(i));
    }
    for i in 0..800u64 {
        assert_eq!(map.get(&i), None);
    }
}

#[test]
fn removing_everything_then_reclaiming_frees_all() {
    let map = EpochMap::<u64, u64, 256>::new();
    for i in 0..100u64 {
        map.insert(i, i);
    }
    for i in 0..100u64 {
        map.remove(&i);
    }
    assert_eq!(map.retired_approx(), 100);
    assert_eq!(map.try_advance_and_reclaim(), 100);
    assert_eq!(map.retired_approx(), 0);
    assert_eq!(map.len_approx(), 0);
}

#[test]
fn reclaim_backs_off_while_a_thread_is_active() {
    let map = EpochMap::<u64, u64, 64>::new();
    map.insert(1, 1);
    map.remove(&1);
    map.enter(3);
    let e = map.epoch();
    assert_eq!(map.try_advance_and_reclaim(), 0);
    assert_eq!(map.epoch(), e);
    map.exit(3);
    assert_eq!(map.try_advance_and_reclaim(), 1);
    assert_eq!(map.epoch(), e + 1);
}

#[test]
fn concurrent_work_with_background_reclaimer() {
    let map = Arc::new(EpochMap::<u64, u64, 8192>::new());
    let stop = Arc::new(AtomicBool::new(false));

    let reclaimer = {
        let m = map.clone();
        let stop = stop.clone();
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                m.try_advance_and_reclaim();
                thread::yield_now();
            }
        })
    };

    let mut workers = vec![];
    for tid in 0..4usize {
        let m = map.clone();
        workers.push(thread::spawn(move || {
            for i in 0..500u64 {
                let k = (tid as u64) * 10_000 + i;
                m.enter(tid);
                m.insert(k, k);
                m.exit(tid);
                if i % 2 == 1 {
                    m.enter(tid);
                    assert_eq!(m.remove(&k), Some(k));
                    m.exit(tid);
                }
            }
        }));
    }
    for w in workers {
        w.join().unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    reclaimer.join().unwrap();

    for tid in 0..4u64 {
        for i in 0..500u64 {
            let k = tid * 10_000 + i;
            let expected = if i % 2 == 0 { Some(k) } else { None };
            assert_eq!(map.get(&k), expected, "key {}", k);
        }
    }
}
