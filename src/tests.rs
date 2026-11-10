#[cfg(test)]
mod tests {
    use super::*;
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
        map.exit(0);
    }

    #[test]
    fn concurrent_inserts() {
        let map = Arc::new(EpochMap::<u64, u64, 8192>::new());
        let mut handles = vec![];

        for tid in 0..8 {
            let m = map.clone();
            handles.push(thread::spawn(move || {
                m.enter(tid);
                for i in 0..500 {
                    let k = (tid as u64) * 1000 + i;
                    m.insert(k, k * 2);
                }
                m.exit(tid);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        map.enter(0);
        let mut found = 0;
        for tid in 0..8 {
            for i in 0..500 {
                let k = (tid as u64) * 1000 + i;
                if map.get(&k) == Some(k * 2) {
                    found += 1;
                }
            }
        }
        map.exit(0);
        assert!(found > 3500, "found only {}", found);
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
        map.exit(0);

        map.try_advance_and_reclaim();
        let after = map.retired_approx();
        assert!(after < before, "reclamation did not reduce tombstones: {} -> {}", before, after);
    }
}
