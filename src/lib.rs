#![cfg_attr(not(test), no_std)]

use core::cell::UnsafeCell;
use core::hash::Hash;
use core::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use core::marker::PhantomData;

const EMPTY: u64 = 0;
const TOMBSTONE: u64 = 1;
const OCCUPIED: u64 = 2;
const MAX_THREADS: usize = 64;

#[repr(C)]
struct Slot<K, V> {
    state: AtomicU64,
    key: UnsafeCell<K>,
    value: UnsafeCell<V>,
    retired_epoch: AtomicU64,
}

pub struct EpochMap<K, V, const N: usize> {
    slots: UnsafeCell<[Slot<K, V>; N]>,
    epoch_global: AtomicU64,
    epoch_locals: [AtomicU64; MAX_THREADS],
    retired_count: AtomicUsize,
    _marker: PhantomData<(K, V)>,
}

unsafe impl<K: Send, V: Send, const N: usize> Sync for EpochMap<K, V, N> {}

impl<K: Copy + Eq + Hash + Default, V: Copy + Default, const N: usize> EpochMap<K, V, N> {
    pub fn new() -> Self {
        assert!(N.is_power_of_two());
        Self {
            slots: UnsafeCell::new(core::array::from_fn(|_| Slot {
                state: AtomicU64::new(EMPTY),
                key: UnsafeCell::new(K::default()),
                value: UnsafeCell::new(V::default()),
                retired_epoch: AtomicU64::new(0),
            })),
            epoch_global: AtomicU64::new(1),
            epoch_locals: core::array::from_fn(|_| AtomicU64::new(0)),
            retired_count: AtomicUsize::new(0),
            _marker: PhantomData,
        }
    }

    fn hash(&self, key: &K) -> usize {
        let mut h: u64 = 0xcbf29ce484222325;
        let bytes = unsafe {
            core::slice::from_raw_parts(key as *const K as *const u8, core::mem::size_of::<K>())
        };
        for &b in bytes {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        (h as usize) & (N - 1)
    }

    pub fn enter(&self, tid: usize) {
        let g = self.epoch_global.load(Ordering::Acquire);
        self.epoch_locals[tid].store(g, Ordering::Release);
    }

    pub fn exit(&self, tid: usize) {
        self.epoch_locals[tid].store(0, Ordering::Release);
    }

    fn min_active_epoch(&self) -> u64 {
        let mut min = u64::MAX;
        for local in &self.epoch_locals {
            let e = local.load(Ordering::Acquire);
            if e != 0 && e < min { min = e; }
        }
        if min == u64::MAX { self.epoch_global.load(Ordering::Acquire) } else { min }
    }

    pub fn try_advance_and_reclaim(&self) {
        let current = self.epoch_global.load(Ordering::Acquire);
        let min_active = self.min_active_epoch();
        if min_active >= current {
            let _ = self.epoch_global.compare_exchange(current, current.wrapping_add(1), Ordering::AcqRel, Ordering::Acquire);
        }
        let safe = self.min_active_epoch();
        for i in 0..N {
            let slot = unsafe { &*(*self.slots.get()).as_ptr().add(i) };
            if slot.state.load(Ordering::Acquire) == TOMBSTONE {
                let retired = slot.retired_epoch.load(Ordering::Acquire);
                if retired != 0 && retired < safe {
                    if slot.state.compare_exchange(TOMBSTONE, EMPTY, Ordering::AcqRel, Ordering::Acquire).is_ok() {
                        self.retired_count.fetch_sub(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let start = self.hash(&key);
        for probe in 0..N {
            let idx = (start + probe) & (N - 1);
            let slot = unsafe { &*(*self.slots.get()).as_ptr().add(idx) };
            loop {
                let state = slot.state.load(Ordering::Acquire);
                if state == EMPTY || state == TOMBSTONE {
                    if slot.state.compare_exchange(state, OCCUPIED, Ordering::AcqRel, Ordering::Acquire).is_ok() {
                        unsafe { *slot.key.get() = key; *slot.value.get() = value; }
                        return None;
                    }
                    continue;
                }
                if state == OCCUPIED {
                    if unsafe { *slot.key.get() } == key {
                        let old = unsafe { *slot.value.get() };
                        unsafe { *slot.value.get() = value; }
                        return Some(old);
                    }
                    break;
                }
                break;
            }
        }
        None
    }

    pub fn get(&self, key: &K) -> Option<V> {
        let start = self.hash(key);
        for probe in 0..N {
            let idx = (start + probe) & (N - 1);
            let slot = unsafe { &*(*self.slots.get()).as_ptr().add(idx) };
            let state = slot.state.load(Ordering::Acquire);
            if state == EMPTY { return None; }
            if state == OCCUPIED && unsafe { *slot.key.get() } == *key {
                return Some(unsafe { *slot.value.get() });
            }
        }
        None
    }

    pub fn remove(&self, key: &K) -> Option<V> {
        let start = self.hash(key);
        for probe in 0..N {
            let idx = (start + probe) & (N - 1);
            let slot = unsafe { &*(*self.slots.get()).as_ptr().add(idx) };
            loop {
                let state = slot.state.load(Ordering::Acquire);
                if state == EMPTY { return None; }
                if state == OCCUPIED && unsafe { *slot.key.get() } == *key {
                    if slot.state.compare_exchange(OCCUPIED, TOMBSTONE, Ordering::AcqRel, Ordering::Acquire).is_ok() {
                        slot.retired_epoch.store(self.epoch_global.load(Ordering::Acquire), Ordering::Release);
                        self.retired_count.fetch_add(1, Ordering::Relaxed);
                        return Some(unsafe { *slot.value.get() });
                    }
                    continue;
                }
                break;
            }
        }
        None
    }

    pub fn retired_approx(&self) -> usize {
        self.retired_count.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::EpochMap;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn basic() {
        let m = EpochMap::<u64, u64, 1024>::new();
        m.enter(0);
        assert_eq!(m.insert(1, 100), None);
        assert_eq!(m.get(&1), Some(100));
        assert_eq!(m.remove(&1), Some(100));
        assert_eq!(m.get(&1), None);
        m.exit(0);
    }

    #[test]
    fn concurrent() {
        let m = Arc::new(EpochMap::<u64, u64, 8192>::new());
        let mut hs = vec![];
        for tid in 0..4 {
            let mm = m.clone();
            hs.push(thread::spawn(move || {
                mm.enter(tid);
                for i in 0..200 { mm.insert((tid as u64)*1000 + i, i); }
                mm.exit(tid);
            }));
        }
        for h in hs { h.join().unwrap(); }
        m.enter(0);
        assert!(m.get(&0).is_some());
        m.exit(0);
    }
}
