//! Lock-free concurrent hash map with epoch-based reclamation.
//! Fixed capacity. Open addressing. Zero dependencies. no_std.

#![no_std]

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
        assert!(N.is_power_of_two(), "capacity must be power of two");
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

    #[inline]
    fn hash(&self, key: &K) -> usize {
        // FNV-1a style hash that works in no_std without external crates
        let mut h: u64 = 0xcbf29ce484222325;
        let bytes = unsafe {
            core::slice::from_raw_parts(
                key as *const K as *const u8,
                core::mem::size_of::<K>(),
            )
        };
        for &b in bytes {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        (h as usize) & (N - 1)
    }

    /// Enter critical section. Must be called before any operation.
    #[inline]
    pub fn enter(&self, tid: usize) {
        debug_assert!(tid < MAX_THREADS);
        let g = self.epoch_global.load(Ordering::Acquire);
        self.epoch_locals[tid].store(g, Ordering::Release);
    }

    /// Exit critical section.
    #[inline]
    pub fn exit(&self, tid: usize) {
        debug_assert!(tid < MAX_THREADS);
        self.epoch_locals[tid].store(0, Ordering::Release);
    }

    fn min_active_epoch(&self) -> u64 {
        let mut min = u64::MAX;
        for local in &self.epoch_locals {
            let e = local.load(Ordering::Acquire);
            if e != 0 && e < min {
                min = e;
            }
        }
        if min == u64::MAX {
            self.epoch_global.load(Ordering::Acquire)
        } else {
            min
        }
    }

    /// Advance global epoch and reclaim tombstones that are safe.
    pub fn try_advance_and_reclaim(&self) {
        let current = self.epoch_global.load(Ordering::Acquire);
        let min_active = self.min_active_epoch();

        if min_active >= current {
            let _ = self.epoch_global.compare_exchange(
                current,
                current.wrapping_add(1),
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }

        let safe_epoch = self.min_active_epoch();
        for i in 0..N {
            let slot = unsafe { &*(*self.slots.get()).as_ptr().add(i) };
            let state = slot.state.load(Ordering::Acquire);
            if state == TOMBSTONE {
                let retired = slot.retired_epoch.load(Ordering::Acquire);
                if retired != 0 && retired < safe_epoch {
                    if slot
                        .state
                        .compare_exchange(TOMBSTONE, EMPTY, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
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
                    if slot
                        .state
                        .compare_exchange(state, OCCUPIED, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        unsafe {
                            *slot.key.get() = key;
                            *slot.value.get() = value;
                        }
                        return None;
                    }
                    continue;
                }
                if state == OCCUPIED {
                    let existing = unsafe { *slot.key.get() };
                    if existing == key {
                        let old = unsafe { *slot.value.get() };
                        unsafe { *slot.value.get() = value };
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
            if state == EMPTY {
                return None;
            }
            if state == OCCUPIED {
                let existing = unsafe { *slot.key.get() };
                if existing == *key {
                    return Some(unsafe { *slot.value.get() });
                }
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
                if state == EMPTY {
                    return None;
                }
                if state == OCCUPIED {
                    let existing = unsafe { *slot.key.get() };
                    if existing == *key {
                        if slot
                            .state
                            .compare_exchange(OCCUPIED, TOMBSTONE, Ordering::AcqRel, Ordering::Acquire)
                            .is_ok()
                        {
                            let epoch = self.epoch_global.load(Ordering::Acquire);
                            slot.retired_epoch.store(epoch, Ordering::Release);
                            self.retired_count.fetch_add(1, Ordering::Relaxed);
                            return Some(unsafe { *slot.value.get() });
                        }
                        continue;
                    }
                    break;
                }
                break;
            }
        }
        None
    }

    pub fn len_approx(&self) -> usize {
        let mut count = 0;
        for i in 0..N {
            let slot = unsafe { &*(*self.slots.get()).as_ptr().add(i) };
            if slot.state.load(Ordering::Relaxed) == OCCUPIED {
                count += 1;
            }
        }
        count
    }

    pub fn retired_approx(&self) -> usize {
        self.retired_count.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests;
