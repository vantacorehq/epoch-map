//! Fixed-capacity concurrent hash map with quiescent-epoch reclamation.
//!
//! * `no_std` (needs `alloc`): the table is allocated **once** in [`EpochMap::new`],
//!   nothing is allocated afterwards.
//! * 100% safe Rust: every slot is a trio of atomics, so there is no data race and no UB.
//! * `get`, `insert` and `remove` never take a lock. The only waiting is a short
//!   spin while another thread is publishing a freshly claimed slot.
//! * Keys and values are machine words (see [`Word`]).
//!
//! # How it works
//!
//! Open addressing with linear probing. A slot moves through
//! `EMPTY -> BUSY -> OCCUPIED -> TOMBSTONE`. A key is written into a slot exactly once
//! and then never changes, so a removed key just becomes a tombstone and a later
//! `insert` of the same key revives that very slot.
//!
//! Tombstones are what [`EpochMap::try_advance_and_reclaim`] frees. Turning a tombstone
//! back into `EMPTY` while other threads probe would break probe chains, so reclamation
//! only runs when **every thread is quiescent**: threads announce themselves with
//! [`EpochMap::enter`] / [`EpochMap::exit`], and reclamation backs off if anyone is
//! inside. Each successful reclamation advances the global epoch.
//!
//! Call `enter(tid)` / `exit(tid)` around a batch of operations if you also call
//! `try_advance_and_reclaim`; each thread needs its own `tid < 64`.

#![no_std]

extern crate alloc;
#[cfg(test)]
extern crate std;

use alloc::boxed::Box;
use core::fmt;
use core::marker::PhantomData;
use core::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

const EMPTY: u64 = 0;
const BUSY: u64 = 1;
const OCCUPIED: u64 = 2;
const TOMBSTONE: u64 = 3;

/// Maximum number of thread ids usable with [`EpochMap::enter`].
pub const MAX_THREADS: usize = 64;

/// Types that can be stored in an [`EpochMap`]: they round-trip through a `u64`.
pub trait Word: Copy + Eq {
    fn to_word(self) -> u64;
    fn from_word(w: u64) -> Self;
}

macro_rules! impl_word {
    ($($t:ty),*) => {$(
        impl Word for $t {
            #[inline] fn to_word(self) -> u64 { self as u64 }
            #[inline] fn from_word(w: u64) -> Self { w as $t }
        }
    )*};
}
impl_word!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize);

impl Word for bool {
    #[inline]
    fn to_word(self) -> u64 {
        self as u64
    }
    #[inline]
    fn from_word(w: u64) -> Self {
        w != 0
    }
}

/// Returned by [`EpochMap::try_insert`] when there is no free slot for a new key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MapFull;

impl fmt::Display for MapFull {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("EpochMap is full")
    }
}

struct Slot {
    state: AtomicU64,
    key: AtomicU64,
    value: AtomicU64,
}

impl Slot {
    fn new() -> Self {
        Slot {
            state: AtomicU64::new(EMPTY),
            key: AtomicU64::new(0),
            value: AtomicU64::new(0),
        }
    }
}

pub struct EpochMap<K, V, const N: usize> {
    slots: Box<[Slot]>,
    epoch_global: AtomicU64,
    epoch_locals: [AtomicU64; MAX_THREADS],
    reclaiming: AtomicBool,
    retired_count: AtomicUsize,
    _marker: PhantomData<fn() -> (K, V)>,
}

#[inline]
fn mix(mut x: u64) -> u64 {
    // splitmix64 finalizer
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

impl<K: Word, V: Word, const N: usize> Default for EpochMap<K, V, N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Word, V: Word, const N: usize> EpochMap<K, V, N> {
    /// Creates an empty map. `N` must be a power of two.
    pub fn new() -> Self {
        assert!(N.is_power_of_two(), "capacity must be a power of two");
        let slots: Box<[Slot]> = (0..N).map(|_| Slot::new()).collect();
        Self {
            slots,
            epoch_global: AtomicU64::new(1),
            epoch_locals: core::array::from_fn(|_| AtomicU64::new(0)),
            reclaiming: AtomicBool::new(false),
            retired_count: AtomicUsize::new(0),
            _marker: PhantomData,
        }
    }

    /// Number of slots.
    pub const fn capacity(&self) -> usize {
        N
    }

    #[inline]
    fn start(key: u64) -> usize {
        (mix(key) as usize) & (N - 1)
    }

    /// Marks thread `tid` (`< MAX_THREADS`) as active. Waits while a reclamation runs.
    pub fn enter(&self, tid: usize) {
        loop {
            while self.reclaiming.load(Ordering::SeqCst) {
                core::hint::spin_loop();
            }
            let g = self.epoch_global.load(Ordering::SeqCst);
            self.epoch_locals[tid].store(g, Ordering::SeqCst);
            if !self.reclaiming.load(Ordering::SeqCst) {
                return;
            }
            self.epoch_locals[tid].store(0, Ordering::SeqCst);
        }
    }

    /// Marks thread `tid` as quiescent.
    pub fn exit(&self, tid: usize) {
        self.epoch_locals[tid].store(0, Ordering::SeqCst);
    }

    /// Current global epoch (grows by one per successful reclamation).
    pub fn epoch(&self) -> u64 {
        self.epoch_global.load(Ordering::Acquire)
    }

    /// Frees tombstones if all threads are quiescent; returns how many slots were freed.
    ///
    /// Returns `0` without doing anything if some thread is between `enter` and `exit`
    /// or another reclamation is running. A tombstone is freed only if the slot after it
    /// is empty, so probe chains stay intact; this cascades backwards through runs.
    pub fn try_advance_and_reclaim(&self) -> usize {
        if self
            .reclaiming
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return 0;
        }
        if self
            .epoch_locals
            .iter()
            .any(|l| l.load(Ordering::SeqCst) != 0)
        {
            self.reclaiming.store(false, Ordering::SeqCst);
            return 0;
        }

        let mut freed = 0usize;
        loop {
            let mut changed = false;
            for i in (0..N).rev() {
                let slot = &self.slots[i];
                if slot.state.load(Ordering::Acquire) == TOMBSTONE
                    && self.slots[(i + 1) & (N - 1)].state.load(Ordering::Acquire) == EMPTY
                {
                    slot.state.store(EMPTY, Ordering::Release);
                    freed += 1;
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }

        if freed > 0 {
            self.retired_count.fetch_sub(freed, Ordering::Relaxed);
        }
        self.epoch_global.fetch_add(1, Ordering::AcqRel);
        self.reclaiming.store(false, Ordering::SeqCst);
        freed
    }

    /// Inserts or updates. Returns the previous value if the key was present.
    ///
    /// Panics if the map is full; use [`try_insert`](Self::try_insert) to handle that.
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        self.try_insert(key, value).expect("EpochMap is full")
    }

    /// Like [`insert`](Self::insert) but reports a full table instead of panicking.
    pub fn try_insert(&self, key: K, value: V) -> Result<Option<V>, MapFull> {
        let kw = key.to_word();
        let vw = value.to_word();
        let start = Self::start(kw);

        for probe in 0..N {
            let slot = &self.slots[(start + probe) & (N - 1)];
            loop {
                match slot.state.load(Ordering::Acquire) {
                    EMPTY => {
                        if slot
                            .state
                            .compare_exchange(EMPTY, BUSY, Ordering::Acquire, Ordering::Relaxed)
                            .is_ok()
                        {
                            slot.key.store(kw, Ordering::Relaxed);
                            slot.value.store(vw, Ordering::Relaxed);
                            slot.state.store(OCCUPIED, Ordering::Release);
                            return Ok(None);
                        }
                    }
                    BUSY => core::hint::spin_loop(),
                    OCCUPIED => {
                        if slot.key.load(Ordering::Relaxed) == kw {
                            let old = slot.value.swap(vw, Ordering::AcqRel);
                            return Ok(Some(V::from_word(old)));
                        }
                        break;
                    }
                    _ => {
                        // TOMBSTONE: the key is kept, so the same key revives its slot.
                        if slot.key.load(Ordering::Relaxed) == kw {
                            if slot
                                .state
                                .compare_exchange(
                                    TOMBSTONE,
                                    BUSY,
                                    Ordering::Acquire,
                                    Ordering::Relaxed,
                                )
                                .is_ok()
                            {
                                slot.value.store(vw, Ordering::Relaxed);
                                slot.state.store(OCCUPIED, Ordering::Release);
                                self.retired_count.fetch_sub(1, Ordering::Relaxed);
                                return Ok(None);
                            }
                            continue;
                        }
                        break;
                    }
                }
            }
        }
        Err(MapFull)
    }

    pub fn get(&self, key: &K) -> Option<V> {
        let kw = key.to_word();
        let start = Self::start(kw);

        for probe in 0..N {
            let slot = &self.slots[(start + probe) & (N - 1)];
            loop {
                match slot.state.load(Ordering::Acquire) {
                    EMPTY => return None,
                    BUSY => core::hint::spin_loop(),
                    OCCUPIED => {
                        if slot.key.load(Ordering::Relaxed) == kw {
                            return Some(V::from_word(slot.value.load(Ordering::Acquire)));
                        }
                        break;
                    }
                    _ => {
                        if slot.key.load(Ordering::Relaxed) == kw {
                            return None;
                        }
                        break;
                    }
                }
            }
        }
        None
    }

    pub fn remove(&self, key: &K) -> Option<V> {
        let kw = key.to_word();
        let start = Self::start(kw);

        for probe in 0..N {
            let slot = &self.slots[(start + probe) & (N - 1)];
            loop {
                match slot.state.load(Ordering::Acquire) {
                    EMPTY => return None,
                    BUSY => core::hint::spin_loop(),
                    OCCUPIED => {
                        if slot.key.load(Ordering::Relaxed) != kw {
                            break;
                        }
                        let v = slot.value.load(Ordering::Acquire);
                        if slot
                            .state
                            .compare_exchange(
                                OCCUPIED,
                                TOMBSTONE,
                                Ordering::AcqRel,
                                Ordering::Relaxed,
                            )
                            .is_ok()
                        {
                            self.retired_count.fetch_add(1, Ordering::Relaxed);
                            return Some(V::from_word(v));
                        }
                    }
                    _ => {
                        if slot.key.load(Ordering::Relaxed) == kw {
                            return None;
                        }
                        break;
                    }
                }
            }
        }
        None
    }

    /// Number of live entries (a snapshot; not exact under concurrent writes).
    pub fn len_approx(&self) -> usize {
        self.slots
            .iter()
            .filter(|s| s.state.load(Ordering::Relaxed) == OCCUPIED)
            .count()
    }

    /// Number of tombstones waiting for reclamation.
    pub fn retired_approx(&self) -> usize {
        self.retired_count.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests;
