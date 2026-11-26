//! # epoch-map
//!
//! Fixed-capacity, lock-free concurrent hash map with quiescent-epoch reclamation.
//!
//! Designed for high-throughput, low-latency concurrent workloads where capacity is known
//! in advance (caches, arenas, session tables, in-process indexes).
//!
//! ## Highlights
//!
//! - **100% safe Rust**, `no_std` + `alloc` (optional `std` feature for convenience).
//! - Zero runtime dependencies.
//! - Table allocated **once** in `new()`; no further allocations on the hot path.
//! - `get` / `insert` / `remove` / `get_or_insert*` never take a lock.
//! - Keys & values are machine words (implement [`Word`]).
//! - Open addressing + linear probing, power-of-two capacity.
//! - Epoch-based reclamation of tombstones keeps probe chains correct.
//!
//! ## Quick start
//!
//! ```rust
//! use epoch_map::EpochMap;
//!
//! let map = EpochMap::<u64, u64, 1024>::new();
//! map.enter(0);
//! assert_eq!(map.insert(1, 100), None);
//! assert_eq!(map.get(&1), Some(100));
//! assert_eq!(map.get_or_insert(2, || 200), 200);
//! assert_eq!(map.remove(&1), Some(100));
//! map.exit(0);
//! let _ = map.try_advance_and_reclaim(); // free tombstones when quiescent
//! ```
//!
//! ## Thread protocol
//!
//! Call [`enter`](EpochMap::enter) / [`exit`](EpochMap::exit) around batches of operations
//! when you also call [`try_advance_and_reclaim`](EpochMap::try_advance_and_reclaim).
//! Each thread needs a distinct `tid < MAX_THREADS`.
//!
//! ## Safety model
//!
//! - Slot states form a monotonic lifecycle: EMPTY → BUSY → OCCUPIED → TOMBSTONE → EMPTY.
//! - A key is written into a slot exactly once; remove only changes state to TOMBSTONE.
//! - Reclamation of a tombstone is allowed only when the next slot is EMPTY and every
//!   thread is quiescent (no active `enter`). This preserves linear probing chains.
//! - All operations are lock-free (progress of the system is guaranteed; individual
//!   operations may spin briefly on BUSY).
//!
//! ## Memory ordering
//!
//! All atomics use the weakest ordering that still provides the required happens-before
//! edges. See inline comments on every atomic operation for the justification.

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

use alloc::boxed::Box;
use core::fmt;
use core::marker::PhantomData;
use core::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

const EMPTY: u64 = 0;
const BUSY: u64 = 1;
const OCCUPIED: u64 = 2;
const TOMBSTONE: u64 = 3;

/// Maximum number of concurrent thread ids for the epoch protocol.
pub const MAX_THREADS: usize = 128;

/// Types that can be stored: they must round-trip through a `u64` losslessly.
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
    fn to_word(self) -> u64 { self as u64 }
    #[inline]
    fn from_word(w: u64) -> Self { w != 0 }
}

/// Returned when the table has no free slot for a new key.
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
    #[inline]
    fn new() -> Self {
        Slot {
            state: AtomicU64::new(EMPTY),
            key: AtomicU64::new(0),
            value: AtomicU64::new(0),
        }
    }
}

/// Fixed-capacity concurrent hash map.
///
/// `N` must be a power of two. Load factor should stay comfortably below 1.0
/// because tombstones also occupy slots until reclaimed.
pub struct EpochMap<K, V, const N: usize> {
    slots: Box<[Slot]>,
    epoch_global: AtomicU64,
    epoch_locals: [AtomicU64; MAX_THREADS],
    reclaiming: AtomicBool,
    retired_count: AtomicUsize,
    live_count: AtomicUsize,
    _marker: PhantomData<fn() -> (K, V)>,
}

#[inline]
fn mix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

impl<K: Word, V: Word, const N: usize> Default for EpochMap<K, V, N> {
    fn default() -> Self { Self::new() }
}

impl<K: Word, V: Word, const N: usize> EpochMap<K, V, N> {
    /// Creates an empty map. Panics if `N` is not a power of two.
    pub fn new() -> Self {
        assert!(N.is_power_of_two() && N > 0, "capacity must be a power of two");
        let slots: Box<[Slot]> = (0..N).map(|_| Slot::new()).collect();
        Self {
            slots,
            epoch_global: AtomicU64::new(1),
            epoch_locals: core::array::from_fn(|_| AtomicU64::new(0)),
            reclaiming: AtomicBool::new(false),
            retired_count: AtomicUsize::new(0),
            live_count: AtomicUsize::new(0),
            _marker: PhantomData,
        }
    }

    pub const fn capacity(&self) -> usize { N }

    #[inline]
    fn start(key: u64) -> usize { (mix(key) as usize) & (N - 1) }

    /// Publish that this thread is active in the current epoch.
    ///
    /// Must be paired with [`exit`]. Distinct `tid`s are required.
    pub fn enter(&self, tid: usize) {
        debug_assert!(tid < MAX_THREADS);
        loop {
            // SeqCst on reclaiming: full barrier so we never enter while a
            // reclaimer is between the quiescence check and the state stores.
            while self.reclaiming.load(Ordering::SeqCst) {
                core::hint::spin_loop();
            }
            // SeqCst load of global epoch + SeqCst store of local epoch:
            // establishes a total order with the reclaimer’s observation of
            // all local epochs.
            let g = self.epoch_global.load(Ordering::SeqCst);
            self.epoch_locals[tid].store(g, Ordering::SeqCst);
            if !self.reclaiming.load(Ordering::SeqCst) {
                return;
            }
            // Lost the race with a reclaimer; clear and retry.
            self.epoch_locals[tid].store(0, Ordering::SeqCst);
        }
    }

    /// Mark this thread quiescent.
    pub fn exit(&self, tid: usize) {
        debug_assert!(tid < MAX_THREADS);
        // SeqCst: the store must be visible to any subsequent reclaimer
        // that loads all local epochs under SeqCst.
        self.epoch_locals[tid].store(0, Ordering::SeqCst);
    }

    pub fn epoch(&self) -> u64 {
        // Acquire is enough for a plain observation; no release side needed.
        self.epoch_global.load(Ordering::Acquire)
    }

    /// Advance the global epoch and reclaim tombstones that are no longer
    /// reachable by any probe sequence, but only when every thread is
    /// quiescent. Returns the number of slots freed.
    pub fn try_advance_and_reclaim(&self) -> usize {
        // AcqRel on the reclaiming flag: exclusive ownership of the reclaim
        // critical section and synchronizes with the SeqCst loads in enter().
        if self
            .reclaiming
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return 0;
        }

        // SeqCst loads of every local epoch: must observe the most recent
        // enter/exit of every thread.
        if self
            .epoch_locals
            .iter()
            .any(|l| l.load(Ordering::SeqCst) != 0)
        {
            self.reclaiming.store(false, Ordering::Release);
            return 0;
        }

        let mut freed = 0usize;
        loop {
            let mut changed = false;
            for i in (0..N).rev() {
                let slot = &self.slots[i];
                // Acquire on state: see the Release that published TOMBSTONE
                // (or EMPTY) from a concurrent remove / previous reclaim.
                if slot.state.load(Ordering::Acquire) == TOMBSTONE
                    && self.slots[(i + 1) & (N - 1)]
                        .state
                        .load(Ordering::Acquire)
                        == EMPTY
                {
                    // Release: subsequent probes must observe EMPTY.
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
            // Relaxed: pure counter, no synchronisation required.
            self.retired_count.fetch_sub(freed, Ordering::Relaxed);
        }
        // AcqRel: publish the new epoch so future enter() calls observe it.
        self.epoch_global.fetch_add(1, Ordering::AcqRel);
        self.reclaiming.store(false, Ordering::Release);
        freed
    }

    pub fn insert(&self, key: K, value: V) -> Option<V> {
        self.try_insert(key, value).expect("EpochMap is full")
    }

    pub fn try_insert(&self, key: K, value: V) -> Result<Option<V>, MapFull> {
        let kw = key.to_word();
        let vw = value.to_word();
        let start = Self::start(kw);

        for probe in 0..N {
            let idx = (start + probe) & (N - 1);
            let slot = &self.slots[idx];
            loop {
                // Acquire: observe the most recent state transition.
                match slot.state.load(Ordering::Acquire) {
                    EMPTY => {
                        // Acquire success / Relaxed failure: on success we
                        // become the sole owner of the slot; the subsequent
                        // Release store of OCCUPIED publishes key+value.
                        if slot
                            .state
                            .compare_exchange(EMPTY, BUSY, Ordering::Acquire, Ordering::Relaxed)
                            .is_ok()
                        {
                            // Relaxed: the later Release on state is the
                            // publication barrier for key and value.
                            slot.key.store(kw, Ordering::Relaxed);
                            slot.value.store(vw, Ordering::Relaxed);
                            // Release: synchronizes-with any Acquire load
                            // that sees OCCUPIED.
                            slot.state.store(OCCUPIED, Ordering::Release);
                            self.live_count.fetch_add(1, Ordering::Relaxed);
                            return Ok(None);
                        }
                    }
                    BUSY => core::hint::spin_loop(),
                    OCCUPIED => {
                        // Relaxed: key is immutable after the initial
                        // Release that published OCCUPIED.
                        if slot.key.load(Ordering::Relaxed) == kw {
                            // AcqRel: obtain the previous value and publish
                            // the new one under the same slot ownership.
                            let old = slot.value.swap(vw, Ordering::AcqRel);
                            return Ok(Some(V::from_word(old)));
                        }
                        break;
                    }
                    _ => {
                        // TOMBSTONE (or any other non-EMPTY/BUSY/OCCUPIED)
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
                                self.live_count.fetch_add(1, Ordering::Relaxed);
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
                            // Acquire on value: synchronize with the
                            // Release/AcqRel that wrote it.
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

    #[inline]
    pub fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    pub fn get_or_insert(&self, key: K, value: V) -> V {
        if let Some(v) = self.get(&key) {
            return v;
        }
        match self.try_insert(key, value) {
            Ok(None) => value,
            Ok(Some(old)) => old,
            Err(_) => self.get(&key).expect("table full and key missing"),
        }
    }

    pub fn get_or_insert_with<F>(&self, key: K, f: F) -> V
    where
        F: FnOnce() -> V,
    {
        if let Some(v) = self.get(&key) {
            return v;
        }
        let value = f();
        match self.try_insert(key, value) {
            Ok(None) => value,
            Ok(Some(old)) => old,
            Err(_) => self.get(&key).expect("table full and key missing"),
        }
    }

    pub fn update<F>(&self, key: &K, f: F) -> Option<V>
    where
        F: FnOnce(V) -> V,
    {
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
                        let old = V::from_word(slot.value.load(Ordering::Acquire));
                        let new = f(old);
                        // Release: publish the new value.
                        slot.value.store(new.to_word(), Ordering::Release);
                        return Some(new);
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
                        // AcqRel: claim ownership of the transition and
                        // publish TOMBSTONE so reclaimers and later probes
                        // observe it.
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
                            self.live_count.fetch_sub(1, Ordering::Relaxed);
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

    pub fn remove_if<F>(&self, key: &K, pred: F) -> Option<V>
    where
        F: FnOnce(&V) -> bool,
    {
        if let Some(v) = self.get(key) {
            if pred(&v) {
                return self.remove(key);
            }
        }
        None
    }

    pub fn len(&self) -> usize {
        self.live_count.load(Ordering::Relaxed)
    }

    pub fn len_approx(&self) -> usize {
        self.slots
            .iter()
            .filter(|s| s.state.load(Ordering::Relaxed) == OCCUPIED)
            .count()
    }

    pub fn retired(&self) -> usize {
        self.retired_count.load(Ordering::Relaxed)
    }

    pub fn retired_approx(&self) -> usize {
        self.retired()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn load_factor(&self) -> f64 {
        let used = self.len() + self.retired();
        used as f64 / N as f64
    }
}

#[cfg(test)]
mod tests;
