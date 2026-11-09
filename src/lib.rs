//! Lock-free concurrent hash map with epoch-based reclamation.
//! Fixed capacity. Open addressing. Zero dependencies. no_std.

#![no_std]

use core::cell::UnsafeCell;
use core::hash::{Hash, Hasher};
use core::sync::atomic::{AtomicU64, Ordering};
use core::marker::PhantomData;

const EMPTY: u64 = 0;
const TOMBSTONE: u64 = 1;
const OCCUPIED: u64 = 2;

#[repr(C)]
struct Slot<K, V> {
    state: AtomicU64,
    key: UnsafeCell<K>,
    value: UnsafeCell<V>,
}

pub struct EpochMap<K, V, const N: usize> {
    slots: UnsafeCell<[Slot<K, V>; N]>,
    epoch_global: AtomicU64,
    epoch_locals: [AtomicU64; 64],
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
            })),
            epoch_global: AtomicU64::new(1),
            epoch_locals: core::array::from_fn(|_| AtomicU64::new(0)),
            _marker: PhantomData,
        }
    }

    #[inline]
    fn hash(&self, key: &K) -> usize {
        let mut h = core::hash::BuildHasherDefault::<core::hash::FnvHasher>::default().build_hasher();
        key.hash(&mut h);
        (h.finish() as usize) & (N - 1)
    }

    /// Enter the current epoch. Call before any operation if you want reclamation safety.
    pub fn enter(&self, tid: usize) {
        let g = self.epoch_global.load(Ordering::Acquire);
        self.epoch_locals[tid].store(g, Ordering::Release);
    }

    /// Exit the epoch.
    pub fn exit(&self, tid: usize) {
        self.epoch_locals[tid].store(0, Ordering::Release);
    }

    /// Try to advance the global epoch if all threads have exited older epochs.
    pub fn try_advance_epoch(&self) {
        let current = self.epoch_global.load(Ordering::Acquire);
        let mut can_advance = true;
        for local in &self.epoch_locals {
            let l = local.load(Ordering::Acquire);
            if l != 0 && l < current {
                can_advance = false;
                break;
            }
        }
        if can_advance {
            self.epoch_global.fetch_add(1, Ordering::AcqRel);
        }
    }

    pub fn insert(&self
    
           
   
                       
   
           
              
       
              
       
       
           
     
       
