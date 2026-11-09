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

pub struct Epoch {
    global: AtomicU64,
    locals: [AtomicU64; 64],
}

pub struct EpochMap<K, V, const N: usize> {
    slots: UnsafeCell<[Slot<K, V>; N]>,
    epoch: Epoch,
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
            epoch: Epoch {
                global: AtomicU64::new(1),
                locals: core::array::from_fn(|_| AtomicU64::new(0)),
            },
            _marker: PhantomData,
        }
    }

    #[inline]
    fn hash(&self, key: &K) -> usize {
        let mut h = core::hash::BuildHasherDefault::<core::hash::FnvHasher>::default().build_hasher();
        key.hash(&mut h);
        (h.finish() as usize) & (N - 1)
    }

    pub fn enter(&self, tid: usize) {
        let g = self.epoch.global.load(Ordering::Acquire);
        self.epoch.locals[tid].store(g, Ordering::Release);
    }

    pub fn exit(&self, tid: usize) {
        self.epoch.locals[tid].store(0, Ordering::Release);
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
                        return Some(unsafe { *slot.value.get() });
                    }
                }
            }
        }
        None
    }

    pub fn contains(&self, key: &K) -> bool {
        self.get(key).is_some()
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

    pub fn clear(&self) {
        for i in 0..N {
            let slot = unsafe { &*(*self.slots.get()).as_ptr().add(i) };
            slot.state.store(EMPTY, Ordering::Release);
        }
    }
}
