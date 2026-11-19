<p align="center">
  <img src="banner.svg" alt="banner" width="100%">
</p>

# epoch-map

Fixed-capacity concurrent hash map with quiescent-epoch reclamation.

- `no_std` (needs `alloc`). The table is allocated once in `new()`, never afterwards.
- 100% safe Rust, zero runtime dependencies. Every slot is built from atomics.
- `get`, `insert`, `remove` take no locks. The only waiting is a short spin while
  another thread is publishing a newly claimed slot.
- Keys and values are machine words: `u8..u64`, `usize`, `i8..i64`, `isize`, `bool`.
- Open addressing, linear probing, power-of-two capacity.

## Usage

```rust
use epoch_map::EpochMap;

let map = EpochMap::<u64, u64, 1024>::new();

map.enter(0); // thread id < 64
assert_eq!(map.insert(1, 100), None);
assert_eq!(map.get(&1), Some(100));
assert_eq!(map.remove(&1), Some(100));
map.exit(0);

map.try_advance_and_reclaim(); // frees tombstones when every thread is quiescent
```

`insert` panics when the table is full; `try_insert` returns `Err(MapFull)` instead.

## Semantics

- A key is written into a slot once. `remove` turns the slot into a tombstone that
  keeps its key; inserting the same key again revives the slot.
- Tombstones for *other* keys are not reused until `try_advance_and_reclaim` frees them,
  so capacity counts live entries plus tombstones. Keep the load factor well below 1.
- `try_advance_and_reclaim` frees tombstones only while no thread is between
  `enter` and `exit` (otherwise it returns `0` and does nothing). Run all operations
  between `enter`/`exit` if you reclaim concurrently. It is always memory-safe; skipping
  `enter` can only make a concurrent reclaim hide a key.
- Updating and removing the same key at the same instant: the update may be dropped.

## Run

```bash
cargo test --release
cargo run --example p99 --release   # latency percentiles on your machine
cargo bench                         # vs DashMap
```

## License

MIT
