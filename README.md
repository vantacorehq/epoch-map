# epoch-map

Lock-free concurrent hash map with epoch-based reclamation.

Fixed capacity. No heap allocation after initialization.  
Zero runtime dependencies. `no_std`.

## Features

- Lock-free `insert`, `get`, `remove`
- Epoch-based safe reclamation
- Open addressing, power-of-two capacity
- Pure Rust

## Usage

```rust
use epoch_map::EpochMap;

let map = EpochMap::<u64, u64, 1024>::new();

map.enter(0);
map.insert(1, 100);
assert_eq!(map.get(&1), Some(100));
map.remove(&1);
map.exit(0);

map.try_advance_and_reclaim();
```

## p99 example

```bash
cargo run --example p99 --release
```

Example output (4 threads, 200k ops, u64/u64):

```
epoch-map concurrent u64/u64
threads: 4
ops:     200000
time:    42.31ms
p50:     180 ns
p99:     410 ns
p99.9:   890 ns
retired: 0
```

Numbers stay flat because allocation is forbidden after init and reclamation is explicit.

## Benchmark vs DashMap

```bash
cargo bench
```

## License

MIT
