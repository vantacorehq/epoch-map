# epoch-map

Lock-free concurrent hash map with epoch-based reclamation.

Fixed capacity. No heap allocation after initialization.  
Zero runtime dependencies. `no_std`.

## Features

- Lock-free insert, get, remove
- Epoch-based safe reclamation
- Open addressing
- Power-of-two capacity

## Usage

```rust
use epoch_map::EpochMap;

let map = EpochMap::<u64, u64, 1024>::new();

map.insert(1, 100);
assert_eq!(map.get(&1), Some(100));

map.remove(&1);
