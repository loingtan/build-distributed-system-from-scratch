Hashing spreads keys evenly, but it scatters neighbours: `k0100` and `k0101` land on unrelated nodes, so "every key from `k0100` to `k0200`" has to ask every node. **Range partitioning** keeps keys in order instead. Each node owns contiguous ranges, so a scan asks only the few nodes whose ranges overlap it. Bigtable, HBase and CockroachDB work this way.

Nobody knows the key distribution in advance, so ranges are created as data arrives: start with one range and **split** it when it grows too big.

## What you build

`put` and `get` as in the hash partitioning task (any node, forward to the owner), plus:

```json
→ { "type": "scan", "from": "k0100", "to": "k0108" }
← { "type": "scan_ok", "entries": [["k0100", 7], ["k0103", 12]] }

→ { "type": "ranges" }
← { "type": "ranges_ok", "ranges": [["", "n1"], ["k0187", "n3"], ["k0301", "n2"]] }
```

`from` is inclusive, `to` exclusive; entries come back in key order. `ranges` is this node's range table: each entry is `[from, owner]`, sorted, the first starting at `""`. A range runs up to the next entry's `from`. `local_keys` is as before.

The starter begins with one range owned by `n1`. When a put takes a range past **40 keys**, its owner splits it at the middle key, moves the upper half (keys and ownership) to another node, and announces the new boundary to **every** node.

## The idea

A split only ever adds a boundary. If boundaries never move, a node can apply announcements in whatever order they arrive and still end up with the same table as everyone else.

## The tests check

- every request is answered within 1000ms; gets return the last value put
- 400ms after the last write, all nodes report the same, well-formed range table
- every key is stored only on the owner of its range; no range holds more than 40 keys; no node stores more than half the keys (40% with six nodes)
- scans return exactly the keys in range, in order
- scans run one at a time, and every message between nodes sent during a scan may involve only the node that took it and the owners of overlapping ranges. So do not send messages on timers.
