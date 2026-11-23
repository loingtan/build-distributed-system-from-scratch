Cache invalidation is famously one of the two hard things in computer science. Now clients write through your nodes, and a client told its write succeeded expects to read it back, from any node.

## What you build

Three caching nodes. A put writes to the database:

```json
→ { "type": "put", "key": "k2", "value": "k2:w7" }
← { "type": "put_ok" }
```

using `{ "type": "write", "key", "value" }`, answered with `write_ok` in 20–50ms. The promise: **once a client has `put_ok`, no get sent afterwards, to any node, returns an older value.** How the nodes tell each other is up to you; `{ "type": "invalidate", "key": "k2" }` sent to each peer is a good start.

## The idea

Deleting the entry is the easy part. The races are not:

- **Other nodes.** The node that took the put must have every peer drop the key, and wait for each to confirm, before replying `put_ok`. Messages between nodes can be lost, so retry until confirmed. (The tests never cut a peer off for long; a real system would also need a rule for a peer that stays unreachable, such as removing it from the pool before replying.)
- **The stale fill.** A read sent before the write can return after the invalidation, still carrying the old value. Storing it puts the old value back. Keep a **generation** per key, bumped by every invalidation; a read stores its answer only if the generation has not changed since it started.
- **Joining an old read.** With single flight, a get after the write must not wait for a read that started before it. Invalidation also forgets the read in flight.

Delete rather than store the new value: two puts racing through two nodes could leave the older one cached.

## The tests check

- every request is answered within 1000ms; `put_ok` only after the database's `write_ok`
- no get sent after a `put_ok` returns an older value
- at most **60%** of gets reach the database
- **Reads and writes**: gets every 8ms and puts every 100ms on 6 keys, through any node
- **Staged races**: held-back reads (answered within 280ms, so keep the 300ms timeout) overtaken by a write; a get while an old read is in flight; invalidations slowed by 80ms
- **Lossy network**: 10% of messages between nodes lost, and two invalidations lost on purpose
