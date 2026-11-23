Expiry has a nasty side effect. A hot key is asked for hundreds of times a second. The moment its entry expires, every request is a miss, and for as long as the database takes to answer, every new request is a miss too. Each sends its own read. The database, which was protected by the cache, suddenly takes the full load for that key, and it is slowest exactly when it is busiest. This is a **cache stampede** (or thundering herd), and it has taken down real sites.

## What you build

The same cache, with one more rule: at most **one database read per key at a time** on each node. The first miss starts the read. Later misses for the same key wait for that read and share its answer.

Reads can fail. Give up on a database read after **300ms**, answer everyone waiting for it with an error, and forget the read so the next miss starts a fresh one:

```json
← { "type": "error", "code": "unavailable" }
```

## The idea

This is called **single flight**, or request coalescing. In JavaScript it falls out of promises: keep a map from key to the promise of the read in progress, and have every waiter `await` the same promise. The subtle part is cleaning up. A read that fails and stays in the map blocks the key for good.

The TTL rule from the last task still holds: nothing more than 1100ms out of date.

## The tests check

- **Bursts**: 25 gets for one key arrive within 5ms, on cold keys and on keys whose entry has just expired; reads take 80–120ms. At most **2** database reads per burst
- **Hot key under load**: one key asked for every 4ms, alternating between two nodes, for 4.5 seconds; reads take 100–200ms. At most **9** reads of it per node (about one per expiry)
- **Lost reads**: the database never answers the reads of two of five bursts. Every get is answered within the client's 1000ms, errors are allowed only for those two bursts, and every get in the later bursts succeeds
