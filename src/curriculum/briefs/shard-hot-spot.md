Partitioning spreads keys evenly, but not requests. When a celebrity posts, millions of requests hit one key, and one key lives on one node. That node is a **hot spot**: it saturates while the others idle, and adding nodes does not help.

The fix described in *Designing Data-Intensive Applications* is **salting**: split the hot key into several sub-keys on different nodes. Writes pick one at random; a read must fetch all of them and combine. That makes reads more expensive, so it is done only for keys that are actually hot.

## What you build

Counters, sharded by `hash(key) % N` as in the first task:

```json
→ { "type": "incr", "key": "post:1042", "delta": 2 }
← { "type": "incr_ok" }

→ { "type": "get", "key": "post:1042" }
← { "type": "get_ok", "value": 17 }
```

A key never incremented reads as `0`. 70% of all requests go to one key, chosen at random each run (in one test, a second key takes over halfway). Your nodes must notice which key is hot by themselves.

## The idea

Only the owner sees all the traffic for a key, so it decides when a key is hot, and tells the others. A counter can be split into parts whose sum is the value. Every node that might hold a part must be asked by a read, and a read through a node that has not heard the news yet must still find every part.

## The tests check

- every request is answered within 1000ms
- a get includes every increment acknowledged before it began, and nothing that was never sent
- a node's load is the number of requests (not replies) delivered to it, from clients and from other nodes. From 600ms after each hot key appears, no node may handle more than **1.5×** the average
- at most **2.6** messages between nodes per client request on average, so salting every key is too expensive
