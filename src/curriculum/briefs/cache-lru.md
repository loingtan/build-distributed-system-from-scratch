Your cache from the last task never forgets. On a real server that ends one way: memory fills up and the process dies. A cache needs a size limit, and a rule for what to throw out when it is full. That rule is the **eviction policy**, and a bad one throws out exactly the keys you need.

## What you build

The same node, holding at most **20 entries**. When storing a new key would make 21, evict one first. Report the current count when asked:

```json
→ { "type": "stats" }
← { "type": "stats_ok", "entries": 20 }
```

Evict the **least recently used** entry (LRU). A hit counts as a use: the key moves to the fresh end of the line. The key at the stale end is the one to evict.

## The idea

The test traffic is a classic trap. One get in three is for one of **5 hot keys**; the other two are part of a **scan**, keys that are asked for once and never again (think of a batch job walking the catalogue). Halfway through, a different 5 keys become hot.

- **LRU** keeps the hot keys: each is used again long before 20 other keys have been touched. Scan keys pass through and fall out. Hit rate is about 30%.
- **FIFO**, evicting the oldest insertion, ignores hits. A hot key is thrown out every couple of rounds however popular it is: about 17%.
- **Random** eviction does little better, about 19%.

A JavaScript `Map` iterates in insertion order, which makes it a ready-made LRU list.

## The tests check

- every get gets the database's value
- `stats` is answered, and is probed every 250ms: never more than **20** entries
- a hit rate of at least **25%** (1 − database reads / gets), with requests every 15ms per node
- after the scan, each node is asked again for 40 scan keys it has seen. A cache of 20 can answer at most **20** of them from memory, whatever `stats` says
- on one node, and on two nodes each with its own stream
