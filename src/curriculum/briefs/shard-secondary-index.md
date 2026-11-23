Records sharded by primary key are easy to fetch by key. "Every red car" is harder: the red cars are spread over all nodes. There are two ways to index them (*Designing Data-Intensive Applications*, chapter on partitioning):

- a **local** index: each node indexes its own records, and a query **scatters** to every node and **gathers** the answers. Writes are simple; every query costs a message to every node.
- a **global** index, **partitioned by term**: the list of red cars lives on one node, whatever nodes the cars live on. A query asks one node. A write must update the index on another node, usually **asynchronously**.

The starter does scatter-gather. Build a global index.

## What you build

```json
→ { "type": "put", "key": "car:117", "value": { "color": "red", "n": 42 } }
← { "type": "put_ok" }

→ { "type": "get", "key": "car:117" }
← { "type": "get_ok", "value": { "color": "red", "n": 42 } }

→ { "type": "find", "color": "red" }
← { "type": "find_ok", "keys": ["car:103", "car:117"] }
```

Keys may come back in any order. Records keep their owner `hash(key) % N`.

## The consistency rule

The index may **lag by at most 250ms**. A find must include every car whose put was acknowledged at least 250ms before the find began (with no later put to it since), and must leave out every such car whose color is now different. Cars written more recently may be in or out.

## The idea

Index updates for one record travel separately and may overtake each other: "add car:117 to red" from an old put can arrive after "remove car:117 from red" from a newer one. Order them by the record's version, not by when they arrive.

## The tests check

- every request answered; gets return the latest record
- finds obey the rule above, also when messages between nodes are held back by up to 80ms
- once writes stop, a find involves at most two nodes: the one that took it and the one holding that color's index
