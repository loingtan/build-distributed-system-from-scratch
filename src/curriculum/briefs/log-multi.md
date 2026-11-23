One node holding the log is one node that can fail. Spread the log over three nodes, and let clients send to and poll from whichever they like.

The starter is your single-node log. Run it: each node hands out its own offset 0.

## What is hard

Assigning offsets needs agreement. Two nodes handling a `send` at the same moment must not choose the same offset, and must not leave the order ambiguous. Agreement between nodes is **consensus**, and it is hard enough to be the capstone of this course.

So here you get to use somebody else's. `node.kv` is a **linearizable** key-value store shared by all nodes: it behaves like one variable, and every operation takes effect at a single instant.

```js
await node.kv.read(key)            // value, or null if the key is missing
await node.kv.write(key, value)
await node.kv.cas(key, from, to)   // true if it held `from` and now holds `to`
```

`cas` (compare-and-swap) is the important one. "Set this to 8, but only if it is still 7" lets many nodes race for the same slot with exactly one winner. Use `null` as `from` to mean "only if the key does not exist yet".

In the Timeline the store appears as its own lane, `kv`.

## The tests check

Everything from the single-node log, now across three nodes:

- offsets are unique per key, **across nodes**
- a message sent after another was acknowledged gets a larger offset, whichever nodes handled them
- every node can serve every acknowledged message
- the second test partitions the nodes from each other for the whole run; each can still reach `kv`
