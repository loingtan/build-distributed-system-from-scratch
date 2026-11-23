A counter sounds trivial: page views, likes, items sold. Now let several nodes accept increments at the same time, drop some of the messages between them, and still ask for the right total everywhere.

The starter does the obvious thing: keep a total, tell peers about each delta. Run all three tests and see how it fails.

## What you build

```json
→ { "type": "add", "delta": 3 }
← { "type": "add_ok" }

→ { "type": "read" }
← { "type": "read_ok", "value": 42 }
```

Deltas are positive. Every `add` must be acknowledged at once, partition or not. A read may be stale while the network is broken, but once it settles **every node must report the true total**.

## Why deltas fail

- A delta lost in transit is gone: the total is too low forever.
- So you retry. But if only the acknowledgement was lost, the delta is applied twice: too high forever.

Applying a delta is not **idempotent**. The way out is to send *state* instead of *changes*, and to design that state so merging it is safe to repeat, in any order.

A data type with that property is called a **CRDT** (conflict-free replicated data type). This one is the grow-only counter.

## The tests check

- adds are acknowledged during a partition
- all nodes converge on the exact total
- a node's reads never go backwards, and nothing is counted twice
