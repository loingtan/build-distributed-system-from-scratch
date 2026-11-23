Last-writer-wins drops one of two concurrent writes without telling anyone. Amazon's Dynamo kept both instead, and handed them back to the shopping-cart code to reconcile. A **multi-value register** does the same: when writes conflict, a read returns all of them as **siblings**, and the next write that has seen them replaces them.

The starter is your LWW register, answering with a list. Run it: the second of two conflicting writes disappears.

## What you build

```json
→ { "type": "write", "key": "doc", "value": "draft-b" }
← { "type": "write_ok" }

→ { "type": "read", "key": "doc" }
← { "type": "read_ok", "values": ["draft-a", "draft-b"] }
```

`values` is in any order; a key never written reads as `[]`. Writes are acknowledged at once, partition or not. The rules:

1. **A write replaces every value its node currently holds for the key.** After a node has received `["draft-a", "draft-b"]`, its write of `"final"` leaves just `["final"]` everywhere.
2. **Concurrent writes are all kept.** Two writes where neither node had seen the other's both appear in `values` after the network heals.
3. A value that was replaced never comes back, and no value appears twice.

Replicas must converge within about **one second** of the network settling.

## Version vectors

A single timestamp always orders two writes, so it cannot say "these are concurrent". A **version vector** can: per node, how many of that node's writes this version has seen. Version X **dominates** Y if every entry of X is at least Y's, and some entry is larger. If neither dominates, they are concurrent.

A write takes the entry-wise maximum of the vectors it replaces and adds one to its own entry, so it dominates all of them. A merge keeps every version that no other version dominates.

## The tests check

- writes in sequence through different nodes end with one value
- writes on both sides of a partition are both kept, and a later write resolves them
- only the last write of each side survives; three replicas, five, lossy links
