Sets were easy to merge because every add can be kept. A register holds *one* value: a user's display name, a configuration setting. Two nodes write different values for the same key at the same time; after the network heals, every replica must agree on one of them.

The starter stamps each write with a Lamport clock and keeps the value with the larger stamp. Run it: on some keys the replicas never agree.

## What you build

One register per key.

```json
→ { "type": "write", "key": "name", "value": "Ada" }
← { "type": "write_ok" }

→ { "type": "read", "key": "name" }
← { "type": "read_ok", "value": "Ada" }
```

A key that was never written reads as `"value": null`. Writes are acknowledged at once, partition or not. The rules:

1. **Every replica picks the same winner.** After the network settles (allow about **one second**), all nodes return the same value for each key.
2. **A write beats every write its node had already seen.** If node B writes after it has received A's value, B's value wins. A value overwritten this way never comes back.
3. A node reads back its own write until a newer one reaches it.

## Last writer wins

Tag every value with **(timestamp, node id)** and keep the larger tag. Use a **Lamport clock** for the timestamp: each write uses `clock + 1`, and every tag received moves `clock` up to at least its timestamp. That gives rule 2.

Two writes made on opposite sides of a partition after the same history get the *same* timestamp. Comparing timestamps alone leaves each side keeping its own value forever. Comparing the node id when timestamps are equal makes the order total, and the replicas converge.

Note what this costs: one of the two concurrent writes is discarded, although both clients were told it succeeded. The next task keeps both.

## The tests check

- a chain of writes through different nodes, each after the previous one has spread
- writes racing on both sides of a partition with equal timestamps
- three replicas, five replicas, and lossy links; every replica converges
- replicas whose clocks disagree by up to **2 seconds**: `node.now()` differs from node to node, so a timestamp taken from it can order a later write before an earlier one
