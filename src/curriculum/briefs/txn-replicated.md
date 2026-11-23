Three replicas, and a hard requirement: **every node answers every transaction immediately**, even when it cannot reach anyone else. That is total availability, and it has a price.

If two sides of a partition both accept writes, neither side can see the other's. No protocol can give you serial execution here. What you can still promise is weaker and still useful:

- a transaction sees its own writes
- reads return values that were really written, never garbage
- once the network heals, **all replicas converge** on the same value for every key

## What you build

The same `txn` messages as before, on three and then five nodes. The starter is your single-node store, running on replicas that never talk.

Two problems to solve:

**Replication.** Writes accepted by one node have to reach the others, despite lost messages.

**Conflicts.** Node A writes `x = 1` while node B writes `x = 2`. After the partition, A hears about 2 and B hears about 1. If each simply keeps the last value it heard, they swap and still disagree. All replicas need a rule that picks the same winner regardless of the order in which they learn about the writes.

You have already built both pieces: timestamps that do not rely on wall clocks, and state that merges the same way in any order.

## What you give up

The losing write of a conflict is silently discarded, although its client was told `txn_ok`. Whether that is acceptable depends on the application. It is the standard trade for staying available.

## The tests check

- every transaction is answered, during partitions too
- a transaction sees its own writes; no invented values
- after the network settles, every node holds the same value for every key
