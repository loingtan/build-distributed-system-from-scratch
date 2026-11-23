Quorums keep reads correct while some replicas are behind. They do nothing to bring those replicas up to date. A node that sat out a partition keeps its old values, and each of them is one fewer good copy than you think you have. Lose two up-to-date replicas of five and the newest write is on a single machine.

## What you build

The same store, with the same client messages. One new promise:

> After a successful `get` of a key on a healthy network, **every** replica holds the newest version of that key within **one second**.

The tests cut off a minority, overwrite every key through the majority, heal the network, read each key once, and then ask every replica for its own copy with `local_get`.

## The idea

A quorum read has already done the detective work. The coordinator asked every replica and compared versions, so it knows the newest version and which replicas answered with something older. **Read repair** is the last step: send the newest value back to those replicas.

Three details matter.

- Send the value **with its original version**. This is a copy of an old write, not a new one. A fresh version could bury a write that is in flight at the same moment.
- The client is answered after R replies. The stale replica is often reply R + 1. Keep listening until the rest have answered or timed out, then repair.
- The coordinator may be the stale one.

Because replicas only accept a higher version, a repair message that is duplicated, delayed or lost can do no harm.

## The tests check

- everything from the previous task: answers, availability, no stale reads
- after each `get`, every replica's `local_get` returns the newest value
- the same with three replicas, and on slow links (5–80ms) where the stale replicas often answer last
