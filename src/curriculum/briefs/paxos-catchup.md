In Paxos, choosing a value and learning it are separate things. A slot is chosen the moment a majority accepts it, and nobody has to be watching when that happens. Every node that wants to apply the log still has to find out. Announcements get lost, a node cut off by a partition misses a whole stretch, and a node that restarts has forgotten everything it had learned. This task makes every node fill those holes, and makes the acceptors' promises survive crashes.

## What you build

The same client messages as the last task: `append` → `append_ok` or `not_leader { leader }`, and `log` → `log_ok { entries }`, the values of slots 1..k with no gap (`null` for a no-op). Now messages are lost, the leader is cut off, and nodes crash and restart with only `node.disk`.

## The idea

**Catch-up.** A node can tell that it's behind if the leader says how far it has got. Put the leader's number of known slots in its heartbeat. A node that knows fewer asks the leader for the values from its first unknown slot onwards, and retries if the answer is lost. The leader also has to resend an `accept` that didn't get a majority, for as long as it leads with that ballot.

**Durability.** As in the single-value task, the acceptor's state is a set of promises: `promised`, plus each slot's `accepted { ballot, value }`. Keep them on `node.disk`, written before you reply:

```js
node.disk.accepted ??= {};
const accepted = node.disk.accepted;   // durable as soon as a slot is set
```

What a node has learned need not be on disk. A follower relearns it from the leader. After the whole cluster restarts, the first leader's phase 1 recovers every accepted slot and re-proposes it.

## Timing

Each append must get `append_ok` within **2s** while a majority is up and connected, except within 2.5s of a fault. After the leader is cut off, the other three must be acknowledging appends again within 2.5s. Every running node must know every chosen slot by 250ms before the end (600ms on the local runner).

## The tests check

- with 15% of messages lost, every node still ends with the whole log
- a cut-off leader and its follower rejoin and catch up; the old leader steps down
- a follower restarts with only its disk and catches up
- the whole cluster restarts; no acknowledged append is lost, and no slot changes value
