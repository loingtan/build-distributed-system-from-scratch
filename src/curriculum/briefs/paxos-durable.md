Every reply an acceptor sends is a promise about its future. "I promise to ignore ballots below 9." "I accepted value 17 in ballot 9." The proposer builds on these promises, so if a crash wipes them, safety goes too. An acceptor that forgets its promise can accept a ballot it swore to refuse. One that forgets its accepted value can quietly undo a choice.

## What you build

The same client messages as before: `propose` → `propose_ok { value }` with the chosen value, and `get` → `get_ok { value }`. Now nodes crash and restart, the network splits, and proposers collide. A restart runs your file from the top. Variables are gone, and only `node.disk` remains:

```js
let promised = node.disk.promised ?? 0;
let accepted = node.disk.accepted ?? null;
```

## The idea

Two things must be durable, and only on the acceptor:

- **promised**, the highest ballot it answered in phase 1
- **accepted**, the last `{ ballot, value }` it accepted

Write them to `node.disk` **before** sending the reply that depends on them. A crash can land the instant a reply leaves, and anything written after that is lost. Proposer and learner state can be rebuilt. A restarted node may answer `get` with `null` until it hears the value again.

When several proposers keep pre-empting each other, nothing gets chosen. Before retrying, wait a random amount of time, and make the wait grow with each failed round. The tests do not force a livelock (no fair test can: with unlucky enough timing, *any* deterministic retry rule can be made to duel forever, which is the FLP result), so a solution without randomness may pass here. Use it anyway: in production, randomised backoff is what makes endless duels vanishingly unlikely.

## Timing

Each proposal must be answered within **2s** of the moment the network is healthy for it. On the majority side of a partition that means at once. On the minority side, and during staged faults, it means from the heal or the restart. A client waits 1s, then retries at the next node. Every node must report the chosen value by 300ms before the end (600ms on the local runner).

## The tests check

- two of five nodes cut off: the majority side still decides, and the minority never decides on its own
- a node promises two proposers, crashes the instant it answers the second, and restarts before either phase 2 arrives
- every node crashes the moment the first `propose_ok` leaves; new proposals after the restart must get the same value
- five proposers at once on slow, lossy links
- throughout: agreement, validity and termination as in the last task
