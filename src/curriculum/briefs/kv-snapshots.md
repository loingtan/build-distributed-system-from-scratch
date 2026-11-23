Your log grows by one entry per write and never shrinks. The disk fills, and a node that restarts replays every write since the cluster began. Yet once an entry is applied, the log is only needed to rebuild the state machine, and the state machine is small: a few keys and a session table.

## What you build

The same store and client protocol (`log` is no longer checked). Now `node.disk`, written out as JSON, must stay under **6,000 bytes** on every node at all times, through hundreds of writes.

## The idea

Take a **snapshot** of the state machine and drop the log entries it covers:

```js
node.disk.snap = { index: 120, term: 4, store: { k1: 7 }, sessions: { c3: { seq: 17, reply: { type: 'cas_ok' } } } };
// node.disk.log now starts with entry 121
```

- Snapshot only **applied** entries, and include the **session table**: without it, a node restarted from the snapshot applies an old duplicate again.
- Every node compacts on its own, for example once its log holds 25 applied entries.
- Index arithmetic changes everywhere: entry `i` is `log[i - snap.index - 1]`, and the term of entry `snap.index` is `snap.term` (needed for `prevTerm` and for votes).
- After a restart, start from the snapshot: its store and session table, with `commitIndex = snap.index`.
- A follower may need an entry the leader has already dropped. Send it the snapshot instead (`install_snapshot`). It replaces the follower's store, session table and log, keeping any later entries that agree with it.

## Timing

`node.disk` is measured every 100ms. After the leader crashes, the two remaining nodes, one of which fell behind, must be committing again within 2.5s.

## The tests check

- a write-heavy load, with node.disk under 6,000 bytes throughout
- a follower that misses 2s of writes and must then help the survivor commit after the leader crashes
- every node crashing at once, then old requests from before the crash arriving while clients only read
- throughout: the history is linearizable and no write is applied twice
