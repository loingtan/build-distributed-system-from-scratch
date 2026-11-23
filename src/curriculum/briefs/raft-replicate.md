Electing a leader was half the job. A leader is useful because it puts writes in **one order**, and every other node then copies that order. That ordered list is the **replicated log**, and it is the core of Raft: any machine that applies the same log in the same order ends in the same state.

Your starter is the leader election solution. Keep elections as they are and give every node a log.

## What you build

Clients append values. Only the leader accepts them:

```json
→ { "type": "append", "value": 17 }
← { "type": "append_ok" }
```

Any other node answers with the leader it believes in (or `null`), and the client tries that node next:

```json
← { "type": "not_leader", "leader": "n2" }
```

The tests read each node's log every 100ms. Each entry is an object with an integer `term` and the `value`; index 1 is the first entry:

```json
→ { "type": "log" }
← { "type": "log_ok", "entries": [{ "term": 1, "value": 17 }, { "term": 1, "value": 4 }] }
```

Keep answering `status` as in the election task.

## The idea

Do not send each entry once and hope. The leader remembers, for every follower, the next index it needs (`nextIndex`), and keeps sending everything from there until the follower says it has it. The message that carries entries is also the heartbeat. A follower must never store an entry with a gap before it, and messages arrive late, twice, and out of order.

## Timing

Clients start at 2s. Each append must get `append_ok` within **2s**; a client waits **500ms** for a reply before trying another node. Logs must match by 250ms before the end of the run.

> Followers here simply append what the leader sends. That is enough while one leader stays in charge; when leadership changes with entries still uncommitted, logs can diverge, and [RL-3 Repair diverged logs](#/c/raft-repair) is where that gets handled properly. This task's tests keep leader changes simple.

## The tests check

- no two nodes ever hold different entries at the same index, and logs only grow
- every acknowledged value is in the log, in the order one client sent them
- every node ends with the same log, including a follower that crashes and comes back empty
