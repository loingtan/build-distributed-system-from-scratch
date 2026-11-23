Many systems are simpler with one node in charge: one place that orders writes and makes decisions. The hard part is choosing that node, and replacing it when it disappears, without ever having two nodes that both believe they lead. That failure is called **split brain**.

This challenge is the election half of **Raft**, the consensus algorithm behind etcd, Consul and many databases.

## How Raft elects a leader

Time is divided into numbered **terms**. Each term has at most one leader.

1. Every node starts as a **follower** with a randomised **election timer**.
2. A **leader** sends **heartbeats** to everyone. Each heartbeat resets the followers' timers.
3. If a follower's timer runs out, it assumes the leader is gone. It increments its term, becomes a **candidate**, votes for itself and asks every peer for a vote.
4. A node grants **one vote per term**, to the first candidate that asks.
5. A candidate with votes from a **majority of the whole cluster** becomes leader.
6. Any node that sees a **higher term** than its own, in any message or reply, adopts it and becomes a follower.

Majorities are what prevent split brain: two majorities of the same cluster always share at least one node, and that node votes only once.

## What you build

The protocol between nodes is yours. The starter suggests `request_vote` and `heartbeat`. The tests only observe nodes through one message:

```json
→ { "type": "status" }
← { "type": "status_ok", "role": "leader", "term": 3, "leader": "n2" }
```

`role` is `"follower"`, `"candidate"` or `"leader"`. `leader` is who this node believes leads its current term, or `null` if it does not know.

## Timing

Timeouts are yours to choose. The tests expect a leader within **2s** of startup, a replacement within **2.5s** of losing one, and a stable cluster in the last second of each run. Heartbeats around every 100ms and election timeouts of 300–600ms work well. Use `node.random()` for the randomness.

> Simplification: here it is fine to restart your election timer whenever you adopt a higher term. Raft's exact rule (reset only on hearing the current leader, granting a vote, or starting an election) matters once logs are involved; [RL-3 Repair diverged logs](#/c/raft-repair) asks for it.

## The tests check

- never two leaders in the same term
- terms never go backwards
- the majority side of a partition elects a leader; the isolated side cannot
- after the network heals, one leader, and everyone agrees who it is
