Your Raft log replicates puts, and a get answers from whatever the node it reaches has applied. Ask a follower just after a write finished and you can get the old value. Clients cannot build on a store like that.

The guarantee they want is **linearizability**: every operation appears to take effect at a single instant between the moment it was sent and the moment its reply arrived, as if one machine held the data and handled requests one at a time.

## What you build

Three operations, answered only by the leader:

```json
→ { "type": "put", "key": "k1", "value": 7 }
← { "type": "put_ok" }

→ { "type": "get", "key": "k1" }
← { "type": "get_ok", "value": 7 }

→ { "type": "cas", "key": "k1", "from": 7, "to": 8 }
← { "type": "cas_ok" }
```

`cas` replies `{ "type": "cas_failed" }` when the key did not hold `from`. An unset key holds `null`. Any other node replies `{ "type": "not_leader", "leader": "n2" }` (or `null`) and the client tries again there. Keep answering `status`: the tests use it to find the leader.

## The idea

Put **every** operation in the log, reads included. The leader replies when the entry is committed and applied, and the reply is what applying it produced: the value read, or whether the cas matched. Every node applies the same entries in the same order, so all operations fall into one order that everyone agrees on.

`not_leader` means "I did not take your request". A leader that steps down while requests wait must not say that: their entries may still commit. Say nothing.

## Timing

Clients start at 1.5s and wait 500ms for each reply. On silence a client gives up on that request (it may or may not have happened) and moves on. While no node is down or cut off, every request must be answered before then. During a partition, the majority side must be answering again within 2.5s.

## The tests check

- the history of every key is linearizable (an exact search for an order that explains every reply)
- the leader crashes and restarts
- the leader is cut off alone while one client keeps reading from it
