Your leader acknowledges an append the moment it has it. If that leader crashes a millisecond later, the client has been told "done" about a write that exists nowhere else. Raft's fix: an entry counts only once it is **committed**, meaning stored on a **majority of the whole cluster**. Only committed entries are applied and acknowledged.

Now the log drives something: a key-value store, the **state machine**.

## What you build

Clients write keys. The leader appends `{ term, key, value }` to its log and replies only once that entry is committed **and applied**:

```json
→ { "type": "put", "key": "k1", "value": 9 }
← { "type": "put_ok" }
```

Non-leaders reply `{ "type": "not_leader", "leader": "n2" }` as before. A read answers from this node's own store (`null` if unset):

```json
→ { "type": "get", "key": "k1" }
← { "type": "get_ok", "value": 9 }
```

`log_ok` now also reports how many entries this node treats as committed:

```json
→ { "type": "log" }
← { "type": "log_ok", "entries": [{ "term": 1, "key": "k1", "value": 9 }], "commit": 1 }
```

## The idea

The leader tracks how far each follower's log matches its own (`matchIndex`). An index stored by the leader plus enough followers to make a majority is committed. Followers learn the commit index from the leader's next message and apply up to it. Every node applies committed entries once each, in log order.

## Timing

While a majority is up, every put gets `put_ok` within **2s** (clients wait 500ms per node). With a majority down, nothing may commit or be acknowledged from 250ms after the crash until the nodes return; from 1s after their return, every put must again get `put_ok` within 2s.

> This task's commit rule is deliberately simple: count copies of an entry on a majority. It has a hole when leaders change (an old-term entry can be counted as committed and later overwritten), and [RL-3 Repair diverged logs](#/c/raft-repair) closes it. The tests here do not stage that case.

## The tests check

- no two nodes ever report different committed entries at one index; `commit` never goes back
- a get on the node that said `put_ok` sees that write
- every acknowledged put is committed; every node's store matches the committed log at the end
