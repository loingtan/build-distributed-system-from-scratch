Most databases let clients hold a transaction open across several round trips: begin, read, think, write, commit. While one client thinks, others run. **Isolation** says what they may see of each other in the meantime. The weakest useful level is **read committed**: nobody sees a write until its transaction commits, and nobody overwrites another transaction's uncommitted write.

## What you build

A single-node database for interactive transactions. Clients pick a fresh txn id for each attempt and send one message per step:

```json
→ { "type": "begin", "txn": "t7" }
← { "type": "begin_ok" }
→ { "type": "write", "txn": "t7", "key": "g1.a", "value": { "by": "t7" } }
← { "type": "write_ok" }
→ { "type": "read", "txn": "t7", "key": "g1.b" }
← { "type": "read_ok", "value": null }
→ { "type": "commit", "txn": "t7" }
← { "type": "commit_ok" }
```

`{ "type": "abort", "txn": "t7" }` rolls a transaction back and is answered `abort_ok`. Values are JSON you store and return unchanged; a key never written reads as `null`. A transaction always reads its own earlier writes.

Any `read`, `write` or `commit` may instead be answered `{ "type": "aborted" }`: the node gave up on the transaction. The client then sends nothing more for that id and retries with a new one. Nothing in this task needs it, so at most 5% of attempts may be aborted.

## The idea

Keep each transaction's writes in a private buffer. Commit publishes the whole buffer in one step; abort throws it away. A dirty read becomes impossible, and so does a dirty write: two half-done transactions never mix their writes in the store.

## The tests check

- every request is answered within one second, with the right type
- **no dirty reads**: a read never returns a value whose transaction had not yet asked to commit, or was rolled back
- reads return values someone wrote; a transaction sees its own writes
- a transaction that begins after a commit finished sees that commit
- **no dirty writes**: after the run, each group of keys (every writer rewrites all three) holds one writer's values
