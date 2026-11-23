Fencing tokens protect the storage only if they never go backwards. A lock server that keeps its counter in memory starts again from 1 after a crash. Its new grants then carry tokens the storage has already seen: the storage either refuses every request from the new holders, or, worse, cannot tell an old holder from a new one. The storage has the same problem from the other side: if it forgets the highest token, a stale holder gets through again, and if it forgets its data, acknowledged work is gone.

## What you build

The same cluster and messages as LS-2: n1 grants leases with tokens, n2 holds the list and fences requests, n3 to n5 do jobs. Now n1 and n2 crash and restart.

```json
→ n1 { "type": "acquire" }
← { "type": "acquire_ok", "granted": true, "lease_ms": 300, "token": 23 }
```

After a restart, the next new grant must carry a token higher than 23, whatever happened before the crash.

## Rules and timing

- `node.disk` survives a crash; everything else is lost, and the code runs again from the top.
- Write to `node.disk` **before** the reply that depends on it: the token counter before an `acquire_ok`, the list and the highest token before a `read_ok` or `write_ok`.
- n1 may forget who held the lock. Granting again right away after a restart is safe: the old holder's token is lower, so n2 refuses it.
- n1 is down for 400ms at a time, n2 for 300ms. Workers keep retrying `acquire` for up to 1.5s and use 1000ms timeouts for n2, so jobs during an outage may answer `done: false`. Jobs that start after the restart must succeed.

## The tests check

- each new grant has a higher token than any before, across restarts
- no job answered `done: true` is missing from the list, even after n2 crashes
- jobs that no crash or pause touched succeed
