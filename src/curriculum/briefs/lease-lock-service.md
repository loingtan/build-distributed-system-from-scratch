Many jobs need one actor at a time: one process rotating a log, one leader writing to a file. On one machine a mutex does it. Across machines, a lock server can hand out the lock, but a client that crashes while holding it would block everyone forever, and the server cannot tell a crashed client from a slow one.

The fix is a **lease**: the lock is granted for a limited time. A holder that wants to keep it must renew before the time runs out. A holder that dies simply stops renewing, and the lock comes free on its own.

## What you build

n1 is the lock server. Three clients, `c1` to `c3`, send it requests; the holder is whoever sent the request (`msg.src`).

```json
→ { "type": "acquire" }
← { "type": "acquire_ok", "granted": true, "lease_ms": 300 }
→ { "type": "renew" }
← { "type": "renew_ok", "ok": true }
→ { "type": "release" }
← { "type": "release_ok" }
```

## The rules

- A lease lasts **300ms** from the moment n1 grants or renews it.
- `acquire` is granted if nobody holds an unexpired lease, or if the requester already does (that is a fresh 300ms). Otherwise `granted: false`.
- `renew` succeeds only for the holder of a lease that has **not yet expired**, and extends it to 300ms from now. After expiry the client must `acquire` again: someone else may have held the lock in between.
- `release` frees the lock only if it comes from the current holder of an unexpired lease. Anything else is answered and ignored.

Clients renew every 100ms. Some go silent while holding the lock, then come back much later to renew and release, as a client paused or cut off would.

## The tests check

- never two holders at once (judged from request and reply times, with the lease rules above)
- a lock nobody holds is granted
- a holder that renews in time keeps the lock; an expired lease cannot be renewed
- every request is answered
