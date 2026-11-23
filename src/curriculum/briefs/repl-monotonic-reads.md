Asynchronous replication lets every backup serve reads, but a client that talks to more than one replica sees the lag. It reads `k3 = 5` from the primary, then `k3 = 3` from a backup that is behind: to the user, time went backwards. Or it writes through one node and reads from another that has not heard of the write yet. DDIA calls the fixes **session guarantees**: *monotonic reads* and *read your writes*.

The primary is `node.nodeIds[0]` again, and replication is asynchronous, as in RP-1.

## What you build

The same store, plus a **token** that clients carry. The protocol is fixed:

- every request carries `token`; a client's first token is `null`
- every `write_ok` and `read_ok` carries a `token`, and the client replaces its token with it
- what a token contains is up to you (any JSON value); an `error` reply leaves the token unchanged

```json
→ { "type": "write", "key": "k3", "value": 5, "token": 17 }
← { "type": "write_ok", "token": 21 }

→ { "type": "read", "key": "k3", "token": 21 }
← { "type": "read_ok", "value": 5, "token": 23 }
```

A replica that is behind the client's token must wait for replication (or ask the primary) rather than answer from its stale copy. Answer within the client's 1s timeout: if you cannot catch up within about 600ms, reply `{ "type": "error", "code": "unavailable" }`. A client with token `null` has seen nothing, so any replica may serve it at once, even one that is cut off.

## The one idea

The client cannot judge freshness, and a replica does not know what this client saw elsewhere. The token carries that knowledge: "I have seen the store as of write N". If every node applies the primary's writes strictly in order, one number says how far a node has got, and the comparison is `applied >= token`.

## The tests check

- writes are acknowledged; every request is answered; reads come from real writes
- every reply carries a token
- no client reads an older value of a key than one it already read (monotonic reads)
- no client misses its own acknowledged write (read your writes)
- with a backup cut off for good, new clients are still served there, and the rest converge
