Clients retry. A reply is lost, a leader crashes, a request arrives twice. A client cannot tell a lost request from a lost reply, so it sends it again. If the store applies every copy, a cas succeeds and then fails, and an overwritten value comes back. The store must make retries harmless.

## What you build

The same store. Every request now names its sender and its place in that sender's sequence:

```json
→ { "type": "cas", "key": "k2", "from": 4, "to": 9, "client_id": "c3", "seq": 17 }
← { "type": "cas_ok" }
```

A client's `seq` only grows. It has one request outstanding at a time, and on silence it sends **the same request** (same `client_id` and `seq`) to another node, for up to 3s. Copies arrive together, after a leader change, and long after the client has moved on.

Each put and cas must take effect **once**, and every copy must get the reply the first one got.

## The idea

Keep a **session table** in the state machine: for each `client_id`, the last `seq` applied and the reply it produced. When a put or cas entry is applied:

- `seq` above the last one: apply it and record it
- `seq` equal to the last one: leave the store alone and reply with the saved reply
- `seq` below: a leftover; leave the store alone and reply with anything, or nothing

Decide when the entry is **applied**, not when the request arrives: the original may still be uncommitted, or committed by a leader that crashed before replying. Every node and every future leader applies the same entries in the same order, so all make the same decision. Gets need no deduplication.

## Timing

While no node is down or cut off, every request is answered within 2s, plus 500ms for each reply the test drops on purpose. 2.5s after a fault the cluster must be answering again.

## The tests check

- lost replies, duplicate copies and delayed old puts, with a stable leader
- a cas whose reply is lost as its leader crashes, retried on the next leader
- a partition, then every node crashing at once while old requests keep arriving
- throughout: the history is linearizable and no write is applied twice
