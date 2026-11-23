**PBFT** (Practical Byzantine Fault Tolerance, 1999) showed that a replicated service can survive lying replicas at a practical cost. Here you build one round of its normal case: four replicas agree on one client request, with f = 1 traitor that may be any replica, the primary included.

## What you build

The primary is **n1** (view 0). The client signs its request with the client key (`"request:" + value`) and sends it to n1. Every replica answers `request_ok`; only the primary acts on it, and only on the first request whose signature verifies.

```json
→ { "type": "request", "value": "pay-alice-10", "sig": "9c01e5ab" }
← { "type": "request_ok" }
```

Send each message below to all replicas, yourself included. Every one carries `"view": 0, "seq": 1`.

1. **Pre-prepare.** The primary sends `{ "type": "pre_prepare", "view": 0, "seq": 1, "value": "pay-alice-10", "client_sig": "9c01e5ab" }`.
2. **Prepare.** Accept a pre_prepare only if it comes from n1, is the first one you accept, and `verify("client", "request:" + value, client_sig)` holds. Then send `{ "type": "prepare", "view": 0, "seq": 1, "value": "pay-alice-10" }`.
3. **Commit.** Once you accepted value v and hold prepares for v from **2f+1 = 3** distinct replicas, send `commit` for v (once).
4. **Decide** v on commits for v from 3 distinct replicas, even if you never accepted v yourself.

```json
→ { "type": "read" }
← { "type": "read_ok", "value": "pay-alice-10" }
```

`value` is `null` until you decide.

## Why it is safe

Two groups of 3 out of 4 share an honest replica, which prepares one value only, so at most one value is ever prepared by 3 replicas, and 3 commits include honest replicas that saw it prepared. The client signature stops even a lying primary from inventing a request.

In one round the prepare phase looks redundant. It matters when a faulty primary is replaced (a **view change**, not built here): a value that may have been decided was prepared by 3 replicas, so the new primary hears of it.

## The tests check

- honest replicas never decide different values
- only a value the client signed is ever decided
- with an honest primary, every honest replica decides the client's request within about 1 s
- with a lying primary, nobody has to decide
