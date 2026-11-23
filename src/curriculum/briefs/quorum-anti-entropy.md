Read repair fixes the keys people read. Most data is written once and then left alone for a long time. Those keys stay on however many replicas happened to be reachable at write time, and nothing ever tops them up.

## What you build

The same store and client messages, with a promise that no longer depends on reads:

> Every replica holds the newest version of every key within **two seconds** after the network heals, or after the last write if the network merely loses messages.

The tests write while part of the cluster is unreachable, wait, and inspect every replica with `local_get` **before sending a single `get`**.

## Two ways to do it

**Hinted handoff.** The coordinator knows which replicas did not acknowledge a write. It keeps a hint (the entry and who missed it) and retries until that replica answers. Cheap and quick, but the hints live on one node and die with it.

**Anti-entropy.** Every replica regularly picks a peer and compares what they hold:

```json
→ { "type": "sync", "versions": { "k1": { "counter": 4, "origin": "n2" } } }
← { "type": "sync_ok", "entries": { "k7": { "value": "pear", "counter": 2, "origin": "n5" } }, "wanted": ["k1"] }
```

The sender lists versions only. The peer returns the entries where it is ahead and names the keys where it is behind; the sender then ships those. Whatever caused the gap, the two end up equal. Real systems compare hashes of key ranges (Merkle trees) instead of every version, but the exchange is the same.

Either design passes. The messages above are a suggestion; the protocol between nodes is yours.

Apply everything received through the same version check as an ordinary write. Then a round that is lost needs no retry. The next one covers it.

## The tests check

- everything from the earlier tasks
- two of five nodes miss a round of writes; after the heal all five agree on every key
- a second partition that begins the moment the first ends, cutting off different nodes
- a network that loses one message in ten between nodes
