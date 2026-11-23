The last task was gentle. Each change reached every node before clients wrote again, there was time to settle, and nobody read during a handoff. Real clusters get none of that. A change reaches some nodes before others, the next change starts while the last handoff is still running, and clients never stop.

## What you build

The same store, with one addition: every `set_members` carries an **epoch**.

```json
→ { "type": "set_members", "epoch": 4, "members": ["n1", "n2", "n4", "n5"] }
← { "type": "set_members_ok" }
```

Epochs count up from 1. Before the first change every node is a member, at epoch 0. The tests send each change to every node, but:

- each node gets it at a different moment, up to 300ms apart;
- changes can be 150ms apart, so one starts before the last has settled;
- one node gets each change only **after the next one**. A `set_members` whose epoch is not newer than one you already have must be ignored. Still reply `set_members_ok`.

`put`, `get`, `lookup` and `local_keys` work as before. Clients send them to any node the whole time, and each key is written by one client at a time. No message is lost, but messages between nodes may be held up for a few hundred ms. What your nodes say to each other is up to you. Any message may carry an epoch, a member list or anything else.

## Three ideas

**Epochs order changes.** Arrival order does not. Compare epochs, and a node that missed a change can catch up from any peer that mentions a newer one.

**Versions order values.** "A value already here is newer" stops being true once changes overlap. A node can get a key back while its old copy is still waiting to be handed off. Tag each write with a version that puts the epoch first. Wherever two copies meet, keep the higher version.

**Do not answer before you know.** A node that has just become an owner may not have the newest value yet. It must not answer until it does, and the old owner must stop accepting writes for that key.

## The tests check

- every request is answered within 1000ms
- a `get` returns the last value acknowledged before it began, or a newer one, never `null` for a written key
- after the last change, every key holds its last acknowledged value
- all nodes agree on the owners, so stale `set_members` were ignored
- each key is stored only on its owner (checked with `local_keys` at least 1.3s after the last change)
