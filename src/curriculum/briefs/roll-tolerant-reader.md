To upgrade a cluster without downtime you upgrade it one node at a time. For a while old and new versions run side by side, and every message has to make sense to both.

## The service

A user directory spread over the cluster. Each key has a **home**, `node.nodeIds[sum of the key's character codes % node count]`. Any node accepts a request and forwards it to the home with `fwd_put` / `fwd_get`.

```json
→ { "type": "put", "key": "u7", "name": "Ada Lovelace" }
← { "type": "put_ok" }
→ { "type": "get", "key": "u7" }
← { "type": "get_ok", "name": "Ada Lovelace" }
```

## The rollout

While clients keep sending requests, the harness upgrades one node at a time. It stops sending traffic that involves the node 250ms before stopping it, sets `node.disk.version = 2`, starts it again 100ms later, and lets traffic back in 100ms after that. Your file runs `runVersion1()` or `runVersion2()` according to that version. **Leave `runVersion1` as it is**: it is already running on the nodes not yet upgraded, and the tests check that they send exactly what version 1 sends.

## What you build

Version 2 records who made each change. `get_ok` gains a field `by`: the id of the client whose put wrote the current name, or `null` when that is unknown.

```json
← { "type": "get_ok", "name": "Ada Lovelace", "by": "c2" }
```

## The one idea

Version 1 reads only the fields it knows and ignores the rest. That makes *adding* a field safe. *Removing or renaming* one breaks it, and a new message type is no better, because version 1 has no handler for it. In the other direction, a message from version 1 simply lacks the new field: absent means unknown.

## The tests check

- not one client request fails or goes unanswered (500ms) during the rollout
- every get returns the latest name, and a non-null `by` is always the right client
- once every node runs version 2, gets report `by` for names written since then
