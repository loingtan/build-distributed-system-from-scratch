Clusters change. A node is added for capacity, or taken out for repair. With `hash(key) % N`, changing N changes the answer for almost every key: go from six nodes to five and about five keys in six get a new owner. In a real store each of those is data to copy across the network.

The starter does exactly that. Run it and read the numbers in the failing check.

## What you build

Two new requests. The tests send `set_members` to **every** node whenever the membership changes, and wait for all the replies before doing anything else.

```json
→ { "type": "set_members", "members": ["n4", "n1", "n6", "n2"] }
← { "type": "set_members_ok" }
```

Members are the nodes that own keys. The list is never empty and may arrive in any order. Until the first `set_members`, the members are all of `node.nodeIds`. A node that is not a member keeps running: it owns nothing, but still answers and forwards requests.

```json
→ { "type": "lookup", "keys": ["user:1000", "user:1001"] }
← { "type": "lookup_ok", "owners": ["n4", "n2"] }
```

`owners` has one entry per key, in the same order: the member that owns that key right now. `put`, `get` and `local_keys` work as before and must route to the node `lookup` names.

## The idea: a hash ring

Picture the hash values as a circle from 0 round to 2^32. Each member sits on the circle at `hash(id)`. A key sits at `hash(key)` and belongs to the **first member at or after it, going clockwise**, wrapping past the top back to the smallest position.

Remove a member and only the keys on its arc change owner: they fall to the next member along. Add one and it takes part of one neighbour's arc. Nothing else moves.

## The tests check

- every node acknowledges `set_members` and answers `lookup` (2,000 keys per request)
- every owner is a current member, and all nodes name the same owner for every key
- across each change, a key gets a new owner **only if its old owner left or its new owner just joined**
- every member owns at least one of the 2,000 keys
- keys written after the last change are stored on the node `lookup` names, and read back through any node
