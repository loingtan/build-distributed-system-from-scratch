Data outlives code. A record version 2 writes today may be read by version 1 tomorrow, after a bug made you roll a node back. So stored data changes in three steps, **expand, migrate, contract**: add the new shape next to the old one, keep both up to date while either version may run, and remove the old shape only once going back is impossible.

## What you build

The directory from the previous tasks. Version 1 keeps `node.disk.users[key] = { name }`; version 2 stores first and last names apart. Names are always two words, and the client protocol does not change.

| when | record on disk |
|---|---|
| version 1 | `{ "name": "Ada Lovelace" }` |
| version 2, before finalize | `{ "name": "Ada Lovelace", "first": "Ada", "last": "Lovelace" }` |
| version 2, after finalize | `{ "first": "Ada", "last": "Lovelace" }` |

The harness upgrades nodes one at a time as before (drain, stop, set `node.disk.version`, restart), but it may also **roll a node back** to version 1 and upgrade it again later. Version 1 updates a record in place: it sets `name` and leaves every other field as it was.

Once every node runs version 2 for good, the harness sends each node

```json
→ { "type": "finalize" }
← { "type": "finalize_ok" }
```

After that, version 1 never runs again. On finalize a node rewrites every record it holds to the final shape and from then on writes only `first` and `last`. A node restarted later must remember that it has finalized.

Fill in `v2Store` and a `finalize` handler; `serve` is shared by both versions. Leave the version-1 code as it is.

## The tests check

- not one client request fails, and every get, on either version, returns the latest name
- before finalize, every record has the right `name`, and records written by version 2 also have the right `first` and `last`
- after finalize, every record has the right `first` and `last` and no `name`
- one node is rolled back in the middle of the rollout; with five nodes, two are
