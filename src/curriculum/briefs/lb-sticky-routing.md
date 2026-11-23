Backends often keep a cache of what they have recently served: user profiles, sessions, rendered pages. A request for a key the backend has seen is fast; anywhere else it is a cache miss. Spreading requests by load scatters each key over every backend, so every backend misses often.

**Sticky routing** sends every request for a key to the same backend.

## What you build

The same messages as before; now the keys repeat:

```json
→ { "type": "get", "key": "user:1042" }
← { "type": "get_ok", "key": "user:1042", "value": "user:1042@b4#s123", "backend": "b4" }
```

In the tests a backend answers a cached key in 3–10ms and a miss in 40–70ms.

The pool still changes through `set_backends`, and the list can arrive in **any order**. When a backend leaves, its keys must move, but every other key should stay where it is, cache warm. When one joins, it should take a fair share and nothing else should move.

## The idea: rendezvous hashing

For a key, give every backend a score, `hash(key + "/" + id)`, and pick the backend with the **highest** score. The answer depends only on the key and the set of backends.

Remove a backend and only the keys it was winning need a new winner; for every other key the highest score is unchanged. Add one and it wins only the keys where it scores highest, about one in N. `hash(key) % N` cannot do this: change N and nearly every key moves.

A hash ring with many points per backend, as in the sharding track, gives the same guarantees.

## The tests check

- while the pool is unchanged, every request for a key goes to **the same backend**
- across a change, a key moves **only if** its old backend left or its new backend just joined
- the same set of backends, in any order, gives the same backend for every key
- each backend gets between **0.6× and 1.5×** an equal share of the keys
- as before: one backend per request, only pool members, the backend's answer within 1000ms
