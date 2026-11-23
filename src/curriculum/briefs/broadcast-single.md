The next four challenges build a **broadcast** system: tell one node a value, and eventually every node knows it. Start with a single node, where the only problem is remembering.

## What you build

Two handlers.

```json
→ { "type": "broadcast", "message": 1007 }
← { "type": "broadcast_ok" }

→ { "type": "read" }
← { "type": "read_ok", "messages": [1000, 1007, 1014] }
```

`message` is always an integer. `read` returns every value the node has seen, in any order, each one once.

## Worth knowing

Messages travel as JSON. A `Set` or `Map` is not JSON; if you put one in a message the simulator stops your node and tells you why. Convert first: `[...mySet]`.

## The tests check

- every broadcast is acknowledged
- the final read contains every acknowledged value
- nothing is invented, nothing is duplicated
