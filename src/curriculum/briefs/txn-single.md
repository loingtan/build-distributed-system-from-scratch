A **transaction** groups several reads and writes into one unit. Clients send the whole group at once and expect the results back together.

## What you build

A key-value store that executes transactions. Each one is a list of operations:

```json
→ { "type": "txn", "txn": [["r", 1, null], ["w", 1, 6], ["r", 1, null]] }
← { "type": "txn_ok", "txn": [["r", 1, null], ["w", 1, 6], ["r", 1, 6]] }
```

- `["r", key, null]` is a read. Replace the `null` with the key's current value (or leave `null` if it has never been written).
- `["w", key, value]` is a write. Store it and return it unchanged.

Run the operations in order, so a read after a write in the same transaction sees that write.

## Why this one is easy

A handler runs to completion before the node looks at its next message. With one node and a synchronous handler, transactions execute one at a time: **serial** execution, the strongest isolation there is. Keeping that guarantee across several nodes is the hard part, and the next challenge deliberately gives some of it up.

## The tests check

- the reply mirrors the request, with reads filled in
- every read returns the most recently written value
