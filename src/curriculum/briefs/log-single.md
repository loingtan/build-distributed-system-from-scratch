Kafka, and systems like it, are built on one data structure: the **append-only log**. Producers add messages to the end. Each message gets an **offset**, its position in the log. Consumers read from any offset they like and record how far they have got.

## What you build

Four handlers. Each `key` is a separate log.

```json
→ { "type": "send", "key": "k1", "msg": 5003 }
← { "type": "send_ok", "offset": 7 }

→ { "type": "poll", "offsets": { "k1": 6, "k2": 0 } }
← { "type": "poll_ok", "msgs": { "k1": [[6, 5001], [7, 5003]], "k2": [] } }

→ { "type": "commit_offsets", "offsets": { "k1": 7 } }
← { "type": "commit_offsets_ok" }

→ { "type": "list_committed_offsets", "keys": ["k1", "k2"] }
← { "type": "list_committed_offsets_ok", "offsets": { "k1": 7 } }
```

- `send` appends and returns the offset the message was stored at.
- `poll` returns, for each key, every message from the given offset onward as `[offset, msg]` pairs.
- `commit_offsets` records how far a consumer has processed. It never moves backwards.
- `list_committed_offsets` returns the committed offset for each key that has one.

## The tests check

- no two messages on a key share an offset
- offsets increase in the order sends were acknowledged
- polls are in order and start where asked
- no acknowledged message is ever missing
