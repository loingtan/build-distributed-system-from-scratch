A Lamport clock guarantees that a cause is stamped before its effect. It cannot do the reverse: given two timestamps 5 and 9, you cannot tell whether the first event caused the second or whether they happened independently on nodes that never spoke.

A **vector clock** can. Instead of one counter, each node keeps one counter **per node**: "how many events of n1, of n2, of n3 do I know about?"

## The rules

1. On every event, add one to **your own** entry.
2. Send the whole clock with each message.
3. On receiving a message, take the larger value for every entry, then add one to your own entry for the receive.

Comparing two clocks tells you how the events relate:

- every entry of A ≤ the matching entry of B: A **happened before** B
- some entries larger each way: A and B are **concurrent**; neither could have influenced the other

Detecting concurrency is what replicated databases need to notice conflicting writes.

## What you build

The same three messages as the Lamport challenge, carrying a `clock` object instead of `ts`.

```json
→ { "type": "event" }
← { "type": "event_ok", "clock": { "n1": 3, "n2": 1, "n3": 0 } }

→ { "type": "notify", "to": "n2" }
     n1 → n2   { "type": "note", "clock": { "n1": 4, "n2": 1, "n3": 0 } }
← { "type": "notify_ok", "clock": { "n1": 4, "n2": 1, "n3": 0 } }
```

A `notify` is one event: tick once, and use that clock for both the note and the reply. Reply inside the handler, without awaiting anything.

## The tests check

The checker keeps its own vector clocks from the messages it sees, and compares them with yours entry by entry. Every reply and every note must match exactly.
