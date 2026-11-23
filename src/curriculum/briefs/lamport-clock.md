Which happened first? On one machine you look at the clock. Across machines you cannot: clocks drift, and two nodes can disagree by seconds. Leslie Lamport's answer was to stop measuring time and count events instead.

A **Lamport clock** is one integer per node, with two rules:

1. Before every event (doing something, sending something), add one.
2. Every message carries the sender's clock. On receiving one, jump to `max(mine, theirs) + 1`.

The result: if event A could have caused event B, then A's timestamp is smaller than B's.

## What you build

```json
→ { "type": "event" }
← { "type": "event_ok", "ts": 4 }

→ { "type": "notify", "to": "n2" }
     n1 → n2   { "type": "note", "ts": 5 }
← { "type": "notify_ok", "ts": 5 }
```

- `event`: a local event. Stamp it and report the stamp.
- `notify`: stamp it, send a `note` carrying that stamp to node `to`, and report the same stamp.
- `note`: arrives from another node. Apply the receive rule.

## The tests check

- on each node, later events get larger timestamps
- after a node receives a note stamped `t`, everything it does is stamped later than `t`

The second test has one busy node and several idle ones. An idle node's clock sits near zero until a note arrives, and then it has to jump.
