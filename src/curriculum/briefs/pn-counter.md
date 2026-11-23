Your counter can only go up. Stock levels, account balances and vote tallies also go down.

The starter is the grow-only counter from the last challenge. Deltas can now be negative. Run it: it fails even on a healthy network.

## What you build

The same messages as before. `delta` may be negative, and the value may go below zero.

```json
→ { "type": "add", "delta": -2 }
← { "type": "add_ok" }
```

## Why it breaks

The grow-only merge takes the larger of two values, on the assumption that larger means newer. That was true while entries only grew. After a decrement the newer value is the *smaller* one, and `max` discards it.

This is the central constraint in CRDT design: the merge needs some way to tell which state is newer, without clocks. State that only ever grows gives you that for free. The trick is to rebuild "can go down" out of parts that only go up.

## The tests check

- the same three scenarios as before: healthy, partitioned, lossy
- every node converges on the exact total
