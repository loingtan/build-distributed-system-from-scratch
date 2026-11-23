"How many clicks per page per second?" sounds easy until events arrive late. A phone drops off the network and sends its clicks a few seconds later. If you count by **arrival time**, those clicks land in the wrong second. Stream processors count by **event time**, the timestamp in each event. They use a **watermark** to decide when a window can be considered finished.

## What you build

Topic `clicks` on the `log` service (poll it as in the last task, starting at offset 0; it is never truncated) holds two kinds of record:

```json
{ "offset": 12, "kind": "event", "key": "cart", "ts": 41730 }
{ "offset": 13, "kind": "watermark", "ts": 41400 }
```

Event time starts around 40000 and has nothing to do with `node.now()`. Watermarks never decrease. They are a guess, not a promise: events with a smaller `ts` still turn up.

Process records in offset order with these rules:

- **Windows** are tumbling, 1000 wide: an event belongs to `[start, start + 1000)` with `start = Math.floor(ts / 1000) * 1000`.
- The **watermark** is the largest watermark `ts` read so far (none at first).
- A window **closes** when the watermark reaches `end + 500` (the **allowed lateness**). When a watermark record closes windows, send each key's count for each closed window to the sink, once:

```json
→ { "type": "window", "key": "cart", "start": 41000, "end": 42000, "count": 7 }
← { "type": "window_ok" }
```

- An event whose window is still open is counted, even if its `ts` is below the watermark.
- An event whose window has already closed is **dropped**. Count how many:

```json
→ { "type": "stats" }
← { "type": "stats_ok", "dropped": 4 }
```

Send nothing for a key that had no events in a window. At the end a watermark far in the future closes every window.

## The idea

The watermark separates *when you know* from *what happened when*. Allowed lateness trades latency and memory, because windows stay open longer, against completeness, because fewer late events are lost. The rules make the result exact, so the checker can compute it too.

## The tests check

- every non-empty window is sent once, with the exact count, and no other windows
- no window is sent before the watermark that closes it has been read
- the dropped count is exact
