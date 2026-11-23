`n2-17` is unique, but it is a poor database key. It is a string, and it tells you nothing about when it was created. Systems such as Twitter's Snowflake pack an id into a single integer whose **sort order follows creation time**, so "newest first" is just "largest id first" and new rows land at the end of an index.

## What you build

`generate` again, with two new requirements:

```json
→ { "type": "generate" }
← { "type": "generate_ok", "id": 2684419077 }
```

- the id is an **integer** below 2^53 (the largest a JSON number holds exactly). In the lab, time starts at 0 when the test starts, so milliseconds stay small; a real system counts from a custom epoch (Twitter's starts in 2010) for the same reason
- ids are **roughly time-ordered**: an id issued at least 50ms after another one was returned must be larger, even if a different node issued it

No coordination, as before: one test partitions the cluster.

## Packing fields into a number

Split the integer into fields, most significant first:

| Field | Purpose |
|---|---|
| time in ms | makes ids sort by creation time |
| node number | keeps two nodes apart in the same millisecond |
| sequence | keeps one node's ids apart in the same millisecond |

`node.now()` gives the time in ms (it may have a fraction). The node number can come from `node.id`: `"n3"` → 3.

One JavaScript trap: the bit-shift operator `<<` works on 32-bit integers and silently truncates. Build the number with multiplication instead: `a * 2 ** 12 + b`.

## Why "roughly"

Real machines' clocks disagree by milliseconds, so two ids made at nearly the same moment on different nodes can sort either way. That is why the test allows 50ms of slack, and why these ids are called *k-sorted* rather than sorted.

## The tests check

- ids are integers, and unique across the cluster
- an id issued 50ms or more after another sorts after it
- every request is answered during a partition
