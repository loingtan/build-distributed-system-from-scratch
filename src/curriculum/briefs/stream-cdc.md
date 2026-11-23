A cache or search index is **derived data**: a copy of the database in another shape. One way to keep it in step is **change data capture**. The database publishes every committed change, in commit order, to a log, and the derived system applies them one after another. It has to apply every change exactly once and in order, even when it crashes.

## What you build

Your node is the derived view. The harness is the database: a `users` table (user → city). Every change goes to topic `users` on the service `log`:

```json
→ { "type": "poll", "topic": "users", "offset": 37, "max": 20 }
← { "type": "poll_ok", "records": [
     { "offset": 37, "key": "u3", "before": "Oslo", "after": "Lima" },
     { "offset": 38, "key": "u5", "before": null, "after": "Accra" } ] }
```

Offsets start at 0. `before: null` is an insert, `after: null` a delete. `records` holds the changes from `offset` onwards (at most `max`, up to 50), or `[]` if there are none yet. Poll again every few tens of milliseconds.

The log keeps a change for **1500 ms** only. Polling below the oldest offset still kept gets:

```json
← { "type": "error", "code": "truncated", "earliest": 52 }
```

Clients read the view:

```json
→ { "type": "view" }
← { "type": "view_ok", "offset": 39, "rows": { "u3": "Lima", "u5": "Accra" }, "counts": { "Lima": 1, "Accra": 1 } }
```

`offset` is the number of changes applied, which is also the next offset to read. `rows` must be exactly the table after changes `0 … offset-1`. `counts` gives the number of users in each city (cities with none can be left out). Answer `view` straight away.

The tests crash your node and restart it 200 ms later. Some poll replies are slow (up to 160 ms) or lost.

## The idea

The view and its offset make one fact together: "this is the table after N changes". Keep both on `node.disk` and update them together. Then a restart picks up exactly where the saved view stops. If the view could fall behind the offset, changes would be skipped; if the offset could fall behind the view, they would be applied twice.

## The tests check

- every view equals the database at the offset it reports, and counts agree with rows
- the offset never goes backwards, not even across a crash
- the node never polls below the oldest offset the log still has
- at the end, every change has been applied
