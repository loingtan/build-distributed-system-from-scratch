A rate limit counts requests per second. But when the upstream slows down, each request holds a connection, memory and a slot in the upstream's own queue for longer, so the same rate means more work in progress. Let that grow without a bound and a slow upstream drags your node down with it. A **bulkhead** bounds how much work can be in progress at once. The name comes from the walls that split a ship's hull so that one leak cannot sink the whole ship.

## What you build

One node, the same service as in the first task, with the 200ms upstream timeout. At most **4 upstream calls may be in flight** at any moment. A request that arrives while all 4 slots are taken is refused at once:

```json
→ { "type": "get", "key": "k9" }
← { "type": "error", "code": "overloaded" }
```

Do not queue it. A queued request would wait past its deadline anyway, and a queue hides the overload from clients who could otherwise back off or go elsewhere. Refusing a request quickly is called **load shedding**.

A slot is taken when the call is sent and given back when the call ends, **however it ends**: a reply, an error or a timeout. A slot that is never given back is lost for good, and after a few such leaks the node refuses everything.

## The idea

Bounding concurrency adapts to latency on its own. When calls take 50ms, 4 slots allow about 80 requests per second; when they take 150ms, about 27. The limit follows the upstream's real capacity without anyone tuning a rate.

## The tests check

- the upstream never has more than **4** calls from your node in progress
- every request gets a reply within **300ms**; refusals arrive within **50ms** and cause no upstream call
- under light load (8 per second), nothing is refused
- with every call taking 150ms and 8 requests arriving together every 300ms, at least **40 of 88** succeed
- with 30% of calls never answered, at least **25 of 74** succeed, so slots must come back after a timeout
