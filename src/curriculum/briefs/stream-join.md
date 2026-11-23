An order stream often needs facts from a table, such as the customer's tier for a discount. In a stream processor the table is itself built from a **changelog**, and joining a stream with it raises a question. Which version of the row counts? The one that was valid when the order happened, or whatever the processor holds when the order happens to be read? Only the first gives the same answer every time you replay the stream. This task does an **event-time** join.

## What you build

Two topics on the `log` service (poll each as before, from offset 0; never truncated).

`customers` is the customer table's changelog. Changes have strictly increasing `ts`. Watermarks promise that no later change has `ts` at or below theirs:

```json
{ "offset": 4, "kind": "change", "customer": "cust-2", "tier": "gold", "ts": 70412 }
{ "offset": 5, "kind": "watermark", "ts": 70300 }
```

`orders`, a little out of `ts` order:

```json
{ "offset": 9, "order": "ord-10", "customer": "cust-2", "amount": 120, "ts": 70388 }
```

Send each order to the sink once, enriched:

```json
→ { "type": "enriched", "order": "ord-10", "customer": "cust-2", "amount": 120, "tier": "silver" }
← { "type": "enriched_ok" }
```

`tier` is the tier from the customer's latest change with `ts <= order.ts`, or `null` if there is none yet.

The customers changelog runs **100–350 ms behind** the orders, so when an order arrives, the change that applies to it may not have been read yet. The table is complete up to the largest `ts` read from `customers` (change or watermark). Only join an order whose `ts` is at or below that. At the end a watermark far in the future releases every order.

## The idea

A table in a stream is a function of time. Keep its versions, and let event time decide which one applies. The price is buffering: orders wait for the slower input to catch up, and old versions must be kept while orders that need them may still arrive.

## The tests check

- every order reaches the sink once, unchanged
- each carries its customer's tier as of the order's `ts`
