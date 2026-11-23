Every piece here is at-least-once. The log replays from wherever you ask. The sink can lose your request or be slow to answer. Your node can crash at any point. Still, the output must look as if each payment had been processed **exactly once**. Kafka Streams and Flink sell this guarantee, and here you build it from two ingredients.

## What you build

Topic `payments` on the `log` service (poll as before, from offset 0):

```json
{ "offset": 41, "account": "acct-2", "amount": -15 }
```

The log keeps a payment for **2000 ms**. Below that, a poll gets `{ "type": "error", "code": "truncated", "earliest": … }`.

Keep a running balance per account, starting at 0. For **every** payment, send the account's balance after that payment to the sink:

```json
→ { "type": "emit", "id": "…", "offset": 41, "account": "acct-2", "balance": 310 }
← { "type": "emit_ok" }
```

The sink stores an emit under its `id` and ignores any later emit with an id it already has (it replies `emit_ok` with `duplicate: true`). You choose the ids. `offset` names the payment.

The tests crash the node (it restarts 200 ms later). They take the sink away for 400 ms at a time, dropping every request, and make its replies take 150–300 ms. Retry an emit until it is acknowledged.

## The idea

Two rules, together:

1. A **checkpoint** holds the input offset and the balances as of that offset. Write it in one step on `node.disk`, and only once the sink has acknowledged every output it covers. After a crash you resume from a consistent point and replay what came after it.
2. Replays send outputs again, so make the sink's deduplication work. A replayed payment must carry the **same id** as the first time. Derive the id from the input, never from a counter or a random number.

## The tests check

- every payment reaches the sink, exactly once, with the exactly-once balance
- a replayed payment produces the same output as the first time
- the node never polls below the oldest offset the log still has
