Your FIFO queue forgets a job the moment it hands it out. If the consumer then crashes halfway through, the job is gone and nobody will ever know. Run the starter against the second test to see it happen.

The fix is to make the consumer say when it has **finished**, and to treat silence as failure.

## What you build

`enqueue` and `dequeue` as before, plus:

```json
→ { "type": "ack", "id": "job-17" }
← { "type": "ack_ok" }
```

- A job that has been handed out is **in flight**. It is not offered to other consumers, but the broker still holds it.
- An `ack` removes the job for good.
- If no `ack` arrives within the **visibility timeout** of **500 ms**, the broker assumes the consumer died and makes the job available again.

Use `node.after(500, ...)`. It returns `{ cancel() }`.

## Timing

All times are measured on the broker, from the moment it sends the `dequeue_ok`:

- a job must not be handed out again sooner than 450 ms after its last hand-out
- 750 ms after an unacknowledged hand-out, a `dequeue` must not come back empty

In these tests a consumer either acks within 150 ms or never acks at all. A job can lose its consumer twice in a row.

## The idea

This is **at-least-once delivery**. The broker cannot tell "the consumer died before doing the work" from "it did the work and died before acking", so it hands the job out again in both cases. No job is lost; the price is that a job may run twice, and consumers have to be written with that in mind.

## The tests check

- every request, including every `ack`, is answered
- no job is lost: every job is eventually acknowledged
- a job in flight stays invisible for 500 ms, and comes back after that
- an acknowledged job is never handed out again
- jobs get their *first* hand-out in the order they were enqueued
