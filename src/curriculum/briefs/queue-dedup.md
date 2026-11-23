The broker now survives dying consumers. It does not yet survive impatient ones, and it does not survive impatient producers either.

- A **producer** that sees no `enqueue_ok` in time sends the same `enqueue` again, with the same `id`. Perhaps the first one never arrived; perhaps it did and only the reply was slow. The retry can come 20 ms later or 900 ms later, long after the job was processed and acknowledged.
- A **consumer** may take longer than the 500 ms visibility timeout and then ack. By then the job is back in the queue, or already with another consumer, who will ack it as well. Some consumers simply send their ack twice.

The starter treats every `enqueue` as a new job and only looks for an acked job among those in flight. Run the tests to see what that does.

## What you build

The same messages as before, with two stronger promises:

```json
→ { "type": "enqueue", "id": "job-17", "payload": { ... } }     (second copy)
← { "type": "enqueue_ok" }                                       (and no second job)

→ { "type": "ack", "id": "job-17" }                              (late, or repeated)
← { "type": "ack_ok" }                                           (and the job is finished)
```

- An id that was ever accepted is never accepted as a new job again, whatever has happened to the first one since.
- An `ack` finishes the job wherever it is. After the `ack_ok` it is never handed out again.
- Every copy is answered. A sender who gets no answer will only try again.

The 500 ms timeout and its slack (not before 450 ms, available by 750 ms) are unchanged.

## The idea

Both operations become **idempotent**: doing them twice has the same effect as doing them once. That needs memory which outlives the job, so keep a record per id that says what became of it. At-least-once delivery combined with idempotent operations is what people mean by "effectively once".

## The tests check

- retried enqueues create no extra jobs, even after the original was acknowledged
- a late ack finishes a job that has gone back to the queue
- an acknowledged job is never handed out again, by an old timer or anything else
- everything the previous task checked
