A web server that resizes photos inside the request makes the user wait, and falls over when a thousand uploads arrive at once. The usual answer is a **work queue**: the server writes down "resize photo 17" and returns; a pool of workers takes jobs off the queue at whatever pace they can manage.

In this track your node is the **broker** that holds the queue. The tests play both sides: producers that add jobs and consumers that ask for them.

## What you build

```json
→ { "type": "enqueue", "id": "job-17", "payload": { "task": "resize", "photo": 17 } }
← { "type": "enqueue_ok" }

→ { "type": "dequeue" }
← { "type": "dequeue_ok", "job": { "id": "job-17", "payload": { "task": "resize", "photo": 17 } } }

→ { "type": "dequeue" }
← { "type": "dequeue_ok", "job": null }
```

`dequeue` hands out the job that has waited longest and removes it from the queue. When nothing is waiting, the answer is `job: null`; the consumer will ask again a little later. The broker never looks inside `payload`. It only has to return it unchanged.

## The idea

Several consumers ask at the same time, and each job must go to exactly one of them. On a real broker that takes a lock. Here a handler runs to completion before the next message is handled, so "take the first job and remove it" cannot be interrupted halfway.

Notice what `dequeue` promises and what it does not. The job has left the broker, but nobody knows yet whether the consumer will finish it. The next task is about that gap.

## The tests check

- every request is answered
- every job is handed out exactly once, payload intact
- jobs go out in the order they were enqueued
- `dequeue` only answers `null` when the queue is really empty
