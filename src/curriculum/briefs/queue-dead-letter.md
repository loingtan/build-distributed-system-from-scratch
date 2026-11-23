Some jobs cannot succeed. The photo is corrupt, the consumer has a bug, and every worker that picks the job up crashes. Such a job is called a **poison message**. With at-least-once delivery it comes back every 500 ms for ever, taking a consumer down each time and wasting capacity that healthy jobs need.

## What you build

Give every job a limited number of attempts: **3 hand-outs**.

- Count how many times each job has been handed out.
- When the visibility timeout of the third hand-out expires without an `ack`, the job does not return to the queue. It moves to the **dead-letter list**.
- A job acknowledged on its first, second or third attempt is an ordinary success and never appears there.

A new request reads the list:

```json
→ { "type": "dead_letters" }
← { "type": "dead_letters_ok", "jobs": [ { "id": "job-5", "payload": { "task": "resize", "photo": 5, "corrupt": true } } ] }
```

The jobs may be listed in any order. A late `ack` for a job that has already been dead-lettered is answered with `ack_ok`; whether the job then stays on the list is up to you. Everything else is unchanged: retried enqueues, late and repeated acks, the 500 ms timeout (not handed out again before 450 ms, available again by 750 ms).

## The idea

A retry only helps against failures that go away: a crashed machine, a network blip. A failure that repeats exactly needs a person. The dead-letter list stops the loop without throwing the job away: someone can look at the payload, fix the cause and enqueue it again.

The limit is a judgement. Too low and a couple of unlucky crashes send a healthy job to the dead letters; too high and a poison job burns many consumers before it is stopped.

## The tests check

- no job is handed out more than 3 times
- `dead_letters` lists exactly the jobs whose third attempt timed out, with their payloads
- jobs that failed once or twice and then succeeded are not in the list
- every job ends up acknowledged or dead-lettered; none is lost
- everything the previous tasks checked
