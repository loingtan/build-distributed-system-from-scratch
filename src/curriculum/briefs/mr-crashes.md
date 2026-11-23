On a thousand machines, some die during every big job. MapReduce does not try to save a dead worker's work. It runs the task again somewhere else. That is safe because a task is **deterministic**: run it twice on the same input split and it writes the same files.

Two things must hold for that to work. The coordinator has to notice that a worker is gone. And a task that dies halfway must leave nothing behind that looks finished.

## What you build

The job, files and messages from the first task, with workers that crash:

- A worker can be killed at any point, for example after the first 25 lines of a 50-line file. It may stay down, or **restart** from the top of the file a few hundred milliseconds later, with no memory of its task. n1 never crashes.
- A busy worker should tell n1 it is alive, for example every 100ms:

```json
→ { "type": "heartbeat" }
```

- n1 puts the task of a worker it has not heard from for **500ms** back in the queue. A worker that asks for work holds no task, so its old task goes back in the queue too.
- **Commit atomically.** An output `out-<r>` must never exist with incomplete content: a downstream job may read it the moment it appears. Write the file under a temporary name that no other attempt uses, then rename it:

```json
→ { "type": "rename", "from": "tmp-n3-7-48213", "to": "out-1" }
← { "type": "rename_ok" }
```

`rename` is atomic and replaces the target. Commit intermediate files the same way.

## The idea

Detect failure with timeouts and redo the work. Atomic commit and deterministic tasks make the redo safe: whichever attempt commits, the result is the same complete file.

## The tests check

- everything the first task checks, with `job_ok` within **3000ms**
- no `out-*` file ever holds fewer lines than its complete content
- no task runs more than **3** times
