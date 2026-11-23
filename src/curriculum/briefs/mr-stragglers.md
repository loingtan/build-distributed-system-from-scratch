A job is as slow as its slowest task. One machine with a failing disk or a noisy neighbour can take ten times longer than the rest, and the whole job waits for it. This machine is a **straggler**: it is not dead and it keeps answering heartbeats, so failure detection never fires.

The MapReduce paper's answer is **backup tasks**. Near the end of a phase, when workers start to run out of work, the coordinator starts a second copy of every task still in progress. Whichever copy finishes first completes the task.

## What you build

The crash-tolerant MapReduce from the previous task, with the same job, files and messages. In these tests one worker is slow: each of its fs requests arrives **150–300ms** late. Its heartbeats are on time. One test also crashes another worker.

- When a worker asks for work and no task of the current phase is idle, give it a **backup copy** of a task that is still running on exactly one other worker. Prefer the task that has been running longest.
- The first `task_done` for a task completes it. Later ones, from a backup or from the original, for a task that is already done, or from a phase that has ended, must change nothing:

```json
→ { "type": "task_done", "kind": "map", "id": 0 }
← { "type": "task_done_ok" }
```

- The losing copy still commits its files when it finishes. With atomic renames of deterministic files, that replaces an output with identical content.

## The idea

Spend a little extra work at the end of each phase to stop one slow machine from setting the job's time. Two copies of a task are safe for the same reasons a re-run is.

## The tests check

- `job_ok` within **1600ms** (**1300ms** for the 150ms straggler, **2000ms** with a crash as well)
- outputs exactly right when `job_ok` is sent and at the end; never half-written
- no task runs more than **3** times; n1 sends no fs requests
- at most **150** node-to-node messages per second
