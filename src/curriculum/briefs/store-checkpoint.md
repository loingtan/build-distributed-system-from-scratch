Your write-ahead log works, but it grows by one entry per write, forever. A store that has taken a million writes replays a million entries on every restart, even if it holds only twenty keys. The disk fills up with history nobody needs.

The fix is a **checkpoint**: now and then, write the whole current state to disk, then throw away the log entries it already covers. Recovery becomes "load the checkpoint, then replay the short log written since".

## What you build

The same store and messages as before:

```json
→ { "type": "put", "key": "k003", "value": "v17" }
← { "type": "put_ok" }
→ { "type": "get", "key": "k003" }
← { "type": "get_ok", "value": "v17" }
```

plus `delete` / `delete_ok`. Keep the log at `node.disk.log` (an array). Where and how you store the checkpoint is up to you.

## The trade-off

A log append writes one small entry. A checkpoint writes everything. Checkpoint on every write and you have thrown away the point of the log; never checkpoint and recovery gets slower without limit. Somewhere in between, every so many writes, is right.

Order matters too. Dropping log entries before the checkpoint that covers them is safely on disk loses data. In this simulator a handler runs to completion unless it `await`s, so a crash cannot land between two lines with no `await` between them.

## The numbers

- `node.disk.log` holds at most **50 entries** after every reply.
- Apart from appending to `node.disk.log`, the disk may change at most **once per 10 writes**, counted over the whole test. Writing a checkpoint and emptying the log in the same handler counts as one change; changes made while the node starts up are not counted.
- Each `put`, and each `delete` of a key that exists, must change `node.disk` before its reply.

## The tests check

- the log stays within 50 entries
- the rest of the disk is rewritten rarely
- after crashes at arbitrary points between checkpoints, every acknowledged write survives
