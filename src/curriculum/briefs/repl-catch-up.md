Backups fall behind. A network link fails for a minute, a machine reboots, a message is lost. Replication that only works when nothing goes wrong is not replication. Every real system needs an answer to the question a returning backup asks: *what did I miss?*

This task is asynchronous again: the primary (`node.nodeIds[0]`) answers `write_ok` as soon as it has the write. The messages are the same as in the first task:

```json
→ { "type": "write", "key": "k3", "value": 9 }
← { "type": "write_ok" }

→ { "type": "read", "key": "k3" }
← { "type": "read_ok", "value": 9 }
```

## What you build

Replication that repairs itself. A backup that was cut off, or crashed and restarted, must end up with every write the primary accepted, applied in the primary's order, with nothing lost and nothing doubled.

Crashes are real here. A crashed node loses all its variables and timers; only `node.disk` survives (see the API). Two rules follow:

- A node that restarts must answer reads **at once**, and never with less than it served before the crash. Whatever it has applied must already be on its disk.
- When the **primary** crashes and restarts, nothing it acknowledged may be lost. While it is down, writes may fail.

## The one idea

A per-key version number tells a backup whether one write is newer than another. It cannot tell it what is *missing*. A **log** can: number every write in one sequence, 0, 1, 2, ... A backup that holds 17 entries has entries 0 to 16, no more and no fewer, and the primary knows to send entry 17 next. If entries only ever extend the log without gaps, order and completeness come for free.

## The tests check

- a backup cut off for 1.5s catches up afterwards
- a quarter of all messages are lost, and every node still ends up complete
- a backup crashes and restarts, a primary crashes and restarts
- no node ever goes back to an older value, and every node converges
