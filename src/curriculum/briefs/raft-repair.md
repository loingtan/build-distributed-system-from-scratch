With one leader in charge, logs only grow. Real clusters are messier. A leader is cut off with entries nobody else has; a new leader writes different entries at the same indexes. When the network heals, one log must give way, without ever touching a committed entry.

Your commit solution falls apart here: two nodes end up reporting different committed entries at one index.

## What you build

The same store and client messages as the last task: `put` → `put_ok` or `not_leader`, `get` → `get_ok`, `log` → `log_ok { entries, commit }`, and `status`.

## The ideas

From the Raft paper:

1. **Log matching.** The leader sends the index **and term** of the entry just before the new ones. A follower accepts only if its own entry there has that term. When it refuses, the leader steps back and tries again.
2. **Conflicts lose.** A follower's entry that differs from the leader's at the same index was never committed: delete it and everything after it. Entries that match stay, even when a late, duplicate message arrives.
3. **The election restriction.** A candidate sends the index and term of its last entry. A voter refuses if its own log is more up to date: a later last term, or the same last term and a longer log.
4. **Commit only your own term.** A leader counts copies only of entries from its own term; older entries commit along with them. An old entry on a majority can still be overwritten (Figure 8 of the paper).

Since some nodes can now never win, reset the election timer only when you hear from the current leader, grant a vote, or stand yourself. Seeing a higher term is not enough.

## Timing

Where a partition leaves a majority connected, it must acknowledge writes again within **2.5s**. From **2.5s after the network heals**, every put must get `put_ok` within **2s**. The Figure 8 test drops client requests while it stages its sequence.

## The tests check

- a leader cut off with writes that can never commit; a stale follower standing for election
- a stranded leader rejoining just as its successor is cut off
- an old message reaching a follower after newer ones, then the leader cut off
- the Figure 8 sequence
- throughout: committed entries never change or disappear
