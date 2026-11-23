Snapshot isolation stops a transaction from seeing the world change, but it does nothing about two transactions changing the same thing. Two clients increment a counter that holds 5. Each reads 5 from its snapshot, adds one, writes 6, commits. The counter says 6 after two increments: a **lost update**. Each transaction did the right thing with what it saw; it was overwriting a write it never saw.

## What you build

The same store, protocol and rules as before, so reads still come from a snapshot taken at `begin` and old versions are still collected. In addition, a transaction that would overwrite a version committed after its snapshot must not commit.

The classic rule is **first committer wins**: at `commit`, if any key the transaction wrote has a newer committed version than its snapshot, answer

```json
← { "type": "aborted" }
```

and forget the transaction. The client retries the whole read-modify-write under a new id, from a fresh snapshot that includes the winner's write. (Detecting the conflict at `write` time instead is fine too, as long as the outcome is the same.)

Aborting is not free, so the tests bound it. The clients in this task increment one to three shared counters as fast as they can, so many attempts really do conflict; a test allows between 60% and 80% of attempts to be aborted, depending on how contended it is. Every logical transaction must commit within 25 attempts. Reads alone never conflict: do not abort a transaction just because a key it **read** has changed.

## The tests check

- everything from the previous task (snapshot reads, no read skew, garbage collection)
- **no lost updates**: no two committed increments read the same version of a counter
- each counter ends equal to the number of committed increments of it
- aborted transactions succeed when retried, and the abort rate stays under the test's bound
