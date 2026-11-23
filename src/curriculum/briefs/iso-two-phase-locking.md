A hospital needs at least one doctor on call per shift. Alice and Bob are both on call and both feel ill. Each asks to go off call at the same moment. Each transaction reads the shift, sees two doctors on call, and takes only its own doctor off. Under snapshot isolation both commit, and nobody is on call. They wrote *different* keys, so first-committer-wins saw no conflict. This is **write skew**: each transaction's decision was based on a premise the other one changed. Only a **serializable** database, one whose result equals some one-at-a-time order of the committed transactions, rules it out.

## What you build

The same protocol and rules: `begin`, `read`, `write`, `commit`, `abort`, any `read`/`write`/`commit` may be answered `aborted`, and the client retries. `versions` is not used. You may drop snapshots and implement **strict two-phase locking**:

- a read takes a **shared** lock on its key, a write an **exclusive** lock; shared is compatible with shared only; a sole shared holder may upgrade to exclusive
- all locks are held until commit or abort
- a request whose lock is busy **waits**: reply when the lock is granted

The two doctors now **deadlock**: each holds a shared lock on both rows and waits for an exclusive lock on its own. Detect it (a cycle in the waits-for graph) and answer one of the requests `aborted`, or abort a request that waited too long. Every request must still be answered within one second.

Lock keys, not the whole database: clients that touch disjoint keys must neither wait (no request over 150ms) nor abort (2% at most). In the contended tests up to 50–70% of attempts may be aborted.

## The tests check

- no dirty reads, no lost updates, transactions see earlier commits
- **committed transactions are serializable**: every write is preceded by a read of the same key, so each key's versions are ordered; a cycle of read, write and anti-dependencies fails the test, naming the transactions
- every shift always has somebody on call, in every committed read and at the end
- independent clients do not wait or abort
