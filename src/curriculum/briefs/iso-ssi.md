Two-phase locking is serializable, but a long report holds shared locks on everything it read, and every writer queues behind it. **Serializable snapshot isolation** (SSI) keeps snapshot reads, so readers and writers never wait for each other, and adds bookkeeping that aborts the transactions that would make the history non-serializable.

## The idea

If transaction R reads a key and a concurrent transaction W writes it (W commits after R's snapshot was taken), R did not see W's write, so R must come before W in any equivalent serial order: an **rw-antidependency** R → W. Every cycle snapshot isolation allows contains a **pivot**: a transaction with an rw edge coming in and an rw edge going out, both with concurrent transactions. In the doctors example each leave reads the row the other writes: two rw edges in opposite directions.

So: remember which keys each transaction read. Keep committed transactions around while some open transaction began before they committed. At commit, find the rw edges between the committing transaction and the concurrent committed ones. If the commit would give the committing transaction, or one of them, both an incoming and an outgoing rw edge, answer `aborted`. When the pivot has already committed, abort the transaction that would complete the structure.

## What you build

Start from your lost-updates store: snapshot at `begin`, first committer wins. The protocol and rules are the same as in the previous task (`versions` is not used). The tests are the previous task's, with one change: instead of independent clients there are **long readers**. Two clients slowly read every counter, about half a second per transaction, while three others increment them. Increment requests must not wait more than 150ms, and at most 20% of the long readers' attempts may be aborted: these readers only overlap single-key increments and can never be part of a cycle.

## The tests check

- no dirty reads, no lost updates, transactions see earlier commits
- committed transactions are serializable, and somebody is always on call
- aborted transactions succeed when retried, within each test's abort bound
- readers do not block writers, and long readers commit
