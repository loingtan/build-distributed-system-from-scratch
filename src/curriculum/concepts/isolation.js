export default [
  {
    term: 'Interactive transactions',
    aliases: ['interactive transaction'],
    summary: 'Transactions the client drives one step at a time (begin, read, write, commit), thinking between steps while others run.',
    detail: `In an **interactive transaction** the client does not send the whole transaction at once. It sends \`begin\`, then a read, looks at the answer, decides what to write, sends the write, and finally \`commit\`. Between those steps other clients' requests run on the same database.

This is how most SQL applications work, and it is why isolation is hard: even if every single request is atomic, a transaction's read and its later write are separated by other transactions' work. The isolation level decides which of those interleavings may become visible.

In this track each attempt has a client-chosen id such as \`t7\`, any step may be answered \`aborted\`, and the client then retries the whole transaction under a new id.`,
    related: ['Read committed', 'Snapshot isolation', 'Aborts and retries'],
  },
  {
    term: 'Read committed',
    summary: 'The isolation level where a transaction only ever sees committed data and never overwrites another transaction\'s uncommitted write.',
    detail: `**Read committed** makes two promises: no **dirty reads** (you never see a write before its transaction commits) and no **dirty writes** (you never overwrite a value another transaction wrote but has not yet committed).

A simple implementation buffers each transaction's writes privately and publishes them in one step at commit. Databases that write in place instead hold a row lock for each written row until commit and keep the old committed value around for readers.

Read committed still allows a transaction to see different data at different moments: reading Alice's two accounts before and after a transfer commits can show money missing (read skew). It is the default level in PostgreSQL, Oracle and SQL Server.`,
    related: ['Dirty reads', 'Dirty writes', 'Snapshot isolation'],
  },
  {
    term: 'Dirty reads',
    aliases: ['dirty read'],
    summary: 'Seeing another transaction\'s write before it has committed, so you may act on data that is later rolled back.',
    detail: `A **dirty read** happens when transaction T1 writes \`x = 3\` and transaction T2 reads 3 while T1 is still open. If T1 then aborts, T2 has seen a value that never existed in any committed state; even if T1 commits, T2 may have seen only part of T1's changes.

The fix is to keep writes invisible until commit. In this track the check is concrete: every value carries the id of the transaction that wrote it, so a read that returns \`{ "by": "t9" }\` before t9 asked to commit, or after t9 was rolled back, is reported as a dirty read.`,
    related: ['Read committed', 'Dirty writes'],
  },
  {
    term: 'Dirty writes',
    aliases: ['dirty write'],
    summary: 'Overwriting a value another transaction wrote but has not committed, which can mix two transactions\' changes.',
    detail: `A **dirty write** is overwriting an uncommitted value. Suppose two buyers update both the listing and the invoice of the same car. If their writes interleave (buyer A sets the listing, buyer B sets the listing and the invoice, buyer A sets the invoice), the car ends up sold to B but invoiced to A.

Preventing dirty writes means a second writer must wait for, or be refused by, the first until it commits or aborts. With private write buffers published atomically at commit, the problem cannot arise: the shared store only ever changes by whole commits. In this track every writer rewrites all three keys of a group, so a final state that mixes two writers' values is a dirty write (or a commit applied only in part).`,
    related: ['Read committed', 'Dirty reads'],
  },
  {
    term: 'Snapshot isolation',
    aliases: ['SI', 'repeatable read'],
    summary: 'Each transaction reads from a consistent snapshot of the database as of its start, ignoring later commits.',
    detail: `Under **snapshot isolation** a transaction sees exactly the commits that finished before it began, plus its own writes, for as long as it runs. A balance report that takes a minute sees one consistent state of all accounts, even though transfers commit meanwhile.

It is implemented with **MVCC**: the database keeps several versions of each key, stamped with the commit that wrote them, and each transaction reads the newest version its snapshot allows. Readers never block writers and writers never block readers.

Snapshot isolation prevents dirty reads, non-repeatable reads and read skew. On its own it does not prevent lost updates (most systems add first-committer-wins for that), and even with that it allows write skew, so it is not serializable. PostgreSQL's REPEATABLE READ and Oracle's SERIALIZABLE are snapshot isolation.`,
    related: ['MVCC', 'Read skew', 'Lost updates', 'Write skew'],
  },
  {
    term: 'MVCC',
    aliases: ['multi-version concurrency control', 'multiversion concurrency control'],
    summary: 'Keeping several timestamped versions of each value so each transaction can read the versions its snapshot allows.',
    detail: `With **multi-version concurrency control** a commit never overwrites a value in place. It appends a new version stamped with its commit timestamp. A transaction notes the newest commit timestamp when it begins (its snapshot) and, for each key, reads the newest version at or below that timestamp.

For example, key \`x\` holds versions at timestamps 3, 7 and 12. A transaction with snapshot 9 reads the version from 7; one with snapshot 12 or later reads 12. A transaction's own uncommitted writes sit in a private buffer and take precedence.

The cost is space: versions accumulate and must be garbage-collected once no open snapshot can see them (PostgreSQL's VACUUM does this).`,
    related: ['Snapshot isolation', 'Garbage collection'],
  },
  {
    term: 'Read skew',
    aliases: ['non-repeatable read', 'non-repeatable reads', 'fuzzy read'],
    summary: 'Reading related values at different moments so they do not fit together, e.g. a transfer appears half done.',
    detail: `**Read skew** happens when a transaction reads some values before another transaction commits and some after. Alice has 500 in each of two accounts; a transfer moves 100 from the second to the first. If her report reads the first account before the transfer commits (500) and the second after (400), it shows 900.

Reading the same key twice and getting different answers is the simplest form, a **non-repeatable read**. Read committed allows both; snapshot isolation prevents them because every read in the transaction comes from the same snapshot.

In this track every writer rewrites all three keys of a group, so a reader that sees two different writers within one group has observed read skew.`,
    related: ['Snapshot isolation', 'Read committed'],
  },
  {
    term: 'Garbage collection',
    aliases: ['version garbage collection', 'vacuum'],
    summary: 'Dropping old versions that no open transaction can read any more, so a multi-version store does not grow for ever.',
    detail: `In an MVCC store every commit adds versions. A version can be dropped once a newer version of the same key is visible to every open snapshot. Concretely: find the oldest snapshot among open transactions; for each key keep the newest version at or below it and everything newer; drop the rest. With no transaction open, one version per key remains.

Getting it wrong in one direction wastes memory; in the other it breaks isolation, because a long-running transaction suddenly reads a newer value than its snapshot allows. A single long-running transaction therefore holds back garbage collection for the whole database, a well-known operational problem with PostgreSQL's VACUUM.`,
    related: ['MVCC', 'Snapshot isolation'],
  },
  {
    term: 'Lost updates',
    aliases: ['lost update'],
    summary: 'Two concurrent read-modify-write cycles on the same value, where the second write silently discards the first.',
    detail: `A **lost update** happens when two transactions read the same value, each compute a new one from it, and both write it back. A counter holds 5; both read 5 and write 6; after two increments the counter says 6.

Snapshot isolation alone does not prevent it, since both reads come from snapshots that predate the other's write. Remedies: atomic operations in the database (\`UPDATE ... SET n = n + 1\`), explicit locks (\`SELECT ... FOR UPDATE\`), compare-and-set, or automatic detection: abort a transaction that would overwrite a version committed after its snapshot (first-committer-wins).`,
    related: ['First-committer-wins', 'Snapshot isolation', 'Aborts and retries'],
  },
  {
    term: 'First-committer-wins',
    aliases: ['first committer wins', 'first-updater-wins', 'first updater wins'],
    summary: 'Abort a transaction at commit if another transaction committed a newer version of any key it wrote since its snapshot.',
    detail: `**First-committer-wins** is how snapshot isolation prevents lost updates. At commit, the database checks every key the transaction wrote: if a version newer than the transaction's snapshot exists, someone else committed that key first, and this transaction would overwrite a write it never saw. It is aborted and the client retries from a fresh snapshot.

A variant, first-updater-wins, detects the conflict at write time instead (PostgreSQL makes the second writer wait for the first and then aborts it if the first committed).

It only compares write sets. Two transactions that write different keys never conflict, which is why write skew slips through.`,
    related: ['Lost updates', 'Write skew', 'Aborts and retries'],
  },
  {
    term: 'Aborts and retries',
    aliases: ['abort and retry', 'transaction retries', 'abort rate'],
    summary: 'A database may abort a transaction to preserve isolation; the client must then rerun the whole transaction.',
    detail: `Optimistic schemes (first-committer-wins, SSI) and deadlock handling in locking schemes resolve conflicts by **aborting** a transaction. The client retries it from the start, under a new id and with fresh reads, because the values it read are exactly what changed.

Aborts cost work and latency, so an isolation mechanism is judged not only by what it prevents but by how often it aborts. Aborting everything is trivially "safe" and useless. In this track every logical transaction must eventually commit, and each test bounds the share of attempts that may be aborted.`,
    related: ['First-committer-wins', 'Serializable snapshot isolation', 'Deadlock detection'],
  },
  {
    term: 'Write skew',
    summary: 'Two transactions read overlapping data, then each write different keys based on what they read, breaking an invariant together.',
    detail: `**Write skew** generalizes lost updates. Alice and Bob are the only doctors on call. Each transaction reads the shift (two on call), decides it is safe to leave, and takes only its own doctor off call. They write different rows, so no write-write conflict exists; under snapshot isolation both commit and nobody is on call.

The pattern: read a set of rows, check a condition, write something that changes whether the condition holds. Booking a meeting room, claiming a username and spending from a shared budget all fit it.

Preventing write skew requires serializability: lock what you read (two-phase locking, or index-range locks for rows that do not exist yet), or detect the read-write dependencies (SSI). Materializing the conflict as a row everybody writes is a manual workaround.`,
    related: ['Serializability', 'Two-phase locking', 'Serializable snapshot isolation', 'rw-antidependencies'],
  },
  {
    term: 'Two-phase locking',
    aliases: ['2PL', 'strict two-phase locking', 'shared and exclusive locks'],
    summary: 'Take a shared lock to read and an exclusive lock to write, and release none until the transaction ends.',
    detail: `Under **two-phase locking** a transaction acquires locks as it goes (the growing phase) and releases them only at the end (the shrinking phase); **strict** 2PL releases all of them at commit or abort. A read needs a **shared** lock, which many transactions may hold at once; a write needs an **exclusive** lock, which excludes everyone else. A sole shared holder may upgrade.

Because a transaction keeps every lock until it ends, nothing it read can change under it, and nothing it wrote can be seen early. The resulting histories are serializable in the order transactions acquired their conflicting locks.

The costs: readers block writers and vice versa, a long transaction stalls everyone who touches its keys, and waiting introduces deadlocks that must be detected or prevented. It was the standard serializable implementation for decades (MySQL InnoDB's SERIALIZABLE, SQL Server's default SERIALIZABLE).`,
    related: ['Deadlock detection', 'Serializability', 'Write skew'],
  },
  {
    term: 'Deadlock detection',
    aliases: ['waits-for graph', 'wound-wait', 'wait-die'],
    summary: 'Finding transactions that wait for each other in a cycle and aborting one so the others can proceed.',
    detail: `With locking, T1 can hold a lock T2 needs while waiting for a lock T2 holds. Neither will ever proceed. A **waits-for graph** has an edge from each waiting transaction to the holders of the lock it wants; a deadlock is a cycle.

A database can check for a cycle each time a transaction starts to wait, and abort the transaction that would close it. Alternatives: abort any wait that lasts longer than a timeout, or prevent cycles with timestamps (wait-die: an older transaction may wait for a younger one, a younger one dies instead; wound-wait: an older one aborts the younger holder).

The doctors example deadlocks every time the two leaves overlap: both hold shared locks on both rows and wait to upgrade their own row to exclusive.`,
    related: ['Two-phase locking', 'Deadlock', 'Aborts and retries'],
  },
  {
    term: 'Serializability',
    aliases: ['serializable', 'serializable isolation'],
    summary: 'The guarantee that the outcome equals some order of running the committed transactions one at a time.',
    detail: `A history is **serializable** if there is some serial order of its committed transactions that would produce the same reads and the same final state. Application code can then be reasoned about as if it ran alone.

A practical test uses the dependency graph: an edge T1 → T2 if T2 read T1's write (wr), overwrote it (ww), or overwrote a version T1 read (rw, an anti-dependency). The history is serializable if and only if this graph has no cycle. Write skew is a two-transaction cycle of rw edges.

Ways to get it: actual serial execution, two-phase locking, or serializable snapshot isolation. This track's checker builds the graph from what clients observed and names the transactions of any cycle.`,
    related: ['Two-phase locking', 'Serializable snapshot isolation', 'rw-antidependencies'],
  },
  {
    term: 'Serializable snapshot isolation',
    aliases: ['SSI'],
    summary: 'Snapshot isolation plus tracking of read-write dependencies, aborting transactions that could complete a cycle.',
    detail: `**Serializable snapshot isolation** (Cahill, Röhm and Fekete, 2008) keeps everything good about snapshot isolation: reads come from a snapshot and never block or are blocked by writes. On top, it tracks which keys each transaction read and detects **rw-antidependencies** with concurrent transactions.

The key theorem: every cycle that snapshot isolation allows contains a pivot, a transaction with an incoming and an outgoing rw edge, both with concurrent transactions. Aborting any transaction that would become (or complete) such a structure is enough for serializability. It sometimes aborts transactions that would have been fine, which is the price of not blocking.

PostgreSQL's SERIALIZABLE level has used SSI since version 9.1; CockroachDB and others use related optimistic techniques.`,
    related: ['rw-antidependencies', 'Snapshot isolation', 'Serializability', 'Optimistic concurrency control'],
  },
  {
    term: 'rw-antidependencies',
    aliases: ['rw-antidependency', 'rw edge', 'rw edges', 'anti-dependency', 'pivot', 'dangerous structure'],
    summary: 'T1 read a version that a concurrent T2 overwrote, so T1 must come before T2 in any equivalent serial order.',
    detail: `If T1 reads key \`x\` and a concurrent T2 writes \`x\` (T2 commits after T1's snapshot was taken), T1 did not see T2's write. In any equivalent serial order T1 must therefore precede T2: an **rw-antidependency** T1 → T2.

In the doctors example, the leave for Alice read Bob's row, which the leave for Bob wrote (Alice → Bob), and the leave for Bob read Alice's row, which the leave for Alice wrote (Bob → Alice): a cycle. A transaction with an rw edge in and an rw edge out is a **pivot**, and refusing to commit pivots is how SSI rules out every such cycle.

Tracking them requires remembering each transaction's read set until no concurrent transaction remains, which is why SSI keeps committed transactions' metadata for a while after they finish.`,
    related: ['Serializable snapshot isolation', 'Write skew', 'Serializability'],
  },
  {
    term: 'Optimistic concurrency control',
    summary: 'Run without blocking, then check for conflicts at commit and abort the transaction if one occurred.',
    detail: `**Optimistic concurrency control** assumes conflicts are rare. Transactions run without locks and are validated at commit: if what they read or wrote conflicts with something committed meanwhile, they abort and retry.

First-committer-wins (validating write sets) and SSI (validating read-write dependencies) are both optimistic. They shine when contention is low and when long read-only work must not block writers; under heavy contention the abort rate climbs and pessimistic locking may do better.`,
    related: ['Serializable snapshot isolation', 'First-committer-wins', 'Aborts and retries'],
  },
];
