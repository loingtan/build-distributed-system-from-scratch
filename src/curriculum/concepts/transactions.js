export default [
  {
    term: 'Transactions',
    aliases: ['transaction'],
    summary: 'A group of reads and writes that a client submits together and the system executes as one unit.',
    detail: `A **transaction** bundles several operations so that the client does not have to reason about each one separately. In this course it is a list such as \`[["r", 1, null], ["w", 1, 6], ["r", 1, null]]\`: the node runs the operations in order and returns the list with reads filled in, so the final read returns 6.

What "one unit" means is spelled out by the guarantees a system gives: **atomicity** (all writes happen or none do), **isolation** (how much concurrent transactions may see of each other) and, for replicated systems, which replicas must agree before the client gets an answer.

On one node with a single-threaded handler, transactions are trivially serial. Across replicas that must stay available during partitions, the strongest isolation levels become impossible and the system must settle for weaker, precisely stated promises.`,
    related: ['Serial execution', 'Serializability', 'Total availability'],
  },
  {
    term: 'Serial execution',
    summary: 'Running transactions literally one after another, so no two ever overlap and isolation comes for free.',
    detail: `With **serial execution** the system runs one transaction to completion before starting the next. No transaction can observe another half-finished, so the outcome is serializable by construction: the order in which they ran is the serial order.

In this course a node handles one message at a time, and a synchronous handler cannot be interrupted. A store that executes each \`txn\` message inside one synchronous handler is therefore serial without any locks.

Serial execution is practical when transactions are short and touch only memory. VoltDB and Redis (for scripts and MULTI/EXEC blocks) use it. It stops working when a transaction must wait for disk, the network or a user, because everything else waits too.`,
    related: ['Transactions', 'Serializability'],
  },
  {
    term: 'Total availability',
    aliases: ['totally available'],
    summary: 'Every node that receives a request answers it, even when it cannot reach any other node.',
    detail: `A system is **totally available** if every non-failed node answers every request without waiting for other nodes. During a partition, each side keeps serving reads and writes on its own.

The cost is consistency. If two sides of a partition each run a transaction that reads what the other writes, both read old values, and no serial order of the two explains what the clients saw. So a totally available system cannot be serializable. It can still promise useful things: a transaction sees its own writes, reads return real values, and replicas converge once the network heals.

Dynamo-style stores, DynamoDB global tables across regions and many caches choose availability. Consensus-based databases such as Spanner or etcd choose consistency and refuse writes on the minority side of a partition.`,
    related: ['Last-writer-wins', 'Anti-entropy', 'Serializability'],
  },
  {
    term: 'Last-writer-wins',
    aliases: ['last write wins', 'LWW', 'last-write-wins'],
    summary: 'Resolve conflicting writes by keeping the one with the largest tag, such as (timestamp, node id), and discarding the rest.',
    detail: `**Last-writer-wins** attaches a tag to every write and, when two replicas hold different values for a key, keeps the value with the larger tag. A common tag is the pair (Lamport timestamp, node id): compare timestamps, break ties by node id. Every replica applies the same comparison, so every replica keeps the same value regardless of the order in which writes arrive.

Example: n1 writes x = 7 tagged (2, n1) and n2 writes x = 9 tagged (2, n2) on opposite sides of a partition. After the heal, every replica keeps x = 9 because n2 > n1. The client that wrote 7 was told it succeeded; its write is gone.

That silent loss is the trade-off. LWW is fine for values where only the latest matters, and wrong for counters, carts or balances. Cassandra resolves cell conflicts this way, and DynamoDB global tables use it between regions. "Last" means largest tag, not latest in real time.`,
    related: ['Lamport timestamp', 'Anti-entropy', 'Total availability'],
  },
  {
    term: 'Anti-entropy',
    summary: 'A background process in which replicas repeatedly compare or exchange state so that anything missed is eventually repaired.',
    detail: `**Anti-entropy** does not track individual messages. Instead, replicas periodically exchange their state (or a summary of it) and merge what they receive. Whatever was missed, for whatever reason, is repaired by the next exchange that gets through.

The simplest form ships the whole store to every peer every few hundred milliseconds. A lost message costs nothing, because the next round carries the same data. More economical forms send only versions or hashes first and transfer values just for the keys that differ; Dynamo and Cassandra compare Merkle trees of key ranges.

Anti-entropy is safe only if merging is idempotent and order-independent, for example "keep the newer version". Then duplicated, reordered or stale exchanges cannot do harm. It complements read repair, which only fixes keys that someone reads.`,
    related: ['Last-writer-wins', 'Convergence', 'Read repair'],
  },
  {
    term: 'Serializability',
    aliases: ['serializable'],
    summary: 'The guarantee that concurrent transactions produce the same result as running them one at a time in some order.',
    detail: `A system is **serializable** if every execution is equivalent to some serial execution of the same transactions: there is an order in which running them one after another yields exactly the reads and final state that were observed.

It is the strongest classic isolation level, and the one that lets programmers ignore concurrency. It does not promise a particular order, only that one exists. Strict serializability adds that the order respects real time.

Serializability needs coordination: some transactions must wait for or abort because of others. That is why it cannot be combined with total availability during a partition, and why databases offer weaker levels such as snapshot isolation or read committed.`,
    related: ['Serial execution', 'Total availability', 'Transactions'],
  },
  {
    term: 'Lamport timestamp',
    aliases: ['Lamport clock', 'logical clock'],
    summary: 'A counter each node increments on every event and raises to any larger value it receives, giving an order that respects causality.',
    detail: `Each node keeps an integer. It increments it before each local event (here: each transaction) and tags the event with it. When it receives a message carrying a timestamp, it sets its counter to the maximum of the two.

The receive rule guarantees that if event A could have influenced event B, A's timestamp is smaller. In the replicated store this means a node's write made after merging a peer's write always beats it.

Concurrent events can share a timestamp, so the tag used for ordering is (timestamp, node id). The result is a total order that every node computes identically, which is exactly what last-writer-wins needs.`,
    related: ['Last-writer-wins'],
  },
];
