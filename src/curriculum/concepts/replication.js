export default [
  {
    term: 'Primary-backup',
    aliases: ['primary backup', 'leader-follower', 'primary-backup replication'],
    summary: 'One node, the primary, decides the order of all writes; the backups copy what it did.',
    detail: `In **primary-backup** replication a single node accepts writes and orders them. Backups never decide anything: a write sent to a backup is forwarded to the primary, and the primary's decisions are replayed on every backup. Any node can serve reads from its local copy.

Example: n1 is the primary. A client sends \`write k3 = 7\` to n2; n2 forwards it to n1, n1 applies it as write number 41 and sends it to n2 and n3.

Because one node orders everything, there are no write conflicts to resolve. The weaknesses are the primary itself (all writes go through it) and failover: promoting a backup safely needs leader election. PostgreSQL, MySQL, Redis and MongoDB replica sets all use this model.`,
    related: ['Replication lag', 'Synchronous replication', 'Sequence numbers'],
  },
  {
    term: 'Replication lag',
    summary: 'The delay between a write being applied on the primary and the same write being applied on a backup.',
    detail: `With asynchronous replication the primary answers the client first and sends the write to backups afterwards. Until the write arrives, a backup returns older data. That window is the **replication lag**: usually milliseconds, but seconds or more when a backup is overloaded or a link is congested.

Lag causes visible anomalies: a client writes through the primary, reads from a backup and does not see its own write; two reads from different backups go backwards in time.

It also bounds what can be lost. If the primary fails for good, writes it acknowledged but had not yet shipped are gone. Systems monitor lag (PostgreSQL reports LSN differences, Redis reports replication offsets) and route reads that must be fresh to the primary.`,
    related: ['Primary-backup', 'Read-your-writes'],
  },
  {
    term: 'Sequence numbers',
    aliases: ['sequence number', 'seq'],
    summary: 'Numbers the primary attaches to writes so backups can apply them in the primary\'s order, whatever order they arrive in.',
    detail: `The network can reorder messages. A **sequence number** puts the order into the message itself: the primary increments a counter for each write and sends the value along. A backup stores \`{ value, seq }\` per key and applies an incoming write only if its \`seq\` is higher than the one it holds.

Example: the primary sends k3 = 7 (seq 1) and k3 = 8 (seq 2). The second arrives first and is applied; the first arrives late, 1 < 2, and is ignored. The backup ends on 8.

Per-key numbers prevent going backwards, but cannot tell a backup which writes it missed. A single log, where the sequence number is the position, can.`,
    related: ['Replication log', 'Primary-backup'],
  },
  {
    term: 'Asynchronous replication',
    summary: 'The primary acknowledges a write as soon as it has applied it, and replicates to backups afterwards.',
    detail: `With **asynchronous replication**, \`write_ok\` means "the primary has it". Writes are as fast as one node, and a slow or unreachable backup never blocks a client.

The cost is replication lag: backups may serve stale reads, and writes acknowledged just before a permanent primary failure are lost.

It is the default in PostgreSQL, MySQL and Redis. Many systems offer a middle ground, waiting for one backup (MySQL semisynchronous replication) or letting individual clients wait (Redis WAIT).`,
    related: ['Synchronous replication', 'Replication lag'],
  },
  {
    term: 'Synchronous replication',
    summary: 'The primary acknowledges a write only after the backups have confirmed applying it.',
    detail: `With **synchronous replication**, \`write_ok\` means "every backup has it" (or, in softer variants, "k backups have it"). A read from any of those nodes after the acknowledgement returns the write or something newer, and a permanent primary failure loses nothing that was acknowledged.

The price is latency (each write waits for the slowest backup) and availability: if one required backup is unreachable, writes cannot be acknowledged. The primary must give up after a deadline and return an explicit error rather than wait forever. Such an error means "unknown", not "not applied": some nodes may already hold the write.

PostgreSQL's \`synchronous_standby_names\` lets operators choose which standbys, and how many (\`FIRST n\` or \`ANY n\`), must confirm.`,
    related: ['Asynchronous replication', 'Availability', 'Read-your-writes'],
  },
  {
    term: 'Read-your-writes',
    aliases: ['read your writes', 'read-your-own-writes'],
    summary: 'After a client\'s write is acknowledged, that client\'s later reads see the write or something newer.',
    detail: `**Read-your-writes** is a guarantee about one client's view: it never fails to see its own acknowledged writes. Without it, a user updates a profile, reloads the page, and the page shows the old profile because the read went to a lagging backup.

Synchronous replication provides it for reads from any node. Asynchronous systems provide it by routing a client's reads to the primary for a while after it writes, or by having the client remember the log position of its last write and wait until the replica has reached it.

It says nothing about other clients' writes; stronger guarantees such as linearizability do.`,
    related: ['Synchronous replication', 'Replication lag'],
  },
  {
    term: 'Availability',
    summary: 'Whether the system answers requests successfully, especially while some nodes or links have failed.',
    detail: `A system is **available** to the extent that non-failed nodes answer requests successfully rather than with errors or silence. It is usually traded against a stronger promise.

Example: with synchronous replication to every backup, one unreachable backup makes every write fail, although the primary is fine. Waiting for a majority instead keeps writes working while a minority is down, at the price of a weaker promise about which nodes have the data.

A useful rule: every guarantee that needs some set of nodes to respond fails when that set cannot be reached. More nodes in the required set means more ways to become unavailable. An honest error, returned within a deadline, is still better than no answer.`,
    related: ['Synchronous replication', 'Total availability'],
  },
  {
    term: 'Replication log',
    summary: 'All writes in one numbered, gap-free sequence, so a backup\'s state is described by a single number: how many entries it has.',
    detail: `A **replication log** is an array of writes in the primary's order; an entry's index is its sequence number. Each backup keeps the same array. A backup that has 17 entries has exactly entries 0 to 16.

That makes catching up simple. The primary sends \`{ from, entries }\`; the backup appends only entries that extend its log exactly (skipping any it has, refusing a batch that would leave a gap) and replies \`have: log.length\`. The primary continues from \`have\` until the backup is complete. Lost requests and replies are retried; duplicates add nothing.

Invariant: every backup's log is a prefix of the primary's. PostgreSQL's WAL with its LSNs, MySQL's binlog and Raft's log all work this way.`,
    related: ['Durability', 'Crash recovery', 'Sequence numbers'],
  },
  {
    term: 'Durability',
    summary: 'A write that was acknowledged survives crashes and restarts.',
    detail: `**Durability** is a promise about what an acknowledgement means: once the client hears \`write_ok\`, the write will not be lost even if nodes crash.

On one machine that means writing to stable storage before replying. In this course \`node.disk\` survives crashes, so the primary appends a write to its on-disk log first and replies afterwards. A reply sent first is a promise the node can forget.

In replicated systems durability can also come from copies: an acknowledged write that is on several machines survives one of them losing its disk. Real disks need an fsync before data is safe, which costs milliseconds, so databases batch many commits into one fsync.`,
    related: ['Replication log', 'Crash recovery', 'Write-ahead logging'],
  },
  {
    term: 'Crash recovery',
    summary: 'Bringing a restarted node back to exactly the state it had committed, using only what survived on disk.',
    detail: `After a crash, everything in memory is gone. **Crash recovery** rebuilds it from durable storage. With a replication log on disk, recovery is one loop: start with an empty store and apply every log entry in order.

The restarted node then answers reads at once, never with less than it served before, because it served only what was already on disk. It reports its true log length, and the primary sends the rest.

The rule that makes this work is ordering: persist first, then act on it (reply, count it in \`have\`, send a message that depends on it). Anything done in the other order can be forgotten.`,
    related: ['Durability', 'Replication log'],
  },
  {
    term: 'Chain replication',
    summary: 'Nodes form a line: writes enter at the head and flow to the tail, which acknowledges writes and serves reads.',
    detail: `In **chain replication** (van Renesse and Schneider, 2004) each node forwards writes to its single successor. The head orders writes; the tail is the last to receive each one, so the tail sends \`write_ok\` and answers all reads.

Because a write reaches the tail last, everything the tail holds is on every node. Reads at the tail are therefore linearizable without any coordination at read time.

The costs: write latency grows with chain length, reads concentrate on one node, and replacing a failed node needs an external configuration manager. CRAQ spreads reads across the chain; FAWN-KV and Hibari are built on chains.`,
    related: ['Head and tail', 'Linearizable reads'],
  },
  {
    term: 'Linearizable reads',
    aliases: ['linearizable read'],
    summary: 'Reads that always reflect every write acknowledged before they began, as if there were only one copy.',
    detail: `A read is **linearizable** if it returns the value of the most recent write that completed before the read started, or of a write concurrent with it, and never something older. Once any client has seen a value, no later read returns an older one.

In chain replication the tail provides this for free: every acknowledged write was appended at the tail before the acknowledgement, and the tail answers reads from that same copy. A backup in asynchronous primary-backup cannot: it may lack an acknowledged write.

Elsewhere linearizable reads cost something: Raft-based systems confirm leadership with a majority (or a lease) before answering, and quorum systems need a write-back phase.`,
    related: ['Chain replication', 'Linearizability'],
  },
  {
    term: 'Head and tail',
    summary: 'The two ends of a replication chain: the head accepts and orders writes, the tail acknowledges them and serves reads.',
    detail: `The **head** is the first node in the chain. It is the only node that accepts writes, assigns each one a position in its log, and passes it to its successor. A write sent to another node is refused with \`not_head\`.

The **tail** is the last node. When a write reaches it, every node has it, so the tail tells the client \`write_ok\`. It is also the only node that answers reads; a read elsewhere is refused with \`not_tail\`.

Splitting the roles spreads the work: the head handles write admission, the tail handles reads and acknowledgements, and each middle node only forwards.`,
    related: ['Chain replication'],
  },
  {
    term: 'Failover',
    summary: 'Replacing a failed primary with a backup, without ending up with two primaries or losing acknowledged writes.',
    detail: `**Failover** promotes a backup when the primary fails. Two things can go wrong. With asynchronous replication, the promoted backup may lack writes the old primary acknowledged; they are lost. And if the old primary was only partitioned, not dead, it may keep accepting writes: two primaries, each ordering writes independently, is called **split brain**.

Avoiding split brain requires agreement on who is primary, usually with numbered terms or epochs that let nodes reject an outdated primary, decided by a consensus protocol or a coordination service.

This track keeps the primary fixed to isolate replication from election; the election and Raft tracks cover the rest.`,
    related: ['Primary-backup', 'Asynchronous replication'],
  },
  {
    term: 'Monotonic reads',
    aliases: ['monotonic read'],
    summary: 'A client that has seen a value never later sees an older one, even when it reads from different replicas.',
    detail: `**Monotonic reads** is a session guarantee: the reads of one client never go backwards in time. Without it, a client that reads from replicas with different lag can read \`k3 = 5\` from an up-to-date node and then \`k3 = 3\` from a lagging one.

The usual fix is a token the client carries. With a single replication log, the token can be a log position: "I have seen everything up to write 21". A replica that has applied fewer writes waits for replication, or forwards the read, before it answers.

The guarantee is per client. Two different clients may still see different states at the same moment; that needs stronger consistency.`,
    related: ['Session guarantees', 'Read-your-writes', 'Replication lag'],
  },
  {
    term: 'Session guarantees',
    aliases: ['session guarantee', 'session token', 'session tokens', 'causal token'],
    summary: 'Promises made to one client about what it will see, kept by passing a token that records what the client has seen.',
    detail: `**Session guarantees** describe what a single client observes across many requests, possibly to different replicas: read your writes, monotonic reads, writes follow reads, monotonic writes. They come from the Bayou project (Terry et al., 1994).

They are cheap because they need no coordination between replicas. The client keeps a token (a log position, or a version vector when there are several origins) and sends it with each request; a replica answers only once it has caught up with the token, and returns a new one that covers its answer.

MongoDB's causally consistent sessions, Azure Cosmos DB's session consistency and DynamoDB-style "read after write" tokens all work this way.`,
    related: ['Monotonic reads', 'Read-your-writes', 'Version vectors'],
  },
  {
    term: 'Consistent prefix reads',
    aliases: ['consistent prefix'],
    summary: 'Readers see causally related writes in the order they happened: never an answer without its question.',
    detail: `**Consistent prefix reads** promises that if one write happened before another (because its writer had seen it), nobody sees the second without the first. DDIA's example: a question and its answer written through different nodes. If a reader's replica receives the answer over a fast link and the question over a slow one, it shows an answer to a question that does not exist yet.

With one leader and one log applied in order, every replica shows a prefix of that log, and the guarantee comes for free. With several origins, a write must carry its **dependencies** and a replica must hold it back until they are applied.

Example: n2 accepts \`a7\` with deps \`{ n1: 4 }\`. n3 has applied only 3 writes from n1, so it keeps \`a7\` pending until n1's fourth write, \`q7\`, arrives.`,
    related: ['Causal dependencies', 'Monotonic reads', 'Multi-leader replication'],
  },
  {
    term: 'Causal dependencies',
    aliases: ['causal dependency', 'dependencies', 'deps'],
    summary: 'The earlier writes a write may depend on, recorded with it so replicas apply it only after them.',
    detail: `A write's **causal dependencies** are the writes its writer could have seen when making it. The answer to a question depends on the question. A replica that respects dependencies never shows a write before what it depends on, which gives causal consistency.

A compact way to record them is a vector of counters, one per origin: \`{ n1: 4, n2: 2 }\` means "the first 4 writes accepted at n1 and the first 2 at n2". A node accepting a write attaches its own applied vector (which, once it has caught up with the client's token, includes everything the client saw).

Recording the whole vector is conservative: it may hold back writes that did not really depend on everything listed. Systems such as COPS track tighter dependencies to reduce waiting.`,
    related: ['Consistent prefix reads', 'Version vectors', 'Causality'],
  },
  {
    term: 'Multi-leader replication',
    aliases: ['multi-leader', 'multi-master replication', 'active-active replication', 'active-active'],
    summary: 'Several nodes accept writes, each replicating them to the others asynchronously.',
    detail: `In **multi-leader replication** more than one node accepts writes, typically one per datacenter or region. Clients write to a nearby leader, so writes are fast, and a region cut off from the others can keep accepting writes.

The cost is write conflicts: two leaders may accept writes to the same key before either hears of the other's. Every node must resolve them by the same deterministic rule, or the replicas diverge for good. Causal ordering across leaders also needs explicit tracking.

Examples: MySQL and PostgreSQL multi-master tools (Tungsten, BDR), CouchDB, and the cross-region modes of Cassandra and DynamoDB global tables.`,
    related: ['Write conflicts', 'Last write wins', 'Primary-backup'],
  },
  {
    term: 'Write conflicts',
    aliases: ['write conflict', 'conflicting writes', 'concurrent writes'],
    summary: 'Two writes to the same key accepted by different nodes, neither of which knew about the other.',
    detail: `A **write conflict** arises when two nodes accept writes to the same key concurrently: n1 sets \`k1 = "red"\` while n3 sets \`k1 = "blue"\`, each before hearing of the other. With arrival-order application n1 ends on blue and n3 on red, and they stay that way.

Resolution needs a rule that depends only on the writes, not on when they arrive: last write wins by timestamp, keeping both as siblings for the application to merge, or a data type whose merge is commutative (a CRDT).

Last write wins is simple but silently discards the losing write; that is acceptable for caches and status fields, less so for a shopping cart.`,
    related: ['Multi-leader replication', 'Last write wins', 'Lamport clock'],
  },
  {
    term: 'Last write wins',
    aliases: ['last-writer-wins', 'LWW', 'last-write-wins'],
    summary: 'Resolve concurrent writes to a key by keeping the one with the highest timestamp and discarding the rest.',
    detail: `**Last write wins** gives each write a timestamp that all nodes order the same way, such as \`[lamportCounter, nodeId]\`, and keeps the highest. Because the outcome depends only on the timestamps, nodes that have seen the same writes agree, whatever the arrival order.

Timestamps must not tie (add the node id) and should respect causality: with wall clocks, a node with a slow clock can lose to a write it has already seen. A Lamport counter avoids that. Concurrent writes still lose data silently.`,
    related: ['Write conflicts', 'Lamport clock'],
  },
  {
    term: 'Lamport clock',
    aliases: ['Lamport clocks', 'Lamport timestamp', 'Lamport timestamps'],
    summary: 'A counter each node increments for its own events and raises past any counter it receives.',
    detail: `A **Lamport clock** is an integer per node: add one for each local event (here, each accepted write) and set it to at least any counter seen in an incoming message. If event A could have influenced event B, A's counter is smaller.

In multi-leader last-write-wins, this means a write always beats every value its node already held, regardless of skewed wall clocks. Pair it with the node id to break ties.`,
    related: ['Last write wins', 'Causality'],
  },
];
