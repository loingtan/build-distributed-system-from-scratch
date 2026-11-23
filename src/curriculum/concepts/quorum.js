export default [
  {
    term: 'Quorum',
    aliases: ['majority quorum'],
    summary: 'A set of replicas large enough that any two such sets share at least one member.',
    detail: `A **quorum** is a subset of nodes whose agreement counts for the whole group. The usual choice is a majority: with 5 nodes, any 3. Any two majorities overlap, because two disjoint sets of 3 would need 6 nodes.

That overlap is what quorum systems rely on. A decision made by one quorum is seen by every later quorum, because at least one member took part in both. Quorum replication uses it for reads and writes; consensus protocols such as Raft and Paxos use it for elections and commits.

With majorities, a system tolerates the failure of any minority: 2 of 5, or 1 of 3. A node on the minority side of a partition cannot assemble a quorum and must refuse.`,
    related: ['Write quorum', 'Read quorum', 'W + R > N'],
  },
  {
    term: 'Leaderless replication',
    aliases: ['leaderless', 'Dynamo-style replication'],
    summary: 'Every replica accepts reads and writes directly; no single node orders writes for the others.',
    detail: `In **leaderless replication** a client can send any request to any replica. The receiving node coordinates that request by forwarding it to the other replicas and waiting for enough of them; there is no primary to fail over.

Example: in a 5-node cluster with W = 3, n4 receives \`put k1\`, stores it, sends it to n1, n2, n3 and n5, and answers after two of them confirm.

The benefit is availability: any majority can serve requests, and a slow node does not slow anyone. The cost is that concurrent writes to one key are not ordered by anyone, so the system needs versions and a conflict rule (last write wins, or keeping siblings), plus repair mechanisms for replicas that fall behind. Dynamo, Cassandra and Riak work this way.`,
    related: ['Coordinator', 'Quorum', 'Last write wins'],
  },
  {
    term: 'Coordinator',
    summary: 'The node that receives a client request and drives it: forwarding to replicas, counting answers and replying.',
    detail: `In a leaderless store, the **coordinator** is simply whichever node a client contacted. For that one request it sends the operation to all replicas in parallel, collects their answers, decides success or failure and replies exactly once.

The coordinator is usually a replica as well, so it applies the request locally and counts itself as the first answer. It also runs read repair, because it is the one that has seen every replica's version.

Every node coordinates some requests. A coordinator is not a leader: it holds no special authority, and two coordinators can handle conflicting writes at the same time.`,
    related: ['Leaderless replication', 'Write quorum'],
  },
  {
    term: 'Write quorum',
    summary: 'The number W of replicas that must store a write before the coordinator reports success.',
    detail: `A **write quorum** is any W replicas that have stored a write. The coordinator sends the write to all N replicas and answers \`put_ok\` once W have confirmed; if that becomes impossible, it answers with an error before the client's timeout.

W trades durability against availability. W = 1 is fast but the write is on one machine. W = N means one dead node blocks all writes. A majority, \`floor(N / 2) + 1\`, survives any minority of failures.

A failed write quorum is not a rollback: the replicas that stored the value keep it, and repair may spread it later. The outcome is "unknown".`,
    related: ['Read quorum', 'W + R > N', 'Quorum'],
  },
  {
    term: 'Read quorum',
    summary: 'The number R of replicas whose answers a read collects before returning the newest value among them.',
    detail: `For a quorum read, the coordinator asks the replicas and waits for R answers, then returns the value with the newest version among them. If fewer than R answer in time, it returns an error.

If W + R > N, the R answers always include one from a replica that stored the last acknowledged write, so the read cannot miss it. With N = 5 and W = 3, R = 3 is the smallest value that works.

A smaller R makes reads cheaper and more available but allows stale reads; a larger R does the opposite. A put's first round, which finds the current highest version, is also a read quorum.`,
    related: ['Write quorum', 'W + R > N', 'Versions'],
  },
  {
    term: 'W + R > N',
    aliases: ['quorum condition', 'quorum intersection'],
    summary: 'If writes wait for W replicas and reads for R out of N, and W + R > N, every read overlaps every completed write.',
    detail: `The **quorum condition** is a counting argument. A write quorum and a read quorum together list W + R replicas, but there are only N distinct ones. If W + R > N, at least one replica appears in both. That replica stored the write and answered the read.

Examples with N = 5: W = 3, R = 3 overlaps; W = 5, R = 1 overlaps (cheap reads, but one dead node blocks writes); W = 2, R = 2 does not, and a read can miss an acknowledged write.

The condition guarantees that a read sees writes that finished before it started. It does not make reads linearizable: during a write, two reads can disagree. And it assumes a fixed replica set; sloppy quorums and membership changes break it.`,
    related: ['Read quorum', 'Write quorum', 'Quorum'],
  },
  {
    term: 'Versions',
    aliases: ['version', 'version number'],
    summary: 'A tag on each stored value, such as (counter, node id), that lets replicas and readers tell which copy is newest.',
    detail: `Replicas can hold different copies of a key, so each copy carries a **version**. In the quorum track it is \`(counter, origin)\`: compare counters, and break ties with the origin node id so that no two different writes ever compare equal.

Two rules use it. A replica replaces its copy only with a newer version, which makes every store, repair and sync message safe to repeat or reorder. A read returns the newest version among its answers.

The counter for a new write must come from a read quorum, not from the coordinator's own copy: a coordinator that missed writes would otherwise mint a version older than one already acknowledged. Systems that must keep concurrent writes use version vectors instead of a single counter.`,
    related: ['Last write wins', 'Idempotent writes', 'Read quorum'],
  },
  {
    term: 'Last write wins',
    aliases: ['last-writer-wins', 'LWW', 'last-write-wins'],
    summary: 'When copies disagree, keep the one with the highest version and silently drop the others.',
    detail: `**Last write wins** resolves conflicts by comparing versions and keeping the largest. In the quorum track the version is \`(counter, origin)\`; in Cassandra it is a timestamp per column.

It always converges, because every replica applies the same comparison. But when two clients write the same key concurrently, both are acknowledged and only one survives. "Last" means highest version, which need not match real time.

Use it where the latest value is all that matters. For counters, sets or carts, keep concurrent versions (siblings with version vectors) or use CRDTs.`,
    related: ['Versions', 'Last-writer-wins'],
  },
  {
    term: 'Read repair',
    summary: 'A quorum read that notices stale replicas sends them the newest version it found.',
    detail: `During a quorum read the coordinator compares every replica's version. **Read repair** uses that knowledge: each replica that answered with something older receives the newest entry, with its original version unchanged.

Example: a get for k1 receives v4 from n1, n2 and n3, answers the client, then receives v2 from n5 and sends n5 \`store k1 v4\`. If the coordinator itself was stale, it repairs its own copy.

The repair must not mint a new version, which could bury a newer write in flight. And the coordinator must keep listening after the first R answers, because the stale replicas are often the slow ones. Read repair only fixes keys that are read; anti-entropy covers the rest. Cassandra and Riak both perform it.`,
    related: ['Anti-entropy', 'Convergence', 'Idempotent writes'],
  },
  {
    term: 'Convergence',
    aliases: ['converge'],
    summary: 'Once writes stop and messages get through, all replicas end up holding the same data.',
    detail: `A replicated system **converges** if, after the last write and once the network allows communication, every replica reaches the same state and stays there. This is the core promise of eventual consistency.

Convergence needs two things: every replica eventually learns every surviving write (through replication, read repair or anti-entropy), and replicas resolve conflicts identically regardless of the order in which they learn of them. "Keep the higher version" satisfies the second because taking a maximum is commutative, associative and idempotent.

It says nothing about how long it takes or what reads return meanwhile. Systems bound the delay with frequent anti-entropy and add quorums for stronger read guarantees.`,
    related: ['Anti-entropy', 'Read repair', 'Last write wins'],
  },
  {
    term: 'Idempotent writes',
    aliases: ['idempotent write'],
    summary: 'Writes that leave the same state whether they are applied once or many times, so they can be retried safely.',
    detail: `A write is **idempotent** if applying it again changes nothing. "Store version (4, n1) of k1, if it is newer than what you have" is idempotent: the first application stores it, every later one finds it already there. "Append to k1" or "increment k1" are not.

In quorum replication every message that moves data, whether an original store, a read repair or an anti-entropy sync, is applied through the same version check. That makes duplicates harmless, reordering harmless (an older version arriving late is ignored) and loss harmless (the next repair or sync carries the same entry).

Idempotence is what lets a system replace careful exactly-once delivery with "send it again until it sticks".`,
    related: ['Versions', 'Read repair', 'Anti-entropy'],
  },
  {
    term: 'Anti-entropy',
    summary: 'Replicas regularly compare what they hold with a peer and exchange whatever one has that the other lacks.',
    detail: `**Anti-entropy** repairs replicas in the background, independently of reads. In the quorum track each node, every 200 ms, sends one random peer its \`{ key: version }\` map; the peer returns entries where it is ahead and names keys where the sender is ahead, which the sender then ships.

Because entries are applied through the "only if newer" check, a lost or repeated round needs no retry. Dynamo, Cassandra (\`nodetool repair\`) and Riak use Merkle trees so that ranges of identical keys cost a single hash comparison.`,
    related: ['Read repair', 'Hinted handoff', 'Gossip', 'Merkle tree'],
  },
  {
    term: 'Hinted handoff',
    aliases: ['hints'],
    summary: 'The coordinator remembers which replicas missed a write and delivers it to them once they are reachable again.',
    detail: `With **hinted handoff**, a coordinator that did not get an acknowledgement from some replica keeps a hint: the entry and the replica that is missing it. It retries until that replica confirms, then discards the hint.

It is quick, because the coordinator already knows exactly who is behind and what to send. Its weakness is that the hints live on one node: if that node crashes or loses them, the missed writes are only repaired by read repair or anti-entropy.

Dynamo and Riak combine it with sloppy quorums: while a home replica is down, another node accepts the write with a hint and hands it over later. Cassandra stores hints on the coordinator for a configurable window.`,
    related: ['Anti-entropy', 'Sloppy quorum'],
  },
  {
    term: 'Gossip',
    aliases: ['gossip protocol', 'epidemic protocol'],
    summary: 'Nodes repeatedly exchange information with randomly chosen peers, so updates spread through the cluster like a rumour.',
    detail: `In a **gossip** protocol each node, every round, picks a random peer and exchanges state with it. A piece of news known by one node reaches about twice as many nodes per round while it is rare, so it reaches everyone in roughly log N rounds, without any node contacting everyone.

Gossip tolerates loss and failures naturally: a missed exchange is just one fewer random contact, and the next round tries a different peer.

Anti-entropy between random pairs is gossip applied to data. The same pattern spreads membership and failure information in Cassandra and in SWIM-based systems such as Consul and Serf.`,
    related: ['Anti-entropy', 'Convergence'],
  },
  {
    term: 'Sloppy quorum',
    summary: 'When home replicas are unreachable, accept the write on other nodes temporarily, trading the overlap guarantee for availability.',
    detail: `In a strict quorum, a write must reach W of the key's N designated replicas. A **sloppy quorum** counts any W reachable nodes, using stand-ins for the unreachable ones; each stand-in holds the write with a hint and hands it to the proper replica later.

Writes then succeed during partitions that would block a strict quorum. But a read quorum of designated replicas may not overlap a write quorum of stand-ins, so W + R > N no longer guarantees fresh reads until handoff completes.

Dynamo and Riak use sloppy quorums by default; Cassandra does not count hints toward the consistency level (except at level ANY).`,
    related: ['Hinted handoff', 'W + R > N'],
  },
  {
    term: 'Merkle tree',
    aliases: ['hash tree'],
    summary: 'A tree of hashes over ranges of keys, letting two replicas find their differences by comparing a few hashes.',
    detail: `A **Merkle tree** hashes each key range at the leaves, and each inner node hashes its children. Two replicas compare root hashes; if equal, they hold the same data. If not, they descend only into the subtrees whose hashes differ.

When two replicas differ in a handful of keys out of millions, this finds them with a number of comparisons proportional to the depth of the tree, rather than sending every key's version.

Dynamo, Cassandra's repair and Riak's active anti-entropy use Merkle trees. The cost is maintaining the tree as data changes.`,
    related: ['Anti-entropy'],
  },
];
