export default [
  {
    term: 'Availability',
    aliases: ['total availability', 'highly available', 'high availability'],
    summary: 'Every request to a node that has not crashed gets a successful answer, even while the network is failing.',
    detail: `A system is **available** if every request received by a non-crashed node eventually gets a non-error response. The strong form used in the CAP theorem, sometimes called **total availability**, demands this even during a network partition: any node that is up must be able to answer, whatever it can or cannot reach.

That rules out any design in which answering requires hearing from another node. If \`n2\` must ask \`n1\` for the next order number, a partition between them makes \`n2\` wait forever. A node can only stay totally available by answering from local state: minting ids from its own node number and counter, accepting a counter increment into its own CRDT entry, or acknowledging a broadcast and replicating it later.

In operations, "availability" also means the fraction of time a service works (99.9% is about 8.8 hours of downtime a year). The two meanings connect: designs that never block on other nodes keep working through failures that would stop a coordinated design, at the cost of weaker consistency.`,
    related: ['Network partition', 'Coordination-free design', 'CAP theorem'],
  },
  {
    term: 'Network partition',
    aliases: ['partitions', 'network partitions', 'netsplit'],
    summary: 'A network failure that splits nodes into groups that cannot reach each other, while every node keeps running.',
    detail: `In a **network partition** some links fail so that the cluster splits into groups: messages flow within a group but are lost between groups. Every node is still up and serving whoever can reach it. The partition ends when the network **heals**, and messages flow again.

From inside one group, a partition is indistinguishable from the other group having crashed: requests go out and nothing comes back. This is what makes partitions hard. A node that waits for the "dead" side may wait forever; a node that carries on alone may make decisions that conflict with the other side's. Example: five nodes split into \`{n1, n2}\` and \`{n3, n4, n5}\` for two seconds. A central id allocator on \`n1\` leaves three nodes unable to issue ids; local id generation keeps all five answering.

Partitions happen in practice through switch failures, misconfigured firewalls, overloaded links and long garbage-collection pauses. Every design in this curriculum has to state what it does during one: stay available and reconcile later (gossip, CRDTs), or stay consistent and refuse requests on the minority side (quorums, consensus).`,
    related: ['Availability', 'CAP theorem', 'Coordination-free design'],
  },
  {
    term: 'Coordination-free design',
    aliases: ['coordination-free', 'coordination free', 'coordination avoidance'],
    summary: 'Each node acts on local state alone, yet the system as a whole stays correct, so no request waits on the network.',
    detail: `**Coordination** is any step where a node must exchange messages with others before acting: asking a leader, taking a distributed lock, waiting for a majority. A **coordination-free** design avoids it on the request path: every node decides using only what it already has, and the design guarantees that these independent decisions never conflict.

Examples: ids made from \`(node id, local counter)\` are unique without asking anyone; a grow-only counter lets each node increment its own entry and merge later; a broadcast node acknowledges the client at once and gossips in the background. Each stays available during partitions and responds in local time.

Not every requirement allows it. "Ids are unique" is coordination-free; "ids are consecutive with no gaps" is not. "The counter converges to the total" is coordination-free; "the balance never goes below zero" is not, because two nodes could each spend the last unit before hearing from each other. Identifying which invariants need coordination, and paying for it only there, is a central skill in distributed design.`,
    related: ['Availability', 'Network partition', 'CRDT'],
  },
  {
    term: 'Snowflake IDs',
    aliases: ['Snowflake ID', 'Snowflake'],
    summary: 'A 64-bit id packed from a timestamp, a machine number and a sequence, so it is unique and sorts by creation time.',
    detail: `A **Snowflake ID**, designed at Twitter in 2010, is a 64-bit integer with three fields, most significant first: 41 bits of milliseconds since a custom epoch, 10 bits of machine number, and 12 bits of sequence (the top bit stays 0). Each machine generates ids alone: it reads its clock, and if the millisecond equals the last one used, increments the sequence; otherwise it resets the sequence to 0.

Uniqueness comes from the machine and sequence fields; order comes from the time field, which dominates the comparison. The layout allows 1,024 machines, 4,096 ids per millisecond per machine, and about 69 years from the epoch.

Variants are everywhere: Discord, Instagram and many databases use the same recipe with different bit budgets. Two practical cautions: the ids exceed 2^53, so JSON clients must receive them as strings, and a clock that steps backwards must not be allowed to reissue a used timestamp.`,
    related: ['Bit packing', 'k-sorted', 'Clock skew', 'UUID'],
  },
  {
    term: 'k-sorted',
    aliases: ['k sorted', 'roughly sorted', 'roughly time-ordered'],
    summary: 'Sorted except within a bounded window: items created close together in time may appear in either order.',
    detail: `A sequence is **k-sorted** if every item is at most a bounded distance *k* from its position in perfect order. Time-based ids are k-sorted rather than sorted because each node reads its own clock, and clocks disagree by the **clock skew**.

Example: \`n1\`'s clock runs 4 ms ahead of \`n2\`'s. \`n1\` issues an id at real time 1,000.0 ms (its clock reads 1,004) and \`n2\` issues one at 1,002.0 ms (its clock reads 1,002). The later id sorts first. Ids created more than 4 ms apart always sort correctly.

So "sort by id" gives creation order only at a resolution coarser than the skew. The lab's checker states its bound explicitly: an id requested 50 ms or more after another was returned must be larger. Applications that need an exact order across nodes need something stronger, such as a single sequencer, a consensus log, or logical clocks.`,
    related: ['Snowflake IDs', 'Clock skew'],
  },
  {
    term: 'Bit packing',
    aliases: ['bit fields', 'bit-packing'],
    summary: 'Storing several small fields in one integer by giving each its own fixed range of bit positions.',
    detail: `**Bit packing** combines fields into one number by shifting each into its own bit range: with a 12-bit sequence and a 6-bit node number below a timestamp, \`id = (ms × 2^6 + node) × 2^12 + seq\`. As long as each field stays within its width (\`node < 64\`, \`seq < 4096\`), the packing is one-to-one: different field values always give different integers, and each field can be read back out.

The most significant field controls sort order; lower fields only break ties. That is why time goes on top in a sortable id.

In JavaScript the shift operators (\`<<\`, \`|\`) work on 32-bit integers and silently drop higher bits, so a 50-bit id built with shifts wraps or turns negative. Use multiplication by powers of two and addition instead, and keep the result below 2^53, the largest integer a JavaScript or JSON number holds exactly.`,
    related: ['Snowflake IDs'],
  },
  {
    term: 'CAP theorem',
    aliases: ['CAP'],
    summary: 'During a network partition, a system must choose between staying available and keeping every node consistent.',
    detail: `The **CAP theorem** (Brewer's conjecture, proved by Gilbert and Lynch in 2002) says a distributed system cannot guarantee all three of: **Consistency** (every read sees the latest write, as if there were one copy), **Availability** (every non-crashed node answers every request) and **Partition tolerance** (the system keeps operating while messages between groups are lost). Since partitions cannot be ruled out in a real network, the practical reading is: *when a partition happens*, choose C or A.

A central id allocator or a majority-quorum database chooses consistency: the minority side refuses requests. Coordination-free id generation, gossip broadcast and CRDTs choose availability: every side keeps answering and the sides reconcile after healing.

CAP is a statement about the partition case only, and about one specific strong consistency model. Much of real system design lives in the space it leaves open: weaker consistency models that can be available, and latency costs paid even when there is no partition.`,
    related: ['Availability', 'Network partition', 'Eventual consistency'],
  },
  {
    term: 'Clock skew',
    aliases: ['clock drift', 'physical clocks', 'wall-clock time'],
    summary: 'The difference between two machines’ clocks at the same instant; it makes timestamps from different nodes unreliable for ordering.',
    detail: `Every machine keeps time with a quartz oscillator that runs slightly fast or slow (**drift**, typically tens of parts per million). Synchronisation protocols such as NTP correct it periodically, but at any instant two clocks differ by some amount, the **skew**: often a few milliseconds within a data centre, and far more when synchronisation fails.

Consequences: an event stamped 10:00:00.003 on \`n1\` may really have happened after one stamped 10:00:00.005 on \`n2\`. Corrections can also step a clock *backwards*, so even one node's readings are not guaranteed to increase; virtual machines can pause and resume with a jump.

So physical timestamps give only approximate order across nodes (time-based ids are **k-sorted**), and code that needs "never decreases" must enforce it itself. Where exact causal order matters, systems use logical clocks instead; where real time matters, systems like Spanner bound the uncertainty explicitly and wait it out.`,
    related: ['k-sorted', 'Logical time', 'Lamport clock'],
  },
  {
    term: 'UUID',
    aliases: ['UUIDs', 'UUIDv4', 'UUIDv7', 'GUID'],
    summary: 'A standard 128-bit identifier that any machine can generate without coordination, either randomly or from time plus randomness.',
    detail: `A **UUID** (universally unique identifier, RFC 9562) is a 128-bit value written as 32 hex digits, like \`0192c2a4-7b1e-7cc3-9a5d-3f1f0e2b8c41\`. Version 4 fills 122 bits with random data: collisions are possible in principle but with a good random source vanishingly unlikely (around one in 10^13 for a trillion ids). No node numbers need to be assigned, which makes v4 the default choice for ids generated on clients.

Random ids do not sort by time, so database inserts scatter across the index. Version 7 fixes that: its top 48 bits are a Unix timestamp in milliseconds and the rest is mostly random, so v7 UUIDs are k-sorted like Snowflake ids while keeping the no-configuration property.

The cost compared with a 64-bit packed id is size: 16 bytes per key, and more in every index that references it.`,
    related: ['Snowflake IDs', 'k-sorted'],
  },
];
