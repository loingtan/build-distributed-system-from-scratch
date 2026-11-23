export default [
  {
    term: 'CRDT',
    aliases: ['CRDTs', 'conflict-free replicated data type', 'conflict-free replicated data types', 'convergent replicated data type'],
    summary: 'A replicated data type that any replica can update alone, whose copies always merge back into the same state without conflicts.',
    detail: `A **CRDT** (conflict-free replicated data type) lets every replica accept updates locally, with no coordination, and guarantees that replicas which have seen the same updates end up in the same state. In the *state-based* form, replicas exchange whole states and combine them with a **merge function** that is commutative, associative and idempotent, so message order, routing and duplication do not matter. In the *operation-based* form, replicas exchange operations that commute, which requires delivering each operation exactly once.

Example: the G-Counter keeps one entry per node (how much was added there), merges by per-entry max, and reports the sum. Two replicas that each added 1 during a partition hold \`{n1: 7, n2: 4}\` and \`{n1: 6, n2: 5}\`; both merge to \`{n1: 7, n2: 5}\`.

CRDTs are fully available and converge after partitions, but they cannot enforce invariants that span replicas (a counter cannot be kept non-negative). Riak's data types, Redis Enterprise Active-Active databases, Akka Distributed Data and collaborative editors such as Automerge and Yjs are built on them.`,
    related: ['Merge functions', 'Monotonicity', 'CRDT composition', 'Eventual consistency', 'G-Counter'],
  },
  {
    term: 'Eventual consistency',
    aliases: ['eventually consistent'],
    summary: 'If updates stop, all replicas eventually return the same value; until then, different replicas may be behind.',
    detail: `A system is **eventually consistent** if, once no new updates are made, every replica eventually converges to the same state. It says nothing about *when*, and nothing about what reads return in the meantime: during a partition, or just after a write, two replicas may report different values, each correct for the updates it has received.

Example: during a partition, \`n1\` has counted 7 likes and \`n2\` 6; both are right about what they have seen. A second after the network heals, both report 10.

The weak definition leaves room for bad designs (replicas could converge to a value that drops some updates). CRDTs give **strong eventual consistency**: replicas that have received the same set of updates are in the same state, with no update lost. Eventual consistency is what systems choose when they put availability first: Dynamo-style databases, DNS, gossip-based membership and CRDT stores all work this way.`,
    related: ['Strong eventual consistency', 'CRDT', 'Anti-entropy', 'Availability'],
  },
  {
    term: 'Merge functions',
    aliases: ['merge function', 'least upper bound'],
    summary: 'The rule that combines two replicas’ states; it must be commutative, associative and idempotent for replicas to converge.',
    detail: `In a state-based CRDT, a replica that receives another replica's state combines the two with a **merge function** ⊔. Three properties make convergence automatic: **commutative** (a ⊔ b = b ⊔ a: the order two states meet does not matter), **associative** ((a ⊔ b) ⊔ c = a ⊔ (b ⊔ c): the route information takes through the cluster does not matter) and **idempotent** (a ⊔ a = a: receiving the same state twice changes nothing).

Good merges: set union, per-entry max of a table whose entries only grow, logical OR of a flag that can only be set. Bad merges: adding two counters (not idempotent: each repeated gossip adds again), max of a single shared counter (loses concurrent increments), "keep whichever arrived last" (not commutative: replicas that received states in different orders disagree).

Formally, such a merge computes the least upper bound of two states in a join-semilattice: the smallest state that includes everything both inputs know.`,
    related: ['CRDT', 'Monotonicity', 'Idempotence'],
  },
  {
    term: 'CRDT composition',
    aliases: ['composing CRDTs'],
    summary: 'Building larger CRDTs from smaller ones: a structure whose parts are CRDTs, merged part by part, is itself a CRDT.',
    detail: `**Composition** builds new CRDTs out of existing ones. If each component has a commutative, associative and idempotent merge, then merging a structure component by component has those properties too, so the whole structure is a CRDT.

The PN-Counter is the classic example: two G-Counters, \`P\` for everything added and \`N\` for everything subtracted, merged as \`(P₁ ⊔ P₂, N₁ ⊔ N₂)\`, with value sum(P) − sum(N). A counter that can go down is assembled from two parts that only go up.

The same move gives sets with removal (a grow-only set of additions plus one of removals, or the more careful observed-remove set), and maps whose values are counters, sets or registers merged key by key, which is how Riak's map type and document CRDTs such as Automerge are structured.`,
    related: ['CRDT', 'PN-Counter', 'G-Counter', 'Monotonicity'],
  },
  {
    term: 'Monotonicity',
    aliases: ['monotonic', 'monotone', 'grow-only'],
    summary: 'State that only moves forward in some order and never back, which lets "larger" stand in for "newer" without any clocks.',
    detail: `A value is **monotonic** if every change moves it in one direction within some order: a set that only gains elements, a counter entry that only increases, a flag that can be set but never cleared. Monotonic state is what makes simple merges correct. If entry \`n1\` of a G-Counter only grows, then of two copies of it the larger is always the more recent, so \`max\` picks the newer value without timestamps.

Break monotonicity and the merge breaks with it. Let an entry go from 5 down to 3 and a merge with an old copy (5) picks 5, discarding the decrement. The fix is to restore monotonicity by restructuring the state: record decrements as increments of a separate grow-only table.

Monotonicity also shows up to users: in a G-Counter, a node's reported value never decreases, so reads never go backwards. More generally, the CALM theorem says that programs whose outputs only grow as inputs arrive (monotonic programs) can be run consistently without coordination.`,
    related: ['Merge functions', 'CRDT composition', 'G-Counter'],
  },
  {
    term: 'Anti-entropy',
    aliases: ['anti entropy', 'anti-entropy repair'],
    summary: 'A background process in which replicas regularly compare or exchange state and repair whatever differences they find.',
    detail: `**Anti-entropy** is the periodic reconciliation of replicas. Instead of relying on every individual update message arriving, each node regularly exchanges state with peers and fixes any differences. Entropy (disorder: replicas drifting apart through lost messages and missed writes) is pushed back by this constant repair.

The simplest form pushes full state on a timer: every 150 ms each node sends its whole G-Counter table to each peer with a fire-and-forget send. A lost message is repaired by the next round; a duplicate is absorbed by an idempotent merge. For large data sets, sending everything is too costly, so nodes first compare compact summaries, such as Merkle trees (hash trees over key ranges) or version vectors, and transfer only the ranges that differ.

The idea goes back to the 1987 Xerox epidemic algorithms paper. Dynamo-style databases (Cassandra's repair, Riak's active anti-entropy) use Merkle trees to find divergent keys between replicas.`,
    related: ['Gossip', 'Eventual consistency', 'Merge functions'],
  },
  {
    term: 'G-Counter',
    aliases: ['grow-only counter', 'GCounter'],
    summary: 'A CRDT counter with one entry per node; each node increments only its own entry, merge takes per-entry max, value is the sum.',
    detail: `A **G-Counter** (grow-only counter) stores a table mapping each node id to how much has been added at that node. An increment at \`n2\` raises only \`n2\`'s entry. To merge a table received from a peer, take the maximum of each entry; the counter's value is the sum of the entries.

Example: \`n1\` holds \`[7, 0, 0]\`, \`n2\` holds \`[3, 2, 0]\`. Merging gives \`[7, 2, 0]\`, value 9. Merging again, or merging \`n2\`'s older copy, changes nothing.

It works because each entry has a single writer and only grows, so max always picks the newest copy, and per-node entries keep concurrent increments separate. It cannot count down; for that, combine two of them into a PN-Counter.`,
    related: ['PN-Counter', 'CRDT', 'Monotonicity'],
  },
  {
    term: 'PN-Counter',
    aliases: ['positive-negative counter', 'PNCounter'],
    summary: 'A CRDT counter that can go up and down, built from two G-Counters: one for increments, one for decrements.',
    detail: `A **PN-Counter** keeps two grow-only tables per node: \`P\` (total added at each node) and \`N\` (total subtracted at each node). A delta of +3 at \`n1\` raises \`P[n1]\` by 3; a delta of −2 raises \`N[n1]\` by 2. Each table merges with per-entry max, and the value is sum(P) − sum(N).

A single table cannot work: a decrement makes an entry smaller, and a later merge with an older, larger copy would restore the old value. Splitting into two tables keeps every entry monotonic, which is the property max relies on.

A PN-Counter can go negative and cannot prevent it: two replicas can each subtract the last unit during a partition. Enforcing a lower bound needs coordination or pre-allocated budgets per replica.`,
    related: ['G-Counter', 'CRDT composition', 'Monotonicity'],
  },
  {
    term: 'Strong eventual consistency',
    aliases: ['SEC'],
    summary: 'Replicas that have received the same updates are in the same state, whatever order the updates arrived in.',
    detail: `**Strong eventual consistency** strengthens eventual consistency in two ways: convergence does not wait for updates to stop, and it involves no rollback or conflict resolution step. Any two replicas that have delivered the same set of updates, in any order and by any route, have equal state.

Example: \`n1\` merges states from \`n2\` then \`n3\`; \`n2\` merges from \`n3\` then \`n1\`. Once both have seen all three nodes' updates, both hold exactly the same table, because merge is commutative, associative and idempotent.

CRDTs are the standard way to get it. It is stronger than plain eventual consistency, which allows replicas to disagree until some later reconciliation, but weaker than linearizability: replicas can still be behind, and nothing stops two replicas from accepting updates that together violate an invariant.`,
    related: ['Eventual consistency', 'CRDT', 'Merge functions'],
  },
];
