// Glossary entries for the "Sets, registers and maps" track. See docs/guides.md.
export default [
  {
    term: 'G-Set',
    aliases: ['grow-only set'],
    summary: 'A replicated set that only supports add; replicas merge by taking the union of their elements.',
    detail: `A **grow-only set** is the simplest set CRDT. Its state is a set of elements, \`add\` inserts one, and merging two replicas takes their union.

Union is commutative, associative and idempotent, so replicas that exchange their sets in any order, any number of times, end with the same elements. If n1 holds {a, b} and n2 holds {b, c}, both end with {a, b, c} after one exchange in each direction, and further exchanges change nothing.

The price is that nothing can ever be removed: deleting an element locally is undone by the next merge from a replica that still has it. Sets with removal (2P-Set, OR-Set) are built from two or more grow-only parts.`,
    related: ['OR-Set', 'Anti-entropy', 'Join semilattice'],
  },
  {
    term: 'Anti-entropy',
    summary: 'A background process in which replicas repeatedly send each other their state, so anything missed is eventually repaired.',
    detail: `In **anti-entropy**, every replica periodically sends its state (or a summary of it) to some or all peers, and each peer merges what it receives. Nobody tracks which individual updates reached whom.

It works because CRDT merges are idempotent: receiving the same state twice is harmless, and a message lost in one round is covered by the next. With full-state gossip every 100ms and a quarter of messages lost, a replica that misses an update is very unlikely to stay behind for more than a few rounds.

The cost is bandwidth proportional to the state size per round. Real systems send digests (Merkle trees in Cassandra and Riak) or only the recent changes (delta-state CRDTs) and fall back to full exchange when a peer has been away.`,
    related: ['G-Set', 'Convergence'],
  },
  {
    term: 'Join semilattice',
    aliases: ['semilattice', 'merge function'],
    summary: 'A set of states with a merge that is commutative, associative and idempotent; the mathematical shape every state-based CRDT has.',
    detail: `A **join semilattice** is a set of values with a partial order and an operation, the **join** (merge), that gives the smallest value at least as large as both inputs. For sets the order is "subset of" and the join is union; for G-Counter entries the order is ≤ and the join is max.

The join is commutative (merge(a, b) = merge(b, a)), associative (grouping does not matter) and idempotent (merge(a, a) = a). Together these mean that replicas that have received the same updates, in whatever order and with whatever duplication, hold the same state.

Designing a state-based CRDT means finding a state that only moves up in some such order. Updates that seem to go down, like a remove or a decrement, are encoded as growth of another component: a tombstone set, or a second counter.`,
    related: ['G-Set', 'Anti-entropy'],
  },
  {
    term: 'OR-Set',
    aliases: ['observed-remove set', 'add-wins set'],
    summary: 'A replicated set where a remove deletes only the adds it has seen, so a concurrent add survives and elements can be re-added.',
    detail: `In an **observed-remove set**, every \`add\` attaches a unique tag to the element. A \`remove\` tombstones the tags its replica currently holds for that element. The element is present while it has at least one tag that is not tombstoned. Both the tag sets and the tombstone set only grow, so merging is union.

Example: "milk" carries tag n1:1 everywhere. During a partition n1 removes milk (tombstoning n1:1) while n2 adds milk again (tag n2:5). After the heal, n2:5 is not tombstoned, so milk is present on every replica: the add wins.

The cost is metadata: tombstones accumulate until they can be garbage-collected. Optimised versions (OR-SWOT in Riak) use version vectors to summarise removed tags instead of storing each one.`,
    related: ['Tombstones', 'Unique tags', 'Add-wins', 'G-Set'],
  },
  {
    term: 'Tombstones',
    aliases: ['tombstone'],
    summary: 'Records that something was deleted, kept so that a stale copy arriving later cannot bring it back.',
    detail: `A **tombstone** is a marker meaning "this was removed". In a replicated system a plain delete is not enough, because a replica that has not heard of the delete will send its copy back and the next merge restores it.

In an OR-Set the tombstones are the removed add-tags; in Cassandra they are special cells with a deletion timestamp. Merging keeps all tombstones, so a removal that reached one replica eventually reaches every replica.

Tombstones grow without bound unless they are collected. They can only be dropped once every replica is known to have seen them; Cassandra does it after a configurable grace period (\`gc_grace_seconds\`), and a replica that was away longer than that can resurrect deleted data.`,
    related: ['OR-Set'],
  },
  {
    term: 'Add-wins',
    aliases: ['add wins'],
    summary: 'A conflict rule for sets: when an add and a remove of the same element are concurrent, the element ends up present.',
    detail: `Two operations are **concurrent** when neither replica had seen the other's operation when it ran. For a set, an add and a remove of the same element can be concurrent, and the data type must decide the result. **Add-wins** keeps the element; remove-wins deletes it.

The OR-Set is add-wins by construction: the remove can only tombstone tags it has seen, and the concurrent add's tag is new. A 2P-Set behaves as remove-wins, and permanently.

Add-wins suits shopping carts and collaborative lists, where silently dropping something a user just added is worse than showing something another user just removed.`,
    related: ['OR-Set', 'Concurrent writes'],
  },
  {
    term: 'Unique tags',
    aliases: ['dots', 'unique tag'],
    summary: 'Identifiers that make each add distinguishable, usually a node id plus a counter, so removes can target exactly the adds they saw.',
    detail: `A **unique tag** identifies one update, typically as (node id, sequence number), such as \`n2:7\`. Because each node increments its own counter, no two nodes can create the same tag without coordination.

Tags let a CRDT tell apart two adds of the same element. That is what allows an OR-Set remove to delete the adds it observed while leaving a concurrent add untouched. A new tag must be created on every add, even when the element is already present.

In the literature a (node, counter) pair is called a **dot**; dotted version vectors use them to track causality compactly.`,
    related: ['OR-Set', 'Version vectors'],
  },
  {
    term: 'LWW register',
    aliases: ['last-writer-wins register', 'LWW-register'],
    summary: 'A replicated single value where every write is tagged and the write with the largest tag wins everywhere.',
    detail: `A **last-writer-wins register** stores one value plus a tag, usually (timestamp, node id). Merging two replicas keeps the value with the larger tag. Because every replica compares tags the same way, they all pick the same winner.

The timestamp should respect causality: a write made after seeing another must get a larger tag. A Lamport clock guarantees this; wall clocks only do if they are well synchronised. Equal timestamps must be broken deterministically, usually by node id.

LWW is simple and compact, but concurrent writes are lost: two clients are both told their write succeeded, and one value silently disappears. Cassandra resolves every cell this way.`,
    related: ['Lamport clock', 'Tie-breaking', 'MV register'],
  },
  {
    term: 'Lamport clock',
    summary: 'A counter per node that is bumped on every event and raised to any larger value received, so effects get larger stamps than their causes.',
    detail: `Each node keeps an integer. On a local event it increments it and stamps the event with the new value; on receiving a stamp it sets its counter to the maximum of the two. If event A could have influenced B, A's stamp is smaller than B's.

In this track it orders writes to an LWW register: a node that has merged a value stamped 41 stamps its next write 42 or more, so the new write wins.

The converse does not hold: a smaller stamp does not mean "happened before". Two concurrent writes can get any stamps, including equal ones, so a tie-break is needed, and detecting concurrency needs version vectors.`,
    related: ['LWW register', 'Version vectors'],
  },
  {
    term: 'Tie-breaking',
    aliases: ['tie-break', 'tie breaking'],
    summary: 'A fixed rule, such as comparing node ids, that orders two tags with equal timestamps so every replica picks the same winner.',
    detail: `A **tie-break** turns a partial order into a total one. With LWW tags (timestamp, node id), two writes with the same timestamp are ordered by node id.

Without it, each replica keeps whichever of the two equal-stamp values it had first, and replicas on opposite sides of a healed partition disagree forever. With Lamport clocks, ties are common: two nodes that have seen the same history and then each write once produce the same timestamp.

Any rule works as long as it is deterministic and every replica uses the same one; it does not need to be fair.`,
    related: ['LWW register', 'Lamport clock'],
  },
  {
    term: 'MV register',
    aliases: ['multi-value register', 'MV-register'],
    summary: 'A replicated value that keeps all concurrent writes as siblings, until a write that has seen them replaces them all.',
    detail: `A **multi-value register** stores a set of versions, each with a version vector. A write replaces every version its replica holds, with a vector that dominates all of theirs. Merging keeps every version that no other version dominates.

If n1 writes "a" and n2 writes "b" without seeing each other, neither vector dominates and a read returns ["a", "b"]. When n3 later reads both and writes "c", its vector dominates both and every replica converges on ["c"].

Nothing acknowledged is silently lost, but readers must cope with several values. Dynamo's shopping cart and Riak's siblings work this way.`,
    related: ['Version vectors', 'Siblings', 'LWW register'],
  },
  {
    term: 'Version vectors',
    aliases: ['version vector'],
    summary: 'A map from node to count recording which updates a version has seen; comparing two tells whether one supersedes the other or they are concurrent.',
    detail: `A **version vector** maps each node to the number of that node's updates a version reflects. X **dominates** Y when every entry of X is at least Y's and one is larger: X has seen everything Y has. If neither dominates, the versions are concurrent.

Example: {n1: 2, n2: 1} dominates {n1: 1, n2: 1}; {n1: 2} and {n2: 1} are concurrent. A write that has seen both produces {n1: 2, n2: 1, n3: 1}.

Vectors grow with the number of writers. Systems bound them by tracking servers rather than clients, pruning old entries, or using dotted version vectors.`,
    related: ['MV register', 'Siblings', 'Lamport clock'],
  },
  {
    term: 'Siblings',
    aliases: ['sibling'],
    summary: 'The several values a multi-value register returns when writes were concurrent and none has yet replaced the others.',
    detail: `**Siblings** are concurrent versions of one key that are all kept. A read returns them together, and the client (or application code) decides what to do, typically by merging them and writing back the result, which replaces them.

Example: during a partition one client sets a document title to "Draft" and another to "Final draft". After the heal both are returned; an editor shows both and the user picks one.

Unresolved siblings accumulate if clients never write back, which is why Riak warns about "sibling explosion".`,
    related: ['MV register', 'Concurrent writes'],
  },
  {
    term: 'Concurrent writes',
    aliases: ['concurrent updates', 'concurrency'],
    summary: 'Writes where neither writer had seen the other’s write; there is no "later" one, so the data type must decide what to keep.',
    detail: `Two writes are **concurrent** when neither happened-before the other: the node making each one had not received the other. Wall-clock time is irrelevant; writes a second apart on opposite sides of a partition are concurrent.

A replicated data type needs a rule for them. LWW picks one by tag and drops the other; an MV register keeps both; an OR-Set lets the add win over a remove; a counter adds both.

Detecting concurrency requires causal metadata such as version vectors. Timestamps alone always produce an order, hiding the conflict.`,
    related: ['Version vectors', 'Add-wins', 'Siblings'],
  },
  {
    term: 'OR-Map',
    aliases: ['observed-remove map', 'map CRDT'],
    summary: 'A replicated map whose keys behave like an OR-Set and whose values are themselves CRDTs, merged key by key.',
    detail: `An **OR-Map** maps keys to embedded CRDTs: counters, registers, sets, or other maps. Keys are added and removed with observed-remove semantics, and two maps merge by merging the values under each key.

A shopping cart maps items to counters. Concurrent +2 and +3 on "apples" give 5; a remove concurrent with an add leaves the item with only the added quantity.

The hard part is the remove: hiding the key is not enough, because the embedded value still holds its old state. The remove must reset exactly the part of the value it observed. Riak's map data type is an OR-Map.`,
    related: ['Nested CRDTs', 'Observed reset', 'OR-Set'],
  },
  {
    term: 'Nested CRDTs',
    aliases: ['nested CRDT', 'composition'],
    summary: 'CRDTs built by placing CRDTs inside others, such as counters inside a map; the merge recurses into each component.',
    detail: `A CRDT made of CRDTs is still a CRDT if it is merged component by component: the merge of the whole is the merge of each part, and each part's merge is commutative, associative and idempotent.

The PN-Counter is two G-Counters; an OR-Set is a set of tags plus a set of tombstones; a cart is a map of counters with observed-remove keys.

Composition is not automatic for operations that cross levels. Removing a key from a map has to affect the nested value, which needs the observed-reset idea.`,
    related: ['OR-Map', 'Observed reset'],
  },
  {
    term: 'Observed reset',
    aliases: ['reset-remove', 'observed-remove reset'],
    summary: 'Removing a map key by clearing exactly the updates to its nested value that the remover had seen, keeping concurrent ones.',
    detail: `With **observed reset**, a remove records which updates to the nested value it saw, and those are cleared on every replica; updates it could not have seen survive.

One way: tag every quantity change, and tombstone the tags on remove. Another: keep a PN-Counter and, on remove, record the per-node counter values seen as a baseline, merged by max; the quantity is the counter minus the baseline.

Without it, a removed item reappears with its old quantity when re-added, or a concurrent add brings back the removed quantity.`,
    related: ['OR-Map', 'Nested CRDTs', 'OR-Set'],
  },
  {
    term: '2P-Set',
    aliases: ['two-phase set'],
    summary: 'A set made of two grow-only sets, adds and removes; once an element is removed it can never be added again.',
    detail: `A **two-phase set** keeps an add set and a remove set, both grow-only. An element is present if it is in the add set and not in the remove set.

Removes stick because the remove set only grows, but forever: re-adding a removed element has no effect, and a remove beats any concurrent add. That makes it suitable only where identifiers are never reused.

The OR-Set fixes both problems by tombstoning individual add-tags instead of elements.`,
    related: ['OR-Set', 'Tombstones'],
  },
];
