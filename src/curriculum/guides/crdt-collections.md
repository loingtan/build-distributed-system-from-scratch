## The problem

Two phones share one shopping list. The list is stored on three servers, n1, n2 and n3, and each phone talks to whichever server is closest. The servers copy changes to each other in the background, so a phone never waits for a server on the other side of the world.

Alice's phone tells n1 to add "milk". n1 stores it, answers at once, and sends the change to n2 and n3. The message to n3 is lost. Nobody notices: n1 sent it, n3 never knew it should expect it. Bob's phone, connected to n3, shows a list without milk, and it will keep doing so forever unless something resends the change.

So n1 resends. But how often, and until when? Suppose it keeps sending its whole list every tenth of a second. That fixes lost messages: n3 eventually receives a copy that contains milk and adds it. It does not fix deletes. Bob removes "milk" on n3. n3 deletes it locally. A tenth of a second later n1's periodic copy arrives, still containing milk, and n3 adds it back. Bob's delete has vanished, and no message was lost: every server did exactly what it was told.

Now suppose instead of a list, the servers store Alice's display name. She changes it to "Ada" on n1 while, during a network partition, she changes it to "Ada L." from her laptop through n2. After the partition heals, n1 and n2 each receive the other's value. If each simply keeps the value that arrived last, n1 ends with "Ada L." and n2 ends with "Ada", and they swap again on the next exchange.

These are the questions this track answers: how replicas that accept writes independently can exchange state so that lost and repeated messages do no harm, how a remove can stick without making the element unusable forever, and what to do when two writes really do conflict.

## The core idea

### State that only grows

A **replica** is one copy of the data, held by one node. We want every replica to accept updates on its own and, once they have exchanged everything, to hold the same value. That property is called **convergence**.

The simplest way to get it is to make the state only ever grow, and to merge two states by combining them. For a set this is the **G-Set** (grow-only set): the state is the set of elements, `add` inserts one, and merging two replicas takes their **union**.

Union has three properties that matter:

- **Commutative**: merging A into B gives the same result as B into A.
- **Associative**: merging A then B then C gives the same result in any grouping.
- **Idempotent**: merging the same state twice changes nothing.

A merge with these three properties is called a **join**, and a set of states with such a merge is a **join semilattice**. A replicated data type built this way is a state-based **CRDT** (conflict-free replicated data type). The three properties together mean that the final state depends only on *which* updates a replica has received, not on the order, the grouping or the number of copies.

That makes the network protocol trivial. Every replica periodically sends its whole state to its peers, and each peer merges what it receives. This is **anti-entropy**. A lost message is repaired by the next round. A duplicate or a stale copy merges to nothing new.

<figure>
<svg viewBox="0 0 600 220" role="img" aria-label="n1 adds milk and gossips; the first copy to n3 is lost, the next round delivers it">
  <g style="font: 13px var(--mono); fill: var(--ink)">
    <text x="10" y="44">n1</text><text x="10" y="114">n2</text><text x="10" y="184">n3</text>
  </g>
  <g style="stroke: var(--line)"><line x1="40" y1="40" x2="590" y2="40"/><line x1="40" y1="110" x2="590" y2="110"/><line x1="40" y1="180" x2="590" y2="180"/></g>
  <circle cx="80" cy="40" r="5" style="fill: var(--accent)"/>
  <text x="62" y="26" style="fill: var(--ink); font: 12px var(--mono)">add milk</text>
  <line x1="130" y1="40" x2="190" y2="110" style="stroke: var(--accent); stroke-width: 2"/>
  <line x1="130" y1="40" x2="200" y2="150" style="stroke: var(--bad); stroke-width: 2; stroke-dasharray: 5 4"/>
  <text x="200" y="160" style="fill: var(--bad); font: 15px var(--sans)">×</text>
  <text x="196" y="128" style="fill: var(--muted); font: 11px var(--mono)">{milk}</text>
  <line x1="300" y1="40" x2="360" y2="110" style="stroke: var(--accent); stroke-width: 2"/>
  <line x1="300" y1="40" x2="380" y2="180" style="stroke: var(--accent); stroke-width: 2"/>
  <line x1="320" y1="110" x2="400" y2="180" style="stroke: var(--accent); stroke-width: 2"/>
  <text x="386" y="198" style="fill: var(--muted); font: 11px var(--mono)">{milk} ∪ {milk} = {milk}</text>
  <text x="120" y="212" style="fill: var(--muted); font: 12px var(--sans)">round 1</text>
  <text x="300" y="212" style="fill: var(--muted); font: 12px var(--sans)">round 2: two copies, merged once</text>
</svg>
<figcaption>Anti-entropy with a G-Set. The first copy to n3 is lost; the next round brings two copies, and union makes the duplicate harmless.</figcaption>
</figure>

### Removing with tombstones

Remove is a problem because it makes the state smaller, and union cannot express "smaller". A replica that deletes an element locally is overruled by the next merge from a replica that still has it.

The standard trick is to record removals in a second structure that also only grows. A **tombstone** is a record saying "this was removed". The obvious version keeps a set of removed *elements*: an element is present if it was added and is not tombstoned. This is a **2P-Set** (two-phase set). Removes now stick. But they stick forever: once "milk" is tombstoned, adding milk again does nothing, because the tombstone still matches.

### Removing what you have seen

The fix is to tombstone something finer than the element. Give every add a **unique tag**, such as the node id plus a counter that node increments on each add: `n1:1`, `n1:2`, `n2:1`. The state is now:

- for each element, the set of tags it has been added with;
- a set of tombstoned tags.

An element is present while at least one of its tags is not tombstoned. A `remove` tombstones every tag its replica currently holds for that element: the adds it has **observed**. Both parts only grow, so both merge by union. This is the **OR-Set** (observed-remove set).

Three good things follow:

1. A remove sticks. The tombstones travel with anti-entropy like everything else, and a stale copy of the tag cannot undo them.
2. Re-adding works. A new add creates a new tag that no tombstone mentions.
3. An add **concurrent** with a remove wins. Two operations are concurrent when neither node had seen the other's operation when it ran. The remove cannot tombstone a tag it has never seen, so the concurrent add's tag survives. This rule is called **add-wins**.

There is one subtle requirement. The add must create a new tag even when the element is already present. Otherwise "add milk" on a replica that already shows milk does nothing, and a concurrent remove deletes the only tag.

<figure>
<svg viewBox="0 0 600 230" role="img" aria-label="During a partition n1 removes milk, tombstoning tag n1:1, while n2 adds milk with tag n2:5. After the heal milk is present">
  <g style="font: 13px var(--mono); fill: var(--ink)">
    <text x="10" y="54">n1</text><text x="10" y="164">n2</text>
  </g>
  <g style="stroke: var(--line)"><line x1="40" y1="50" x2="590" y2="50"/><line x1="40" y1="160" x2="590" y2="160"/></g>
  <rect x="150" y="20" width="190" height="170" style="fill: var(--accent-soft); stroke: none; opacity: 0.5"/>
  <text x="200" y="110" style="fill: var(--muted); font: 12px var(--sans)">partition</text>
  <text x="50" y="40" style="fill: var(--muted); font: 11px var(--mono)">milk: {n1:1}</text>
  <text x="50" y="182" style="fill: var(--muted); font: 11px var(--mono)">milk: {n1:1}</text>
  <circle cx="200" cy="50" r="5" style="fill: var(--bad)"/>
  <text x="160" y="72" style="fill: var(--ink); font: 11px var(--mono)">remove: ✝n1:1</text>
  <circle cx="230" cy="160" r="5" style="fill: var(--good)"/>
  <text x="190" y="148" style="fill: var(--ink); font: 11px var(--mono)">add: +n2:5</text>
  <line x1="360" y1="50" x2="420" y2="160" style="stroke: var(--accent); stroke-width: 2"/>
  <line x1="370" y1="160" x2="430" y2="50" style="stroke: var(--accent); stroke-width: 2"/>
  <text x="440" y="40" style="fill: var(--ink); font: 11px var(--mono)">{n1:1, n2:5} ✝{n1:1}</text>
  <text x="440" y="182" style="fill: var(--ink); font: 11px var(--mono)">{n1:1, n2:5} ✝{n1:1}</text>
  <text x="440" y="110" style="fill: var(--good); font: 12px var(--sans)">milk present: n2:5 lives</text>
</svg>
<figcaption>An OR-Set remove tombstones only the tags it has seen. The add made concurrently on the other side has a new tag, so after the heal milk is present everywhere.</figcaption>
</figure>

### Registers: one value per key

A **register** holds a single value. Two concurrent writes cannot both be "the value", so the data type has to choose.

The **LWW register** (last-writer-wins) tags every write with a pair **(timestamp, node id)** and keeps the value with the larger tag. Merging two replicas keeps the larger of their two tags, which is commutative, associative and idempotent.

The timestamp must respect causality: if node B writes after it has received A's value, B's tag must be larger, or B's newer write would lose to the value it meant to replace. A **Lamport clock** guarantees this. Each node keeps an integer; a write uses the integer plus one; every tag received raises the integer to at least that timestamp.

Lamport clocks often produce equal timestamps for concurrent writes: two nodes that have seen the same history and then each write once both pick the same next number. That is why the node id is part of the tag. Comparing node ids on equal timestamps is a **tie-break**: it makes the order total, so every replica picks the same winner.

The **MV register** (multi-value) refuses to choose. It keeps every concurrent value as a **sibling**, and a read returns all of them. To tell "newer" from "concurrent" it uses **version vectors**: for each node, how many of that node's writes this version has seen. Version X **dominates** Y if every entry of X is at least Y's and some entry is larger. A write takes the entry-wise maximum of the vectors of every version it replaces and adds one to its own entry, so it dominates them all. A merge keeps every version that no other version dominates.

### Maps of CRDTs

A shopping cart is a map from item to quantity. The natural design nests CRDTs: an **OR-Map** whose keys behave like an OR-Set and whose values are counters, merged key by key. Two concurrent "+2 apples" and "+3 apples" then give 5, as a counter should.

The difficulty is remove. Hiding the key leaves the counter underneath with its old total, so re-adding the item brings the removed quantity back. The remove must clear exactly the part of the nested value it observed and nothing else, an idea called **observed reset**. The simplest way to get it is to reuse the OR-Set: every add is a tagged quantity change, a remove tombstones the changes it has seen, and the quantity is the sum of the changes still alive.

## A worked example

Three nodes run an OR-Set. Each sends its full state to both peers every 100ms. Tags are written `node:counter`; tombstones are marked ✝.

| Time | Event | n1 | n2 | n3 |
|---|---|---|---|---|
| 0ms | Client adds "milk" at n1 | milk{n1:1} | – | – |
| 100ms | n1 gossips; the copy to n3 is lost | milk{n1:1} | milk{n1:1} | – |
| 200ms | Next round reaches n3 | milk{n1:1} | milk{n1:1} | milk{n1:1} |
| 300ms | Partition: {n1} against {n2, n3} | | | |
| 320ms | Client removes "milk" at n1 | milk{n1:1} ✝n1:1 | milk{n1:1} | milk{n1:1} |
| 330ms | Client adds "milk" at n2 | | milk{n1:1, n2:1} | |
| 340ms | Client adds "eggs" at n3 | | | eggs{n3:1} |
| 400ms | n2 and n3 gossip within their side | (absent) | milk{n1:1, n2:1}, eggs{n3:1} | same as n2 |
| 1300ms | Partition heals | | | |
| 1400ms | Full exchange | milk{n1:1, n2:1}, eggs{n3:1} ✝n1:1 | same | same |

Reads after 1400ms: every node returns `["milk", "eggs"]`. Walk through why:

- At 100ms a message was lost. Nothing tracked it; the next round simply carried the same state again.
- At 320ms n1's remove tombstoned the only tag n1 had seen, `n1:1`. Read at n1 between 320ms and the heal returns `[]`.
- At 330ms n2's add created a fresh tag `n2:1`, although n2 already showed milk.
- After the heal, every replica has tags `{n1:1, n2:1}` for milk and tombstone `n1:1`. Tag `n2:1` is alive, so milk is present: the concurrent add won.

Now continue. At 1500ms a client removes "milk" at n3, which by now holds both tags. n3 tombstones `n1:1` and `n2:1`. After the next round every replica has both tombstones and milk is gone everywhere. Suppose n1 had missed that round because of another lost message: its state still lists milk's tags, but merging n3's state at the round after adds the tombstones, and milk disappears. No sequence of merges can bring it back, because no replica can "un-tombstone" a tag.

For contrast, run the same events with an LWW register on key "list-owner": n1 writes "Ann" and n2 writes "Bo" during the partition, both with Lamport timestamp 4. After the heal both replicas compare (4, n1) and (4, n2), and every replica keeps "Bo". Ann's client was told its write succeeded; the value is gone. An MV register would instead return `["Ann", "Bo"]` until some client writes again.

## Why it is correct

The guarantee is **strong eventual convergence**: any two replicas that have received the same set of updates, directly or through merges, hold the same state, and therefore answer reads the same way. Together with anti-entropy, which ensures that every update eventually reaches every replica once the network allows it, this means all replicas agree after the network settles.

The argument has two parts.

**Merge is a join.** For each data type, the merge must be commutative, associative and idempotent.

- G-Set: union is all three.
- OR-Set: the state is a pair of sets (tags per element, tombstones), merged by union componentwise. A pair of joins is a join.
- LWW register: the merge keeps the larger tag. `max` over a total order is a join; the total order is exactly why the node id tie-break is needed. Without it, (4, n1, "Ann") and (4, n2, "Bo") are incomparable and "keep mine on a tie" is not commutative.
- MV register: the merge takes the union of versions and drops the dominated ones. The result is the set of maximal elements of the union, which does not depend on order or duplication.
- Cart: the OR-Set of tagged quantity changes, with each tag's quantity fixed when the tag is created. Copying a tag twice copies the same number.

**Updates only move up.** Each local update produces a state that is at least as large as before in the semilattice order (a new tag, a new tombstone, a larger LWW tag, a dominating version). So a replica's state is always the join of all updates it has received, and the join of the same set of updates is the same everywhere.

The semantic rules follow from the structure:

- *A remove sticks*: tombstones are never deleted, and presence requires a live tag.
- *Add wins over a concurrent remove*: the remove tombstones only tags present at its replica; a concurrent add's tag was not present there by definition.
- *A causally later LWW write wins*: a node that merged a tag with timestamp t has clock at least t, so its next write has timestamp at least t + 1.
- *MV keeps concurrent writes*: neither version vector dominates the other, so neither is dropped. A later write that saw both has a vector that dominates both, so both are dropped.

Assumptions: tags are unique (node ids are unique and counters never repeat, which needs care after a crash), messages are not corrupted, and the network eventually delivers some message between every pair of replicas. Under a permanent partition the two sides converge separately and stay apart.

> A state-based CRDT does not prevent conflicts. It makes the outcome of every conflict a deterministic function of the set of updates, so every replica computes the same one.

## Trade-offs and failure modes

**Metadata grows.** An OR-Set holds a tag for every add and a tombstone for every removed tag, forever. A list where "milk" is added and removed every week accumulates hundreds of entries for one element. Tombstones can only be dropped once every replica has seen them; dropping one earlier lets a replica that missed it resurrect the element. Version vectors grow with the number of distinct writers.

**Bandwidth.** Full-state anti-entropy sends the whole state every round. That is fine for the small sets in these tasks and wasteful for large ones. Real systems send digests and only exchange the parts that differ, or send recent changes (delta-state CRDTs) and fall back to full state when a peer has been away.

**Lost updates are a choice, not a bug.** LWW discards all but one concurrent write, although every client was told its write succeeded. If the value is a counter, a set or a document, LWW is the wrong type. Wall-clock timestamps make it worse: a node whose clock runs a minute fast wins every conflict for a minute, even against writes made later.

**Siblings push the problem to the application.** An MV register never loses a write, but readers must cope with several values and are expected to write back a resolution. If they do not, siblings pile up.

**No invariants across replicas.** No CRDT can enforce "at most one person holds this seat" or "the quantity never goes below zero": two replicas can each make a locally valid change that is invalid together. Those need coordination, such as a quorum or a leader.

**Semantics may surprise users.** Add-wins means a remove can appear to be undone by an add the user never saw. The cart in the last task keeps an item at quantity zero or below until it is removed. These are deliberate rules; the point is that they are the same on every replica.

## In real systems

**Riak** exposes CRDT data types directly: counters, sets, maps, registers and flags. Its set is an optimised OR-Set (the OR-SWOT) that uses version vectors instead of a tombstone per tag, and its map nests the other types under observed-remove keys, as in the cart task. For ordinary keys without a data type, Riak can return siblings found by version vectors, like an MV register, and expects the client to resolve them.

**Amazon Dynamo**, as described in the 2007 paper, stored shopping carts with vector clocks and returned all concurrent versions to the application, which merged carts by union. The paper notes that a removed item could reappear after such a merge, the very problem the OR-Set solves.

**Cassandra** resolves every column value by last-writer-wins on a client- or server-supplied timestamp, and records deletes as tombstones that are kept for `gc_grace_seconds` (ten days by default) before compaction removes them. A replica that is down longer than that and then rejoins can bring deleted data back.

**Redis Enterprise** (Active-Active databases) uses CRDTs so that several geo-distributed clusters can all accept writes; its documentation describes, for example, add-wins semantics for sets and counter semantics for increments.

## Common misconceptions

- **"Convergence means every replica is always up to date."** It means replicas that have received the same updates agree. During a partition each side sees only its own updates, and reads there are stale.
- **"A CRDT never loses a write."** Which concurrent writes survive depends on the type. An LWW register silently drops all but one; only an MV register or a type with a merge for the values (sets, counters) keeps them all.
- **"Timestamps detect conflicts."** Timestamps always order two writes, which hides the conflict. Detecting that two writes are concurrent takes causal metadata such as version vectors.
- **"A 2P-Set is just an OR-Set with less metadata."** It has different semantics: removed elements can never return, and a remove beats any concurrent add.
- **"You can build any data type by nesting CRDTs."** Nesting merges correctly, but operations that cross levels, such as removing a key whose value is a counter, need explicit design (observed reset) or they produce wrong quantities.

## How the tasks build it

[CS-1 Grow-only set](#/c/crdt-g-set) starts with the simplest join, union, and asks for anti-entropy. The starter sends each add once; its tests lose a quarter of the messages and partition the cluster, so only a design that keeps resending state converges on exactly the acknowledged elements.

[CS-2 Observed-remove set](#/c/crdt-or-set) adds remove. Its scripted tests remove an element at a node that has seen it, remove during a partition while another side still holds the element, re-add after a remove, and race an add against a remove on opposite sides of a partition. A plain delete, a 2P-Set and an add that skips a new tag each fail a different one of these.

[CS-3 Last-writer-wins register](#/c/crdt-lww-register) stores one value per key. Its tests chain writes through different nodes, each after the previous one has spread, and then make both sides of a partition write the same key after the same history, which produces equal Lamport timestamps. Only a total order on tags lets every replica pick the same winner.

[CS-4 Multi-value register](#/c/crdt-mv-register) keeps concurrent writes. Its tests check that writes in sequence end with one value, that writes on both sides of a partition are both kept, that a later write replaces both, and that only the last write of each side survives.

[CS-5 Shopping cart (OR-Map)](#/c/crdt-or-map) nests counters in a map. Its tests check that concurrent increments and decrements all count, that a removed item stays removed, that re-adding starts from zero, and that an add concurrent with a remove keeps only its own quantity.

## Further reading

- [Shapiro, Preguiça, Baquero, Zawirski: A comprehensive study of Convergent and Commutative Replicated Data Types](https://inria.hal.science/inria-00555588) — the catalogue: G-Set, 2P-Set, OR-Set, LWW and MV registers, each with its merge and a proof.
- [DeCandia et al.: Dynamo: Amazon's Highly Available Key-value Store](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf) — the shopping cart, vector clocks and siblings that motivated much of this work.
- [Riak KV documentation: Data types](https://docs.riak.com/riak/kv/latest/developing/data-types/index.html) — how a production database exposes sets, maps, counters and registers to applications.
- [crdt.tech](https://crdt.tech/) — a curated collection of papers, talks and implementations.
- [Bieniusa et al.: An optimized conflict-free replicated set](https://arxiv.org/abs/1210.3368) — how to keep OR-Set semantics without a tombstone for every removed tag.
