## The problem

Three nodes count likes on a post. Any node can take a like, and every node should eventually report the same total. The obvious design: each node keeps a number, and when it takes a like it adds to its number and tells the others "add 1".

A client likes the post on `n1`. `n1` goes from 41 to 42 and sends "add 1" to `n2` and `n3`. The message to `n3` is lost. `n3` reports 41 forever: the like has vanished from one replica, and nothing will ever bring it back.

So `n1` retries until it gets an acknowledgement. `n2` receives "add 1", adds it, and acknowledges; the acknowledgement is lost. `n1` retries, and `n2` adds 1 again. `n2` now reports 43 forever. The like has been counted twice.

Both failures come from the same place: "add 1" is not idempotent, and over a network that loses messages you can only choose between losing it and duplicating it. You could attach request ids and deduplicate, as the messaging track does, but then every node must remember every id it has ever seen from every other node, forever. There is a better way: stop sending changes and send state, and design that state so that combining two copies is always safe.

## The core idea

### Send state, not operations

Instead of telling peers what *happened* ("add 1"), each node periodically tells them what it *knows* (its whole counter state). The receiver combines the incoming state with its own using a **merge function**. If merging has the right properties, it does not matter whether a state message is lost (the next one carries the same information and more), duplicated (merging the same state twice changes nothing), or arrives out of order (an older state cannot undo a newer one).

What properties? Write `merge(a, b)` as a ⊔ b. It must be:

- **commutative**: a ⊔ b = b ⊔ a, so the order in which two nodes' states are combined does not matter;
- **associative**: (a ⊔ b) ⊔ c = a ⊔ (b ⊔ c), so the grouping of merges, the route the information takes through the cluster, does not matter;
- **idempotent**: a ⊔ a = a, so merging something you already have changes nothing.

A replicated data type whose states merge this way is a **CRDT**, a conflict-free replicated data type. (This is the *state-based* family. There is also an *operation-based* family that sends operations but requires the network layer to deliver each one exactly once, which is the thing we are trying to avoid.) Any node can update its copy without asking anyone, so CRDTs are fully available during partitions, and they still converge.

### Why one number cannot merge

Try to make the plain counter a CRDT. Suppose `n1` and `n2` both start at 10, and during a partition each takes one like: both now hold 11. The partition heals and they exchange states. What is 11 ⊔ 11?

- If merge is **max**, the result is 11. One like is lost.
- If merge is **sum**, the result is 22. Ten likes were invented, and merging again would make it 33: sum is not idempotent.

No function of two numbers can work, because the numbers do not contain the information needed. 11 and 11 could be "the same like seen twice" or "two different likes". The state must record *where* each increment happened.

### The grow-only counter

The **G-Counter** (grow-only counter) keeps one entry per node: how much has been added *at that node*. A node only ever increases its own entry. The counter's value is the sum of all entries, and merge takes the maximum of each entry separately.

<figure>
<svg viewBox="0 0 600 240" role="img" aria-label="n1 holds n1:3 n2:0 n3:0 and n2 holds n1:3 n2:2 n3:0; n1's newer state n1:7 n2:0 n3:0 merges entry-wise with n2's to give n1:7 n2:2 n3:0">
  <g style="font: 13px var(--sans); fill: var(--ink)">
    <text x="30" y="24">n1's table</text><text x="230" y="24">n2's table</text><text x="430" y="24">merge = max per entry</text>
  </g>
  <g style="fill: var(--surface); stroke: var(--line)">
    <rect x="30" y="36" width="150" height="96" rx="6"/><rect x="230" y="36" width="150" height="96" rx="6"/>
  </g>
  <rect x="430" y="36" width="150" height="96" rx="6" style="fill: var(--accent-soft); stroke: var(--accent)"/>
  <g style="font: 13px var(--mono); fill: var(--ink)">
    <text x="46" y="62">n1: 7</text><text x="46" y="88">n2: 0</text><text x="46" y="114">n3: 0</text>
    <text x="246" y="62">n1: 3</text><text x="246" y="88">n2: 2</text><text x="246" y="114">n3: 0</text>
    <text x="446" y="62">n1: 7</text><text x="446" y="88">n2: 2</text><text x="446" y="114">n3: 0</text>
  </g>
  <g style="font: 12px var(--sans); fill: var(--muted)">
    <text x="110" y="62">newer</text><text x="310" y="88">newer</text>
    <text x="512" y="62">from n1</text><text x="512" y="88">from n2</text>
  </g>
  <g style="font: 13px var(--mono); fill: var(--ink)">
    <text x="30" y="156">value 7</text><text x="230" y="156">value 5</text><text x="430" y="156">value 9</text>
  </g>
  <text x="196" y="90" style="font: 18px var(--sans); fill: var(--muted)">⊔</text>
  <text x="396" y="90" style="font: 18px var(--sans); fill: var(--muted)">=</text>
  <text x="30" y="196" style="font: 12px var(--sans); fill: var(--ink)">Each entry is owned by one node and only grows, so the larger value is always the newer one.</text>
  <text x="30" y="216" style="font: 12px var(--sans); fill: var(--muted)">Merging again, or merging n2's older copy of n1's entry (3), changes nothing.</text>
</svg>
<figcaption>A G-Counter merge. Per-node entries make every increment attributable, so max loses nothing and counts nothing twice.</figcaption>
</figure>

Why max works now: entry `n1` is written only by `n1`, and only upwards. Any copy of entry `n1` elsewhere in the cluster is a value that `n1`'s entry held at some point. The larger of two copies is therefore the more recent one, and taking it loses nothing. Max is commutative, associative and idempotent, and doing it per entry keeps all three properties.

Back to the partition, with per-node entries. If `n1` had taken 6 likes and `n2` 4 before the split, both hold `{n1: 6, n2: 4}` (value 10). During the partition `n1` takes one like, `{n1: 7, n2: 4}`, and `n2` takes one, `{n1: 6, n2: 5}`. Both report 11. After healing, either merge order gives `{n1: 7, n2: 5}`: 12. Correct.

### Monotonicity

The property that made max safe has a name. A state is **monotonic** if it only moves "upwards" in some order and never back. For a G-Counter the order is entry-wise: state a is below state b if every entry of a is ≤ the matching entry of b. Every local update moves a node's state up (one entry increases), and every merge moves it up (the result is ≥ both inputs). The merge result is in fact the *smallest* state above both inputs, its least upper bound. A set of states with such a merge is called a join-semilattice, and it is the mathematical shape of every state-based CRDT.

Monotonicity has a visible benefit: since the state only grows and the value is the sum of entries, the value a node reports never decreases. A client that reads 42 from a node will never later read 41 from the same node.

### Eventual consistency and anti-entropy

The guarantee CRDTs give is **eventual consistency**: if updates stop, all replicas eventually converge to the same value. While updates are still flowing, or the network is partitioned, different replicas can report different values; each one is correct for what it has heard so far. CRDTs give a stronger form, **strong eventual consistency**: any two replicas that have received the same set of updates, by whatever route and in whatever order, are in the same state. No extra round of conflict resolution is needed.

How do states spread? The simplest way is **anti-entropy**: every node periodically sends its whole state to its peers, whether or not anything changed. In the lab's reference design every node sends its table to every peer every 150 ms, with a plain fire-and-forget send. A lost message does not matter: the next round carries the same information. A duplicate does not matter: merge is idempotent. No acknowledgements, no retry queues, no request ids.

### When values must go down

Allow negative deltas and the G-Counter breaks. Suppose `n1`'s entry is 5 and it takes a delta of −2, setting its entry to 3. When `n2`, which holds the old copy 5, merges `n1`'s new state, max picks 5. The decrement is discarded. The problem is that the entry no longer only grows, so "larger" no longer means "newer". Monotonicity was not a detail; it was the whole reason max worked.

The fix is to rebuild "up and down" from parts that only go up. The **PN-Counter** keeps two G-Counters: one table `P` of everything ever added at each node, and one table `N` of everything ever subtracted. A delta of +3 at `n1` increases `P[n1]` by 3; a delta of −2 increases `N[n1]` by 2. Both tables only grow, both merge with per-entry max, and the value is sum(P) − sum(N).

<figure>
<svg viewBox="0 0 600 220" role="img" aria-label="Single table: n1 entry goes 5 to 3, merge with old copy 5 picks 5 and loses the decrement. Two tables: P stays 5, N goes 0 to 2, merges keep both">
  <text x="20" y="22" style="font: 13px var(--sans); fill: var(--ink)">One table: entry n1 after −2</text>
  <text x="320" y="22" style="font: 13px var(--sans); fill: var(--ink)">Two tables: P (adds) and N (subtracts)</text>
  <g style="font: 13px var(--mono); fill: var(--ink)">
    <text x="20" y="60">n1 now: 3</text><text x="20" y="90">n2's copy: 5</text>
    <text x="320" y="60">n1 now: P 5, N 2</text><text x="320" y="90">n2's copy: P 5, N 0</text>
  </g>
  <line x1="20" y1="106" x2="260" y2="106" style="stroke: var(--line)"/>
  <line x1="320" y1="106" x2="580" y2="106" style="stroke: var(--line)"/>
  <g style="font: 13px var(--mono)">
    <text x="20" y="132" style="fill: var(--bad)">max → 5   (−2 lost)</text>
    <text x="320" y="132" style="fill: var(--good)">max → P 5, N 2</text>
    <text x="320" y="156" style="fill: var(--good)">value 5 − 2 = 3</text>
  </g>
  <g style="font: 12px var(--sans); fill: var(--muted)">
    <text x="20" y="190">The newer value is the smaller one, and max cannot know that.</text>
    <text x="320" y="190">Both tables only grow, so max is safe again.</text>
  </g>
</svg>
<figcaption>A PN-Counter is two G-Counters. Decrements become increments of a second table, restoring monotonicity.</figcaption>
</figure>

This is **CRDT composition**: a structure built from CRDTs, merged part by part, is itself a CRDT. The pair `(P, N)` merges as `(P₁ ⊔ P₂, N₁ ⊔ N₂)`, and because each part's merge is commutative, associative and idempotent, so is the pair's. Larger CRDTs are built the same way: maps whose values are counters, sets made of a grow-only set of additions and a grow-only set of removals, and so on.

> A merge that is commutative, associative and idempotent makes the order, grouping and duplication of messages irrelevant. Lost messages are repaired by the next exchange, and replicas converge.

## A worked example

Three nodes run a G-Counter with anti-entropy every 150 ms: each node sends its table to both peers. Tables are written `[n1, n2, n3]`. Between t = 200 and t = 1,000 ms the network is partitioned into `{n1}` and `{n2, n3}`. Independently of the partition, some messages are lost.

| t (ms) | Event | n1 | n2 | n3 |
|---|---|---|---|---|
| 0 | start | [0,0,0] | [0,0,0] | [0,0,0] |
| 100 | add 3 at n1 | [3,0,0] | | |
| 150 | gossip: n1 → n2 arrives; n1 → n3 **lost** | | [3,0,0] | [0,0,0] |
| 200 | **partition** {n1} \| {n2, n3} | | | |
| 250 | add 2 at n2 | | [3,2,0] | |
| 300 | gossip: n2 → n3 arrives (n3 learns n1's 3 via n2); all messages to and from n1 lost | | | [3,2,0] |
| 400 | add 4 at n1; add 1 at n3 | [7,0,0] | | [3,2,1] |
| 450 | gossip: n3 → n2 arrives | | [3,2,1] | |
| 500 | reads: n1 = 7, n2 = 6, n3 = 6 | 7 | 6 | 6 |
| 1,000 | **network heals** | | | |
| 1,050 | gossip: n1 → n2 arrives; n2 → n1 arrives; n3 → n1 arrives (older copy of n1's entry, ignored by max); n1 → n3 **lost**; n2 → n3 left before n2's merge and carries nothing new | [7,2,1] | [7,2,1] | [3,2,1] |
| 1,200 | gossip: n2 → n3 arrives; everything else is a duplicate | [7,2,1] | [7,2,1] | [7,2,1] |
| 1,250 | reads: every node = 10 | 10 | 10 | 10 |

Ten was added in total (3 + 2 + 4 + 1), and every node reports 10 once the network settles. Compare with sending deltas:

- At t = 150 the "add 3" to n3 was lost. With deltas, n3 would be short by 3 forever, unless n1 retried. With state, n3 learned n1's entry at t = 300 *from n2*, a node that had nothing to do with the original add. Information routes around losses.
- At t = 500 the replicas disagree, and each is right for what it has heard. n1 cannot know about n2's and n3's adds during the partition, but it can keep accepting its own.
- At t = 1,050 n1 merged an old state from n3 that still said `n1: 3`. With max, the stale value lost to n1's own 7. A stale or reordered message cannot undo progress.
- Many messages after t = 1,200 carry nothing new. They are merged and change nothing. No acknowledgements were needed anywhere.

## Why it is correct

The guarantee: **once updates stop and the network lets messages through, every node reports the exact total of all acknowledged adds, and no node's reads ever go backwards** (for the G-Counter).

Fix one entry, say entry `n1`. Let *T* be the true total added at `n1`. Three facts hold at all times:

1. **Every copy of entry `n1`, anywhere, is a value that `n1`'s own entry held at some point**, and is therefore ≤ *T*. Only `n1` creates new values for this entry; everyone else only copies them, directly or through other nodes, and the max of past values is a past value.
2. **`n1`'s own copy equals *T*.** `n1` adds every delta exactly once, when it handles the client request. Merging cannot lower its copy (max), and by fact 1 cannot raise it.
3. **Copies only increase.** Merge takes the max; no step decreases an entry.

So the sum on any node is at most the true total, which rules out double counting, and it is non-decreasing, which rules out reads going backwards. For convergence: after updates stop, `n1` keeps sending a state containing *T* to each peer every round. If links are fair-lossy (a message sent repeatedly eventually gets through) and partitions eventually heal, every peer eventually merges a state with entry `n1` = *T*, and by fact 3 keeps it. Applying this to every entry, every node eventually holds the true value in every entry, and reports the true total.

Notice what was *not* needed: no assumption about message order, no deduplication, no acknowledgements. Those are exactly what the merge properties make unnecessary. The PN-Counter's argument is the same argument applied separately to each of its two tables.

The assumptions that are needed:

- **Each node is the only writer of its own entry**, and node ids are unique. Two nodes writing the same entry would hide each other's increments.
- **A node never loses its own entry.** If `n1` crashes and restarts with an empty table while peers hold `n1: 7`, and then takes an add of 2 before it hears from anyone, its entry is 2. The next merge takes max(2, 7) = 7 and the 2 is gone for good. Fact 2 above fails. A node must persist its own entry, or recover it from peers before accepting new adds.
- **Messages eventually get through.** A permanent partition stops convergence, as it must for any protocol.

## Trade-offs and failure modes

**State size and bandwidth.** A G-Counter carries one entry per node that has ever written to it, and anti-entropy ships the whole state every round. For three nodes that is nothing; for a counter written by thousands of clients, or a set with millions of elements, full-state gossip is too expensive. Production systems use **delta-state CRDTs**, which send only the part of the state that changed since the last acknowledged exchange (still merged with the same function, so still safe to duplicate), or compare digests first and send only what differs.

**No global invariants.** A CRDT can enforce nothing that depends on all replicas at once. A PN-Counter used as a stock level cannot guarantee "never below zero": with 1 item left, two replicas on opposite sides of a partition can each sell it. Each replica sees a valid local state, and the merged state is −1. Invariants like this need coordination, or an escrow design that splits the allowed budget among replicas in advance.

**Stale reads.** Eventual consistency only promises convergence once updates stop. A read during a partition or before the next gossip round can be behind. A user who adds on one node and immediately reads from another may not see their own add.

**Churn and garbage.** Entries for nodes that have left the cluster stay in every table forever unless the cluster agrees to fold them in, which needs coordination. Sets with removals need tombstones or more elaborate designs to stay small.

**Not every type has a natural CRDT.** Counters and sets merge well. Data whose concurrent updates have no meaningful automatic merge (two different values written to the same field) needs a policy: last-writer-wins by a total order, which discards one write, or multi-value registers that keep both and leave the choice to the application.

## In real systems

**Riak.** Riak offers built-in convergent data types: counters (a PN-Counter), sets, maps, registers and flags. Applications update them without reading first, and Riak merges replicas' states during read repair and anti-entropy. Maps are a direct use of composition: each field is itself a CRDT, merged field by field.

**Redis Enterprise Active-Active.** Redis Enterprise's Active-Active databases replicate across geographically separate clusters that all accept writes. They are built on CRDTs: counters, sets and other Redis types have conflict-free semantics, so a write made in one region during a partition merges with writes made elsewhere instead of being overwritten.

**Akka Distributed Data.** Akka's Distributed Data module provides CRDTs such as `GCounter`, `PNCounter`, `GSet`, `ORSet` and `LWWMap` for sharing state among the nodes of an Akka cluster. Updates are applied locally and spread by gossip, with the option of waiting for a chosen number of replicas when the caller needs stronger guarantees.

## Common misconceptions

- **"Merge by adding states."** Adding is not idempotent: every repeated gossip message would add the same increments again, and the value would grow without bound. Merge must be safe to repeat.
- **"Max of a single number is enough."** Two replicas that each added 1 to 10 both hold 11; max gives 11 and loses one. Per-node entries are what make max correct.
- **"CRDTs are just last-writer-wins."** Last-writer-wins discards all but one concurrent write. A G-Counter keeps every increment from every replica; the merge combines information instead of choosing between versions.
- **"Eventually consistent means the data may be wrong."** It means replicas may be *behind*. A CRDT replica never reports a value that cannot be explained by some subset of the real updates, and all replicas agree once they have seen the same updates.
- **"A PN-Counter can model a bank balance."** It can track one, but it cannot prevent overdrafts, because preventing them requires knowing what other replicas have spent.

## How the tasks build it

[CR-1 Grow-only counter](#/c/g-counter) starts from a counter that sends deltas to its peers and asks for one that converges. Its three tests are a healthy network, a partition during which adds keep arriving on both sides, and a five-node network that loses 40% of messages. Every add must be acknowledged at once, and once the network settles every node must report the exact total. For grow-only counters the tests also check that a node's reads never go backwards and that nothing is ever counted twice, so both delta-based shortcuts (no retries, or retries without deduplication) fail.

[CR-2 Counter with decrements](#/c/pn-counter) allows negative deltas. The starter is a working grow-only counter, and it fails even on a healthy network, which is the point: the merge relied on monotonicity, and decrements break it. The same three scenarios run again, with every node required to converge on the exact total. It exercises CRDT composition: building a counter that goes down out of parts that only go up.

## Further reading

- [Shapiro, Preguiça, Baquero and Zawirski, A comprehensive study of Convergent and Commutative Replicated Data Types (2011)](https://inria.hal.science/inria-00555588) — the reference catalogue: G-Counter, PN-Counter, sets, registers, graphs, with proofs.
- [crdt.tech](https://crdt.tech/) — a curated collection of CRDT papers, talks and implementations maintained by researchers in the field.
- [Kleppmann, CRDTs: The Hard Parts (talk)](https://martin.kleppmann.com/2020/07/06/crdt-hard-parts-hydra.html) — where simple CRDTs stop being simple: text editing, moves and metadata growth.
- [Almeida, Shoker and Baquero, Delta State Replicated Data Types (2016)](https://arxiv.org/abs/1603.01529) — how to keep the safety of state-based merging while sending only small deltas.
- [Akka Distributed Data documentation](https://doc.akka.io/docs/akka/current/typed/distributed-data.html) — a production CRDT library, with clear descriptions of each data type and its consistency options.
