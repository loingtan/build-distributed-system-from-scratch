## The problem

Five nodes, `n1` to `n5`, each hold a full copy of a key-value store. There is no leader: a client may send a write to any node. Two simple designs come to mind, and both fail.

**Write to everyone, wait for everyone.** A client sends `put k1 = apple` to `n1`. `n1` stores it and forwards it to the other four, waiting for all of them to confirm. `n5` crashed a minute ago. `n1` waits, and the client waits, until something times out. One dead machine out of five has stopped every write in the cluster.

**Write anywhere, answer at once.** `n1` stores `apple`, replies `put_ok`, and forwards the write in the background. The forwards to `n4` and `n5` are lost in a brief partition. Another client sends `get k1` to `n5`, which answers from its own copy: `null`. The write was acknowledged, and a read made afterwards cannot see it. Worse, if `n1` loses its disk now, `apple` exists only on `n2` and `n3`, and nobody knows which nodes have it.

What is wanted is something in between: wait for *enough* replicas that a later read is guaranteed to find the write, but not so many that one or two failures stop the system. And once some replicas have fallen behind, something has to bring them back up to date. This track builds exactly that, in the style of Amazon's Dynamo and the systems it inspired.

## The core idea

### Leaderless replication and the coordinator

In **leaderless replication** every replica accepts reads and writes, and no node orders writes for the others. The node that receives a client request becomes the **coordinator** for that one request: it forwards the request to the replicas, collects their answers and replies to the client. Every node is a coordinator for some requests and a plain replica for others. The coordinator is also a replica itself, so it applies the request locally and counts its own answer.

Three numbers describe the configuration:

- **N**, the number of replicas of each key (here, every node holds every key, so N is the cluster size);
- **W**, how many replicas must confirm a write before the coordinator answers `put_ok`;
- **R**, how many replicas must answer a read before the coordinator answers `get_ok`.

### Write quorums

A **quorum** is a set of replicas large enough that a decision made by it holds for the whole group. A **write quorum** is any W replicas that have stored a write. The coordinator sends the write to all N replicas at once, not one after another, and answers as soon as W have confirmed. If fewer than W can confirm, for example because the coordinator is on the minority side of a partition, it answers with an error after a timeout:

```json
{ "type": "error", "text": "fewer than 3 replicas acknowledged the write" }
```

W is a dial. W = 1 is fast and loses the write if that one replica dies. W = N blocks every write when any replica is down. A majority, W = ⌊N/2⌋ + 1, survives any minority of failures and keeps working through them.

An error does not undo anything. The replicas that were reached keep the write; a leaderless store has no rollback. The honest reading of an error is "unknown": the write may or may not become visible later.

### Read quorums and overlap

A **read quorum** is the set of R replicas whose answers a read uses. The coordinator asks all replicas, waits for R answers and returns the newest value among them.

Why does this find the last write? Because of one inequality, **W + R > N**. A write quorum and a read quorum together contain W + R members, but there are only N distinct replicas. If W + R > N, at least one replica must be in both. That replica stored the write, and its answer is among the R the read uses.

<figure>
<svg viewBox="0 0 600 200" role="img" aria-label="Five replicas; a write quorum of n1, n2, n3 and a read quorum of n3, n4, n5 overlap in n3">
  <rect x="50" y="40" width="300" height="110" rx="20" style="fill: var(--accent-soft); stroke: var(--accent)"/>
  <rect x="250" y="50" width="300" height="110" rx="20" style="fill: none; stroke: var(--m2); stroke-width: 2; stroke-dasharray: 6 4"/>
  <g style="font: 13px var(--mono); fill: var(--ink)">
    <circle cx="100" cy="100" r="22" style="fill: var(--surface); stroke: var(--line)"/><text x="89" y="105">n1</text>
    <circle cx="190" cy="100" r="22" style="fill: var(--surface); stroke: var(--line)"/><text x="179" y="105">n2</text>
    <circle cx="300" cy="100" r="22" style="fill: var(--surface); stroke: var(--good); stroke-width: 2"/><text x="289" y="105">n3</text>
    <circle cx="410" cy="100" r="22" style="fill: var(--surface); stroke: var(--line)"/><text x="399" y="105">n4</text>
    <circle cx="500" cy="100" r="22" style="fill: var(--surface); stroke: var(--line)"/><text x="489" y="105">n5</text>
  </g>
  <g style="font: 12px var(--sans); fill: var(--muted)">
    <text x="60" y="30">write quorum (W = 3) stored v2</text>
    <text x="360" y="185">read quorum (R = 3)</text>
    <text x="262" y="140" style="fill: var(--good)">in both</text>
  </g>
</svg>
<figcaption>N = 5, W = R = 3. Any three replicas share at least one member with any other three, so the read hears from a replica that has v2.</figcaption>
</figure>

### Versions

The read gets R answers that may disagree. It needs to know which is newest. So every stored value carries a **version**: a pair `(counter, origin)`, where `counter` is a number and `origin` is the node id of the coordinator that created the write. Version A is newer than B if its counter is higher, or the counters are equal and A's origin is greater. The node id makes the order total: two different writes never have equal versions.

Two rules use versions:

- a replica replaces its copy only with a **newer** version;
- a read returns the value with the newest version among its answers.

The first rule is what makes every replication message an **idempotent write**: applying it once or five times gives the same state, and a stale or reordered copy does nothing. That property is what makes it safe to retry, repair and gossip freely later.

Picking the newest version and discarding the rest is **last write wins**: if two clients write concurrently, the one with the higher version survives and the other is silently lost.

### The counter trap: two rounds per write

Where does the coordinator get the counter for a new write? The obvious answer, "one more than the counter in my own copy", is wrong. Suppose `n1` was cut off while the rest of the cluster wrote `k1` three times. Its own copy has counter 1. It numbers the next write 2, stores it on three replicas, and answers `put_ok`. But the replicas that hold counter 4 reject it as older, and the next read returns the value from counter 4. An acknowledged write has vanished.

The fix uses the overlap property again. A `put` runs two rounds:

1. **Read round:** ask the replicas for their version of the key and wait for R answers. Take the highest counter among them.
2. **Write round:** store `(highest + 1, own id)` on all replicas and wait for W confirmations.

The read round is a quorum, so it overlaps the write quorum of the last acknowledged put and sees its counter. The new write therefore gets a higher version than any acknowledged write.

### Read repair

Quorums keep reads correct while some replicas are behind; they do nothing to bring those replicas up to date. A replica that sat out a partition keeps old values, and each of them is one fewer good copy than the operator thinks.

A quorum read has already done the detective work: it compared versions and knows which replicas answered with something older. **Read repair** is the final step: send the newest entry back to those replicas, *with its original version*. This is a copy of an old write, not a new write. Minting a fresh version could bury a newer write that is in flight at the same time.

Two details matter. The client is answered after R replies, but the stale replica is often reply R + 1, so the coordinator keeps listening until every replica has answered or timed out, and repairs then. And the coordinator itself may be the stale one, so it repairs its own copy too.

<figure>
<svg viewBox="0 0 600 240" role="img" aria-label="Coordinator n1 asks four peers; three answers arrive and the client is answered; n5 answers late with an old version and is repaired">
  <g style="font: 13px var(--mono); fill: var(--ink)">
    <text x="10" y="34">n1</text><text x="10" y="84">n2</text><text x="10" y="134">n3</text><text x="10" y="184">n5</text><text x="10" y="224">client</text>
  </g>
  <g style="stroke: var(--line)"><line x1="50" y1="30" x2="590" y2="30"/><line x1="50" y1="80" x2="590" y2="80"/><line x1="50" y1="130" x2="590" y2="130"/><line x1="50" y1="180" x2="590" y2="180"/><line x1="50" y1="220" x2="590" y2="220"/></g>
  <line x1="70" y1="30" x2="110" y2="80" style="stroke: var(--accent); stroke-width: 1.5"/>
  <line x1="70" y1="30" x2="120" y2="130" style="stroke: var(--accent); stroke-width: 1.5"/>
  <line x1="70" y1="30" x2="150" y2="180" style="stroke: var(--accent); stroke-width: 1.5"/>
  <line x1="115" y1="80" x2="160" y2="30" style="stroke: var(--m1); stroke-width: 1.5"/>
  <line x1="125" y1="130" x2="190" y2="30" style="stroke: var(--m1); stroke-width: 1.5"/>
  <text x="140" y="20" style="font: 11px var(--mono); fill: var(--muted)">v4, v4 + own v4: R = 3</text>
  <line x1="200" y1="30" x2="230" y2="220" style="stroke: var(--good); stroke-width: 2"/>
  <text x="236" y="212" style="font: 11px var(--mono); fill: var(--good)">get_ok</text>
  <line x1="155" y1="180" x2="330" y2="30" style="stroke: var(--m2); stroke-width: 1.5"/>
  <text x="250" y="170" style="font: 11px var(--mono); fill: var(--bad)">late: v2</text>
  <line x1="350" y1="30" x2="420" y2="180" style="stroke: var(--good); stroke-width: 2; stroke-dasharray: 5 3"/>
  <text x="430" y="172" style="font: 11px var(--mono); fill: var(--good)">store v4 (repair)</text>
</svg>
<figcaption>The client is answered after R replies. The coordinator keeps listening, sees that n5 is behind, and sends it v4 unchanged.</figcaption>
</figure>

### Anti-entropy and hinted handoff

Read repair only fixes keys that somebody reads. A key written once during a partition and then left alone stays on however many replicas were reachable at the time. Two mechanisms fix that in the background.

**Hinted handoff:** the coordinator remembers which replicas did not acknowledge a write (the hint) and keeps retrying them until they answer. It is quick and cheap, but the hints live on one node and are lost if that node crashes.

**Anti-entropy:** every replica regularly picks a peer and compares what they hold. In the simplest version, each node every 200 ms sends one random peer a map of `{ key: version }`, without values. The peer replies with the entries where it is ahead and the list of keys where the sender is ahead; the sender then ships those. This pairwise random exchange is a form of **gossip**: information spreads through repeated contacts between random pairs, and after a few rounds every node has heard everything.

Both apply what they receive through the same "only if newer" check, so a lost, duplicated or outdated sync message does no harm and needs no retry; the next round covers it. The end state is **convergence**: once writes stop and the network is connected, every replica holds the newest version of every key.

## A worked example

N = 5, W = R = 3. Every rpc between nodes times out after 300 ms. Versions are written `(counter, origin)`.

| Time (ms) | Event | n1 | n2 | n3 | n4 | n5 |
|---|---|---|---|---|---|---|
| 500 | `put k1=apple` via n5. Read round: all report nothing, counter 0. Store (1,n5) everywhere. `put_ok` after 3 acks. | apple (1,n5) | apple (1,n5) | apple (1,n5) | apple (1,n5) | apple (1,n5) |
| 1000 | Partition: {n1, n2} cut off from {n3, n4, n5}. | | | | | |
| 1200 | `put k1=banana` via n4. Read round: n3, n4, n5 report counter 1. Store (2,n4). Acks from n3, n4, n5: `put_ok`. | apple (1) | apple (1) | banana (2,n4) | banana (2,n4) | banana (2,n4) |
| 1400 | `put k1=cherry` via n5. Read round sees 2. Store (3,n5). The store message to n3 is **lost**; acks from n5 and n4 only. With 2 of 3, the coordinator waits until the rpcs to n3, n1 and n2 have timed out, then replies **error**. | apple (1) | apple (1) | banana (2) | cherry (3,n5) | cherry (3,n5) |
| 1500 | `put k1=date` via n1. Read round reaches only n1 and n2: 2 < R. **error** within 300 ms. Nothing stored. | apple (1) | apple (1) | banana (2) | cherry (3) | cherry (3) |
| 2000 | Partition heals. | | | | | |
| 2050 | `put k1=elder` via n1. Read round: n1 (1), n2 (1), n4 (3) are the first three answers. Highest counter 3, so the new version is (4,n1). Store to all; acks from n1, n2, n3: `put_ok`. n4 and n5 also accept: (4,n1) is newer than (3,n5). | elder (4,n1) | elder (4,n1) | elder (4,n1) | elder (4,n1) | elder (4,n1) |

Three things to notice. At 1400 ms the write of `cherry` was reported as an error, yet it was stored on two replicas and briefly became the newest version in the cluster. That is the "unknown" outcome: if `elder` had never been written, a later read or anti-entropy round would have spread `cherry` to everyone. At 1500 ms the minority side refused correctly. At 2050 ms, had `n1` taken its counter from its own copy, it would have written `(2,n1)`. Every replica except `n1` and `n2` would have kept its newer copy. But a replica acknowledges a store whether or not it replaces its copy (that is what makes stores idempotent), so three acknowledgements would still arrive, `n1` would answer `put_ok`, and the next read would return `cherry`: an acknowledged write lost. The read round prevented that.

Now suppose instead that no write happens after the heal, and `n3` holds `banana (2)` while `n4` and `n5` hold `cherry (3)` and `n1`, `n2` hold `apple (1)`. Within a few anti-entropy rounds, each pairwise exchange moves `(3,n5)` one hop further, and all five replicas hold `cherry` without any client reading the key.

## Why it is correct

The assumptions: replicas do not lose their stores (no disk loss), the set of N replicas is fixed, and messages may be lost, delayed, duplicated or reordered. "Last acknowledged write" means a put whose `put_ok` was sent before the operation in question began.

**Any two quorums intersect.** If W + R > N, a set of W replicas and a set of R replicas cannot be disjoint, since together they would need W + R > N distinct members. The same holds for two sets of R (the read round of a put against a read) when 2R > N, which is true for majorities.

**Replicas never go backwards.** A replica replaces its entry only with a strictly newer version. So each replica's version of a key only increases over time.

**A new put gets a higher version than every acknowledged put.** Let P be an acknowledged put with version v, stored on a write quorum of W replicas. A later put's read round hears from R replicas; one of them is in P's write quorum and, because replicas never go backwards, holds v or newer. The new counter is at least v's counter plus one, so the new version is higher than v.

**A get never returns a value older than the last acknowledged put.** The same overlap gives the get an answer of at least v from some replica, and the get returns the newest of its answers.

**Repair and anti-entropy converge.** Every repair and sync message carries an existing entry with its original version, and replicas apply it with "keep the newer". Keeping the maximum is commutative, associative and idempotent, so the result does not depend on order, duplication or loss of individual messages. Anti-entropy keeps comparing random pairs forever; on a connected network the newest version of each key reaches every replica with probability 1.

What this does **not** guarantee is linearizability. While a put is in progress and stored on only some replicas, one get may see the new value and a later get, hitting a different quorum, may see the old one. Making reads linearizable requires the reader to write the value back to a quorum before answering (the ABD algorithm).

## Trade-offs and failure modes

**Latency.** A put takes two round trips (read round, write round) and a get one. Each waits for the R-th or W-th fastest replica, not the slowest, which is the reason to send to all N at once: a slow node does not slow the request.

**Availability is bounded by the quorum.** With majorities, a coordinator on the minority side of a partition refuses everything. Choosing W = 1 or R = 1 makes one operation more available at the expense of the other, and dropping below W + R > N buys availability by giving up the read guarantee. Some systems use **sloppy quorums**: when the home replicas are unreachable, they accept writes on other nodes and hand them back later. That keeps writes available but breaks the overlap argument until the hand-off completes.

**Concurrent writes are lost.** Last-write-wins discards one of two concurrent puts to a key. The two-round put makes acknowledged-then-lost impossible for *sequential* puts, but two puts that overlap in time both get acknowledged and only one survives. Dynamo itself kept both versions with vector clocks and let the application merge.

**Read repair alone leaves cold data under-replicated.** A rarely read key can sit on fewer than W up-to-date replicas for months; one more failure then loses it. Anti-entropy fixes this but costs bandwidth. Sending every version every round is O(keys) per exchange; real systems compare Merkle trees (hash trees over key ranges) so that equal ranges cost one hash.

**Disk loss breaks the argument.** If a replica in a write quorum loses its data and rejoins empty, the overlap no longer guarantees anything for writes it held. Anti-entropy restores them only if another replica still has them.

**Membership changes are dangerous.** The overlap argument assumes everyone agrees on the set of N replicas. If nodes are added or removed and coordinators briefly disagree on the set, two "quorums" may not intersect.

**Deletes need tombstones.** Removing a key from a replica's map lets anti-entropy resurrect it from a peer. A delete must be a write with a version and a "deleted" marker.

## In real systems

**The Dynamo paper** (Amazon, 2007) introduced this combination: a preference list of N replicas per key chosen by consistent hashing, configurable R and W, sloppy quorums with hinted handoff during failures, and Merkle-tree anti-entropy between replicas. It used vector clocks rather than last-write-wins, returning conflicting versions to the application.

**Apache Cassandra** lets each request choose its consistency level: `ONE`, `QUORUM`, `ALL`, `LOCAL_QUORUM` (a majority within one data centre), and others. Reading and writing at `QUORUM` gives W + R > N. Cassandra stores hints for unreachable replicas (hinted handoff), performs read repair, and provides `nodetool repair`, which builds Merkle trees to find and stream differences. Conflicts are resolved per column by write timestamp, last write wins.

**Riak** exposes N (`n_val`), R and W per bucket or per request, plus PR and PW to require primary (non-fallback) replicas. It uses sloppy quorums with hinted handoff by default and offers active anti-entropy based on hash trees. Unlike this track, Riak can keep concurrent writes as siblings instead of applying last-write-wins.

## Common misconceptions

- **"W + R > N makes the store linearizable."** It guarantees that a read sees the last write that *finished* before it began. During a write, different reads can disagree and even go backwards. Linearizable reads need a write-back round.
- **"A failed quorum write was rolled back."** There is no rollback. The replicas that stored it keep it, and read repair or anti-entropy may spread it. An error means "not confirmed", and the client should treat it like a timeout.
- **"The coordinator can number a write from its own copy."** A coordinator that missed writes holds an old counter. Its write is acknowledged and then loses to an older one. The counter must come from a quorum.
- **"Read repair is enough to keep replicas in sync."** It only touches keys that are read. Background anti-entropy is needed for everything else.
- **"Amazon DynamoDB is the Dynamo from the paper."** The DynamoDB service shares the name and goals, but its published design replicates each partition with a leader using Multi-Paxos; it is not a leaderless quorum store.

## How the tasks build it

[QR-1 Quorum writes](#/c/quorum-writes) builds the write path: any node coordinates a put, sends it to all replicas in parallel, counts itself, and answers once W have stored it or with an error in time. It exercises leaderless replication, write quorums and the coordinator role. The tests check that at least W replicas hold every acknowledged value, that puts keep working with two of five nodes partitioned or crashed, and that a coordinator that can reach fewer than W refuses.

[QR-2 Versions and quorum reads](#/c/quorum-reads) adds overwrites, gets and versions. It exercises read quorums, W + R > N and last write wins, and its tests are built to catch the counter trap: nodes that were cut off coordinate reads and writes right after the heal, a three-replica cluster loses one node for a while, and on slow links two puts to each key race through different nodes at once. No get may return a value older than the last acknowledged put.

[QR-3 Read repair](#/c/quorum-read-repair) makes a read leave every replica up to date. It exercises read repair, convergence and idempotent writes. The tests cut off a minority, overwrite every key, heal, read each key once and then inspect every replica directly; with slow links, the stale replicas often answer after the first R, so stopping at R is not enough.

[QR-4 Anti-entropy](#/c/quorum-anti-entropy) removes the dependence on reads. It exercises anti-entropy (or hinted handoff), gossip and convergence. The tests inspect every replica before any get is sent, after a minority partition, after two partitions in a row that cut off different nodes, and on a network that drops one message in ten.

## Further reading

- [Dynamo: Amazon's Highly Available Key-value Store](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf) — DeCandia et al., SOSP 2007. Quorums, hinted handoff, read repair and Merkle-tree anti-entropy in one system.
- [Apache Cassandra: Dynamo architecture](https://cassandra.apache.org/doc/latest/cassandra/architecture/dynamo.html) — how a production system configures replication, consistency levels, hints and repair.
- [Probabilistically Bounded Staleness for Practical Partial Quorums](https://www.vldb.org/pvldb/vol5/p776_peterbailis_vldb2012.pdf) — Bailis et al., VLDB 2012. How stale reads actually are when W + R ≤ N.
- [Sharing Memory Robustly in Message-Passing Systems](https://dl.acm.org/doi/10.1145/200836.200869) — Attiya, Bar-Noy and Dolev. The ABD algorithm: quorum reads and writes made linearizable with a write-back phase.
- [Designing Data-Intensive Applications, chapter 5](https://dataintensive.net/) — Kleppmann on leaderless replication, quorum conditions, sloppy quorums and the limits of last-write-wins.
