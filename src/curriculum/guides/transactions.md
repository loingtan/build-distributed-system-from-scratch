## The problem

Take three replicas of a small key-value store, `n1`, `n2` and `n3`, and one rule that sounds reasonable: when a replica hears about a write from a peer, it applies it. The newest information wins, so to speak.

At time 0 the network splits. `n1` is on one side, `n2` and `n3` on the other. A client talking to `n1` writes `x = 1`. A client talking to `n2` writes `x = 2`. Both replicas answer at once, because the rule for this store is that it never refuses a request. At time 3 seconds the network heals and each side forwards what it accepted. `n1` receives "x = 2" and applies it. `n2` receives "x = 1" and applies it. `n3` receives both, in whichever order the network delivers them.

The result: `n1` holds 2, `n2` holds 1, and `n3` holds one or the other. The replicas have swapped values and still disagree. Nothing will fix this, because there are no more writes to forward. Every future reader gets an answer that depends on which replica it asks.

The same thing happens without a partition. If `n1` writes `x = 1` and `n2` writes `x = 2` within a few milliseconds of each other, the two replication messages cross on the wire. Each replica ends on the value it heard last, and "last" is a property of the network, not of the writes.

There is a second, quieter problem. A client sends a group of operations: read `x`, write `y`, read `y`. It wants them treated as one unit, and it wants the read of `y` to see the write it just made. Even on one machine this needs a definition. On three machines that must answer while they cannot talk to each other, part of that definition has to give.

This track is about both: what a transaction means on one node, and what you can still promise when replicas must always answer.

## The core idea

### Transactions on one node

A **transaction** is a list of reads and writes that a client submits together and that the system executes as a unit. In this course a transaction looks like this:

```json
{ "type": "txn", "txn": [["r", 1, null], ["w", 1, 6], ["r", 1, null]] }
```

Each operation is `[op, key, value]`. A read carries `null` and the node fills in the current value; a write carries the value to store. The reply is the same list with the reads filled in:

```json
{ "type": "txn_ok", "txn": [["r", 1, null], ["w", 1, 6], ["r", 1, 6]] }
```

The first read returned `null` because key 1 had never been written. The second returned 6 because the transaction had just written it.

What should happen when two clients send transactions at the same time? The gold standard is **serializability**: the result must be the same as if the transactions had run one after another, in some order, with no overlap. The simplest way to get it is **serial execution**: actually run them one after another. On a single node whose message handler runs to completion before the next message is looked at, a synchronous handler gives you this for free. No locks, no isolation levels, nothing to configure.

Serial execution is not a toy. Some production databases (VoltDB, and Redis for its scripts) execute transactions on a single thread for exactly this reason: if each transaction is short and touches only memory, one thread is fast and the hardest concurrency bugs disappear.

### Replicas that always answer

Now add replicas, and a requirement: **total availability**. Every node that receives a transaction must answer it, even when it cannot reach any other node. A node may not wait for its peers, ask a leader, or refuse.

This rules out serializability. Suppose the network splits and one side runs "read x, write y = 1" while the other runs "read y, write x = 1". Both start from x = y = 0, so both reads return 0. A serial order would put one transaction first, and then the second would have seen the first one's write. Neither did. No serial order explains what the clients observed. Since each side had to answer without hearing from the other, no protocol could have prevented this. That is the core of the argument that totally available systems cannot be serializable (Bailis and colleagues work it out in detail in the paper on highly available transactions).

So the promise becomes weaker, but still precise:

- a transaction sees its own earlier writes;
- every read returns a value that some client really wrote (or `null`), never something invented;
- once the network heals and writes stop, **all replicas converge**: they hold the same value for every key.

The first two are easy: apply the transaction to the local store and reply. The third needs two mechanisms, one for spreading writes and one for resolving conflicts.

<figure>
<svg viewBox="0 0 600 220" role="img" aria-label="A partition separates n1 from n2 and n3; each side accepts a write to x; after the heal, applying in arrival order leaves the replicas swapped">
  <g style="font: 13px var(--mono); fill: var(--ink)">
    <text x="10" y="44">n1</text><text x="10" y="114">n2</text><text x="10" y="184">n3</text>
  </g>
  <g style="stroke: var(--line)"><line x1="40" y1="40" x2="590" y2="40"/><line x1="40" y1="110" x2="590" y2="110"/><line x1="40" y1="180" x2="590" y2="180"/></g>
  <rect x="120" y="60" width="220" height="30" style="fill: var(--accent-soft); stroke: none"/>
  <text x="150" y="80" style="fill: var(--muted); font: 12px var(--sans)">partition: n1 | n2, n3</text>
  <circle cx="160" cy="40" r="5" style="fill: var(--m1)"/><text x="140" y="28" style="fill: var(--ink); font: 12px var(--mono)">x=1</text>
  <circle cx="220" cy="110" r="5" style="fill: var(--m2)"/><text x="200" y="134" style="fill: var(--ink); font: 12px var(--mono)">x=2</text>
  <line x1="360" y1="40" x2="430" y2="110" style="stroke: var(--m1); stroke-width: 2"/>
  <line x1="360" y1="110" x2="430" y2="40" style="stroke: var(--m2); stroke-width: 2"/>
  <line x1="360" y1="40" x2="450" y2="180" style="stroke: var(--m1); stroke-width: 2"/>
  <line x1="360" y1="110" x2="470" y2="180" style="stroke: var(--m2); stroke-width: 2"/>
  <g style="font: 12px var(--mono); fill: var(--bad)">
    <text x="480" y="44">holds 2</text><text x="480" y="114">holds 1</text><text x="480" y="184">holds 2</text>
  </g>
</svg>
<figcaption>Applying whatever arrives last: after the heal n1 and n2 swap values and the cluster never agrees again.</figcaption>
</figure>

### Spreading writes: anti-entropy

The obvious way to replicate a write is to send it to every peer when it happens. On a network that loses a third of its messages, that leaves most writes missing on some replica. You could add acknowledgements and retries per write, but there is a cruder and more robust approach.

**Anti-entropy** is any background process in which replicas repeatedly exchange or compare their state, so that whatever was missed gets repaired without anyone tracking which message was lost. The simplest version: every 200 milliseconds, each node sends its entire store to every peer, and each peer merges what it receives. A lost message costs nothing; the next round carries the same information. A partition costs nothing either; the first round after the heal carries everything.

Sending the whole store is wasteful for large data sets, and real systems compare digests or hash trees instead. For a store of a few dozen keys it is ideal, because it is impossible to get wrong in the "forgot to resend" sense.

### Resolving conflicts: last-writer-wins

Anti-entropy guarantees that every replica eventually hears about every write. It does not say what to do when two writes to the same key meet. As the first section showed, "keep whichever arrived last" depends on arrival order and does not converge.

The fix is to make the decision depend only on the writes themselves. Give every write a **tag** that all replicas can compare the same way, and keep the write with the larger tag. This rule is called **last-writer-wins** (LWW). The word "last" refers to the tag order, not to wall-clock time or arrival time.

What should the tag be? A wall clock is tempting and wrong: clocks on different machines drift, so a write made later in real time can carry an earlier timestamp. Instead, use a **Lamport timestamp**: each node keeps a counter `clock`.

- Before it executes a transaction, it increments `clock` and tags the transaction's writes with the new value.
- When it receives entries from a peer, it sets `clock = max(clock, incoming timestamp)`.

The receive rule matters. Without it, a node that has been quiet keeps a small counter, and its next write loses to an old write from a busy node, even though it happened after the node saw that write. With the rule, anything a node writes after hearing about a write gets a larger timestamp than that write.

Two writes can still carry the same timestamp, for example when both sides of a partition start from the same clock value. So the tag is the pair **(timestamp, node id)**, compared timestamp first, node id second. Node ids are unique, so no two writes from different nodes ever tie. Writes from the same node in the same transaction share a tag, but they come from one transaction on one node, and the later one in the list simply overwrites the earlier one locally before anyone else sees it.

<figure>
<svg viewBox="0 0 600 200" role="img" aria-label="Each replica keeps a value with a timestamp and node id; merging always keeps the larger pair, whatever the order">
  <g style="font: 13px var(--sans); fill: var(--ink)">
    <rect x="20" y="30" width="160" height="56" rx="6" style="fill: var(--surface); stroke: var(--m1)"/>
    <text x="34" y="52" style="font: 13px var(--mono); fill: var(--ink)">x = 1</text>
    <text x="34" y="74" style="font: 12px var(--mono); fill: var(--muted)">(ts 4, n1)</text>
    <rect x="20" y="120" width="160" height="56" rx="6" style="fill: var(--surface); stroke: var(--m2)"/>
    <text x="34" y="142" style="font: 13px var(--mono); fill: var(--ink)">x = 2</text>
    <text x="34" y="164" style="font: 12px var(--mono); fill: var(--muted)">(ts 4, n2)</text>
    <line x1="180" y1="58" x2="300" y2="100" style="stroke: var(--line); stroke-width: 2"/>
    <line x1="180" y1="148" x2="300" y2="100" style="stroke: var(--line); stroke-width: 2"/>
    <rect x="300" y="72" width="130" height="56" rx="6" style="fill: var(--accent-soft); stroke: var(--accent)"/>
    <text x="316" y="96">compare ts,</text><text x="316" y="116">then node id</text>
    <line x1="430" y1="100" x2="470" y2="100" style="stroke: var(--accent); stroke-width: 2"/>
    <rect x="470" y="72" width="115" height="56" rx="6" style="fill: var(--surface); stroke: var(--good)"/>
    <text x="484" y="96" style="font: 13px var(--mono); fill: var(--ink)">x = 2</text>
    <text x="484" y="116" style="font: 12px var(--mono); fill: var(--muted)">kept everywhere</text>
  </g>
</svg>
<figcaption>Equal timestamps: the node id breaks the tie. Every replica runs the same comparison, so every replica keeps x = 2.</figcaption>
</figure>

### Putting it together

Each replica stores, per key, `{ value, ts, by }`. On a transaction it bumps the clock, runs the operations against its local store (tagging writes), and replies immediately. In the background it periodically sends its store to its peers. On receiving a store, it advances its clock and, for each key, keeps whichever entry has the larger `(ts, by)`.

That is the whole design. It is the same pattern as a state-based CRDT register: a state, and a merge that every replica computes identically.

## A worked example

Three replicas, all clocks at 0, empty stores. Anti-entropy runs every 200 ms. From t = 600 ms to t = 3200 ms the network is split into `{n1}` and `{n2, n3}`.

| Time (ms) | Event | n1 store / clock | n2 store / clock | n3 store / clock |
|---|---|---|---|---|
| 100 | Client asks n1: `[["w","x",5]]`. n1 bumps clock to 1, stores, replies `txn_ok`. | x=5 (1,n1) / 1 | — / 0 | — / 0 |
| 200 | Anti-entropy: n1 sends its store to n2 and n3. The copy to n3 is lost. | x=5 (1,n1) / 1 | x=5 (1,n1) / 1 | — / 0 |
| 400 | Next round: n1 and n2 both send; n3 receives x=5 (1,n1). The lost message is repaired. | x=5 (1,n1) / 1 | x=5 (1,n1) / 1 | x=5 (1,n1) / 1 |
| 600 | Partition starts. | | | |
| 900 | Client asks n1: `[["r","x",null],["w","x",7]]`. Clock 2. Reply `[["r","x",5],["w","x",7]]`. | x=7 (2,n1) / 2 | x=5 (1,n1) / 1 | x=5 (1,n1) / 1 |
| 1000 | Client asks n2: `[["w","x",9],["r","x",null]]`. Clock 2. Reply reads 9: the transaction sees its own write. | x=7 (2,n1) / 2 | x=9 (2,n2) / 2 | x=5 (1,n1) / 1 |
| 1200 | Anti-entropy inside the majority side: n3 learns x=9 (2,n2), clock to 2. Messages to and from n1 are dropped. | x=7 (2,n1) / 2 | x=9 (2,n2) / 2 | x=9 (2,n2) / 2 |
| 2000 | Client asks n3: `[["w","y",1]]`. Clock 3. | x=7 (2,n1), no y / 2 | x=9, y=1 after next round | x=9 (2,n2), y=1 (3,n3) / 3 |
| 3200 | Partition heals. | | | |
| 3400 | n1 receives n2's store: x=9 (2,n2) beats x=7 (2,n1) because n2 > n1; y=1 is new. n2 receives n1's store: x=7 (2,n1) loses, nothing changes. | x=9, y=1 / 3 | x=9, y=1 / 3 | x=9, y=1 / 3 |

From 3400 ms on, every replica answers every read the same way. Note what happened to the transaction at 900 ms: its client was told `txn_ok`, and its write `x = 7` is gone. It lost to a write made at roughly the same time on the other side. That loss is not a bug in the implementation; it is the price of last-writer-wins.

Note also the role of the clock rule at 1200 ms. If n3 had not advanced its clock to 2 when it merged, its write at 2000 ms would have carried timestamp 1, and an older write with timestamp 2 or higher on any key would beat it. For `y` it happens not to matter, but on a shared key it would.

## Why it is correct

There are three guarantees to check, under these assumptions: nodes do not crash and lose their stores, every partition eventually heals, and a message between two connected nodes is delivered with some probability greater than zero (so repeated sends eventually get through).

**A transaction sees its own writes and never reads invented values.** The node executes the transaction's operations in order against its local store, inside one synchronous handler. Nothing else runs in between. A read returns whatever the store holds, which is either `null` or a value some write put there. Merged entries only ever carry values that some node received from a client.

**Every replica eventually learns every write.** Anti-entropy sends each node's full store to every peer every 200 ms, forever. Once the network is connected, each round has a positive chance of delivering, so with probability 1 each peer eventually receives a store containing the entry (or a newer one for the same key). Entries are never removed, only replaced by entries with larger tags, so the information that some write exists is never lost, only superseded.

**All replicas converge on the same value.** For a single key, each replica keeps the entry with the maximum `(ts, by)` among all entries it has seen. The maximum of a set does not depend on the order in which you look at its elements, on how many times you see each one, or on how you group them. In CRDT language, the merge is **commutative, associative and idempotent**. Because every replica eventually sees the same set of entries for each key (previous paragraph), every replica computes the same maximum. Because `by` is unique per node and a node never reuses a `(ts, by)` pair for a different value in a later transaction (its clock strictly increases), the maximum is a single entry, not a tie.

The Lamport receive rule adds a further property that the tests check indirectly: if a node writes a key after it has merged an entry for that key, its new write has a larger timestamp and therefore wins. So the replica's own later writes are not silently lost to earlier writes it already knew about. Writes that are truly concurrent (neither side had seen the other) are ordered arbitrarily but consistently.

What is **not** guaranteed: serializability, or even that a multi-key transaction's writes win or lose together. If transaction A writes x and y with tag (5, n1) and transaction B writes only y with tag (6, n2), the final state has A's x and B's y. Each key is resolved on its own.

## Trade-offs and failure modes

**Lost updates are silent.** Last-writer-wins throws away every concurrent write but one, and the losers' clients were told their transactions succeeded. For a cache or a "last seen" field this is fine. For a shopping cart, a counter or a bank balance it is data loss. The alternatives keep both versions: siblings with version vectors (the client merges them), or data types whose merge combines rather than picks (counters, sets, the CRDTs from earlier in the course).

**Read-modify-write is unsafe.** "Read the counter, write counter + 1" on two sides of a partition produces two writes of the same value, one of which wins. The increment of the other side vanishes. Any logic that reads, computes and writes back is exposed.

**Full-state anti-entropy does not scale.** Every round sends every key to every peer: with K keys and N nodes, about K × N² entries per round. Real systems send digests and exchange only differences, use hash trees to find differences cheaply, or gossip to one random peer per round instead of all.

**Clock rule failures.** Forgetting to advance the clock on merge breaks the "my later write wins" property. Using wall-clock time instead of Lamport time breaks it in a different way: a node whose clock runs fast wins every conflict for as long as it is ahead, and a node whose clock runs slow loses writes it made after seeing others.

**Deletes need tombstones.** If a node deletes a key by removing it from its map, the next anti-entropy round brings the old value back from a peer. A delete must be a write of a special "deleted" value with its own tag, kept long enough for every replica to see it. Garbage-collecting tombstones safely requires knowing that every replica has them.

**Crashes.** This design keeps everything in memory. A replica that crashes and restarts with an empty store recovers its data from peers through anti-entropy, but its clock restarts at 0. Its first writes may then lose to older writes until a merge advances the clock. Persisting the clock, or taking the maximum of everything merged before accepting writes, fixes this.

## In real systems

**Apache Cassandra** stores a write timestamp with every cell (every column value of every row). When replicas disagree, the cell with the higher timestamp wins; this is last-writer-wins at the granularity of a column. The timestamp is normally assigned from the coordinator's or client's wall clock, which is why Cassandra deployments care about clock synchronisation and why "lightweight transactions" (built on Paxos) exist for operations that cannot tolerate lost updates.

**Amazon DynamoDB global tables** replicate a table across regions, and every region accepts writes. When the same item is written concurrently in two regions, the documented conflict resolution is last-writer-wins: one version survives and the other is discarded.

**Riak** lets the operator choose. With last-writer-wins enabled it behaves like the system in this track. With it disabled, replicas keep concurrent versions as **siblings**, tracked with version vectors, and return all of them on read so the application can merge them. Riak also offers CRDT data types (counters, sets, maps) that merge without losing updates.

**Google Spanner** and **CockroachDB** take the other side of the trade. They provide serializable transactions across replicas by running every write through a consensus protocol. The price is availability: a replica cut off from a majority of its group cannot commit writes until the partition heals.

## Common misconceptions

- **"Last-writer-wins keeps the write that happened last."** It keeps the write with the largest tag. With Lamport timestamps, two writes made on opposite sides of a partition are concurrent, and which one "wins" is decided by node id, not by which happened later in real time. With wall-clock tags, clock skew decides.
- **"If every node applies the newest message it receives, replicas converge."** "Newest received" depends on network delivery order, which differs between nodes. Convergence needs a rule that depends only on the writes, so every node computes the same answer.
- **"Total availability means you lose consistency entirely."** You lose serializability, not everything. Read-your-own-writes within a transaction, no invented values and eventual convergence all hold, and some stronger levels (such as read committed and monotonic atomic view) are achievable with total availability too.
- **"Anti-entropy is just retrying failed messages."** Retries track individual messages and need acknowledgements. Anti-entropy compares or ships state, so it also repairs writes nobody knew were missing: messages lost without trace, replicas that were down, writes received before a crash.
- **"A transaction's writes win or lose together under LWW."** Each key is resolved independently. A transaction can win on one key and lose on another to a different concurrent transaction.

## How the tasks build it

[TX-1 Single-node transactions](#/c/txn-single) is the starting point: a key-value store that executes a list of reads and writes in order and returns the list with reads filled in. It exercises the transaction format and serial execution. The tests send 80 transactions from three clients and check that the reply mirrors the request and that every read returns the most recent write, including writes made earlier in the same transaction. The interesting part is noticing why a synchronous handler already makes this serial.

[TX-2 Totally-available transactions](#/c/txn-replicated) puts that store on three and then five replicas that must answer every transaction at once. You need replication that survives lost messages and a conflict rule that picks the same winner on every node. The tests check a healthy network (writes on one node must appear on all), a partition in which both sides keep writing the same keys before the network heals, and a network that drops a third of all messages. After each run settles, every node must hold the same value for every key, and no read may return a value nobody wrote.

## Further reading

- [Highly Available Transactions: Virtues and Limitations](https://www.vldb.org/pvldb/vol7/p181-bailis.pdf) — Bailis et al., VLDB 2014. Which isolation levels can be had with total availability and which provably cannot.
- [Jepsen: Consistency Models](https://jepsen.io/consistency) — a clickable map of consistency and isolation models, with notes on which ones are totally available.
- [Time, Clocks, and the Ordering of Events in a Distributed System](https://lamport.azurewebsites.net/pubs/time-clocks.pdf) — Lamport, 1978. The origin of the logical clocks used for the tags here.
- [Gossip Glomers: Totally-Available Transactions](https://fly.io/dist-sys/6a/) — the Maelstrom challenge this track's message format comes from, with links to the underlying checker.
- [Designing Data-Intensive Applications](https://dataintensive.net/) — Kleppmann, chapters 5 and 7, on conflict resolution, last-write-wins and what isolation levels actually guarantee.
