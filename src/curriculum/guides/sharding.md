## The problem

Three nodes, n1, n2 and n3, form a key-value store. A client writes `user:7 = 42` through n1, and n1 stores it. A second client reads `user:7` through n2. n2 has never heard of `user:7` and answers `null`. Nothing crashed and no message was lost; the store is simply wrong, because nobody agreed on where `user:7` lives.

One fix is to store every key on every node. Then no node holds more than one node can hold, and every write costs work on every node: adding machines adds no capacity at all. The point of a cluster is that each node holds and serves only part of the data. Splitting the keys between nodes is **partitioning**, also called **sharding**, and each part is a **partition** or **shard**.

Partitioning raises three questions, and this track answers them in order:

1. Given a key, which node owns it? Every node must reach the same answer without asking anyone.
2. When a node joins or leaves, how many keys change owner? With the naive answer to question 1, almost all of them.
3. When ownership changes, how do the values get to their new owners without losing writes, reviving old values, or answering from a node that does not yet have the newest copy?

## The core idea

### Hash partitioning and request routing

A **hash function** turns a string into a number in a fixed range, here 0 to 2^32 − 1. A good one is deterministic (the same key gives the same number on every node, every time) and scatters inputs: `user:1` and `user:2` land far apart. Hashing is not encryption; any fast, well-mixed function such as FNV-1a with a final mixing step will do.

The simplest owner rule is `owner = nodes[hash(key) mod N]`, where `nodes` is the cluster's node list in an order every node shares. Because the hash scatters keys, each node gets close to 1/N of them.

A client may send a request to any node. **Request routing** is what happens next: the receiving node computes the owner and, if it is not the owner, forwards the request and relays the answer.

```text
client → n1   { "type": "get", "key": "user:7" }
n1 → n3       { "type": "get", "key": "user:7" }
n3 → n1       { "type": "get_ok", "value": 42 }
n1 → client   { "type": "get_ok", "value": 42 }
```

Two extra messages per request at most, and only the owner ever touches the stored value. Real systems also route in the client (the client learns the owner map and goes straight there) or in a proxy layer; the owner rule is the same.

### Why `mod N` breaks when the cluster changes

Remove one node of six and N goes from 6 to 5. For a key, `hash mod 6` and `hash mod 5` agree only by coincidence, so about five keys in six get a new owner. In a real store each of those is data to copy across the network, for a change that should have moved one sixth of the data.

### Consistent hashing: the ring

**Consistent hashing** fixes this. Picture the hash range as a circle, the **hash ring**, from 0 round to 2^32, where 2^32 is the same point as 0. Each member node sits on the ring at `hash(nodeId)`. A key sits at `hash(key)` and belongs to the **first node at or after it, going clockwise**, wrapping past the top to the smallest position.

The **membership** is the current list of nodes that own keys. The ring is a function of the membership alone, so every node that knows the same member list computes the same owners. Sort the points by position (breaking ties by id) and the result does not depend on the order in which the list arrived.

<figure>
<svg viewBox="0 0 620 300" role="img" aria-label="A hash ring with three nodes; keys belong to the next node clockwise; when n2 leaves only its keys move to n3">
  <circle cx="150" cy="150" r="110" style="fill: none; stroke: var(--line); stroke-width: 2"/>
  <circle cx="460" cy="150" r="110" style="fill: none; stroke: var(--line); stroke-width: 2"/>
  <g style="fill: var(--accent)"><circle cx="150" cy="40" r="7"/><circle cx="245" cy="205" r="7"/><circle cx="55" cy="205" r="7"/></g>
  <g style="font: 13px var(--mono); fill: var(--ink)"><text x="140" y="26">n1</text><text x="255" y="215">n2</text><text x="22" y="215">n3</text></g>
  <g style="fill: var(--m3)"><circle cx="228" cy="72" r="5"/><circle cx="150" cy="260" r="5"/><circle cx="72" cy="72" r="5"/></g>
  <g style="font: 12px var(--mono); fill: var(--m3)"><text x="236" y="68">k1→n2</text><text x="128" y="282">k2→n3</text><text x="10" y="66">k3→n1</text></g>
  <path d="M 180 46 A 110 110 0 0 1 258 160" style="fill: none; stroke: var(--muted); stroke-width: 1.5; stroke-dasharray: 3 3"/>
  <text x="100" y="155" style="fill: var(--muted); font: 12px var(--sans)">clockwise</text>
  <g style="fill: var(--accent)"><circle cx="460" cy="40" r="7"/><circle cx="365" cy="205" r="7"/></g>
  <circle cx="555" cy="205" r="7" style="fill: none; stroke: var(--bad); stroke-width: 2; stroke-dasharray: 3 2"/>
  <g style="font: 13px var(--mono); fill: var(--ink)"><text x="450" y="26">n1</text><text x="332" y="215">n3</text></g>
  <text x="565" y="215" style="fill: var(--bad); font: 13px var(--mono)">n2 ×</text>
  <g style="fill: var(--m3)"><circle cx="538" cy="72" r="5"/><circle cx="460" cy="260" r="5"/><circle cx="382" cy="72" r="5"/></g>
  <g style="font: 12px var(--mono)"><text x="500" y="62" style="fill: var(--warn)">k1→n3</text><text x="438" y="282" style="fill: var(--m3)">k2→n3</text><text x="330" y="66" style="fill: var(--m3)">k3→n1</text></g>
  <text x="390" y="150" style="fill: var(--muted); font: 12px var(--sans)">only k1 moves</text>
</svg>
<figcaption>Left: each key belongs to the next node clockwise. Right: n2 leaves, and only the keys on its arc (k1) fall to the next node along.</figcaption>
</figure>

Remove a member and only the keys on its arc change owner: they fall to the next member clockwise. Add a member and it takes over part of one neighbour's arc. No other key moves. With N members, a join or leave moves about 1/N of the keys, which is the minimum possible: those keys have to move somewhere.

### Virtual nodes: evening out the load

One point per node gives arcs of whatever length the hash happens to produce. With six random points the largest arc is often several times the smallest, so one node owns 40% of the keys and another 2%. The busiest node limits the whole cluster. And when a node leaves, its whole arc lands on a single neighbour, which may double that neighbour's load.

**Virtual nodes** put each member on the ring many times, at `hash(id + "#" + i)` for i from 0 to V − 1. Every point still stands for its real node. A member's share is now the sum of V small arcs, and a sum of many random pieces is close to its average: the relative spread shrinks roughly as 1/√V. With V = 200 each member's share is typically within a few per cent of fair. When a member leaves, its many small arcs fall to many different neighbours, so the extra load is shared too.

<figure>
<svg viewBox="0 0 620 210" role="img" aria-label="Bar chart: with one point per node shares are very uneven; with 200 points per node they are close to even">
  <g style="font: 13px var(--sans); fill: var(--ink)"><text x="20" y="22">1 point per node</text><text x="330" y="22">200 points per node</text></g>
  <line x1="20" y1="180" x2="290" y2="180" style="stroke: var(--line)"/>
  <line x1="330" y1="180" x2="600" y2="180" style="stroke: var(--line)"/>
  <line x1="20" y1="130" x2="290" y2="130" style="stroke: var(--faint); stroke-dasharray: 4 3"/>
  <line x1="330" y1="130" x2="600" y2="130" style="stroke: var(--faint); stroke-dasharray: 4 3"/>
  <text x="240" y="125" style="fill: var(--muted); font: 11px var(--sans)">fair share</text>
  <g style="fill: var(--m1)">
    <rect x="30" y="160" width="30" height="20"/><rect x="72" y="60" width="30" height="120"/><rect x="114" y="150" width="30" height="30"/>
    <rect x="156" y="110" width="30" height="70"/><rect x="198" y="172" width="30" height="8"/><rect x="240" y="138" width="30" height="42"/>
  </g>
  <g style="fill: var(--m2)">
    <rect x="340" y="126" width="30" height="54"/><rect x="382" y="133" width="30" height="47"/><rect x="424" y="128" width="30" height="52"/>
    <rect x="466" y="131" width="30" height="49"/><rect x="508" y="124" width="30" height="56"/><rect x="550" y="134" width="30" height="46"/>
  </g>
  <g style="font: 12px var(--mono); fill: var(--ink)">
    <text x="36" y="198">n1</text><text x="78" y="198">n2</text><text x="120" y="198">n3</text><text x="162" y="198">n4</text><text x="204" y="198">n5</text><text x="246" y="198">n6</text>
    <text x="346" y="198">n1</text><text x="388" y="198">n2</text><text x="430" y="198">n3</text><text x="472" y="198">n4</text><text x="514" y="198">n5</text><text x="556" y="198">n6</text>
  </g>
</svg>
<figcaption>Keys owned per node (illustrative). One random point each gives arcs of wildly different lengths; many points each average out.</figcaption>
</figure>

The cost is a bigger ring: 6 members × 200 points is 1,200 entries to sort on every membership change, and a lookup must binary-search it for the first point at or after the key's hash instead of scanning.

### Rebalancing: moving the data

So far a membership change moved *ownership*. The values stayed where they were written. **Rebalancing** moves the data to match the new ring, and it is where sharded stores most often lose writes.

After learning new members, each node walks its store, finds keys whose owner is no longer itself, and sends them to their new owners: a **handoff**. Two rules make it safe:

- **Delete only after the new owner confirms.** This is an **acknowledged transfer**: the sender keeps the key until the receiver replies, and retries on a timer until it does. If the handoff message is lost and the sender had already deleted the key, the value would exist nowhere.
- **Never let an old copy overwrite a newer one.** A client `put` can reach the new owner before the handoff does. If the handoff then overwrites it, an acknowledged write reverts. When every node learns the change before clients write again, "a value already present is newer" is a sufficient rule.

### Epochs, versions and fencing: changes under load

That last assumption does not hold in a real cluster. A membership change reaches nodes at different moments, the next change starts before the last handoff finishes, and clients never stop. Three mechanisms take its place.

**Epochs order changes.** An **epoch** is a number that increases with every membership change. A node keeps the highest epoch it has seen and ignores any `set_members` whose epoch is not newer, so a change delayed in the network cannot undo a later one. If every message between nodes carries the sender's epoch and member list, a node that missed a change catches up from whichever peer mentions it.

**Versions order values.** Each stored value carries a **version**, assigned by the owner when it accepts a write, with the epoch in the high-order part: for example `epoch × 1,000,000 + counter`. Wherever two copies of a key meet (a handoff, a collected copy) the higher version wins. Taking the maximum gives the same result in any order, so arrival order stops mattering. These are **versioned values**.

**Fencing stops the old owner.** A new owner that answers at once may lack writes still sitting on the old owner, and the old owner, not yet aware of the change, may keep accepting writes. **Fencing** means making sure the old owner can no longer act as owner before the new one serves. Here the new owner, before serving in epoch e, asks every other node for its copies of the keys it now owns, with a `collect` message carrying epoch e. A node that answers first adopts epoch e, so from that moment it will forward, not accept, writes for those keys. Once every node has answered, the new owner holds the newest version of every key it owns, and no one else will accept a write for them.

<figure>
<svg viewBox="0 0 620 230" role="img" aria-label="Timeline: n4 learns epoch 5 and becomes owner of k, collects from n2 which adopts epoch 5 and stops accepting writes, then n4 serves">
  <g style="font: 13px var(--mono); fill: var(--ink)"><text x="10" y="44">n2</text><text x="10" y="124">n4</text><text x="10" y="194">client</text></g>
  <g style="stroke: var(--line)"><line x1="50" y1="40" x2="610" y2="40"/><line x1="50" y1="120" x2="610" y2="120"/><line x1="50" y1="190" x2="610" y2="190"/></g>
  <text x="60" y="30" style="fill: var(--muted); font: 12px var(--sans)">epoch 4, owns k</text>
  <line x1="90" y1="190" x2="130" y2="40" style="stroke: var(--m3); stroke-width: 2"/>
  <text x="104" y="175" style="fill: var(--m3); font: 12px var(--mono)">put k=8</text>
  <text x="140" y="56" style="fill: var(--m3); font: 12px var(--mono)">k=8 v4000007</text>
  <text x="150" y="112" style="fill: var(--accent); font: 12px var(--sans)">learns epoch 5: owns k, not ready</text>
  <line x1="200" y1="120" x2="270" y2="40" style="stroke: var(--accent); stroke-width: 2"/>
  <text x="206" y="80" style="fill: var(--accent); font: 12px var(--mono)">collect e5</text>
  <text x="280" y="30" style="fill: var(--accent); font: 12px var(--sans)">adopts epoch 5, stops owning k</text>
  <line x1="280" y1="40" x2="350" y2="120" style="stroke: var(--accent); stroke-width: 2"/>
  <text x="318" y="70" style="fill: var(--accent); font: 12px var(--mono)">k=8 v4000007</text>
  <line x1="300" y1="190" x2="330" y2="120" style="stroke: var(--m3); stroke-width: 2"/>
  <text x="250" y="210" style="fill: var(--m3); font: 12px var(--mono)">get k (held)</text>
  <text x="370" y="112" style="fill: var(--good); font: 12px var(--sans)">all peers answered: ready</text>
  <line x1="420" y1="120" x2="460" y2="190" style="stroke: var(--good); stroke-width: 2"/>
  <text x="470" y="200" style="fill: var(--good); font: 12px var(--mono)">get_ok 8</text>
</svg>
<figcaption>Fencing by collect: n2 adopts epoch 5 before it answers, so it can no longer accept writes for k, and n4 serves only once every peer has answered.</figcaption>
</figure>

## A worked example

Four nodes, positions on a ring scaled to 0–99 for readability (one point per node to keep the trace short): n1 at 20, n2 at 55, n3 at 85. n4 is running but not a member. Keys: `a` at 10, `b` at 30, `c` at 60, `d` at 92. So `a` and `d` belong to n1 (`d` wraps past 99 to n1 at 20), `b` to n2, `c` to n3. n4 will sit at 40. Versions are `epoch × 1,000,000 + counter`.

| t (ms) | event | effect |
|---|---|---|
| 0 | epoch 1 is the current membership {n1, n2, n3}; `b = 4` stored on n2 at version 1000003 | |
| 100 | `set_members` epoch 2, members {n1, n2, n3, n4}, reaches n4 and n2 | `b` at 30 now belongs to n4 (first point at or after 30 is n4 at 40). No other key moves |
| 101 | n4 learns epoch 2, owns `b`, is not ready; sends `collect` e2 to n1, n2, n3 | requests for `b` that reach n4 now are held |
| 103 | n1 and n2 receive the collect, adopt epoch 2 and answer. n1 has nothing for `b`; n2 answers `b = 4 v1000003` | n4 merges `b = 4`. n2 will now forward writes for `b` to n4 |
| 106 | the collect to n3 is **delayed** in the network | n4 keeps waiting; it cannot know n3 has nothing newer |
| 110 | client sends `put b = 9` to n3, which still believes epoch 1 | n3 thinks n2 owns `b` and forwards it, tagged epoch 1 |
| 113 | n2 receives the forwarded put; it is at epoch 2, where n4 owns `b` | n2 forwards to n4 with epoch 2; the put waits at n4 |
| 160 | the delayed collect reaches n3, which adopts epoch 2 and answers (nothing for `b`) | n4 has heard from every peer: ready |
| 160 | n4 serves the held put | `b = 9` at version 2000001 (epoch 2 beats any epoch-1 version) |
| 180 | n2's handoff timer sends `b = 4 v1000003` to n4 | n4 merges: 1000003 < 2000001, keeps `b = 9`; replies; n2 deletes its copy |
| 200 | the epoch-2 `set_members` finally reaches n1 and n3, and a stale epoch-1 copy reaches n3 | both are already at epoch 2, so they ignore these as not newer, and still reply `set_members_ok` |
| 250 | client sends `get b` to n1 | n1 forwards to n4: `get_ok 9` |

The faults are a delayed collect, a stale `set_members`, and a node (n3) routing with an old member list. Each was harmless for a specific reason. The delayed collect only delayed n4; it never let it serve early. n3's stale routing sent the put to n2, which knew better and forwarded it on. The handoff of `b = 4` arrived after the newer write and lost on version. The stale `set_members` lost on epoch.

Now suppose instead that at t=180 the handoff message had been lost. n2 keeps `b = 4` and its timer resends 100ms later. Until n4 replies, `b` exists on two nodes, which is harmless (n2 no longer serves it); deleting it before the reply could have lost it entirely.

## Why it is correct

**Agreement on owners.** The owner of a key is a deterministic function of the key and the member set: sort all points by position, binary-search for the first at or after `hash(key)`, wrap. Any two nodes that hold the same member set therefore name the same owner. Epochs make "the same member set" well defined: the highest epoch wins, and every node eventually sees it because it travels on every message.

**Minimal movement.** Adding member m inserts m's points and changes nothing else. A key's successor point changes only if one of m's points now lies between the key and its old successor, in which case the new owner is m. Removing m deletes its points; a key changes owner only if its successor was one of m's points. So a key gets a new owner only if its old owner left or its new owner just joined. That is the property the consistent-hashing tests check.

**No lost values.** A copy is deleted only after another node has confirmed it holds an equal or newer version. By induction, the newest version of every key is always held by at least one node.

**No reverted values.** Merge keeps the maximum version. Maximum is commutative, associative and idempotent, so the outcome of any set of handoffs and collects is independent of their order and of duplicates. A value can only be replaced by a higher version.

**No stale reads after a change.** The new owner of key k in epoch e serves only after every other node has answered its collect for epoch e. Each node adopts epoch e before answering, so after its answer it never again accepts a write for k as owner in an earlier epoch. Every write accepted before the answer is in the answer. So when the new owner starts serving, it holds the highest version of k ever acknowledged, and versions it assigns are higher still because its epoch is higher.

The assumptions: one client writes a given key at a time (so "last acknowledged write" is well defined), messages may be delayed and reordered but nodes do not crash and lose their stores, and every node does eventually answer. If a node can be down indefinitely, a collect that waits for everyone never finishes; real systems replace "every node" with a quorum or a consensus-backed configuration, which the Raft tracks build.

## Trade-offs and failure modes

**Hot keys defeat any hash.** Hashing spreads keys, not load. If one key takes half the traffic, its owner takes half the traffic. Fixes live above the partitioner: caching, or splitting the hot key into several sub-keys.

**Range queries become scatter-gather.** Hash partitioning destroys key order. A query for "every user from 1000 to 2000" must ask every node. Systems that need ordered scans (Bigtable, HBase, CockroachDB) use **range partitioning** instead: contiguous key ranges assigned to nodes, split and moved as they grow.

**Rebalancing costs bandwidth and tail latency.** Moving 1/N of the data competes with client traffic. Virtual nodes spread the cost over all nodes, but it is still there, and throttling it makes the cluster run unbalanced for longer.

**Availability during a change.** With fencing by collect, a new owner cannot serve until every peer answers. One slow or dead node stalls requests for every key that changed owner. That is the price of never answering stale.

**Ring size.** More virtual nodes mean more even load and more points to store, sort and search, and more small ranges to track during rebalancing. Some systems (Redis Cluster, Riak, Couchbase) use a fixed number of slots instead, assigned to nodes, which makes the moving units explicit.

**What this does not protect against.** A node that crashes takes its keys with it: partitioning without replication divides data, it does not protect it. Every real sharded store combines this with replication: each shard is stored on several nodes (Dynamo's preference list, Cassandra's replication factor, a Raft group per range in CockroachDB and TiKV).

**If epochs are not monotonic** (two coordinators issue the same epoch number with different member lists), nodes disagree permanently about owners. Epochs must come from one source of truth, which in production is usually a consensus system such as ZooKeeper or etcd.

## In real systems

**Amazon Dynamo and Apache Cassandra** use a consistent hash ring with virtual nodes. Dynamo's 2007 paper describes placing each physical node at many ring positions and the resulting improvement in load balance, and stores each key on the next several distinct nodes clockwise (its preference list). Cassandra assigns each node a configurable number of tokens (`num_tokens`) on the ring.

**Redis Cluster** does not use a ring. It splits the key space into 16,384 fixed hash slots (CRC16 of the key, modulo 16384) and assigns slots to nodes. A node asked for a key it does not own replies with a `MOVED` redirect to the owner, and during resharding a slot being migrated answers with `ASK` redirects until the move completes. Hash tags (`{user1}:cart`) let related keys share a slot.

**Apache Kafka** partitions each topic into a fixed number of partitions and, by default, chooses a keyed record's partition from a hash of the key modulo the partition count. That is `mod N` partitioning, and it shows the problem: adding partitions to a topic changes the partition for most keys, breaking per-key ordering across the change.

**CockroachDB and TiKV** use range partitioning: the key space is cut into contiguous ranges (regions in TiKV) that split when they grow and are moved between nodes to balance load. Each range is replicated by its own Raft group, and the cluster's range-to-node mapping is itself stored in the database.

## Common misconceptions

- **"Hashing the key gives even load."** It gives an even spread of *keys*. Uneven key popularity, or too few ring points, still overload one node.
- **"Consistent hashing means no data moves when nodes change."** About 1/N of the keys still move; the gain is that *only* those move, instead of nearly all of them.
- **"The node that received the write should store it."** Then a key ends up on whichever node each client happened to pick. Ownership must depend on the key and the membership alone.
- **"Last message wins is good enough for handoffs."** With overlapping changes, an old copy can arrive after a newer write. Only explicit epochs and versions order changes and values correctly.
- **"A new owner can serve as soon as it learns it owns the key."** Writes may still be arriving at the old owner. Until the old owner has been fenced off and its copies collected, the new owner can return stale data.

## How the tasks build it

[SH-1 Hash partitioning](#/c/shard-hash-routing) gives every key one owner with `hash(key) mod N` and forwards requests to it. The tests read keys through different nodes than wrote them, check with `local_keys` that each key lives on exactly one node, require each node to hold a fair share, and cap the number of messages between nodes per request.

[SH-2 Consistent hashing](#/c/shard-consistent-hashing) replaces `mod N` with a ring and adds `set_members` and `lookup`. The tests change the membership, sometimes delivering the list in a different order, and check that all nodes agree on owners and that a key changes owner only when its old owner left or its new owner just joined.

[SH-3 Virtual nodes](#/c/shard-virtual-nodes) evens out the shares. With 4,000 keys every member must own between 0.7 and 1.3 times its fair share at every cluster size from six down to three, while keeping the minimal-movement property. Large rings make a linear search too slow.

[SH-4 Rebalancing](#/c/shard-rebalancing) moves the values after each change. Its tests remove and add nodes, overwrite keys during the handoff, and drop 10% of messages between nodes, then check that no key is lost, none reverts to an older value, and each sits only on its owner.

[SH-5 Changes under load](#/c/shard-epochs) removes the gentle assumptions: changes carry epochs, reach nodes up to 300ms apart, overlap, and one node always gets each change late; clients read and write the whole time. The tests check that no read ever goes back in time and that stale `set_members` are ignored, including when a node leaves and rejoins before its own handoff has landed.

[SH-6 Key-range partitioning](#/c/shard-key-range) trades hashing for ordered ranges. One range starts on n1 and splits at its middle key whenever it passes 40 keys, handing the upper half to another node and announcing the boundary to everyone. The tests check that all nodes agree on the range table, that no range or node is too big, and, watching messages between nodes, that a scan involves only the owners of overlapping ranges.

[SH-7 Hot spots](#/c/shard-hot-spot) sends 70% of all requests to one counter. The owner must notice, and increments of that key are salted over all nodes while reads add up every part. The tests count requests per node, require exact reads, and cap the messages per request so that salting every key fails.

[SH-8 Global secondary index](#/c/shard-secondary-index) finds records by color through an index partitioned by term and updated asynchronously. The tests allow the index to lag by 250ms, hold back messages so that index updates overtake each other, and check that a settled find asks only one index node.

## Further reading

- [Consistent Hashing and Random Trees (Karger et al., 1997)](https://www.cs.princeton.edu/courses/archive/fall09/cos518/papers/chash.pdf): the paper that introduced consistent hashing, for web caching.
- [Dynamo: Amazon's Highly Available Key-value Store](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf): section 4.2 on the ring and virtual nodes, section 6.2 comparing partitioning strategies measured in production.
- [Redis Cluster specification](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/): fixed hash slots, redirects and live slot migration, a clear alternative to the ring.
- [A Fast, Minimal Memory, Consistent Hash Algorithm (Lamping and Veach, 2014)](https://arxiv.org/abs/1406.2294): jump consistent hash, which gets even shares and minimal movement with no ring at all, for numbered buckets.
- [Designing Data-Intensive Applications, chapter 6 (Kleppmann)](https://dataintensive.net/): the clearest book treatment of partitioning, rebalancing strategies and request routing.
