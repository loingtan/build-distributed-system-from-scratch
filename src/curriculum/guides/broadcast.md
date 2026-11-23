## The problem

Three nodes keep the same list of values: chat messages, configuration entries, anything that every node should know. A client tells `n1` the value 7. `n1` adds 7 to its list, replies "ok" to the client, and sends `{"type": "gossip", "message": 7}` to `n2` and `n3`. The copy to `n2` arrives. The copy to `n3` is dropped by the network. `n1` never finds out, because nothing tells it. From now on, a client that reads from `n1` or `n2` sees 7 and a client that reads from `n3` does not, and nothing in the system will ever fix it.

Now try the opposite mistake. To be thorough, every node forwards every gossip message it receives to all its peers. `n2` gets 7 from `n1` and sends it to `n1` and `n3`. `n1` gets it back and sends it to `n2` and `n3` again. Each copy creates two more. Within a fraction of a second the network carries thousands of copies of one value, and the nodes spend all their time passing it around. This is a **message storm**, and in the lab the simulator stops the run when it sees one.

Broadcast is the problem of getting a value told to one node to every node, despite lost messages and partitions, without drowning the network. It looks simple, and the first three versions everyone writes are wrong in instructive ways.

## The core idea

### State that survives between messages

Each node needs **state**: data it keeps in memory between handling one message and the next. Here the state is the collection of values the node has heard about. The natural type is a **set**, an unordered collection with no duplicates, because the question a node must answer is "which values do I know?", not "in what order and how many times did I hear them?".

The set has a property that matters later: adding a value that is already present changes nothing. Adding to a set is **idempotent**. So whatever else goes wrong, receiving the same value twice can never corrupt a node's state.

### Flooding with deduplication

The simplest way to spread a value is **flooding**: when a node learns a value, it sends it to all of its neighbours. The fix for the message storm is **deduplication**: a node forwards a value only the *first* time it learns it. If the value is already in its set, the node has already forwarded it, and forwarding again would only create more copies.

```text
learn(v):
    if v in seen: return          -- already known, already forwarded
    seen.add(v)
    for each peer: send gossip(v)
```

Both the client-facing `broadcast` handler and the node-facing `gossip` handler call `learn`. The client handler also replies to the client straight away. It must not wait for other nodes: during a partition it could wait forever, and the client would see a timeout for a value the node actually has.

Flooding with deduplication terminates: each node forwards each value exactly once, so with *n* nodes in a full mesh each value costs at most *n* × (*n* − 1) messages and then stops. With five nodes that is 20 messages per value.

<figure>
<svg viewBox="0 0 600 260" role="img" aria-label="A value told to n1 floods to n2, n3, n4; each forwards once; copies that reach a node which already has the value stop there">
  <g style="stroke: var(--line); stroke-width: 1">
    <line x1="110" y1="60" x2="300" y2="40"/><line x1="110" y1="60" x2="300" y2="130"/><line x1="110" y1="60" x2="300" y2="220"/>
    <line x1="300" y1="40" x2="300" y2="130"/><line x1="300" y1="130" x2="300" y2="220"/><line x1="300" y1="40" x2="300" y2="220"/>
  </g>
  <line x1="122" y1="58" x2="286" y2="42" style="stroke: var(--accent); stroke-width: 2.5"/>
  <line x1="122" y1="66" x2="286" y2="126" style="stroke: var(--accent); stroke-width: 2.5"/>
  <line x1="120" y1="72" x2="290" y2="210" style="stroke: var(--accent); stroke-width: 2.5"/>
  <rect x="40" y="44" width="70" height="32" rx="6" style="fill: var(--accent-soft); stroke: var(--accent)"/>
  <g style="fill: var(--surface); stroke: var(--accent)">
    <rect x="290" y="24" width="56" height="32" rx="6"/><rect x="290" y="114" width="56" height="32" rx="6"/><rect x="290" y="204" width="56" height="32" rx="6"/>
  </g>
  <g style="font: 13px var(--mono); fill: var(--ink)">
    <text x="52" y="65">n1: 7</text><text x="302" y="45">n2</text><text x="302" y="135">n3</text><text x="302" y="225">n4</text>
  </g>
  <text x="12" y="30" style="font: 12px var(--sans); fill: var(--muted)">client: broadcast 7</text>
  <g style="font: 12px var(--sans); fill: var(--ink)">
    <text x="370" y="34">round 1: n1 → n2, n3, n4</text>
    <text x="370" y="56">(3 messages, each a first sighting)</text>
    <text x="370" y="110">round 2: n2, n3, n4 each forward</text>
    <text x="370" y="130">to their two other peers</text>
    <text x="370" y="150">(6 messages, plus 3 back to n1)</text>
  </g>
  <g style="font: 12px var(--sans); fill: var(--muted)">
    <text x="370" y="200">every round-2 copy finds 7 already</text>
    <text x="370" y="218">in the set: no further forwarding</text>
  </g>
</svg>
<figcaption>Flooding in a four-node mesh. Each node forwards a value once, on first sight; duplicates are absorbed by the set, so the flood ends after two rounds.</figcaption>
</figure>

This is the simplest form of **gossip**: nodes spreading information to each other peer to peer, with no central coordinator, until everyone has it. Gossip protocols in general are also called epidemic protocols, because information spreads the way an infection does: each node that has it passes it on to others. Many real gossip protocols pick a few random peers each round instead of all of them; flooding is the deterministic, all-peers version.

### Surviving loss: acknowledgements and retries

Flooding assumes every message arrives. It does not: a lost message from `n1` to `n3` simply never happens, and if `n1` was the only node that would tell `n3`, `n3` never learns the value. In a partition, every message across the split is lost.

The sender cannot see loss directly. A plain send is **fire-and-forget**: the sender learns nothing about the outcome. To know, it needs the receiver to say so. An **acknowledgement** (ack) is a reply whose only meaning is "I received this". The sender then follows a simple rule: keep sending the value to a peer until that peer acknowledges it. Resending after a timeout is a **retry**.

This gives **at-least-once delivery**: as long as the peer is eventually reachable, the value eventually arrives, possibly more than once. It arrives more than once whenever the *ack* is lost rather than the value. The sender cannot tell those cases apart, so it retries, and the receiver gets a second copy.

That is where idempotence pays off. A second copy of value 7 finds 7 already in the set and changes nothing. The receiver must still acknowledge it, because the sender is retrying precisely since it never saw the first ack. If the receiver stays silent for duplicates, the sender retries forever.

<figure>
<svg viewBox="0 0 600 230" role="img" aria-label="n1 sends 7 to n3; the first copy is lost; the retry arrives but its ack is lost; the third copy is a duplicate and is acknowledged">
  <g style="font: 13px var(--mono); fill: var(--ink)"><text x="10" y="44">n1</text><text x="10" y="164">n3</text></g>
  <g style="stroke: var(--line)"><line x1="40" y1="40" x2="590" y2="40"/><line x1="40" y1="160" x2="590" y2="160"/></g>
  <line x1="60" y1="40" x2="110" y2="120" style="stroke: var(--bad); stroke-width: 2; stroke-dasharray: 5 4"/>
  <text x="108" y="132" style="fill: var(--bad); font: 15px var(--sans)">×</text>
  <line x1="220" y1="40" x2="270" y2="160" style="stroke: var(--accent); stroke-width: 2"/>
  <circle cx="272" cy="160" r="5" style="fill: var(--good)"/>
  <line x1="276" y1="160" x2="320" y2="90" style="stroke: var(--bad); stroke-width: 2; stroke-dasharray: 5 4"/>
  <text x="318" y="88" style="fill: var(--bad); font: 15px var(--sans)">×</text>
  <line x1="380" y1="40" x2="430" y2="160" style="stroke: var(--accent); stroke-width: 2"/>
  <circle cx="432" cy="160" r="5" style="fill: var(--surface); stroke: var(--muted); stroke-width: 2"/>
  <line x1="436" y1="160" x2="486" y2="40" style="stroke: var(--good); stroke-width: 2"/>
  <g style="font: 12px var(--mono); fill: var(--ink)">
    <text x="50" y="30">gossip 7</text><text x="200" y="30">retry</text><text x="360" y="30">retry</text><text x="476" y="30">gossip_ok</text>
  </g>
  <g style="font: 12px var(--sans); fill: var(--muted)">
    <text x="236" y="186">adds 7, acks</text><text x="400" y="186">already has 7, acks again</text>
    <text x="40" y="214">Every 300 ms without an ack, n1 sends again. n3 acknowledges every copy, new or not.</text>
  </g>
</svg>
<figcaption>Retrying until acknowledged turns a lossy link into at-least-once delivery. The duplicate on the third attempt is harmless because adding to a set is idempotent.</figcaption>
</figure>

> At-least-once delivery plus an idempotent operation gives the effect of exactly-once. The sender makes sure nothing is lost; the set makes sure nothing counts twice.

### Paying for it: batching and topology

Correct flooding with acknowledgements is expensive. With nine nodes, the node that hears a value from the client sends it to eight peers; each of those sends it to its seven other peers. That is 8 + 8 × 7 = 64 gossip messages and 64 acks, about 128 node-to-node messages for one value, before any retries. Messages cost CPU, bandwidth and, in a cloud, money. Two independent levers bring the number down.

**Batching** sends many values in one message. Instead of gossiping each value as it arrives, a node keeps, for each neighbour, the set of values that neighbour has not yet acknowledged, and flushes that whole set in one message on a timer. If twelve values arrive during one 250 ms interval, one message carries all twelve. The receiver acknowledges the batch, and the sender then removes exactly the values that were *in that batch* from the pending set. It must not clear the whole set: values that arrived while the batch was in flight are still pending and would be silently dropped. If the ack is lost, the next flush resends the batch plus anything new, and the receiver's set absorbs the duplicates.

**Topology** is the choice of who talks to whom. A full mesh, where every node tells every other node, gives one hop of latency and the most redundancy, at roughly *n*² messages per value. A **star** sends everything through one hub: a leaf tells the hub, the hub tells every other leaf. That is at most two hops and about 2*n* messages. A ring needs only two neighbours per node but up to *n*/2 hops. A tree sits in between.

<figure>
<svg viewBox="0 0 600 220" role="img" aria-label="Full mesh of five nodes with ten links, compared with a star of five nodes with four links through hub n1">
  <text x="70" y="20" style="font: 13px var(--sans); fill: var(--ink)">Full mesh: 10 links, 1 hop</text>
  <text x="370" y="20" style="font: 13px var(--sans); fill: var(--ink)">Star: 4 links, ≤ 2 hops</text>
  <g style="stroke: var(--line); stroke-width: 1.5">
    <line x1="150" y1="50" x2="230" y2="110"/><line x1="150" y1="50" x2="200" y2="190"/><line x1="150" y1="50" x2="100" y2="190"/><line x1="150" y1="50" x2="70" y2="110"/>
    <line x1="230" y1="110" x2="200" y2="190"/><line x1="230" y1="110" x2="100" y2="190"/><line x1="230" y1="110" x2="70" y2="110"/>
    <line x1="200" y1="190" x2="100" y2="190"/><line x1="200" y1="190" x2="70" y2="110"/><line x1="100" y1="190" x2="70" y2="110"/>
  </g>
  <g style="stroke: var(--accent); stroke-width: 2">
    <line x1="450" y1="120" x2="450" y2="50"/><line x1="450" y1="120" x2="530" y2="120"/><line x1="450" y1="120" x2="450" y2="190"/><line x1="450" y1="120" x2="370" y2="120"/>
  </g>
  <g style="fill: var(--surface); stroke: var(--ink)">
    <circle cx="150" cy="50" r="16"/><circle cx="230" cy="110" r="16"/><circle cx="200" cy="190" r="16"/><circle cx="100" cy="190" r="16"/><circle cx="70" cy="110" r="16"/>
    <circle cx="450" cy="50" r="16"/><circle cx="530" cy="120" r="16"/><circle cx="450" cy="190" r="16"/><circle cx="370" cy="120" r="16"/>
  </g>
  <circle cx="450" cy="120" r="18" style="fill: var(--accent-soft); stroke: var(--accent)"/>
  <g style="font: 12px var(--mono); fill: var(--ink)">
    <text x="141" y="54">n1</text><text x="221" y="114">n2</text><text x="191" y="194">n3</text><text x="91" y="194">n4</text><text x="61" y="114">n5</text>
    <text x="441" y="124">n1</text><text x="441" y="54">n2</text><text x="521" y="124">n3</text><text x="441" y="194">n4</text><text x="361" y="124">n5</text>
  </g>
</svg>
<figcaption>Fewer links mean fewer messages per value and more hops. In the star, every value passes through the hub, so the hub's health decides how fast anything spreads.</figcaption>
</figure>

Every one of these choices is a point on a curve of **latency versus cost**: fewer, fuller messages and fewer links mean cheaper propagation and slower propagation. Batching adds up to one flush interval of delay per hop. A star adds a hop and makes one node a bottleneck: while the hub is unreachable, nothing propagates at all (values are not lost, since leaves keep retrying, but they wait). A good design picks the cheapest point that still meets its latency target and keeps the retry rule, so that efficiency never costs correctness.

## A worked example

Four nodes, flooding with acknowledgements (no batching): each node retries each value to each peer every 300 ms until acked, and does not send a value back to the peer it learned it from. At t = 100 ms the network partitions into `{n1, n2}` and `{n3, n4}`; it heals at t = 1,500 ms. Latency is about 10 ms.

| t (ms) | Event | Sets afterwards |
|---|---|---|
| 0 | client → n1 `broadcast 7`; n1 adds 7, replies `broadcast_ok` at once | n1 {7} |
| 0 | n1 → n2, n3, n4 `gossip 7` | |
| 10 | n2, n3, n4 add 7 and ack | all {7} |
| 100 | **partition** {n1, n2} \| {n3, n4} | |
| 200 | client → n3 `broadcast 9`; n3 adds 9, replies at once | n3 {7, 9} |
| 200 | n3 → n1, n2: **lost** (partition); n3 → n4: arrives | n4 {7, 9} |
| 210 | n4 adds 9, acks; n4 → n1, n2 `gossip 9`: **lost** | |
| 500, 800, … | n3 and n4 retry 9 to n1 and n2 every 300 ms; all lost | n1, n2 still {7} |
| 1,500 | **network heals** | |
| 1,700 | n3's retries reach n1 and n2; both add 9 and ack; **the ack from n1 is lost** | n1, n2 {7, 9} |
| 1,700 | n1 forwards 9 to n2 and n4; n2 forwards 9 to n1 and n4 (neither sends back to n3) | |
| 1,710 | all of these copies, and n4's own retries to n1 and n2, are duplicates; each is acked | |
| 2,000 | n3, with no ack from n1, retries; n1 already has 9, acks; n3 stops | all {7, 9} |

Reads from any node now return `[7, 9]`. Along the way, n1 received value 9 four times (from n3 twice, from n2 and from n4) and n2 three times. Each extra copy was absorbed by the set and acknowledged. Two features of the design carried the run: the client got its `broadcast_ok` immediately even though half the cluster was unreachable, and the retry loops kept running through the partition so that delivery resumed by itself once it healed. A version without retries would have left n1 and n2 without 9 forever; a version that only acknowledged new values would have kept n3 retrying to n1 for the rest of the run.

## Why it is correct

The guarantee: **every value acknowledged to a client eventually reaches every node, and no node ever reports a value nobody broadcast or reports a value twice.** The second half is easy: nodes only add values they received, and a set holds each value once.

The first half rests on one invariant and a reachability argument.

*Invariant: a node acknowledges `gossip v` only after `v` is in its set.* So an ack from peer *p* proves that *p* has *v*. A sender that keeps retrying *v* to *p* until it sees an ack therefore stops only when *p* has *v*.

*Spreading:* consider the set of nodes that hold *v*. Each of them has an outstanding delivery of *v* to each neighbour that has not acknowledged it. As long as some neighbour lacks *v*, at least one delivery is pending, and it is retried indefinitely. If each retry has some chance of getting through once the network has healed, the delivery eventually succeeds and the set of holders grows. It cannot shrink. With *n* nodes it can grow at most *n* − 1 times, so after finitely many successful deliveries every node that is connected to the origin through neighbour links holds *v*.

Each step depends on an assumption:

- **Links are fair-lossy.** A message sent again and again is eventually delivered, and partitions eventually heal. A permanent partition makes delivery impossible for any protocol.
- **The topology is connected.** In a star, every leaf must be connected to the hub; in any topology, every node must be reachable from every other through neighbour links.
- **Nodes do not lose their memory.** The pending retries live in memory. If a node crashes after acknowledging a client but before any peer acknowledged the value, the value is lost. The tests in this track do not crash nodes; making broadcast survive crashes needs durable storage, covered in later tracks.

*Termination of traffic:* each node starts deliveries of *v* only once (on first sight), to a fixed set of neighbours, and each delivery stops at its first ack. So once the network is healthy, the total number of messages per value is finite. Deduplication is what turns an infinite storm into a bounded flood.

## Trade-offs and failure modes

**Messages.** A full mesh with flooding costs on the order of *n*² messages per value, which is fine for five nodes and ruinous for five hundred. Batching divides the count by the number of values per flush; topology reduces the number of links. In the lab's nine-node budget test, a batched star sends roughly 16 gossip messages and 16 acks per 250 ms flush (at most one per leaf to the hub and one from the hub to each leaf). With about twelve values arriving per flush interval, that is under three messages per value, against about 128 for unbatched mesh flooding.

**Latency.** Batching adds up to one flush interval per hop, and each extra hop adds a network delay. A star with a 250 ms flush and 100 ms links can take about 700 ms in the worst case to reach every node: a flush wait and a link at the origin, then the same again at the hub.

**Fault tolerance.** Redundant paths make propagation robust: in a mesh, a value reaches *n3* from any node that has it. In a star, every value passes through the hub, so when the hub is cut off, propagation stops until it returns. Correctness survives (values wait in the pending sets) but latency during faults gets worse.

**Memory and long partitions.** Retry state grows with the number of unacknowledged values per peer. During a long partition, every new value adds an entry for every unreachable peer, and every retry resends the whole backlog. Real systems bound this by switching from pushing individual values to **anti-entropy**: periodically comparing summaries (hashes, version numbers) with a peer and sending only what is missing.

**What it does not give you.** Broadcast here guarantees eventual delivery, not order. Two values broadcast to different nodes can arrive at a third node in either order, and different nodes can see them in different orders. Applications that need every node to see values in the same order need total-order broadcast, which is equivalent to consensus.

## In real systems

**Apache Cassandra.** Cassandra nodes spread cluster membership and node state (which nodes exist, which are up, which token ranges they own) by gossip. Roughly once per second each node exchanges state with a few other nodes chosen at random, and each piece of state carries a version number, so a node keeps only the newest version of each entry it hears about. That is deduplication by version rather than by value, and it lets information reach the whole cluster in a number of rounds that grows with the logarithm of its size.

**HashiCorp Serf and Consul.** Consul's cluster membership is built on Serf and the memberlist library, which implement a gossip protocol based on SWIM. Membership changes and user events spread by gossip to a few random peers per round, and messages are piggybacked on the protocol's regular failure-detection traffic to save messages: batching, in effect.

**Bitcoin.** New transactions and blocks spread through the peer-to-peer network by flooding with deduplication. A node that learns a new transaction announces its hash to its peers with an `inv` message; a peer that does not have it yet requests the full transaction. Already-known hashes are ignored, so each node relays each transaction once, and announcing hashes first keeps duplicate full copies off the network.

## Common misconceptions

- **"Sending to every peer is enough."** Only on a network that never loses a message. Without acknowledgements and retries, one dropped message leaves a node permanently behind.
- **"Retries cause duplicate data."** They cause duplicate *messages*. If the operation they carry is idempotent, as adding to a set is, the data is unaffected.
- **"Acknowledge the client after the value has reached the other nodes."** That makes every broadcast wait for the slowest or most distant node, and during a partition it waits forever. Acknowledge as soon as the value is stored locally and replicate in the background.
- **"Gossip is unreliable, best-effort spreading."** Gossip with retries or periodic anti-entropy is a reliable protocol: it guarantees eventual delivery under the assumptions above. What it does not guarantee is order or a fixed deadline.
- **"Fewer messages is always better."** Each cut in messages costs latency or redundancy. A star saves the most messages and is the most fragile when its hub is unreachable.

## How the tasks build it

[BR-1 Single-node broadcast](#/c/broadcast-single) is about state: one node must remember every value it is told and return them on read, each once, in any order. The test checks that every broadcast is acknowledged and that the read contains exactly the acknowledged values. It is where you meet the rule that messages are plain JSON, so a set must be turned into an array before it is sent.

[BR-2 Multi-node broadcast](#/c/broadcast-multi) adds four more nodes and a protocol between them that you design. A value told to any node must be readable on every node 1.5 seconds after the last broadcast. The busy test sends 150 values to catch designs that forward everything: those produce a message storm and the run is stopped.

[BR-3 Fault-tolerant broadcast](#/c/broadcast-faults) keeps the same promise on a broken network: a two-second partition between two groups, and then a network that silently drops a third of all node-to-node messages. It exercises acknowledgements, retries and idempotent receipt. Designs that wait for peers before answering the client fail the partition test; designs that stop retrying, or that only acknowledge new values, fail to converge.

[BR-4 Efficient broadcast](#/c/broadcast-efficient) puts a price on it: nine nodes with 50–100 ms links, at most six node-to-node messages per broadcast, and every value on every node within 1.5 seconds. A second test brings back a partition with no budget, so the cheaper design must still retry correctly. Batching and topology are the levers; the hard part is acknowledging batches without dropping values queued in the meantime.

## Further reading

- [Demers et al., Epidemic Algorithms for Replicated Database Maintenance (1987)](http://bitsavers.trailing-edge.com/pdf/xerox/parc/techReports/CSL-89-1_Epidemic_Algorithms_for_Replicated_Database_Maintenance.pdf) — the paper that introduced rumour-mongering and anti-entropy, with analysis of how fast information spreads.
- [Fly.io Gossip Glomers](https://fly.io/dist-sys/) — the Maelstrom challenge series that this track's broadcast tasks are modelled on.
- [Apache Cassandra architecture overview](https://cassandra.apache.org/doc/latest/cassandra/architecture/overview.html) — how a widely deployed database uses gossip for membership and state.
- [Das, Gupta and Motivala, SWIM (2002)](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf) — gossip-based membership with piggybacked dissemination, the basis of HashiCorp's memberlist.
- [Bitcoin developer guide: P2P network](https://developer.bitcoin.org/devguide/p2p_network.html) — transaction and block relay by inventory announcements, a large-scale flooding protocol with deduplication.
