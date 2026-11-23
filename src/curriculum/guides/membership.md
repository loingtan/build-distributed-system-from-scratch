## The problem

A cluster of servers needs to know who is in it. A sharded database needs to know which replicas can take writes, a load balancer needs to know which backends to route to, and a service-discovery system exists only to answer that one question. The answer changes: machines crash, restart, get added and get removed.

The obvious design is the heartbeat detector from the election track. Every node sends "I am alive" to every other node every 100ms, and suspects any peer it has not heard from for 500ms. With five nodes, each one sends 40 messages per second. It works.

Now grow the cluster to 500 nodes. Each node sends 499 heartbeats every 100ms: almost 5,000 messages per second, and receives as many. The cluster as a whole moves 2.5 million messages per second whose only content is "nothing happened". The failure detector has become the largest workload on the network, and one overloaded node that falls behind on processing heartbeats now looks dead to everyone.

Cost is not the only problem. Take five nodes, n1 to n5, and suppose the network link between n1 and n2 starts dropping every packet, while all other links are fine. n1 stops hearing n2's heartbeats and suspects n2. n2 suspects n1. Both are alive and serving traffic; n3, n4 and n5 can reach both perfectly well. Whatever n1 does with its suspicion (stop sending n2 requests, re-replicate n2's data, tell a client n2 is gone) is wrong.

And the views disagree. n1 thinks n2 is down; n3 thinks everything is fine. If n4 really crashes, each node finds out on its own, at a different moment, depending on when it last happened to hear from n4. For a while, some nodes route requests to n4 and some do not.

So a membership protocol for a large cluster has to do four things at once:

1. keep the per-node cost constant as the cluster grows,
2. not mistake one bad path for a dead node,
3. get every node to the same view quickly, and
4. let a node that was wrongly accused, or that crashed and restarted, get back in.

SWIM, published by Das, Gupta and Motivala in 2002, does all four. This track builds it in four steps.

## The core idea

### Probe one member per period

The first change is to stop talking to everybody. Time is divided into **protocol periods**, 200ms in this track. In each period, every node picks *one* other member and sends it a **ping**. The member answers with an **ack**. If the ack arrives before a short timeout, the target is alive. If not, the prober has a reason to be suspicious.

Count the messages. Each node sends one ping per period, and on average receives one ping (since every node pings someone), which it answers with one ack. That is about two messages per node per period, ten per second, whether the cluster has 5 members or 5,000. The per-node load no longer depends on the cluster size. We call a fixed cap on that load a **message budget**; the tests in this track allow 30 messages per node per second, which heartbeats exceed as soon as the cluster has more than a handful of nodes.

Does detection get slower? Less than you might think. A given member is chosen by each of the other N-1 nodes with probability 1/(N-1) per period, so the chance that *nobody* probes it in a period is about (1 - 1/(N-1))^(N-1), roughly 37% for any large N. Some node notices a crash within one or two periods on average, independent of N. What does grow is the time until *every* node has probed the crashed member itself, which is why we will need dissemination.

How a node picks its target matters. Picking uniformly at random gives the right average, but occasionally one member goes unprobed for many periods by bad luck. SWIM instead walks a **shuffled list**: shuffle the peers, probe them in that order one per period, and reshuffle when the list runs out. Every member is then probed at least once per pass, so the gap between two probes of the same member is at most 2(N-1)-1 periods, and still unpredictable enough that nodes do not fall into lockstep.

### Ask for a second opinion

A missing ack has several explanations: the target crashed, the ping was lost, the ack was lost, or the path between the two nodes is broken. Only the first is a reason to suspect the target. SWIM separates them with **indirect probing**.

When a direct ping goes unanswered, the prober picks k other members at random (k = 3 is typical) and sends each a **ping-req**: "please ping n2 for me". Each helper pings the target with its own timeout and reports whether it got an ack. If *any* helper did, the target is alive. Only if the direct ping and every indirect path fail does the prober suspect it.

<figure>
<svg viewBox="0 0 600 260" role="img" aria-label="n1 pings n2 and the ping is lost; n1 sends ping-req to n3, n4 and n5; n3 pings n2, gets an ack and reports ok to n1">
  <g style="font: 13px var(--mono); fill: var(--ink)">
    <text x="10" y="34">n1</text><text x="10" y="84">n2</text><text x="10" y="134">n3</text><text x="10" y="184">n4</text><text x="10" y="234">n5</text>
  </g>
  <g style="stroke: var(--line)">
    <line x1="40" y1="30" x2="590" y2="30"/><line x1="40" y1="80" x2="590" y2="80"/><line x1="40" y1="130" x2="590" y2="130"/><line x1="40" y1="180" x2="590" y2="180"/><line x1="40" y1="230" x2="590" y2="230"/>
  </g>
  <line x1="60" y1="30" x2="110" y2="72" style="stroke: var(--bad); stroke-width: 2; stroke-dasharray: 5 4"/>
  <text x="108" y="78" style="fill: var(--bad); font: 15px var(--sans)">×</text>
  <text x="62" y="20" style="fill: var(--muted); font: 12px var(--sans)">ping</text>
  <line x1="200" y1="30" x2="240" y2="130" style="stroke: var(--accent); stroke-width: 2"/>
  <line x1="200" y1="30" x2="250" y2="180" style="stroke: var(--accent); stroke-width: 2"/>
  <line x1="200" y1="30" x2="260" y2="230" style="stroke: var(--accent); stroke-width: 2"/>
  <text x="150" y="20" style="fill: var(--muted); font: 12px var(--sans)">ping-req ×3</text>
  <line x1="240" y1="130" x2="290" y2="80" style="stroke: var(--m2); stroke-width: 2"/>
  <line x1="290" y1="80" x2="340" y2="130" style="stroke: var(--good); stroke-width: 2"/>
  <text x="300" y="72" style="fill: var(--muted); font: 12px var(--sans)">ping, ack</text>
  <line x1="340" y1="130" x2="400" y2="30" style="stroke: var(--good); stroke-width: 2"/>
  <text x="380" y="22" style="fill: var(--good); font: 12px var(--sans)">ok: true</text>
  <line x1="410" y1="30" x2="590" y2="30" style="stroke: var(--good); stroke-width: 4; opacity: 0.4"/>
  <text x="450" y="50" style="fill: var(--ink); font: 12px var(--sans)">n2 stays alive</text>
</svg>
<figcaption>The direct ping from n1 to n2 is lost. n1 asks three helpers; n3 reaches n2 and reports back, so n1 does not suspect n2. (n4 and n5 are doing the same; one positive answer is enough.)</figcaption>
</figure>

In the broken-link scenario from the problem section, n1's helpers n3, n4 and n5 each reach n2 over their own links. n1 hears "ok" and keeps n2 alive. One bad path no longer looks like a dead node. Random loss is handled the same way: with 2% of messages lost, a direct ping-and-ack fails about once in 25 tries, but for a probe to fail *falsely* the direct exchange and all three four-message indirect paths must fail together, roughly once in 50,000 probes.

The extra messages are paid only when a direct ping fails, so in a healthy cluster indirect probing costs almost nothing. The prober sends its ping-reqs in parallel and gives them a timeout longer than the helper's own ping timeout plus two network hops; with a 60ms ping timeout and a 120ms ping-req timeout, the whole probe fits inside one 200ms period.

### Spread the news on messages you already send

When a probe fails, the prober now knows something the rest of the cluster should know. Sending a message to every member would bring back the O(N) cost we just removed, and anyone who missed that one message would never hear the news.

SWIM uses **gossip dissemination** instead, and does it at no extra message cost by **piggybacking**. Each node keeps a small buffer of recent **membership updates**, entries such as `{ "id": "n4", "status": "dead" }`. Every ping, ack, ping-req and ping-req reply it sends carries a few of them, least-sent first:

```json
{ "type": "ping", "updates": [{ "id": "n4", "status": "dead" }, { "id": "n2", "status": "alive" }] }
```

A node that receives an update it did not know applies it to its own view and puts it in its *own* buffer with a fresh send count. That is what makes the update spread. Each entry is attached to a bounded number of outgoing messages, about 3·log₂(N+1) (12 for a cluster of 12), and then dropped, so buffers stay small.

This is **infection-style dissemination**, the "I" in SWIM. Every informed node passes the update to the node it pings and to every node that pings it, so the number of informed nodes roughly doubles each period. Twelve nodes are all informed after about four periods; a thousand after about ten. Because each update travels on many messages along many paths, losing a few slows it down slightly instead of stopping it.

<figure>
<svg viewBox="0 0 600 220" role="img" aria-label="Number of nodes that know an update, period by period: 1, 2, 4, 8, 12 out of 12">
  <g style="stroke: var(--line)"><line x1="60" y1="180" x2="580" y2="180"/><line x1="60" y1="20" x2="60" y2="180"/></g>
  <g style="font: 12px var(--sans); fill: var(--muted)">
    <text x="20" y="184">0</text><text x="14" y="104">6</text><text x="14" y="34">12</text>
    <text x="95" y="200">period 1</text><text x="195" y="200">2</text><text x="295" y="200">3</text><text x="395" y="200">4</text><text x="495" y="200">5</text>
  </g>
  <rect x="90" y="167" width="50" height="13" style="fill: var(--accent)"/>
  <rect x="180" y="153" width="50" height="27" style="fill: var(--accent)"/>
  <rect x="280" y="127" width="50" height="53" style="fill: var(--accent)"/>
  <rect x="380" y="73" width="50" height="107" style="fill: var(--accent)"/>
  <rect x="480" y="20" width="50" height="160" style="fill: var(--good)"/>
  <g style="font: 13px var(--mono); fill: var(--ink)">
    <text x="108" y="160">1</text><text x="198" y="146">2</text><text x="298" y="120">4</text><text x="398" y="66">8</text><text x="492" y="14">12</text>
  </g>
</svg>
<figcaption>An update spreading through twelve nodes by piggybacking: each informed node tells at least the member it probes, so the informed count roughly doubles every period.</figcaption>
</figure>

With dissemination, the node that detects a crash is the only one that has to detect it. Everyone else learns from gossip within a few periods, and the whole cluster converges on the same view.

### Suspect first, and let the accused answer

So far a failed probe makes the target dead, and dead is final. That is too harsh. A node that pauses for half a second, for a garbage-collection pause or a brief network blip, is declared dead by one prober, and the news spreads to everyone. It is still running, but the cluster has thrown it out for good. A crashed node that restarts has the same problem: "dead" beats everything, so it can never come back.

SWIM's refinement adds an intermediate **suspect** state and **incarnation numbers**.

An **incarnation number** is a counter each node keeps about itself. Only the node itself ever increases it. Every membership update now carries the incarnation it refers to: `{ "id": "n3", "status": "suspect", "inc": 2 }`. Two updates about the same node are ordered like this:

| Compare | Winner |
|---|---|
| different incarnations | the higher incarnation |
| same incarnation | dead beats suspect beats alive |

A node applies an update only if it beats what it already has for that member, and only then gossips it further. That one ordering rule is the entire merge logic.

With it, the protocol becomes:

- A failed probe marks the target **suspect** at its current incarnation, and the suspicion is gossiped like any update.
- Every node that accepts the suspicion starts a timer, the **suspicion timeout** (2.5 seconds in this track). If the member is still suspect *at the same incarnation* when it fires, it is declared **dead** at that incarnation, and that is gossiped too.
- A node that hears it is suspect or dead at an incarnation at least its own **refutes** the rumour: it sets its incarnation to one more than the rumour's and gossips `alive` at the new number. Since alive(3) beats suspect(2), the refutation overrides the suspicion everywhere it reaches.
- A node keeps its incarnation on disk and adds one every time it starts. A restarted node therefore announces itself at a number higher than any rumour about its previous life, which lets it **rejoin**.

Refutation only works if the suspect hears about the suspicion quickly, so when a node piggybacks updates on a message, it puts the ones *about the receiver* first. A suspect that is pinged, or that pings anyone, learns about its own suspicion on that very exchange.

## A worked example

Five nodes, n1 to n5, all at incarnation 0, 200ms periods, a 2.5-second suspicion timeout. At t = 1,500ms the network cuts n3 off from everyone for 500ms; at 2,000ms it heals. Times are approximate.

| Time | Event | Views |
|---|---|---|
| 1,500 | n3 is partitioned. | all alive |
| 1,520 | n1's period: it pings n3. The ping is dropped by the partition. | |
| 1,580 | n1's 60ms ack timeout expires. It sends ping-req for n3 to n2, n4, n5. Each pings n3; all three pings are dropped. | |
| 1,610 | Meanwhile n3's own period: it pings n5, and the ping is dropped. Its ping-reqs to n1, n2 and n4 are dropped too; at about 1,790 n3 marks n5 **suspect(0)**. | n3: n5 suspect |
| 1,700 | n1's 120ms ping-req timeout expires: no helper said ok. n1 marks n3 **suspect(0)**, buffers it, and starts a 2.5s timer that would fire at 4,200. | n1: n3 suspect |
| 1,720 | n1 pings n4 for its next period; the ping carries suspect(n3, 0). n4 accepts it, starts its own timer (fires at 4,220) and buffers it. The ack from n4 to n1 is lost to random loss; n1's ping-reqs for n4 succeed through n2, so nothing changes. | n1, n4: n3 suspect |
| 1,750–1,950 | suspect(n3, 0) spreads on pings and acks to n2 and n5. | n1, n2, n4, n5: n3 suspect |
| 2,000 | The partition heals. | |
| 2,030 | n5 pings n3. Updates about the receiver go first, so the ping carries suspect(n3, 0). | |
| 2,031 | n3 sees a suspicion of itself at incarnation 0 ≥ its own 0. It sets its incarnation to **1**, buffers alive(n3, 1), and acks. The ack carries alive(n3, 1). | n5: n3 alive(1) |
| 2,031 | n3's own buffer still holds suspect(n5, 0). It rides on the same ack; n5 sees a suspicion of itself, bumps to incarnation 1 and buffers alive(n5, 1). | |
| 2,100–2,600 | alive(n3, 1) and alive(n5, 1) spread. Each beats the suspect(…, 0) entries wherever it arrives. | |
| ~2,700 | Every view is all alive. | all alive |
| 4,200 | n1's suspicion timer for n3 fires. n3's entry is now alive at incarnation 1, not suspect at 0, so the timer does nothing. | all alive |

Two things to notice. First, nobody needed to decide that the suspicion was wrong; the higher incarnation simply won wherever it arrived, and the stale timers checked the incarnation before acting. Second, the lost ack at 1,720 caused no suspicion at all: indirect probing absorbed it.

If n3 had really crashed instead, no refutation would come. At about 4,200 n1's timer would fire, n1 would mark n3 **dead(0)** and gossip it, and the other nodes' own timers would fire at about the same time. If n3 later restarted, it would read incarnation 0 from disk, start at incarnation 1, and gossip alive(n3, 1) on its first pings. That beats dead(n3, 0), so it would be back in every view within a few periods.

## Why it is correct

SWIM's guarantees are about a **failure detector** and about **dissemination**, and both are probabilistic. It helps to state them precisely.

**Completeness: every crashed node is eventually declared dead by every live node.** A crashed node never acks and never refutes. Every live node keeps probing every member it does not believe dead, and with shuffled round-robin targeting each member is probed at least once every 2(N-1)-1 periods by each node. The first failed probe (direct and indirect) produces a suspicion; no refutation can come, because only the crashed node can raise its own incarnation; so after the suspicion timeout the suspicion becomes dead. Even if gossip lost every message about it, each node would eventually probe the crashed member itself and reach the same conclusion. Gossip only makes it faster.

**Fast dissemination.** An update reaches all nodes in O(log N) periods with high probability. The argument is the epidemic one: in each period every informed node contacts at least one member chosen at random, so while few nodes are informed their number roughly doubles each period, and once most are informed the remaining ones are each contacted with high probability per period. Retransmitting each update about 3·log₂(N+1) times per node makes the chance that some node is never reached very small, even with some message loss.

**Convergence: all nodes end with the same view.** This follows from the ordering rule. Updates about one member are ordered by the pair (incarnation, status rank), and every node keeps the maximum it has seen. Taking a maximum is commutative, associative and idempotent: it does not matter in which order updates arrive, how many times, or by which path. Once the same set of updates has reached every node, every node holds the same entry for every member. This is the same reasoning that makes a CRDT converge.

**Accuracy: a live node is not declared dead, under stated assumptions.** Suppose a live node is suspected. The suspicion is gossiped, and the suspect learns of it as soon as it exchanges a message with any node that has it buffered (updates about the receiver are sent first). It then raises its incarnation above the suspicion's, and alive at the new number beats suspect at the old one everywhere. Every suspicion timer checks the incarnation before declaring death, so the timers armed for the old incarnation do nothing. The node is safe as long as the refutation reaches every node before that node's suspicion timer fires: the suspicion timeout must exceed the time for the rumour to reach the suspect plus the time for the refutation to spread. In this track, after a 0.5-second isolation that is about one second, comfortably below 2.5.

Incarnation numbers carry an argument of their own. Only the node itself increments its number, so there is no need for synchronised clocks: a higher number means "the node said something after this rumour began". Persisting the number and increasing it on every start extends that across crashes; a new life always starts above anything said about the old one.

What SWIM does not guarantee is that all nodes agree *at the same instant*. It is weakly consistent: views converge, but during the few periods while an update spreads, different nodes see different things. Systems that need agreement at a single point (exactly one lock holder, exactly one leader) need consensus on top.

## Trade-offs and failure modes

**Detection time versus load.** The protocol period sets both. Halving it halves detection time and doubles every node's message rate. The suspicion timeout adds directly to detection time: with a 2.5-second timeout, a crash takes about three seconds to become "dead" even though it was suspected within half a second. Systems that react to suspicion (for example by sending fewer requests to a suspect) recover some of that time.

**Suspicion timeout versus false deaths.** The timeout must cover the time for the accused to hear the rumour and for its refutation to spread, which grows with log N and with loss. memberlist scales the timeout with the logarithm of the cluster size for that reason. Too short, and a brief pause becomes a death; too long, and crashed nodes linger in everyone's view as suspects.

**Slow or overloaded members.** SWIM assumes a node that processes messages is healthy. A node that is overloaded, or whose own network is congested, can fail to answer pings in time and also fail to *send* timely acks for other people's probes, causing it to accuse healthy nodes. HashiCorp's Lifeguard extension addresses this: a node that notices it is missing its own deadlines becomes less quick to suspect others.

**Asymmetric and partial failures.** Indirect probing handles one bad path. It does not help if the target can receive but not send, or if a whole rack is partitioned from the rest: then the minority side sees the majority as dead and the majority sees the minority as dead. When the partition heals, refutations at higher incarnations reconcile the views, but for the duration both sides act on different memberships. SWIM detects, it does not decide; deciding who owns what during a partition needs a quorum-based protocol.

**Message size.** Piggybacking is free in message count but not in bytes. After a large event (a rack of 40 nodes going down at once) every buffer holds many updates, and messages grow. Implementations cap how many updates ride on each message and rely on the bounded retransmission count to drain the buffers.

**Slow full convergence.** Gossip is fast on average but probabilistic. If every message carrying an update to some node is lost, that node finds out only when it probes the member itself. That is rare, but it means "within a few periods" is a statement about probability, not a hard bound.

## In real systems

**HashiCorp memberlist, Serf and Consul.** memberlist is a Go library implementing SWIM with suspicion, incarnation numbers, piggybacked gossip, and the Lifeguard extensions. Serf builds a decentralised cluster membership and event system on it, and Consul uses Serf for gossip among the agents in a datacenter (the LAN pool) and among servers across datacenters (the WAN pool). memberlist also adds a periodic full state exchange over TCP between random pairs ("push-pull"), which repairs views that gossip alone missed.

**Apache Cassandra.** Cassandra nodes gossip once per second with up to three other nodes, exchanging heartbeat state with a generation number (set when the node starts) and a version number (incremented as the node runs). That pair plays the role of SWIM's incarnation: a restarted node has a higher generation, so its new state wins. Cassandra's failure detector is the phi-accrual detector, which turns heartbeat inter-arrival times into a suspicion level rather than a yes/no timeout. It is gossip-based membership, but not SWIM's probing scheme.

**Uber Ringpop.** Ringpop, used inside Uber for application-level sharding in Node.js services, implements SWIM-style membership (pings, ping-reqs, suspicion, incarnation numbers and piggybacked dissemination) and builds a consistent-hash ring on the resulting member list, so each service instance knows which keys it owns.

## Common misconceptions

- **"SWIM guarantees everyone agrees on the membership."** It guarantees views converge, quickly and with high probability. At any instant, nodes may disagree while an update spreads. Agreement at a single point needs consensus.
- **"Indirect probing makes false positives impossible."** It makes a false suspicion from one bad path or one lost message very unlikely. A node that is genuinely unreachable from everyone for a while, such as during a pause or a partition, will still be suspected; that is what the suspect state and refutation are for.
- **"A suspected node should be ignored until it is declared dead."** A suspect is still a member and is still probed. Suspicion is a question waiting for an answer, and the suspect must be reachable to answer it.
- **"A refutation just needs to say 'I am alive'."** At the same incarnation, alive loses to suspect. Only a strictly higher incarnation overrides the rumour, which is why the suspect increments its number.
- **"Gossip is slow because it is random."** It is logarithmic: each period the number of informed nodes roughly doubles, so a thousand nodes learn of a change in about ten periods, without anyone sending an extra message.

## How the tasks build it

[SW-1 Probe one member at a time](#/c/swim-probe) replaces all-to-all heartbeats with one ping per protocol period and a shuffled round-robin choice of target. Its tests check that a crashed node stops being reported alive within three seconds, that nobody is falsely suspected on a healthy network, and that each node stays within a budget of 30 messages per second in clusters of 5 and 16 nodes, which heartbeats cannot do.

[SW-2 Indirect probes](#/c/swim-indirect) adds ping-req. One test breaks a single link between two live nodes for the whole run, and nobody may suspect anyone; another adds 2% message loss and a crash, which must still be detected without any false suspicion.

[SW-3 Spread the news by piggybacking](#/c/swim-gossip) declares failed members dead and spreads updates on the probe traffic. In a twelve-node cluster, a crash must be reported dead by every node within three seconds, and within two seconds of the first node reporting it, with the same message budget, including a run with two crashes and message loss.

[SW-4 Suspicion and incarnation numbers](#/c/swim-suspicion) adds the suspect state, the suspicion timeout, refutation and rejoin. A node is cut off for half a second and must never be declared dead; every view must be all alive again soon after the network heals. A crashed node must be dead everywhere within six seconds, and after restarting it must be alive everywhere within two and a half.

## Further reading

- [SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf) — the original paper by Das, Gupta and Motivala; short, and it explains the analysis behind probing, ping-req and the suspicion mechanism.
- [Lifeguard: Local Health Awareness for More Accurate Failure Detection](https://arxiv.org/abs/1707.00788) — HashiCorp's measurements of false positives in SWIM caused by slow members, and the extensions memberlist adopted.
- [Consul: Gossip protocol](https://developer.hashicorp.com/consul/docs/architecture/gossip) — how a production system uses SWIM-based gossip for LAN and WAN membership.
- [hashicorp/memberlist](https://github.com/hashicorp/memberlist) — a readable, widely used Go implementation; its README lists how it extends SWIM.
- [Epidemic Algorithms for Replicated Database Maintenance](http://bitsavers.trailing-edge.com/pdf/xerox/parc/techReports/CSL-89-1_Epidemic_Algorithms_for_Replicated_Database_Maintenance.pdf) — Demers et al., the paper that introduced the epidemic analysis gossip dissemination relies on.
