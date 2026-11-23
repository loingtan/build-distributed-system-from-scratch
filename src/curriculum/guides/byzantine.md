## The problem

Four replicas, `n1` to `n4`, keep copies of a small ledger. A client asks `n1` to record one payment, and `n1` forwards it to the others. Every earlier track in this course assumed a node that is working says true things: it may crash, it may be slow, its messages may be lost, but what it sends is what the protocol told it to send.

Now suppose `n1` has a bug, or a flipped bit in memory, or an attacker on the machine. It sends "pay Alice 10" to `n2` and "pay Mallory 99" to `n3` and `n4`. Each message, on its own, looks perfectly normal. `n2` records the payment to Alice, `n3` and `n4` record the payment to Mallory, and the ledger now has two different histories. Nobody crashed. No message was lost. The replication protocol did exactly what it was designed to do, and it produced a disagreement that no amount of retrying will repair.

The trouble gets worse with voting. Say the rule is "a value is decided once two of the three other replicas confirm it". `n4` is the faulty one. It tells `n2` "I confirm Alice" and tells `n3` "I confirm Mallory". If `n2` already believed Alice and `n3` already believed Mallory, each now holds two confirmations and decides. One liar was enough to turn a correct-looking majority rule into a split.

A node that can behave in any way at all, including lying, sending different things to different peers, and staying silent at exactly the wrong moment, is called **Byzantine**. This track is about protocols that keep their promises when up to `f` of the nodes are Byzantine. It covers three ideas: making a broadcast consistent when the sender lies, using signatures and quorums of size 2f+1 to decide, and running one agreement round of PBFT, the protocol that made this practical.

## The core idea

### What a Byzantine node can and cannot do

A **Byzantine fault** is any deviation from the protocol. We assume nothing about what a faulty node does: it may lie, contradict itself, collude with other faulty nodes, or behave perfectly until the worst moment. A protocol is **Byzantine fault tolerant** if its guarantees hold as long as at most `f` nodes are faulty, whatever those nodes do.

That sounds hopeless, so it helps to be precise about what a faulty node *cannot* do:

- It cannot pretend to be another node on a direct link. We assume **authenticated channels**: when `n3` receives a message, it knows for certain which node sent it. In this lab the network sets `msg.src`; in production TLS or per-link MACs give the same property.
- It cannot forge a signature of an honest node. If statements are signed, a liar can sign anything in its own name, but nothing in anyone else's.
- It cannot make more than `f` nodes faulty. The whole design rests on that bound.

The single most damaging thing it *can* do is **equivocation**: telling different nodes different things. Every message is individually plausible, so no node can detect the lie from its own inbox. Detection needs nodes to compare notes.

### Quorums that overlap in an honest node

Crash-tolerant protocols such as Raft rely on majorities. Any two majorities of the same cluster share at least one node, and since that node does not lie, it will not have voted for two different leaders in one term. With liars, one shared node is not enough: it might be the liar.

So the overlap must contain at least `f + 1` nodes, guaranteeing at least one honest member. Now add the second constraint: a node can never wait for more than `n - f` replies, because `f` nodes may simply never answer. Two groups of `n - f` overlap in `n - 2f` nodes. Requiring `n - 2f >= f + 1` gives **n >= 3f + 1**.

With `f = 1` that is four nodes, and the quorum size is `n - f = 2f + 1 = 3`. Any two groups of three out of four share two nodes; at most one is the liar, so at least one honest node is in both. That honest node said one thing to everybody. This property, **quorum intersection** with an honest witness, is the argument behind every protocol in this track.

<figure>
<svg viewBox="0 0 600 230" role="img" aria-label="Two quorums of three out of four nodes overlap in n2 and n3; the traitor n4 can be in one overlap but not both overlap members">
  <g style="font: 14px var(--mono); fill: var(--ink)">
    <circle cx="150" cy="115" r="26" style="fill: var(--surface); stroke: var(--line)"/><text x="139" y="120">n1</text>
    <circle cx="270" cy="70" r="26" style="fill: var(--surface); stroke: var(--line)"/><text x="259" y="75">n2</text>
    <circle cx="270" cy="160" r="26" style="fill: var(--surface); stroke: var(--line)"/><text x="259" y="165">n3</text>
    <circle cx="390" cy="115" r="26" style="fill: var(--surface); stroke: var(--bad)"/><text x="379" y="120">n4</text>
  </g>
  <ellipse cx="225" cy="115" rx="130" ry="95" style="fill: none; stroke: var(--m1); stroke-width: 2; stroke-dasharray: 6 4"/>
  <ellipse cx="315" cy="115" rx="130" ry="95" style="fill: none; stroke: var(--m2); stroke-width: 2"/>
  <text x="70" y="30" style="fill: var(--muted); font: 13px var(--sans)">quorum A: n1 n2 n3</text>
  <text x="380" y="30" style="fill: var(--muted); font: 13px var(--sans)">quorum B: n2 n3 n4</text>
  <text x="440" y="120" style="fill: var(--bad); font: 13px var(--sans)">traitor</text>
  <text x="190" y="222" style="fill: var(--ink); font: 13px var(--sans)">overlap n2, n3: at least one is honest</text>
</svg>
<figcaption>With n = 4 and f = 1, any two quorums of 3 share 2 nodes, so at least one honest node sits in both.</figcaption>
</figure>

### Reliable broadcast: echo, then ready

The first building block is **reliable broadcast**: a sender sends one value, and the honest nodes must end up delivering the same value or nothing at all, even if the sender lies. Three properties make that precise:

- **Validity**: if the sender is honest, every honest node delivers its value.
- **Agreement**: no two honest nodes deliver different values for the same sender.
- **Totality**: if one honest node delivers, every honest node eventually delivers.

Bracha's protocol (1987) achieves this without signatures, using only authenticated channels, in three message types:

1. The sender sends `propose` with its value to everyone.
2. Each node **echoes** the first value it received from that sender, to everyone, once.
3. A node that sees **2f+1 matching echoes** sends `ready` for that value, once.
4. A node that sees **f+1 matching readies** also sends `ready`, if it has not already. This is called **amplification**.
5. A node that sees **2f+1 matching readies** delivers.

```json
{ "type": "propose", "value": "apple" }
{ "type": "echo", "sender": "n1", "value": "apple" }
{ "type": "ready", "sender": "n1", "value": "apple" }
```

Why does it work? The echo round pins the value down. If "apple" gathered 2f+1 echoes and "cherry" also did, the two groups would share an honest node, and honest nodes echo only once. So at most one value can ever trigger a ready from the echo rule.

The ready round makes delivery all-or-nothing. A node that delivers saw 2f+1 readies, so at least f+1 came from honest nodes, and those honest nodes sent their ready to everyone. Every other honest node therefore eventually sees at least f+1 readies, which by amplification makes it send its own. Then every honest node is ready, and every honest node collects 2f+1 readies and delivers. Without amplification, a liar can arrange for exactly one honest node to cross the threshold and leave the others stuck one short, as the worked example below shows.

<figure>
<svg viewBox="0 0 620 250" role="img" aria-label="Message rounds of Bracha broadcast: propose from n1, echoes among all nodes, readies among all nodes, delivery at each honest node">
  <g style="font: 13px var(--mono); fill: var(--ink)">
    <text x="10" y="44">n1</text><text x="10" y="94">n2</text><text x="10" y="144">n3</text><text x="10" y="194">n4</text>
  </g>
  <g style="stroke: var(--line)"><line x1="40" y1="40" x2="610" y2="40"/><line x1="40" y1="90" x2="610" y2="90"/><line x1="40" y1="140" x2="610" y2="140"/><line x1="40" y1="190" x2="610" y2="190"/></g>
  <g style="stroke: var(--m1); stroke-width: 2"><line x1="70" y1="40" x2="150" y2="90"/><line x1="70" y1="40" x2="150" y2="140"/><line x1="70" y1="40" x2="150" y2="190"/></g>
  <g style="stroke: var(--m2); stroke-width: 1.5"><line x1="180" y1="90" x2="280" y2="140"/><line x1="180" y1="90" x2="280" y2="190"/><line x1="180" y1="140" x2="280" y2="90"/><line x1="180" y1="140" x2="280" y2="190"/><line x1="180" y1="190" x2="280" y2="90"/><line x1="180" y1="190" x2="280" y2="140"/></g>
  <g style="stroke: var(--m3); stroke-width: 1.5"><line x1="320" y1="90" x2="430" y2="140"/><line x1="320" y1="90" x2="430" y2="190"/><line x1="320" y1="140" x2="430" y2="90"/><line x1="320" y1="140" x2="430" y2="190"/><line x1="320" y1="190" x2="430" y2="90"/><line x1="320" y1="190" x2="430" y2="140"/></g>
  <g style="fill: var(--good)"><circle cx="470" cy="90" r="6"/><circle cx="470" cy="140" r="6"/><circle cx="470" cy="190" r="6"/></g>
  <g style="fill: var(--muted); font: 13px var(--sans)">
    <text x="80" y="225">propose</text><text x="200" y="225">echo (3 to ready)</text><text x="335" y="225">ready (3 to deliver)</text><text x="490" y="225">deliver</text>
  </g>
</svg>
<figcaption>Bracha broadcast with an honest sender n1: one propose, an all-to-all echo round, an all-to-all ready round, then delivery everywhere.</figcaption>
</figure>

### Signatures and quorum certificates

Bracha broadcast never forwards anyone else's statement: each echo is counted only by the node that received it straight from its author. Many protocols need more. A node that missed some votes would like to be told "three nodes voted blue" and believe it. Over an authenticated channel that claim is worthless, because the forwarder could have invented it.

A **digital signature** fixes this. If `n2` signs the text `vote:blue` with its private key, anybody can verify that `n2` said it, no matter who delivers the bytes. A bundle of 2f+1 signed votes for the same value from distinct nodes is a **quorum certificate**: a proof, checkable by anyone, that a quorum agreed. A node that receives a valid certificate can decide without having seen a single vote itself.

```json
{ "type": "decided", "value": "blue",
  "cert": [ { "voter": "n1", "value": "blue", "sig": "1b4e09a2" },
            { "voter": "n2", "value": "blue", "sig": "77c0de13" },
            { "voter": "n4", "value": "blue", "sig": "c2b512d8" } ] }
```

Checking a certificate means checking every entry: the voter is a real node, the entry is for the claimed value, the signature verifies against that voter's key, and the voters are distinct. Three copies of one node's vote are one vote.

Two things signatures do not do. They do not stop a node from lying in its own name, and they do not stop equivocation: a traitor can sign "blue" for one peer and "red" for another. And in a network without timing guarantees they do not lower the 3f+1 bound, because the bound comes from waiting for only `n - f` replies, not from forgery.

### One round of PBFT

**PBFT** (Practical Byzantine Fault Tolerance, Castro and Liskov, 1999) orders client requests across `3f + 1` replicas. One replica, the **primary**, proposes; the others, the **backups**, check. In the normal case a request goes through three phases:

1. **Pre-prepare.** The primary assigns the request a view and sequence number and sends it to all replicas.
2. **Prepare.** A replica that accepts the pre-prepare sends `prepare` for that value to all. It accepts only a pre-prepare from the current primary, only the first one for that sequence number, and only for a request the client really signed.
3. **Commit.** A replica holding the pre-prepare plus **2f+1 matching prepares** is **prepared** and sends `commit`. On **2f+1 matching commits** the value is decided.

The client signature means a lying primary cannot invent a request. The prepare quorum means no two values can be prepared in the same view: two groups of 2f+1 prepares would share an honest replica, and an honest replica prepares only once. The commit quorum means at least f+1 honest replicas are prepared, so the decision survives later events.

If the primary is faulty, the round may simply not finish. Real PBFT then runs a **view change**: replicas time out, move to view `v + 1` with the next replica as primary, and carry over every value that might have been decided. The tasks in this track stop at one view, so with a lying primary nobody is required to decide; they only must not disagree.

## A worked example

Take the fourth reliable-broadcast test. Four nodes, `f = 1`, and the sender `n1` is the traitor. A client asks `n1` to broadcast "apple". Here is what `n1` actually does, and how the honest nodes respond. Times are approximate.

| t (ms) | Event | What the honest nodes now hold |
|---|---|---|
| 100 | `n1` sends `propose apple` to `n2` and `n3`, and `propose cherry` to `n4` | nothing yet |
| 108 | `n2`, `n3` echo apple to all; `n4` echoes cherry to all | |
| 108 | `n1` echoes apple to `n2` and `n3`, cherry to `n4` | |
| 118 | `n2` has apple echoes from `n1`, `n2`, `n3`: three, so it sends `ready apple` | `n2` ready |
| 118 | `n3` has apple echoes from `n1`, `n2`, `n3`: sends `ready apple` | `n2`, `n3` ready |
| 118 | `n4` has apple from `n2`, `n3` and cherry from `n1`, `n4`: no value reaches three | |
| 120 | `n1` sends `ready apple` to `n2` only; its readies to `n3` and `n4` are never sent | |
| 128 | `n2` holds readies from `n1`, `n2`, `n3`: three, so `n2` **delivers apple** | `n2` delivered |
| 128 | `n3` holds readies from `n2`, `n3`: two. `n4` holds readies from `n2`, `n3`: two | |
| 128 | Amplification: `n4` sees two matching readies, so it sends `ready apple` | `n4` ready |
| 138 | `n3` and `n4` now hold readies from `n2`, `n3`, `n4`: both **deliver apple** | all delivered |

The fault here is a lie by omission: `n1` sent its ready to one node and stayed silent towards the others, while proposing a different value to `n4`. Without the amplification rule, the run stops at t = 128 with `n2` delivered and `n3`, `n4` stuck at two readies forever. Agreement holds, but totality is broken: one honest node acted on a broadcast the others never see. The f+1 rule repairs it, and it is safe because two readies for apple must include one honest node, which only readied a value that already had three echoes.

Now the decisive check: could "cherry" ever have been delivered anywhere? It would need three echoes, but only `n4` and the traitor ever echoed cherry. The honest overlap of any two quorums rules it out.

## Why it is correct

All three protocols rest on one counting fact. With `n = 3f + 1` nodes, at most `f` of them faulty, and quorums of `2f + 1`:

> Any two quorums share at least `2(2f + 1) - (3f + 1) = f + 1` nodes, so at least one honest node is in both.

From this:

**Reliable broadcast, agreement.** An honest node sends ready for a value either after 2f+1 matching echoes or after f+1 matching readies. Consider the first honest node to send ready for any value; it must have used the echo rule. If two different values each reached 2f+1 echoes, some honest node echoed both, which it never does. So every honest ready, and therefore every delivery (which needs f+1 honest readies), is for the same value.

**Reliable broadcast, totality.** A delivery needs 2f+1 readies, at least f+1 of them honest, sent to all. So every honest node eventually receives f+1 readies, sends its own, and every honest node then receives readies from all `2f + 1` honest nodes. This assumes messages between honest nodes are eventually delivered; it does not assume any timing.

**Quorum certificates, agreement.** A valid certificate holds 2f+1 votes from distinct nodes with valid signatures. Two certificates for different values would share an honest voter who signed both, and an honest node votes once. Signatures guarantee nobody can put a vote in an honest node's name, so a certificate cannot be assembled from fake entries.

**PBFT round, agreement.** An honest replica decides v on 2f+1 commits; at least f+1 come from honest replicas that were prepared for v, each holding 2f+1 matching prepares. If another honest replica decided w, some honest replica would have prepared both v and w in view 0, which it never does. **Validity** follows from the client signature: an honest replica prepares only a value the client signed, and any decided value was prepared by honest replicas. **Termination** with an honest primary: every honest replica accepts its pre-prepare, all `2f + 1` honest replicas prepare and commit the same value, so each collects 2f+1 of each.

What is not guaranteed: none of this says anything if more than `f` nodes are faulty, and the PBFT round promises no progress when the primary lies. Liveness under a faulty primary needs the view change and some eventual bound on message delays.

## Trade-offs and failure modes

**More replicas.** Tolerating one fault costs four machines instead of three, and tolerating two costs seven. Each replica holds the full state, so storage and hardware cost grow accordingly.

**More messages.** Echo, ready, prepare and commit are all-to-all rounds: `O(n^2)` messages per decision. With four nodes that is trivial; with a hundred it dominates. HotStuff and its descendants route votes through the leader and aggregate signatures to bring this down to linear, at the price of an extra round.

**Signatures cost CPU.** Verifying an Ed25519 signature takes tens of microseconds, far more than a hash. PBFT's normal case uses MACs on authenticated links for speed and reserves signatures for the view change, where statements must be forwarded.

**Independence is an assumption.** `f` is a bound on how many nodes fail *at once*. Four replicas running the same binary hit the same bug together and count as four faults, not one. Real BFT deployments that care about bugs, not just attackers, need diverse implementations or at least diverse configurations; this is expensive and rarely done fully.

**A faulty primary costs time.** Even with a view change, a slow-but-not-silent primary can keep throughput low without triggering timeouts. Later protocols (Aardvark, rotating-leader designs) were built specifically to limit this.

**No protection against bad input.** Agreement says replicas agree; it does not say the agreed value is a good idea. If a client with a valid key signs a harmful request, every replica faithfully executes it. Access control is a separate problem.

**Thresholds are brittle.** Using the wrong quorum size does not fail loudly. A system that waits for `n - f` replies is correct with `n = 3f + 1` and silently unsafe with fewer nodes, as the three-node test in this track shows. A threshold of `f + 1` matching messages looks "more than the liars" and still allows disagreement, because two such groups need not share an honest node.

## In real systems

**Tendermint / CometBFT** (the consensus engine of the Cosmos ecosystem) runs a round-based protocol with a proposer, a prevote phase and a precommit phase, each requiring more than two thirds of the voting power. The structure maps closely onto PBFT's pre-prepare, prepare and commit, and a committed block carries the precommit signatures as its certificate.

**HotStuff**, designed for the Libra (later Diem) blockchain, keeps the 3f+1 bound and 2f+1 quorums but has replicas send votes to the leader, who aggregates them into a quorum certificate attached to the next proposal. This makes leader replacement as cheap as a normal round and cuts communication to linear in `n`.

**BFT-SMaRt**, a Java library used in research and in some permissioned blockchain deployments such as a Hyperledger Fabric ordering service, implements a PBFT-style protocol with view changes, state transfer and reconfiguration.

**Bitcoin** tolerates Byzantine participants in a different way. It has no fixed membership, so there are no quorums to count; instead, proof-of-work makes voting expensive, and safety is probabilistic and holds only while honest participants control most of the computing power.

## Common misconceptions

- **"Signatures make Byzantine faults easy."** They stop forgery of honest nodes' statements, not lying or equivocation. A traitor can sign two conflicting votes. Quorums with honest overlap are still needed, and without timing guarantees the 3f+1 bound still holds.
- **"A majority is enough as long as liars are a minority."** Two majorities can overlap in a single node, and that node may be the liar. With Byzantine faults the overlap needs f+1 members, which is what pushes the cluster to 3f+1.
- **"f+1 matching messages prove a value."** They prove at least one honest node sent it, which is enough to *join in* (the ready amplification rule) but not to *decide*. Deciding needs 2f+1 so that any two deciding groups share an honest node.
- **"Byzantine faults only matter for blockchains and attackers."** Corrupted memory, a flipped bit in a message, a buggy replica or a misconfigured node all produce wrong but well-formed messages. Most crash-tolerant systems answer this with checksums and assertions rather than BFT, but the failure class is real.
- **"If the primary lies, PBFT decides something wrong."** A lying primary can stall the round, or let some replicas decide while others do not, but it cannot make honest replicas decide different values or a value the client never signed.

## How the tasks build it

[BZ-1 Echo, ready, deliver](#/c/bft-reliable-broadcast) builds Bracha's reliable broadcast with authenticated links and no signatures. The tests run a silent traitor, a traitor that forges echoes and readies, a sender that splits the cluster two ways, and a sender that lets exactly one honest node collect enough readies. They check validity for honest senders, agreement and totality for every sender, and that nothing is delivered for a node that never broadcast.

[BZ-2 Signed votes and 3f+1](#/c/bft-quorum-cert) adds signatures. Nodes decide on 2f+1 signed votes and pass the decision on as a quorum certificate that others verify. The tests include a traitor that votes both ways and sends a vote with a bad signature, forged certificates (duplicate voters, signatures that do not verify) alongside a genuine one, and a cluster of only three nodes where the right answer is never to decide. Every reported decision must carry a certificate the checker can verify.

[BZ-3 A PBFT round](#/c/bft-pbft) runs pre-prepare, prepare and commit with 2f+1 quorums and client-signed requests. With an honest primary, a silent or lying backup must not stop every honest replica from deciding the client's request in time, even when the backup sends a pre-prepare of its own. With a lying primary that equivocates between two signed requests, or invents a request, honest replicas must never disagree or decide an unsigned value.

## Further reading

- [Practical Byzantine Fault Tolerance (Castro and Liskov, OSDI 1999)](https://web.archive.org/web/2024/https://pmg.csail.mit.edu/papers/osdi99.pdf): the protocol behind the third task, including the view change this track leaves out.
- [The Byzantine Generals Problem (Lamport, Shostak and Pease, 1982)](https://lamport.azurewebsites.net/pubs/byz.pdf): where the name comes from, and the argument that three generals cannot handle one traitor.
- [Living with asynchrony: Bracha's reliable broadcast (Decentralized Thoughts)](https://decentralizedthoughts.github.io/2020-09-19-living-with-asynchrony-brachas-reliable-broadcast/): a short, careful proof of the echo and ready thresholds.
- [HotStuff: BFT Consensus in the Lens of Blockchain (Yin et al., 2018)](https://arxiv.org/abs/1803.05069): how quorum certificates and a linear communication pattern simplify leader replacement.
- [CometBFT documentation](https://docs.cometbft.com/): a production protocol with proposals, prevotes and precommits that follows the same quorum logic.
