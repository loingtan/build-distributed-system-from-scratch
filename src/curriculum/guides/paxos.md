## The problem

Three servers, n1, n2 and n3, have to agree on which of two clients gets a lock. Client A asks n1 and client B asks n3, at the same moment. The obvious design is: whoever is asked decides, then tells the others. n1 decides "A" and sends that to n2 and n3. n3 decides "B" and sends that to n1 and n2. Now each node holds whichever message reached it first, or its own decision. n1 says A and n3 says B, and both clients believe they hold the lock.

Patching this by hand gets you nowhere. "Ask the others first" raises the question of what the others should say when two nodes ask at once. "Let the lowest-numbered node decide" works until n1 crashes. Then nobody decides, or someone takes over while n1 is only slow, and the original problem is back. Add lost messages and nodes that restart having forgotten what they said, and every simple rule has a run in which two nodes end up with different answers.

What we want is called **consensus**:

- **Agreement**: no two nodes ever decide different values.
- **Validity**: the decided value is one that somebody actually proposed.
- **Termination**: if the network behaves for long enough, every node eventually decides.

We also want it with no fixed leader, with any node able to start the process, and with messages that may be lost, delayed or reordered. Paxos, invented by Leslie Lamport, does exactly this. It is short to describe and easy to get subtly wrong. This guide builds it up one rule at a time and gives the reason for each rule.

## The core idea

### Three roles

Paxos describes three roles. In this track every node plays all three.

- A **proposer** suggests a value on behalf of a client and tries to get it chosen.
- An **acceptor** votes on proposals. The acceptors together form the memory of the system.
- A **learner** finds out which value was chosen.

### Majorities

The first idea is to decide by **majority**: a value is decided when more than half of all acceptors have accepted it. With five acceptors that means three. Any two majorities overlap. Three of five and another three of five must share at least one acceptor. So whatever one majority did, some member of every later majority knows about it.

A majority alone is not enough, though. Suppose each acceptor simply accepts the first value it hears. With three proposers each reaching a different acceptor first, every acceptor holds a different value, and no value can ever reach a majority. Acceptors therefore have to be able to change their minds. But if they change their minds freely, a value that already had a majority could later be replaced.

### Ballots

Paxos resolves this with numbered attempts called **ballots**. Every attempt by a proposer carries a ballot number. Ballots are unique, so two proposers never use the same one, and totally ordered. A simple scheme is `round × N + index`. With three nodes, n1 owns ballots 3, 6, 9…, n2 owns 4, 7, 10… and n3 owns 5, 8, 11…. An acceptor may accept a newer ballot after an older one. That is how a stuck attempt gets replaced. A value is **chosen** when a majority of acceptors have accepted it *in the same ballot*.

Now the danger is that ballot 5 gets value A chosen and then ballot 8 gets value B chosen. The whole of Paxos exists to prevent that. The rule is: **once a value is chosen, every higher ballot proposes that same value.** If that holds, every later majority can only ever accept A.

### Phase 1: prepare and promise

A proposer cannot know whether some value is already chosen. Learning that is not even its job. So it asks. Before proposing anything with ballot *b*, it sends `prepare(b)` to all acceptors and waits for a majority to answer. An acceptor answering `prepare(b)` does two things:

1. It **promises** never again to accept a ballot lower than *b*. It records *b* as `promised`.
2. It reports the last proposal it accepted, as `(ballot, value)`, or nothing if it has accepted none.

If the acceptor has already promised a ballot higher than *b*, it refuses.

```json
→ { "type": "prepare", "ballot": 8 }
← { "type": "prepare_ok", "ok": true, "promised": 8, "accepted": { "ballot": 5, "value": "A" } }
```

### Phase 2: accept

With promises from a majority, the proposer picks the value to propose using one rule:

> If any promise reports an accepted value, propose the value from the promise with the highest ballot. Only if none of them reports anything may the proposer use its own value.

It then sends `accept(b, value)` to the acceptors. Each acceptor accepts unless it has promised some ballot higher than *b*. When a majority accept, the value is chosen.

```json
→ { "type": "accept", "ballot": 8, "value": "A" }
← { "type": "accept_ok", "ok": true }
```

<figure>
<svg viewBox="0 0 600 250" role="img" aria-label="Proposer n1 sends prepare to n2 and n3, receives promises, then sends accept and receives acceptances">
  <g style="font: 13px var(--mono); fill: var(--ink)">
    <text x="10" y="44">n1</text><text x="10" y="124">n2</text><text x="10" y="204">n3</text>
  </g>
  <g style="stroke: var(--line)"><line x1="40" y1="40" x2="590" y2="40"/><line x1="40" y1="120" x2="590" y2="120"/><line x1="40" y1="200" x2="590" y2="200"/></g>
  <g style="stroke: var(--accent); stroke-width: 2">
    <line x1="70" y1="40" x2="140" y2="120"/><line x1="70" y1="40" x2="150" y2="200"/>
  </g>
  <g style="stroke: var(--m2); stroke-width: 2">
    <line x1="150" y1="120" x2="220" y2="40"/><line x1="160" y1="200" x2="235" y2="40"/>
  </g>
  <g style="stroke: var(--accent); stroke-width: 2">
    <line x1="300" y1="40" x2="370" y2="120"/><line x1="300" y1="40" x2="380" y2="200"/>
  </g>
  <g style="stroke: var(--good); stroke-width: 2">
    <line x1="380" y1="120" x2="450" y2="40"/><line x1="390" y1="200" x2="465" y2="40"/>
  </g>
  <g style="font: 12px var(--sans); fill: var(--muted)">
    <text x="60" y="26">prepare(6)</text><text x="185" y="26">promises</text>
    <text x="290" y="26">accept(6, v)</text><text x="430" y="26">accepted: chosen</text>
    <text x="70" y="235">phase 1</text><text x="300" y="235">phase 2</text>
  </g>
  <line x1="265" y1="30" x2="265" y2="215" style="stroke: var(--faint); stroke-dasharray: 3 4"/>
</svg>
<figcaption>The two phases. Each needs answers from a majority, counting the proposer's own acceptor. Here n1 plus either peer would do.</figcaption>
</figure>

### Why the value rule works

Suppose value A was chosen in ballot 5, so a majority M accepted (5, A). A later proposer with ballot 8 collects promises from some majority P. M and P overlap in at least one acceptor, and that acceptor reports something accepted at ballot 5 or higher. It could report a newer accepted value, from a ballot between 5 and 8. But by induction every such ballot also proposed A. So the highest-ballot value among the promises is A, and ballot 8 proposes A too. The section on correctness spells this out.

### Learning the result

When a proposer gets accepts from a majority, it knows its value is chosen. Nobody else does yet. It tells the other nodes. Messages get lost, so it keeps telling each one until that node confirms. A node that missed the announcement can also find out by proposing something itself. Its phase 1 will lead it to the chosen value. Choosing and learning are separate steps. A value can be chosen while no node knows it, for example if the proposer crashes just after the last accept arrives. That turns out to matter.

### Persistence

An acceptor's two pieces of state, `promised` and `accepted`, are promises to the rest of the cluster. If a crash wipes them, the acceptor can accept a ballot it swore to refuse, or forget a value it helped choose. Both are written to stable storage before the acceptor replies. The proposer and learner need nothing on disk. A restarted proposer just picks a fresh ballot above everything it hears of.

### Many values: Multi-Paxos

A service needs an agreed sequence of commands, not one value. **Multi-Paxos** keeps a log of **slots** and runs a separate Paxos instance in each. Doing both phases for every command would cost two round trips each time. But phase 1 doesn't depend on the value, so a single `prepare(b)` can cover every slot at once. Acceptors promise *b* for all slots and report everything they have accepted, slot by slot. The node that collects a majority of these promises becomes the **stable leader**. From then on each new command needs only phase 2, `accept(b, slot, value)`, which is one round trip.

<figure>
<svg viewBox="0 0 600 220" role="img" aria-label="A Multi-Paxos log of slots: slots 1 to 4 chosen, slot 5 a hole filled by a no-op, slot 6 accepted on a majority">
  <g style="font: 12px var(--sans); fill: var(--muted)"><text x="20" y="30">slot</text></g>
  <g style="font: 13px var(--mono); fill: var(--ink)">
    <text x="88" y="30">1</text><text x="168" y="30">2</text><text x="248" y="30">3</text><text x="328" y="30">4</text><text x="408" y="30">5</text><text x="488" y="30">6</text>
  </g>
  <g style="stroke: var(--line); fill: var(--surface)">
    <rect x="60" y="45" width="70" height="40"/><rect x="140" y="45" width="70" height="40"/><rect x="220" y="45" width="70" height="40"/><rect x="300" y="45" width="70" height="40"/>
  </g>
  <rect x="380" y="45" width="70" height="40" style="fill: var(--surface); stroke: var(--warn); stroke-dasharray: 5 4"/>
  <rect x="460" y="45" width="70" height="40" style="fill: var(--accent-soft); stroke: var(--accent)"/>
  <g style="font: 13px var(--mono); fill: var(--ink)">
    <text x="85" y="70">17</text><text x="165" y="70">4</text><text x="245" y="70">23</text><text x="325" y="70">8</text><text x="398" y="70">null</text><text x="485" y="70">31</text>
  </g>
  <g style="font: 12px var(--sans); fill: var(--muted)">
    <text x="60" y="115">chosen and known to every node</text>
    <text x="372" y="115">nobody reported it:</text><text x="372" y="131">the new leader fills a no-op</text>
    <text x="462" y="160">accepted on a majority</text><text x="462" y="176">under an old ballot:</text><text x="462" y="192">re-proposed as 31</text>
  </g>
</svg>
<figcaption>What a new leader does after phase 1. Slots reported in the promises keep their highest-ballot value; holes below the highest reported slot get a no-op, so the log can be applied in order.</figcaption>
</figure>

A new leader re-proposes, in every slot its promises mention, the accepted value with the highest ballot. Those values may already be chosen. Every slot below the highest mentioned one that nobody reported gets a **no-op**, so the log has no holes and can be applied in order. The leader sends heartbeats. A node that stops hearing them prepares a higher ballot and takes over, and the old leader steps down the first time an acceptor refuses it.

## A worked example

Three nodes. Client c1 proposes 42 at n1 and client c2 proposes 99 at n3. Ballots are `round × 3 + index`, so n1's first ballot is 3 and n3's is 5. The link between n1 and n3 is down, and partway through n1's messages stop reaching n2 as well. This is the run the *Chosen, but nobody knows* test stages.

| t (ms) | Event | State afterwards |
|---|---|---|
| 0 | c1 → n1: propose 42. n1 prepares ballot 3 at itself and n2 (n3 unreachable). | n1, n2: promised 3 |
| 10 | Promises from n1 and n2: a majority, neither has accepted anything. n1 proposes its own value. | |
| 12 | n1 sends accept(3, 42) to n2. | n1, n2: accepted (3, 42) |
| 22 | n2's acceptance reaches n1. Two of three: **42 is chosen**. n1 answers c1 `propose_ok 42`. | Only n1 knows |
| 22 | n1's "decided 42" to n2 is lost, and so is everything n1 sends from here on. | n2 has accepted 42 but doesn't know it is chosen |
| 400 | c2 → n3: propose 99. n3 prepares ballot 5 at itself and n2. | n3, n2: promised 5 |
| 410 | n2's promise says: accepted (3, 42). n3's own says nothing. | |
| 410 | **Rule:** highest accepted ballot among the promises is 3, value 42. n3 proposes 42, not 99. | |
| 420 | accept(5, 42) at n3 and n2: both accept. | n2, n3: accepted (5, 42) |
| 430 | n3 answers c2 `propose_ok 42`. Agreement holds. | |
| 1900 | The network heals. n1's retries of "decided 42" reach n2 and n3. | Every node reports 42 |

A proposer that skipped the rule at 410 would send accept(5, 99). n2 has promised 5, so it would accept, and 99 would be chosen by n2 and n3, while n1 has already told c1 that 42 won. The test catches exactly that: n1 reports 42 and n3 reports 99.

Now add a crash. Before anything is accepted, n2 promises ballot 3 to n1 and then ballot 5 to n3, and crashes the instant its promise to n3 leaves. Suppose n2 kept `promised` only in memory. It restarts with promised = 0. n1's delayed accept(3, 42) arrives, and n2 accepts it, so 42 is chosen (n1, n2). Then n3's accept(5, 99) arrives. Ballot 5 is higher than 3, so n2 accepts that too, and 99 is chosen (n2, n3). Two values chosen. With `promised` on disk, n2 refuses ballot 3. n1 retries at ballot 6, and its phase 1 then finds whatever n2 and n3 settled on.

## Why it is correct

**Claim (agreement).** If value *v* is chosen in ballot *b*, then every proposal with ballot *b′* > *b* carries the value *v*. All accepted values from *b* on are therefore *v*, and no other value can reach a majority in any ballot.

**Argument**, by induction on *b′*. Let *M* be the majority that accepted (*b*, *v*). The proposer of *b′* got promises from a majority *P* before choosing its value. *M* and *P* share an acceptor *a*.

- *a* accepted (*b*, *v*) before promising *b′*. If it had promised *b′* first, it would have refused ballot *b* < *b′*. So *a*'s report to the proposer of *b′* contains an accepted ballot of at least *b*.
- The proposer takes the highest-ballot value among its promises. That ballot *c* satisfies *b* ≤ *c* < *b′*. If *c* = *b*, the value is *v*, since only one value is ever proposed per ballot. If *b* < *c* < *b′*, the induction hypothesis says ballot *c* proposed *v*.

Either way the proposer of *b′* proposes *v*.

The argument needs these assumptions:

- Ballots are unique, so one ballot never carries two values. That's why ballots include the node index.
- Acceptors keep their promises and accepted values, even across crashes. That's why both go to disk before the reply.
- "Majority" means more than half of a fixed, known set of acceptors. Two majorities computed against different cluster sizes need not overlap.
- Messages may be lost, duplicated, delayed or reordered, and nodes may crash and restart. None of that breaks the argument, which never relies on timing.

**Validity** is direct: a proposer proposes either its client's value or a value it read from an acceptor, and that value came from some earlier proposer.

**Termination is not guaranteed.** The FLP result shows that no deterministic protocol can guarantee both agreement and termination in an asynchronous network with even one crash. Paxos keeps agreement and gives up guaranteed progress. Two proposers can pre-empt each other forever. In practice, random backoff or a stable leader makes progress overwhelmingly likely once the network is calm, which is why the tests can demand an answer within 2s.

For Multi-Paxos the same argument applies to each slot separately. A new leader re-proposes, per slot, the highest-ballot value from its promises, which is exactly what the single-decree rule demands. Filling a hole with a no-op is safe because a slot that nobody in a majority reports cannot have been chosen.

## Trade-offs and failure modes

**Messages and latency.** Single-decree Paxos costs two round trips to a majority. Multi-Paxos with a stable leader costs one round trip per command, plus heartbeats, but every command goes through the leader. The leader's network link and CPU become the bottleneck.

**Availability.** A majority must be up and connected. Five nodes tolerate two failures and three nodes tolerate one. A minority partition can do nothing, by design: if it could decide, the majority side could decide differently.

**Disk.** Every promise and every acceptance needs a durable write before the reply. Real systems batch these writes, because an fsync per message dominates latency.

**Duelling proposers.** Without a leader or backoff, competing proposers can keep invalidating each other's ballots. Leader election in Multi-Paxos is itself just phase 1, so even elections can duel. Randomised timeouts make that unlikely.

**What it does not protect against.**

- Byzantine nodes: an acceptor that lies about its accepted value breaks the argument.
- Lost disks: an acceptor restored from an old backup has effectively forgotten its promises.
- Changing the set of acceptors carelessly: if two nodes disagree about who the acceptors are, their "majorities" may not overlap.

Reconfiguration needs its own protocol, usually by agreeing on the new configuration as a log entry.

**Learning is separate.** A value can be chosen while no node knows it. Learners must be told, with retries, and nodes that fall behind need a way to catch up. Otherwise the decision exists but nobody can act on it.

**Unbounded logs.** Multi-Paxos acceptors keep accepted values per slot forever unless the log is compacted. That requires snapshots of the state machine and a way to ship them to nodes that are far behind.

## In real systems

**Google Chubby** is a lock service that ran Multi-Paxos among five replicas. The paper *Paxos Made Live* describes how far the real system was from the pseudocode: disk corruption, master leases so reads can skip consensus, group membership changes, and a large testing effort.

**Google Spanner** runs a Multi-Paxos group for every shard (split). Its leaders hold time-based leases, and a write commits once a majority of the group's replicas have logged it. Transactions across splits use two-phase commit, in which each participant is itself a Paxos group.

**Apache Cassandra** uses single-decree Paxos for its lightweight transactions (`INSERT … IF NOT EXISTS`, conditional updates). Each such operation runs prepare/promise and propose/accept among the replicas of one partition key, then commits. That is why lightweight transactions cost several round trips more than ordinary writes.

**Raft**, used by etcd and many others, is close to Multi-Paxos with a stable leader. Its terms play the role of ballots, and its election restriction replaces the "re-propose what you find" step: only a candidate whose log is already up to date can win.

## Common misconceptions

- **"The value is chosen when the proposer says so."** It is chosen when a majority accepts it in one ballot, even if the proposer crashes before counting. That is why later proposers must look for accepted values.
- **"An acceptor that accepted a value has committed to it."** One acceptor's accepted value can be replaced by a higher ballot. Only a majority in the same ballot makes a value permanent.
- **"Paxos needs a leader."** Single-decree Paxos is safe with any number of simultaneous proposers. A leader only helps it make progress and saves phase 1 in Multi-Paxos.
- **"Persisting the chosen value is enough."** The acceptor's promise and accepted value must be durable. A node that remembers only what it learned can still break a choice that was in progress when it crashed.
- **"Paxos guarantees an answer."** It guarantees safety always and progress only when the network is calm long enough. No algorithm can promise both in an asynchronous network.

## How the tasks build it

[PX-1 Single-decree Paxos](#/c/paxos-single) is the core protocol: ballots, prepare/promise, accept, and announcing the result until every node confirms. The tests race five proposers against each other, drop a fifth of all messages, and stage a run in which a value is chosen while only its proposer knows. A proposer that ignores accepted values in the promises chooses a second value.

[PX-2 Crashes and partitions](#/c/paxos-durable) makes the acceptor's promises durable. One test crashes a node the instant it answers a prepare and replays the delayed phase 2 messages in the worst order. Another restarts the whole cluster the moment the first answer goes out. A minority partition must not get in the way, and five proposers on slow, lossy links must still finish within the time limit.

[PX-3 A log of slots](#/c/paxos-multi) moves to Multi-Paxos: phase 1 once for all slots, phase 2 per command, heartbeats and leader change. The decisive test crashes the leader the instant it acknowledges an append, before anyone else knows that slot is chosen. The new leader must recover it from the accepted values.

[PX-4 Catching up](#/c/paxos-catchup) handles learning and durability for the log. Lost announcements, a leader cut off with a follower, a follower restarting with only its disk, and a full-cluster restart all leave nodes with holes they must fill. No acknowledged append may be lost.

## Further reading

- [Paxos Made Simple](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf), Leslie Lamport. The algorithm and its correctness argument in plain prose, by its inventor.
- [The Part-Time Parliament](https://lamport.azurewebsites.net/pubs/lamport-paxos.pdf), Leslie Lamport. The original paper, told as the story of a Greek parliament; worth it once you know the algorithm.
- [Paxos Made Live: An Engineering Perspective](https://research.google.com/archive/paxos_made_live.pdf), Chandra, Griesemer and Redstone. What it took to run Multi-Paxos in production for Chubby.
- [Paxos Made Moderately Complex](https://www.cs.cornell.edu/courses/cs7412/2011sp/paxos.pdf), Robbert van Renesse and Deniz Altinbuken. Complete pseudocode for Multi-Paxos with leaders, acceptors and replicas.
- [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf), Ongaro and Ousterhout. Raft, the most common alternative, with an explicit comparison to Paxos.
