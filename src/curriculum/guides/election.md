## The problem

Three nodes run a service where one of them, n1, is in charge: it decides the order of writes, hands out locks, or runs the nightly job. At 10:00:00 n1's network cable is pulled. n2 and n3 hear nothing more from it. Should one of them take over?

Suppose n2 waits one second, decides n1 is dead, and declares itself leader. At the same moment n3 reaches the same conclusion and also declares itself leader. Now two nodes accept writes, and they accept them in different orders. Or suppose n1 was not dead but paused for two seconds by a garbage collector. It wakes up, still believes it leads, and carries on beside whoever replaced it. Either way the cluster has two leaders. This is **split brain**, and it is how replicated systems corrupt data.

There are two separate problems here:

1. **Noticing** that a node has stopped responding, without raising alarms over ordinary delays and lost packets.
2. **Agreeing** on exactly one replacement, even though nodes cannot tell a dead node from a slow one, and even when the network splits the cluster in two.

This track solves the first with heartbeats and timeouts, and the second with the leader-election half of Raft. The next two tracks (Raft log replication, then a linearizable key-value store) build on the election you write here, so it is worth getting the rules exactly right.

## The core idea

### Failure detection: you can only suspect

A **heartbeat** is a small message a node sends regularly, say every 100ms, that means only "I am alive". Each node records when it last heard from each peer. A **timeout** is how long a peer may stay silent before it is treated as failed. A node whose silence has exceeded the timeout is **suspected**.

The word matters. In a network where messages can be delayed by an unknown amount, a crashed node, a slow node, and a node behind a broken link all look the same from outside: silent. No algorithm can tell them apart for certain. A **failure detector** therefore never *knows*; it suspects, and it must cope with being wrong in both directions.

<figure>
<svg viewBox="0 0 620 210" role="img" aria-label="n2 receives heartbeats from n1 every 100ms; one is lost without causing suspicion; after n1 is cut off, n2 suspects it once silence exceeds 500ms">
  <g style="font: 13px var(--mono); fill: var(--ink)"><text x="10" y="44">n1</text><text x="10" y="144">n2</text></g>
  <g style="stroke: var(--line)"><line x1="40" y1="40" x2="610" y2="40"/><line x1="40" y1="140" x2="610" y2="140"/></g>
  <g style="stroke: var(--accent); stroke-width: 2">
    <line x1="60" y1="40" x2="75" y2="140"/><line x1="110" y1="40" x2="125" y2="140"/><line x1="210" y1="40" x2="225" y2="140"/><line x1="260" y1="40" x2="275" y2="140"/>
  </g>
  <line x1="160" y1="40" x2="170" y2="100" style="stroke: var(--bad); stroke-width: 2; stroke-dasharray: 5 4"/>
  <text x="166" y="112" style="fill: var(--bad); font: 15px var(--sans)">×</text>
  <text x="140" y="170" style="fill: var(--muted); font: 12px var(--sans)">one loss: gap 150ms, fine</text>
  <rect x="290" y="28" width="320" height="24" style="fill: var(--bad); opacity: 0.12"/>
  <text x="300" y="22" style="fill: var(--bad); font: 12px var(--sans)">n1 cut off: heartbeats go nowhere</text>
  <line x1="275" y1="150" x2="525" y2="150" style="stroke: var(--warn); stroke-width: 2"/>
  <text x="330" y="168" style="fill: var(--warn); font: 12px var(--sans)">silence grows past the 500ms timeout</text>
  <rect x="525" y="128" width="85" height="24" rx="3" style="fill: var(--warn); opacity: 0.25"/>
  <text x="532" y="145" style="fill: var(--ink); font: 12px var(--sans)">suspect n1</text>
  <text x="40" y="200" style="fill: var(--muted); font: 12px var(--sans)">time →  heartbeat every 100ms</text>
</svg>
<figcaption>A single lost heartbeat leaves a 200ms gap, well under the timeout. Real silence crosses it and n1 becomes suspected.</figcaption>
</figure>

The timeout is the one number that decides how a detector behaves:

- **Too short**, and a single delayed or lost heartbeat raises a false alarm. If suspicion triggers a failover, the cluster changes leaders for nothing, and changing leaders has a cost.
- **Too long**, and a real failure goes unnoticed while requests pile up.

A good rule is several heartbeat intervals. If 10% of messages are lost independently, missing one heartbeat happens every ten intervals; missing five in a row has probability 0.1^5, one in a hundred thousand. Compute suspicion when it is needed (`now − lastHeard > timeout`) rather than storing a flag, and a peer that starts talking again stops being suspected with no extra code.

### Why suspicion is not enough to pick a leader

Suppose every node runs a perfect-looking detector and the rule is "if the leader is suspected, the lowest-numbered unsuspected node takes over". Partition the cluster into {n1, n2} and {n3, n4, n5}. Each side suspects the other. n1 leads one side and n3 leads the other. Detection gave each side a consistent view of *its own* world, and the two views disagree. Something has to make the decision on behalf of the whole cluster, and that something is a vote that needs a majority.

### Raft's election: terms, roles, votes

**Raft** is a consensus algorithm designed by Diego Ongaro and John Ousterhout to be easier to understand than Paxos. It splits into leader election (this track), log replication (the next track) and safety rules that tie them together.

Time in Raft is divided into **terms**, numbered 1, 2, 3 and so on. Each term begins with an election and has **at most one leader**. A term may have no leader at all if its election fails; then a new term starts. Every node stores a **current term**, and terms only go up. Terms act as a logical clock: a message carries its sender's term, and a smaller term than yours means the sender is out of date.

Each node is in one of three **roles**:

- **Follower**: passive. It answers requests from leaders and candidates. Every node starts as a follower.
- **Candidate**: trying to become leader in a new term.
- **Leader**: the one node in its term that sends heartbeats and (in the next track) decides the order of writes.

<figure>
<svg viewBox="0 0 620 230" role="img" aria-label="Role transitions: follower to candidate on timeout; candidate to leader on a majority of votes; candidate back to candidate on timeout; candidate or leader to follower on seeing a higher term">
  <g style="fill: var(--surface); stroke: var(--line); stroke-width: 1.5">
    <rect x="30" y="90" width="120" height="50" rx="25"/><rect x="250" y="90" width="120" height="50" rx="25"/><rect x="470" y="90" width="120" height="50" rx="25"/>
  </g>
  <g style="font: 14px var(--sans); fill: var(--ink)"><text x="62" y="120">follower</text><text x="280" y="120">candidate</text><text x="508" y="120">leader</text></g>
  <path d="M 150 105 L 248 105" style="stroke: var(--accent); stroke-width: 2"/>
  <text x="152" y="96" style="fill: var(--accent); font: 12px var(--sans)">election timeout</text>
  <path d="M 370 105 L 468 105" style="stroke: var(--good); stroke-width: 2"/>
  <text x="372" y="96" style="fill: var(--good); font: 12px var(--sans)">votes from majority</text>
  <path d="M 290 90 C 280 40, 340 40, 330 90" style="fill: none; stroke: var(--accent); stroke-width: 2"/>
  <text x="250" y="38" style="fill: var(--accent); font: 12px var(--sans)">timeout: new term, try again</text>
  <path d="M 250 128 L 152 128" style="stroke: var(--warn); stroke-width: 2"/>
  <text x="150" y="150" style="fill: var(--warn); font: 12px var(--sans)">sees current leader or higher term</text>
  <path d="M 530 140 C 530 210, 90 210, 90 142" style="fill: none; stroke: var(--warn); stroke-width: 2"/>
  <text x="250" y="222" style="fill: var(--warn); font: 12px var(--sans)">sees a higher term</text>
</svg>
<figcaption>The three roles. Every arrow into "follower" is caused by learning of a newer term, or (for a candidate) of a leader in its own term.</figcaption>
</figure>

### The rules, exactly

These are the election rules from Figure 2 of the Raft paper, the one-page summary every implementation is checked against. The log-related parts are added in the next track.

**For every node, always.** If any message you receive, request or reply, carries a term T greater than your current term: set your current term to T, forget your vote, and become a follower. If a request carries a term smaller than yours, reject it and include your term in the reply, so the sender learns it is stale.

**Followers.** Each follower runs an **election timer**. It restarts the timer when it receives a heartbeat from the leader of its current term, or when it grants a vote. If the timer fires, the follower becomes a candidate.

**Starting an election.** A candidate:

1. increments its current term,
2. votes for itself (records `votedFor = self`),
3. restarts its election timer, with a fresh random value,
4. sends a vote request to every other node.

```json
{ "type": "request_vote", "term": 4 }
{ "type": "request_vote_ok", "term": 4, "granted": true }
```

**Granting a vote.** A node receiving a vote request first applies the higher-term rule. Then it grants the vote only if the request's term equals its own current term and it has not voted for anyone else in this term (its `votedFor` is empty or already names this candidate). Granting records `votedFor` and restarts the election timer. This is **one vote per term**, first come first served.

**Winning.** A candidate that collects votes from a **majority of the whole cluster**, counting its own, becomes leader. In a cluster of N nodes a majority is ⌊N/2⌋ + 1: 2 of 3, 3 of 5, 4 of 7. A **majority quorum** is any set of nodes that large. It is a majority of all nodes, never a majority of the nodes that happen to be reachable.

**Leading.** A new leader immediately sends a heartbeat to every node and keeps sending one every interval (say 100ms). In full Raft the heartbeat is an AppendEntries message with no entries, but in this track it can be its own message type. It carries the leader's term.

**Losing.** A candidate that receives a heartbeat from a leader whose term is at least its own accepts that leader and becomes a follower. A candidate whose timer fires before it wins starts a new election in the next term.

**Ignoring stale results.** Vote replies can arrive late. Count a granted vote only if you are still a candidate in the term you asked about.

### Randomised timeouts

If every follower used the same timeout, they would all become candidates at the same instant, each vote for itself, and nobody would win: a **split vote**. They would then all time out again together, forever. Raft avoids this by drawing each election timeout at random from a range, such as 300–600ms, every time the timer is restarted. Usually one node times out first, sends its requests, and collects the votes before anyone else wakes up. If a split vote does happen, the next round's fresh random timeouts make a repeat unlikely.

The timing must satisfy *heartbeat interval ≪ election timeout*. If the timeout were shorter than the gap between heartbeats, followers would start elections against a perfectly healthy leader.

## A worked example

Five nodes, n1–n5, election timeouts drawn from 300–600ms, heartbeats every 100ms. At t=0 n1 is leader of term 1 and everyone knows it.

| t (ms) | event | state afterwards |
|---|---|---|
| 0–2000 | n1 sends heartbeats `{term: 1}` every 100ms; each one restarts the followers' timers | n1 leader of term 1 |
| 2000 | **partition**: n1 is isolated from n2–n5 | n1 still believes it leads; its heartbeats are lost |
| 2370 | n3's timer (drawn as 370ms after its last heartbeat at 2000) fires first | n3: term 2, candidate, votedFor n3, 1 vote |
| 2371 | n3 sends `request_vote {term: 2}` to n1, n2, n4, n5 | the copy to n1 is lost in the partition |
| 2373 | n2 and n5 have term 1 < 2: adopt term 2, become followers, grant; restart their timers | n2, n5 votedFor n3 |
| 2373 | n4 grants likewise, but its **reply is lost** | n4 votedFor n3 |
| 2375 | n3 receives grants from n2 and n5: 3 votes of 5 | n3 is leader of term 2; sends heartbeats |
| 2410 | n3's first heartbeat `{term: 2}` reaches n2, n4 and n5; n4 learns who won and restarts its timer again | n4 follows n3 |
| 2375–5000 | n3 leads term 2. n1 keeps sending `{term: 1}` heartbeats into the void, and keeps believing | two nodes claim leadership, but in different terms |
| 5000 | partition heals | |
| 5010 | n1's heartbeat `{term: 1}` reaches n2; n2 is at term 2 and ignores it | n1 still believes it leads |
| 5040 | n3's heartbeat `{term: 2}` reaches n1; 2 > 1, so n1 adopts term 2, clears its vote, becomes a follower, and records n3 as leader | everyone agrees: n3 leads term 2 |

Points to notice:

- Losing n4's reply did not matter: n3 needed three votes and had them. Losing n1's vote request did not matter either. A majority does not need everyone.
- Between 2375 and 5040 two nodes believed they were leader. That is allowed, because they led *different terms*. The guarantee is one leader per term. n1 could not do any harm with its stale belief: it could not reach a majority, so in the next track nothing it proposes can commit.
- n1 was a leader, so it ran no election timer and stayed at term 1. Had a *follower* been cut off instead, it could never win on its own side: its timer would fire, it would ask four peers, get no answers, and increase its term again and again. On rejoining with that high term it would force everyone else to adopt it and hold a fresh election. That disruption is real, and the Trade-offs section returns to it.
- n2 restarted its timer when it granted its vote at 2373. Had it not, it might have started its own election moments later and split the vote.

## Why it is correct

The property Raft guarantees here is **Election Safety**: at most one leader is elected in any given term.

The argument has two parts.

1. **Each node votes at most once per term.** A node grants a vote only when `votedFor` is empty or names the same candidate, and it records the vote before replying. `votedFor` is cleared only when the term increases. So within one term, a node's vote goes to one candidate.
2. **Any two majorities of the same cluster share at least one node.** Two sets that each hold more than N/2 of the N nodes have a combined size above N, so they must overlap.

Suppose two candidates, A and B, both won term 7. A's voters form a majority and B's voters form a majority, so some node voted for both A and B in term 7. That contradicts part 1. So at most one leader per term.

Two more facts make the cluster converge on that leader:

- **Terms only increase**, and a node that sees a higher term steps down. A leader from an old term therefore loses its role the moment it exchanges a message with anyone who has moved on. Combined with Election Safety, every node that has heard from the current term agrees on its leader.
- **A minority cannot elect anyone.** Votes come only from reachable nodes, and the threshold is a majority of the whole cluster. In a partition, at most one side has a majority. The other side may hold elections forever and never succeed.

What is *not* guaranteed is that an election ever succeeds. Fischer, Lynch and Paterson proved in 1985 that no deterministic algorithm can guarantee agreement in bounded time in an asynchronous network with even one crash. Raft's safety never depends on timing; its liveness does. Randomised timeouts make split votes rare, so in practice a leader emerges within a few timeouts once a majority can talk.

For the failure detector, the guarantee is weaker still and phrased in two halves. **Completeness**: a node that stays silent is eventually suspected by everyone, within the timeout. **Accuracy**: a live node is not suspected, which holds only as long as its messages arrive within the timeout. No detector can be fully complete and fully accurate on an asynchronous network; you choose the timeout to make false suspicion rare enough.

## Trade-offs and failure modes

**Failover time versus false failovers.** Detection takes at least one timeout, and an election takes another timeout plus a round trip. A 300–600ms timeout means a leader replacement in roughly half a second to a second. Lowering it speeds up failover and makes spurious elections (a GC pause, a slow disk, a network hiccup) more likely. Production systems often use timeouts of a second or more.

**A majority must be up.** With 5 nodes, the cluster survives 2 failures; with 3 nodes, 1. A cluster that has lost its majority stays leaderless by design. More nodes tolerate more failures but make every vote (and, later, every write) wait for more replies.

**Disruptive rejoiners.** A node cut off from the others keeps timing out and incrementing its term. When it returns, its high term forces the healthy leader to step down and an election to run, even though nothing was wrong. Raft's **Pre-Vote** extension fixes this: a would-be candidate first asks whether peers would vote for it, without incrementing any term, and only proceeds if a majority would.

**Stale leaders in the meantime.** A leader on the minority side of a partition still believes it leads until it hears a higher term. Anything that leader does on its own authority, such as answering a read from local state, can be stale. The next tracks deal with this: writes must reach a majority, and reads must confirm leadership first.

**Clocks barely matter, but pauses do.** Raft uses timers only to decide when to *start* an election, never for safety. A node whose clock runs fast just starts elections early. A process that pauses for several seconds, however, wakes as a stale leader and must notice the new term before doing anything that matters.

**Crashes and memory.** In this track a crash is not tested. In the next, a node that votes, crashes and restarts must remember its vote, or it can vote twice in one term and break Election Safety. That is why Raft keeps the current term and `votedFor` on disk.

## In real systems

**etcd** is the key-value store behind Kubernetes, and it runs Raft. Its defaults are a 100ms heartbeat interval and a 1000ms election timeout, and its tuning guide recommends setting the election timeout to at least ten times the round-trip time between members. etcd's Raft library implements Pre-Vote and a check-quorum option, under which a leader steps down if it has not heard from a majority recently.

**HashiCorp Consul** uses Raft among its server nodes to elect a leader and replicate its catalog. For detecting failed nodes across the whole cluster it uses a different mechanism, Serf, which is based on the SWIM gossip protocol: nodes probe random peers and ask others to probe on their behalf before declaring a node failed.

**Apache Cassandra** has no leader, but it still needs failure detection. It uses a phi accrual failure detector: instead of a fixed timeout, each node tracks the distribution of heartbeat inter-arrival times from each peer and computes a suspicion level that rises continuously with silence. A node is marked down when that level crosses a configurable threshold (`phi_convict_threshold`).

**Kubernetes** controllers that must not run twice (the scheduler, the controller manager) elect a leader through a Lease object stored in etcd. The holder renews the lease periodically; others take over only after it expires. The safety comes from etcd's consensus underneath, with lease expiry acting as the failure detector.

## Common misconceptions

- **"A timeout tells you a node has crashed."** It tells you that you have not heard from it. The node may be slow, paused, or behind a broken link, and may still be acting on its own beliefs.
- **"A majority of the nodes I can reach is enough."** On each side of a partition a node can reach a majority of the nodes it can reach. Only a majority of the whole, fixed cluster guarantees a single winner.
- **"Two nodes believing they are leader means the algorithm failed."** Raft promises one leader *per term*. An old leader in an old term is harmless as long as everything it does requires a majority that has moved on.
- **"Fixed timeouts are fine if they are long enough."** Length does not stop nodes timing out together after a leader dies. Without randomness, simultaneous candidacies split the vote repeatedly.
- **"Only requests carry terms that matter."** A higher term in a *reply* (to a heartbeat or a vote request) must also make a leader or candidate step down. Missing this is the usual cause of two nodes claiming leadership after a partition heals.

## How the tasks build it

[EL-1 Failure detector](#/c/failure-detector) is heartbeats and suspicion on their own. One test isolates a node for three seconds and requires every other node to suspect it, and only it, within 1.5s, then to withdraw suspicion after the network heals. The other drops one message in ten with nobody failed, and allows no false alarms at all: the timeout must sit between those two limits.

[EL-2 Leader election](#/c/leader-election) is the election half of Raft, observed only through each node's reported role, term and believed leader. The tests require a leader within 2s of startup and no needless re-elections on a stable network; isolate the leader of five and require the other four to replace it within 2.5s and the old leader to stand down when the network heals; and run back-to-back partitions. Throughout they check that no term ever has two leaders and no node's term goes backwards.

## Further reading

- [In Search of an Understandable Consensus Algorithm (Ongaro and Ousterhout)](https://raft.github.io/raft.pdf): the Raft paper. Section 5.2 is leader election, and Figure 2 is the summary to keep open while coding.
- [The Raft Consensus Algorithm](https://raft.github.io/): the project page, with an interactive visualisation where you can stop nodes and watch elections happen.
- [The Secret Lives of Data: Raft](https://thesecretlivesofdata.com/raft/): an animated, step-by-step walkthrough of election and replication.
- [SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf): failure detection that scales to large clusters by probing random peers instead of heartbeating everyone.
- [etcd tuning guide](https://etcd.io/docs/v3.5/tuning/): how a production Raft system chooses its heartbeat interval and election timeout, and why.
