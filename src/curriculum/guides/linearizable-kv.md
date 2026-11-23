## The problem

The Raft log track ended with a replicated log that commits puts on a majority and applies them to a key-value store on every node. A get answered from whatever store it reached. Here is what that allows, in a cluster of five where n1 leads term 2:

1. At t=1000 a client's `put x = 2` commits and is acknowledged. x was 1 before.
2. At t=1010 another client sends `get x` to n4. n4 has the entry in its log but will not learn it is committed until the next heartbeat. It answers 1.

The second client asked *after* the write finished and got the old value. Worse: at t=2000 n1 is cut off from the others. n3 wins term 3, and `put x = 3` commits there. A client still talking to n1 asks for x. n1 believes it leads, its store says 2, and it answers 2 for as long as the partition lasts.

There is a third problem, unrelated to reads. A client sends `cas x from 3 to 4`. The leader commits and applies it, and the reply is lost. The client cannot tell a lost request from a lost reply, so it retries. The retry is committed too, finds x = 4, not 3, and answers `cas_failed`. The client is told "no" about a change that happened.

This track turns the Raft log into a key-value store that behaves like a single machine: every operation takes effect exactly once, at one instant, in one order, and no read is ever stale. Then it makes reads cheap and the log small without giving any of that up. It assumes the election and log rules from the two previous tracks.

## The core idea

### Linearizability

**Linearizability** is the strongest single-object consistency guarantee. A store is linearizable if every operation appears to take effect at a single instant, its **linearization point**, somewhere between the moment the client sent the request and the moment it received the reply, and the resulting order explains every reply. In other words, the cluster behaves as if one machine held the data and handled requests one at a time.

The key word is *between*. If operation A's reply arrived before operation B was sent, A must come before B. Two operations that overlap in time may be ordered either way, but they must be ordered *some* way, and every reply must agree with that one order.

<figure>
<svg viewBox="0 0 620 220" role="img" aria-label="Client A's put x=3 completes before client B's get starts; B's get returning 2 is not linearizable">
  <g style="font: 13px var(--mono); fill: var(--ink)"><text x="10" y="54">client A</text><text x="10" y="124">client B</text></g>
  <g style="stroke: var(--line)"><line x1="80" y1="50" x2="610" y2="50"/><line x1="80" y1="120" x2="610" y2="120"/></g>
  <rect x="120" y="38" width="190" height="24" rx="4" style="fill: var(--accent-soft); stroke: var(--accent)"/>
  <text x="150" y="55" style="fill: var(--ink); font: 12px var(--mono)">put x=3 … put_ok</text>
  <circle cx="240" cy="50" r="5" style="fill: var(--accent)"/>
  <text x="200" y="30" style="fill: var(--accent); font: 11px var(--sans)">takes effect here</text>
  <rect x="360" y="108" width="190" height="24" rx="4" style="fill: var(--surface); stroke: var(--bad)"/>
  <text x="380" y="125" style="fill: var(--ink); font: 12px var(--mono)">get x … get_ok 2</text>
  <line x1="310" y1="70" x2="310" y2="150" style="stroke: var(--faint); stroke-dasharray: 4 3"/>
  <text x="250" y="168" style="fill: var(--muted); font: 12px var(--sans)">A's reply arrived before B began</text>
  <text x="80" y="198" style="fill: var(--bad); font: 13px var(--sans)">Any point inside B's interval comes after x=3, so returning 2 is a stale read: not linearizable.</text>
</svg>
<figcaption>Each bar runs from request sent to reply received. An operation's effect must fall inside its own bar.</figcaption>
</figure>

### Every operation through the log

The simplest linearizable store puts **every** operation in the log, reads included:

```json
{ "term": 3, "op": "put", "key": "x", "value": 7 }
{ "term": 3, "op": "get", "key": "x" }
{ "term": 3, "op": "cas", "key": "x", "from": 7, "to": 8 }
```

**Compare-and-set** (cas) changes a key only if it currently holds an expected value: `cas x from 7 to 8` sets x to 8 if x is 7, and fails otherwise. It is how several clients update one key safely, by reading a value, computing a new one, and retrying if someone else changed it first.

The leader appends the entry, replicates it, and replies only when it is committed **and applied**. The reply is whatever applying produced: `put_ok`, the value read, `cas_ok` or `cas_failed`. Write one `apply(entry)` function that changes the store and returns the reply; every node runs it on the same entries in the same order and computes the same results.

Why is this linearizable? The commit order of the log is the single order. Each operation's linearization point is the moment its entry is applied on the leader, which happens after the client sent it (the leader had to receive it first) and before the reply (the leader replies after applying). A get on a cut-off leader simply never commits, so it never answers stale; the client times out and tries elsewhere.

One subtlety about replies. When a leader steps down while requests are waiting, it must not tell those clients `not_leader`. Their entries are already in its log and may still be committed by the next leader. `not_leader` means "I did not take your request", which would be false. Say nothing; the client times out and knows the outcome is unknown. (The solutions in the previous track replied `not_leader` here; that is harmless for the tests there, but it is a lie this track's checker can catch.)

### Client sessions and exactly-once

Clients retry, and a retry of a non-idempotent operation like cas is dangerous. The store needs to recognise a retry and give back the original answer.

Each client picks a **client id** and numbers its requests with a **sequence number** `seq` that only grows. A retry reuses the same id and seq. Both travel in the log entry. The state machine keeps a **session table**: for each client id, the last seq it applied and the reply it produced. When a put or cas entry is applied:

- `seq` greater than the recorded one: apply it, record `{ seq, reply }`.
- `seq` equal: a duplicate. Leave the store alone and return the recorded reply.
- `seq` smaller: a stale leftover from a request the client has given up on. Leave the store alone; reply with anything or nothing.

This gives **exactly-once** semantics for writes: each request takes effect once however many copies arrive, and every copy gets the reply the first one got. Gets need no deduplication, because they change nothing. This is a form of **deduplication**, and the crucial design choice is *where* it happens.

> Deduplicate when the entry is applied, from a table that is part of the replicated state machine, not when the request arrives.

Checking on arrival fails in two ways. Two copies of a request can both be in the log before either is applied, so neither sees the other in the table. And a new leader may hold the original entry, committed by its crashed predecessor, without having applied it yet, so on arrival the retry looks new. At apply time, every node, and every future leader, sees the same entries in the same order and makes the same decision. The session table is rebuilt after a restart by applying the log, exactly like the store.

<figure>
<svg viewBox="0 0 620 200" role="img" aria-label="Log holds the original cas and its retry at indexes 7 and 9; applying index 7 records the reply; applying index 9 returns the saved reply without changing the store">
  <g style="font: 12px var(--mono); fill: var(--muted)"><text x="96" y="30">6</text><text x="176" y="30">7</text><text x="256" y="30">8</text><text x="336" y="30">9</text></g>
  <g style="fill: var(--surface); stroke: var(--line)"><rect x="60" y="40" width="76" height="44"/><rect x="140" y="40" width="76" height="44"/><rect x="220" y="40" width="76" height="44"/><rect x="300" y="40" width="76" height="44"/></g>
  <rect x="140" y="40" width="76" height="44" style="fill: var(--accent-soft); stroke: var(--accent)"/>
  <rect x="300" y="40" width="76" height="44" style="fill: var(--accent-soft); stroke: var(--accent); stroke-dasharray: 4 3"/>
  <g style="font: 11px var(--mono); fill: var(--ink)">
    <text x="66" y="58">put y=1</text><text x="66" y="74">c2 #4</text>
    <text x="146" y="58">cas x 3→4</text><text x="146" y="74">c1 #17</text>
    <text x="226" y="58">get x</text>
    <text x="306" y="58">cas x 3→4</text><text x="306" y="74">c1 #17</text>
  </g>
  <text x="400" y="56" style="fill: var(--ink); font: 12px var(--sans)">apply 7: x=3, so x:=4,</text>
  <text x="400" y="72" style="fill: var(--ink); font: 12px var(--sans)">sessions[c1] = {17, cas_ok}</text>
  <text x="60" y="120" style="fill: var(--ink); font: 12px var(--sans)">apply 9: seq 17 equals sessions[c1].seq, so leave x alone and return the saved cas_ok.</text>
  <text x="60" y="142" style="fill: var(--bad); font: 12px var(--sans)">Without the table: x is 4, not 3, so the retry answers cas_failed for a change that happened.</text>
  <text x="60" y="170" style="fill: var(--muted); font: 12px var(--sans)">Index 9 was appended by a new leader after the client retried; it may arrive before 7 is applied anywhere.</text>
</svg>
<figcaption>Two copies of one cas in the log. The decision is made when each is applied, the same way on every node.</figcaption>
</figure>

### Read-index: reads without log entries

Putting gets in the log costs a disk write on a majority and a replication round for an operation that changes nothing. Most workloads are mostly reads. But a leader cannot just answer from its own store: being leader is a belief, not a fact. A cut-off leader keeps the belief while a majority elects a successor and commits new writes.

**Read-index** lets the leader answer a get without a log entry, safely. To serve a read, the leader:

1. **Makes sure an entry of its own term is committed.** A newly elected leader holds every committed entry (Leader Completeness) but cannot yet tell which of its entries are committed, because it may only count entries of its own term. So on election it appends a **no-op entry**, an empty entry of its term that `apply` ignores. Until the no-op commits, reads wait.
2. **Records its commit index as the read's index.** Every write acknowledged before the read arrived is at or below this index.
3. **Confirms it is still leader**: a round of AppendEntries sent *after* the read arrived, answered by a majority with replies in its own term (success or failure does not matter; a reply in the same term means that follower has not moved on). If a newer leader existed, at least one of that majority would have voted for it and would answer with the higher term.
4. **Waits until the read's index has been applied**, then answers from its store.

The heartbeat round can do step 3 for every read that arrived before it was sent, so a burst of reads costs one round, not one each. If the leader steps down, its waiting reads can be answered `not_leader`, unlike waiting writes, because a read has no effect: telling the client it did not happen is true.

A **stale read** is exactly what each step prevents. Step 1 stops a new leader missing a write its predecessor acknowledged. Step 3 stops a deposed leader serving old data. Step 4 stops the leader answering before its own store has caught up with what was committed.

**Leader leases** go one step further and skip step 3, by assuming that no other node can become leader until some time after followers last heard from this one. That needs bounded clock drift and followers that refuse to vote while the lease runs. Read-index relies on messages alone.

### Snapshots and log compaction

The log grows by one entry per write and never shrinks. Yet once an entry is applied, the log is only needed to rebuild the state machine, which is small: a few keys and a session table. **Log compaction** replaces an applied prefix of the log with a **snapshot**: a copy of the state machine as it stood after some index.

```json
{ "index": 120, "term": 4, "store": { "x": 7 }, "sessions": { "c3": { "seq": 17, "reply": { "type": "cas_ok" } } } }
```

After taking it, entries 1–120 are dropped and the log starts at 121. The rules:

- Snapshot only **applied** entries, which are therefore committed, and record the index and term of the last one covered. The term is needed for the consistency check (`prevTerm` at index 120) and for vote comparisons when the log is empty.
- Include the **session table**. A node restarted from a snapshot without it would apply an old duplicate again.
- Each node compacts **independently**, for example whenever 25 applied entries have accumulated. No coordination is needed, because every node's state at index i is identical.
- After a restart, load the snapshot's store and sessions and start at `commitIndex = snapshot.index`.
- Index arithmetic changes everywhere: entry i is now at log position `i − snapshot.index − 1`, and the last index is `snapshot.index + log.length`.

A follower may need entries the leader has already discarded, after being cut off for a while. The leader then sends **InstallSnapshot**, carrying its snapshot. A follower that receives a snapshot past its own commit index replaces its store and session table with it and sets its commit index to the snapshot's index. If its log has an entry at the snapshot's index with the snapshot's term, it keeps the entries after that point (they agree with the leader by Log Matching); otherwise it discards its whole log. A snapshot at or behind the follower's commit index tells it nothing new and is ignored.

<figure>
<svg viewBox="0 0 620 200" role="img" aria-label="Leader has snapshot through index 120 and log 121 to 130; follower stopped at index 95; leader sends InstallSnapshot, then entries from 121">
  <g style="font: 13px var(--mono); fill: var(--ink)"><text x="10" y="56">leader</text><text x="10" y="146">follower</text></g>
  <rect x="90" y="36" width="240" height="32" rx="3" style="fill: var(--m2); opacity: 0.25"/>
  <rect x="90" y="36" width="240" height="32" rx="3" style="fill: none; stroke: var(--m2)"/>
  <text x="120" y="57" style="fill: var(--ink); font: 12px var(--mono)">snapshot ≤ 120 (store, sessions)</text>
  <rect x="330" y="36" width="200" height="32" style="fill: var(--surface); stroke: var(--line)"/>
  <text x="380" y="57" style="fill: var(--ink); font: 12px var(--mono)">log 121 … 130</text>
  <rect x="90" y="126" width="190" height="32" style="fill: var(--surface); stroke: var(--line)"/>
  <text x="120" y="147" style="fill: var(--ink); font: 12px var(--mono)">log 1 … 95</text>
  <text x="290" y="147" style="fill: var(--muted); font: 12px var(--sans)">nextIndex 96 ≤ 120: entries gone</text>
  <line x1="210" y1="68" x2="230" y2="124" style="stroke: var(--accent); stroke-width: 2"/>
  <text x="150" y="100" style="fill: var(--accent); font: 12px var(--mono)">install_snapshot</text>
  <line x1="420" y1="68" x2="440" y2="124" style="stroke: var(--good); stroke-width: 2"/>
  <text x="440" y="100" style="fill: var(--good); font: 12px var(--mono)">then entries from 121</text>
  <text x="90" y="188" style="fill: var(--ink); font: 12px var(--sans)">Follower: store and sessions := snapshot, commit = 120, log emptied (its entry 120 does not exist).</text>
</svg>
<figcaption>A follower too far behind for the leader's log gets the snapshot instead, then ordinary AppendEntries from the next index.</figcaption>
</figure>

## A worked example

Three nodes. n1 leads term 4, with the no-op for term 4 at index 10 committed. The store holds x = 3. Client c1 is on sequence 17. Each client waits 500ms before retrying.

| t (ms) | event | result |
|---|---|---|
| 0 | c1 sends `cas x 3→4, c1 #17` to n1 | n1 appends it at index 11 and replicates |
| 3 | n2 stores index 11 and replies; n1 now has 2 of 3, entry is from term 4: commit 11 | n1 applies: x = 4, `sessions[c1] = {17, cas_ok}` |
| 3 | n1 sends `cas_ok` to c1; the reply is **lost**. Then n1 **crashes**, before its copy of index 11 reaches n3 | c1 hears nothing. n2 has entry 11 but commit 10; n3 has only 1–10 |
| 420 | n2's timer fires: term 5. n3 compares last entries: n2 ends (11, t4), n3 ends (10, t4); same last term, n2's log is longer, so n3 grants | n2 leads term 5 |
| 421 | n2 appends a no-op at index 12 (term 5); n3 stores 11 and 12 | n2 commits 12, and with it 11. Applying 11: x = 4, session c1 = {17, cas_ok}. Applying 12: nothing |
| 500 | c1 times out and retries the **same** request, `#17`, at n3 | n3 replies `not_leader, leader: n2` |
| 505 | c1 sends `#17` to n2; n2 appends it at index 13 | |
| 508 | index 13 commits. Applying it: seq 17 equals `sessions[c1].seq` | store untouched; n2 returns the saved `cas_ok` |
| 520 | c2 sends `get x` to n2 | n2's read index is `max(commit 13, no-op 12) = 13`; it starts a heartbeat round |
| 523 | n3 answers in term 5 | n2 plus n3 is a majority: confirmed. Applied index 13 ≥ 13, answer `get_ok 4` |
| 600 | n1 restarts, with log 1–11 and term 4 on disk, commit 0, empty store | |
| 610 | a delayed copy of c1's original `#17` reaches n1, which is not leader (role follower after restart) | `not_leader` |
| 620 | n2's heartbeat reaches n1 with term 5 and `prevIndex 11` (n1's `nextIndex` has been 12 since n2's election); n1 has (11, t4) and accepts entries 12–13 | n1 adopts term 5, holds 1–13, commit 13; it reapplies 1–13 and rebuilds the same store and session table |

What each mechanism did:

- **The lost reply plus crash.** The cas had been applied on n1 and committed on a majority. The election restriction ensured the new leader held entry 11, and its no-op committed it. The client's retry became entry 13, and the session table, rebuilt on n2 by applying 11, recognised it. The client got `cas_ok`, the true answer, and x changed once.
- **The read after the election.** n2 would not answer reads until its no-op (12) was committed, and confirmed leadership after the read arrived. Had n1 been only cut off rather than crashed, a get sent to it would never be confirmed: its heartbeat round gets no replies at all. When the partition heals the replies carry term 5, n1 steps down and answers the waiting read `not_leader`, never `get_ok 3`.
- **The restart.** n1 rebuilt its store *and* its session table purely by reapplying the committed log; nothing about sessions was stored separately. With snapshots, it would load both from the snapshot and reapply only what follows.

## Why it is correct

**Writes and log reads are linearizable.** Raft's State Machine Safety gives every node the same sequence of applied entries. Define each operation's linearization point as the application of the *first* copy of its entry on the leader that answers it. That point lies after the request was sent (the entry had to be created from it) and before the reply (the reply is produced by applying). The commit order is a total order consistent with real time: if A's reply arrived before B was sent, A's entry was committed before B's entry was even created, so A precedes B. Every reply was computed by applying the operation at its position in that order, so the order explains every reply.

**Sessions give exactly-once.** The session table is a deterministic function of the applied log prefix, so all nodes agree on it at every index. A request with (client, seq) changes the store only when applied with a seq greater than the recorded one, and records it at once; every later copy has seq equal or smaller and leaves the store alone. Because a client has one request outstanding and its seq only grows, the first applied copy of each request is the one that takes effect, and a later copy with an equal seq returns the reply of that first copy. This assumes the session table is never lost, which is why it must be in snapshots.

**Read-index is linearizable.** Let the read arrive at time t, and let w be any write acknowledged before t. Then w was committed before t, by some leader of term ≤ the current term. If the leader serving the read has term T and its no-op of term T is committed, its commit index covers every entry committed in earlier terms (Leader Completeness puts them in its log; the no-op commits them). Confirmation by a majority after t proves that no leader of a term greater than T existed at the time those followers replied, so nothing could have been committed in a newer term before t. So the read index, taken at or after t, covers w; waiting until it is applied means the store reflects w. The read's linearization point is the moment of confirmation.

**Snapshots change nothing observable.** A snapshot at index i equals the state machine after applying entries 1…i, and only committed entries are folded into it, so replacing them by the snapshot preserves every node's applied state. InstallSnapshot replaces a follower's state with the leader's state at an index committed on the leader, which by State Machine Safety is the state every node has, or will have, at that index.

## Trade-offs and failure modes

**Reads through the log** cost a log append, a majority round trip and a disk write each. Simple, and always correct.

**Read-index** saves the disk write and the log growth, but still costs a round trip to a majority per batch of reads, and all reads still go to the leader. Raft's dissertation also describes serving read-index reads on followers: the follower asks the leader for the current read index, then waits until it has applied that far.

**Leases** make leader reads purely local, at the cost of trusting time. A clock that runs slow, or a process pause longer than the lease, can let an old leader serve stale reads after a new one has been elected. Production systems that use leases add safety margins and careful clock assumptions.

**Session tables grow.** One row per client that has ever written. Real systems expire sessions, which reopens the hole for a client that retries after its session was dropped. Raft's paper suggests agreeing on expiry through the log so every node expires the same sessions at the same index.

**Snapshot frequency.** Frequent snapshots keep the disk small and restarts fast but cost CPU and I/O to serialise the state; rare snapshots keep long logs. Large state machines need snapshots written incrementally or in the background (copy-on-write), not all at once inside a request handler.

**InstallSnapshot is expensive.** A lagging follower receives the entire state. For large stores this is sent in chunks, and a follower that keeps falling behind can be stuck receiving snapshots.

**Unknown outcomes remain.** A client that times out does not know whether its write happened. Sessions let it find out by retrying with the same seq; without retries, an unknown outcome stays unknown. Nothing in this design hides that from the client.

## In real systems

**etcd** offers linearizable reads by default and implements them with Raft's ReadIndex: the leader confirms its leadership with a heartbeat round and waits for the read index to be applied. Clients can instead request serializable reads, served from a member's local state without that check, which is faster and may be stale.

**HashiCorp Consul** exposes the choice to each request: a `consistent` read confirms leadership with a quorum round before answering; the `default` mode trusts the leader's lease and may be stale for a short window after a leadership change; and `stale` lets any server answer.

**CockroachDB** gives each range a leaseholder that serves reads without a Raft round, using a time-based lease plus its hybrid logical clocks to stay correct. That is the lease approach, made safe with careful clock bounds.

The **Raft paper** itself, in section 8, describes this track: client sessions with sequence numbers for exactly-once writes, a no-op at the start of each term, and leadership confirmation before answering read-only queries. Section 7 describes snapshots and InstallSnapshot.

## Common misconceptions

- **"The leader always has the latest data, so it can answer reads directly."** A leader cut off by a partition still believes it leads while a successor commits new writes. It must confirm leadership after the read arrives.
- **"Checking for duplicates when the request arrives is enough."** Two copies can both be in the log before either is applied, and a new leader may hold the original unapplied. Only a check at apply time, from replicated state, is consistent across nodes.
- **"A newly elected leader can serve reads at once, since it has every committed entry."** It has them but does not know its commit index until an entry of its own term commits. Hence the no-op.
- **"Replying `not_leader` to waiting writes on stepping down is the polite thing."** The entries may still commit under the next leader. Silence is the only true answer. For reads, `not_leader` is fine.
- **"A snapshot of the store is enough."** Anything replies depend on is part of the state machine. Leave out the session table and a restarted node applies old duplicates again.

## How the tasks build it

[KV-1 Get, put and cas through the log](#/c/kv-state-machine) turns the log into a store with put, get and cas, all through the log, answered only by the leader with the result of applying. The checker searches for an order that explains every reply on every key. The leader crashes and restarts, and a cut-off leader keeps receiving reads from one client: it must never answer them.

[KV-2 Exactly-once with client sessions](#/c/kv-sessions) adds `client_id` and `seq` to every request. Replies are dropped, a quarter of writes arrive twice, old puts are delivered again long after, a cas commits while its reply is lost and its leader crashes, and finally every node restarts at once with old requests still arriving. No write may apply twice.

[KV-3 Reads that are never stale](#/c/kv-read-index) takes gets off the log. The log may hold at most one committed entry per write sent, plus one per term, so reads must use read-index. A cut-off leader keeps receiving reads; and a leader crashes the instant it acknowledges a write, after which clients only read, starting with that key, so the new leader needs its no-op.

[KV-4 Compact the log with snapshots](#/c/kv-snapshots) keeps `node.disk` under 6,000 bytes through hundreds of writes. A follower cut off for two seconds must be brought back with InstallSnapshot and then help commit after the leader crashes; and the whole cluster restarts with old requests arriving again, so the snapshot must carry the sessions.

## Further reading

- [In Search of an Understandable Consensus Algorithm (Extended Version)](https://raft.github.io/raft.pdf): section 8 covers client sessions, the no-op entry and read-only queries; section 7 covers snapshots.
- [Linearizability: A Correctness Condition for Concurrent Objects (Herlihy and Wing, 1990)](https://www.cs.cmu.edu/~wing/publications/HerlihyWing90.pdf): the definition, and why it can be checked one object at a time.
- [Jepsen: Linearizability](https://jepsen.io/consistency/models/linearizable): a short, precise statement of the model and where it sits among the others.
- [Consensus: Bridging Theory and Practice (Ongaro's dissertation)](https://github.com/ongardie/dissertation): chapter 6 on client interaction (sessions, read-index, leases) and chapter 5 on log compaction, in much more depth than the paper.
- [etcd API guarantees](https://etcd.io/docs/v3.5/learning/api_guarantees/): how a production system states its linearizable and serializable read options.
