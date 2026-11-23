## The problem

The election track gave a cluster one leader per term. A leader is useful because it can put writes in one order. So let it: n1, leader of term 2 in a cluster of three, receives `put x = 5`, appends it to its own list of writes, replies `put_ok`, and starts copying it to n2 and n3.

Two things can go wrong right away.

First, n1 crashes before either copy arrives. n2 wins term 3. It has never heard of `x = 5`, yet the client was told the write succeeded. The write is lost.

Second, suppose n1 is not dead but cut off, and keeps accepting writes: `x = 6` at position 4 of its list, `y = 1` at position 5. Meanwhile n2, now leader of term 3, accepts `x = 9` at position 4. The partition heals. Two nodes hold different writes at position 4. If each applies its own list, the replicas disagree about `x` forever.

This track is the second half of Raft. It defines exactly when a write counts (it is **committed**), how a leader repairs a follower whose log has diverged, which candidates may win an election so that no committed write is ever lost, and what must be on disk for any of this to survive a crash. Everything from the election track still applies.

## The core idea

### A replicated log feeding a state machine

A **state machine** here means any deterministic program that takes commands one at a time and changes its state: in these tasks, a key-value store where `put x = 5` sets `x`. Deterministic means the same commands in the same order always produce the same state.

A **replicated log** is a numbered list of commands that every node stores in the same order. If every node applies the same log, in order, to its own copy of the state machine, every copy ends in the same state. That is the whole strategy: agree on the log, and the state follows.

Each **entry** in the log holds a command and the **term** in which a leader first received it. Its **index** is its position, starting at 1. An entry is identified by the pair (index, term).

The leader is the only node that adds entries. It receives a command from a client, appends an entry to its own log, and replicates it to the followers.

### AppendEntries, nextIndex and matchIndex

The leader replicates with one message type, **AppendEntries**. In Raft it carries:

```json
{
  "type": "append_entries",
  "term": 3,
  "prevIndex": 4,
  "prevTerm": 2,
  "entries": [{ "term": 3, "key": "x", "value": 9 }],
  "commit": 4
}
```

`prevIndex` and `prevTerm` identify the entry just before the new ones. `entries` may be empty, in which case the message is a heartbeat. `commit` is the leader's commit index, explained below.

The leader keeps two numbers for each follower:

- **nextIndex**: the index of the next entry to send that follower. A new leader sets it optimistically to its own last index + 1, assuming the follower is up to date, and lowers it when the follower refuses.
- **matchIndex**: the highest index known to be identical on that follower. A new leader sets it to 0, because it knows nothing yet, and raises it only from successful replies.

Do not send each entry once and hope. On every heartbeat tick and after each append, the leader sends each follower everything from its `nextIndex` on. A lost message is covered by the next send, and a follower that restarts empty is refilled the same way.

<figure>
<svg viewBox="0 0 620 250" role="img" aria-label="Leader log of 6 entries; follower n2 has 5 matching entries; follower n3 has 3 entries; nextIndex and matchIndex shown; commit index 5">
  <g style="font: 12px var(--mono); fill: var(--muted)"><text x="92" y="22">1</text><text x="142" y="22">2</text><text x="192" y="22">3</text><text x="242" y="22">4</text><text x="292" y="22">5</text><text x="342" y="22">6</text><text x="20" y="22">index</text></g>
  <g style="font: 13px var(--mono); fill: var(--ink)"><text x="10" y="55">n1 (L)</text><text x="10" y="105">n2</text><text x="10" y="155">n3</text></g>
  <g style="fill: var(--surface); stroke: var(--line)">
    <rect x="80" y="35" width="44" height="30"/><rect x="130" y="35" width="44" height="30"/><rect x="180" y="35" width="44" height="30"/><rect x="230" y="35" width="44" height="30"/><rect x="280" y="35" width="44" height="30"/><rect x="330" y="35" width="44" height="30"/>
    <rect x="80" y="85" width="44" height="30"/><rect x="130" y="85" width="44" height="30"/><rect x="180" y="85" width="44" height="30"/><rect x="230" y="85" width="44" height="30"/><rect x="280" y="85" width="44" height="30"/>
    <rect x="80" y="135" width="44" height="30"/><rect x="130" y="135" width="44" height="30"/><rect x="180" y="135" width="44" height="30"/>
  </g>
  <g style="font: 12px var(--mono); fill: var(--ink)">
    <text x="90" y="55">t1</text><text x="140" y="55">t1</text><text x="190" y="55">t1</text><text x="240" y="55">t2</text><text x="290" y="55">t3</text><text x="340" y="55">t3</text>
    <text x="90" y="105">t1</text><text x="140" y="105">t1</text><text x="190" y="105">t1</text><text x="240" y="105">t2</text><text x="290" y="105">t3</text>
    <text x="90" y="155">t1</text><text x="140" y="155">t1</text><text x="190" y="155">t1</text>
  </g>
  <line x1="327" y1="28" x2="327" y2="175" style="stroke: var(--good); stroke-width: 2; stroke-dasharray: 5 3"/>
  <text x="258" y="192" style="fill: var(--good); font: 12px var(--sans)">commit index = 5</text>
  <text x="400" y="100" style="fill: var(--accent); font: 12px var(--mono)">n2: match 5, next 6</text>
  <text x="400" y="150" style="fill: var(--accent); font: 12px var(--mono)">n3: match 3, next 4</text>
  <text x="80" y="222" style="fill: var(--ink); font: 13px var(--sans)">Index 5 is on n1 and n2: 2 of 3, a majority, and it is from the leader's term (t3).</text>
  <text x="80" y="242" style="fill: var(--ink); font: 13px var(--sans)">Index 6 is only on n1, so it is not committed yet.</text>
</svg>
<figcaption>Each box is an entry labelled with its term. The leader tracks how much of each follower matches and what to send next.</figcaption>
</figure>

### Committing and applying

An entry is **committed** once it is durable in the sense that matters for a cluster: it will never be lost or replaced, whatever happens next. The leader's **commit index** is the highest index it knows to be committed. Raft's rule (with one refinement, below) is that an entry is committed when it is stored on a **majority of the whole cluster**, counting the leader. The leader computes this from `matchIndex`: index N is committed if the leader plus the followers with `matchIndex ≥ N` number at least ⌊N/2⌋ + 1 of the nodes.

Only committed entries are **applied** to the state machine, strictly in index order, each exactly once. The leader replies to the client only after the entry is committed *and applied*, so a later read on the leader sees the write. Followers learn the commit index from the `commit` field of the next AppendEntries and apply up to it, but never past the last entry that message covered: they set their commit index to `min(commit, prevIndex + entries.length)`. Without that cap, a follower holding a stale entry at an index the leader has committed with different content could apply the wrong command.

### Log matching: the consistency check

Logs diverge when leaders change. A deposed leader may hold entries nobody else has, and its successor may have written different entries at the same indexes. Raft repairs this with one check on every AppendEntries.

The follower accepts the message only if its own log has an entry at `prevIndex` whose term is `prevTerm` (or `prevIndex` is 0). Otherwise it refuses, and the leader lowers `nextIndex` for that follower and tries again, one step further back each time, until it reaches an index where the two logs agree. From there the leader's entries overwrite whatever the follower had.

Why does checking one entry suffice? Because of the **Log Matching Property**: if two logs contain an entry with the same index and term, then the logs are identical in all entries up to and including that index. A leader creates at most one entry per index in its term, and entries never move, so the same (index, term) means the same command. And the consistency check ensures, by induction, that a follower only ever extends a log that matched the leader's at `prevIndex`. So agreement at one point implies agreement on everything before it.

When an accepted message's entries meet the follower's existing entries, the follower compares them one by one:

- Same index, **same term**: the entry is already there. Keep it.
- Same index, **different term**: a **conflict**. The follower's entry came from a deposed leader and was never committed. Delete it **and everything after it**, then append the leader's.
- Index past the end of the follower's log: append.

The second rule is **conflict resolution**, and the first is easy to get wrong. Truncating on any accepted message, rather than only on a conflict, breaks under reordering: a delayed, duplicate AppendEntries carrying entries 3–4 would cut off entries 5–7 that the follower already acknowledged and the leader already counted toward a commit.

<figure>
<svg viewBox="0 0 620 240" role="img" aria-label="Repair: leader term 3 log versus follower with conflicting term-2 entries at index 4 and 5; first attempt fails, second matches at index 3, follower truncates and appends">
  <g style="font: 12px var(--mono); fill: var(--muted)"><text x="112" y="22">1</text><text x="162" y="22">2</text><text x="212" y="22">3</text><text x="262" y="22">4</text><text x="312" y="22">5</text></g>
  <g style="font: 13px var(--mono); fill: var(--ink)"><text x="10" y="55">leader n3</text><text x="10" y="105">n1 before</text><text x="10" y="205">n1 after</text></g>
  <g style="fill: var(--surface); stroke: var(--line)">
    <rect x="100" y="35" width="44" height="30"/><rect x="150" y="35" width="44" height="30"/><rect x="200" y="35" width="44" height="30"/><rect x="250" y="35" width="44" height="30"/>
    <rect x="100" y="85" width="44" height="30"/><rect x="150" y="85" width="44" height="30"/><rect x="200" y="85" width="44" height="30"/>
    <rect x="100" y="185" width="44" height="30"/><rect x="150" y="185" width="44" height="30"/><rect x="200" y="185" width="44" height="30"/><rect x="250" y="185" width="44" height="30"/>
  </g>
  <g style="fill: var(--bad); opacity: 0.2"><rect x="250" y="85" width="44" height="30"/><rect x="300" y="85" width="44" height="30"/></g>
  <g style="stroke: var(--bad); fill: none"><rect x="250" y="85" width="44" height="30"/><rect x="300" y="85" width="44" height="30"/></g>
  <g style="font: 12px var(--mono); fill: var(--ink)">
    <text x="110" y="55">t1</text><text x="160" y="55">t1</text><text x="210" y="55">t2</text><text x="260" y="55">t3</text>
    <text x="110" y="105">t1</text><text x="160" y="105">t1</text><text x="210" y="105">t2</text><text x="260" y="105">t2</text><text x="310" y="105">t2</text>
    <text x="110" y="205">t1</text><text x="160" y="205">t1</text><text x="210" y="205">t2</text><text x="260" y="205">t3</text>
  </g>
  <text x="370" y="60" style="fill: var(--bad); font: 12px var(--sans)">1: prev (4, t3)? n1 has t2 at 4: refuse</text>
  <text x="370" y="100" style="fill: var(--good); font: 12px var(--sans)">2: prev (3, t2)? matches: accept</text>
  <text x="370" y="140" style="fill: var(--ink); font: 12px var(--sans)">index 4: t2 vs t3 conflict, so</text>
  <text x="370" y="158" style="fill: var(--ink); font: 12px var(--sans)">delete 4 and 5, append leader's 4</text>
</svg>
<figcaption>The leader backs up until the logs agree, then the follower drops its conflicting suffix. The deleted entries were never committed.</figcaption>
</figure>

### The election restriction

Repair overwrites followers with the leader's log. That is only safe if the leader already holds every committed entry. Raft guarantees it at election time.

A vote request now carries the candidate's last log index and last log term:

```json
{ "type": "request_vote", "term": 5, "lastLogIndex": 7, "lastLogTerm": 3 }
```

A voter refuses, even if it has not voted this term, unless the candidate's log is **at least as up to date** as its own. "Up to date" compares the last entries: the log whose last entry has the **later term** is more up to date; if the last terms are equal, the **longer** log is more up to date. Compare terms first. A long log full of entries from an old, deposed leader is less up to date than a short log ending in a newer term.

This is the **election restriction**. A committed entry is on a majority; a winner has votes from a majority; the two majorities share a node; and that node would have refused any candidate missing the entry.

Some nodes can now never win, which changes one timer rule. Figure 2 says a follower starts an election when its timer expires "without receiving AppendEntries from the current leader or granting a vote". Restart the timer only on those events (or when standing yourself), not merely on seeing a higher term in a vote request. Otherwise a stale node that keeps standing, and keeps losing, also keeps resetting everyone's timers, and the one node that could win never gets to stand.

### Commit only entries from your own term

Here is the refinement promised above. A leader must not decide that an entry from an *earlier* term is committed just because it is now stored on a majority. Figure 8 of the Raft paper shows why, in five nodes:

1. Term 2: n1 leads and replicates an entry at index 2 to n2 only. Then n1 crashes.
2. Term 3: n5 is elected (with votes from n3, n4 and itself), accepts a different entry at index 2, and crashes before replicating it.
3. Term 4: n1 restarts and is elected. It replicates its term-2 entry at index 2 to n3. That entry is now on n1, n2, n3: a majority.
4. If n1 counted that as committed and then crashed, n5 could win term 5. Its last entry is from term 3, newer than term 2, so n2, n3 and n4 all consider it more up to date. n5 would then overwrite index 2 everywhere with its term-3 entry, erasing an entry that had been "committed".

So the rule in Figure 2 is: the leader sets its commit index to N only if a majority has `matchIndex ≥ N` **and** the entry at N has the leader's current term. Older entries become committed indirectly: once an entry of the current term after them is committed, everything before it is committed too, by the Log Matching Property. In step 3 above, n1 is safe as soon as it commits an entry of term 4 at index 3.

A newly elected leader therefore often appends an empty **no-op** entry of its own term at once, so that earlier entries commit without waiting for a client write.

### Persistent state

Every guarantee so far rests on promises. A follower that replied "I have entry 12" let the leader count it. A node that voted in term 5 promised not to vote again in term 5. If a crash erases a promise, the guarantee goes with it.

Raft's Figure 2 names exactly three pieces of **persistent state**, to be written to stable storage before responding to any request:

- `currentTerm`: so terms never go backwards across a restart;
- `votedFor`: so a node cannot vote twice in one term;
- `log[]`: so an acknowledged entry is not lost from the majority that holds it.

Save before you send. A node that grants a vote and crashes before saving it can restart, be asked again in the same term, and grant it to someone else: two leaders in one term. A follower that acknowledges entries before they are on disk can lose them, and the majority that made them committed is no longer a majority.

Everything else is volatile and rebuilt: role (restart as follower), `commitIndex` (start at 0), `lastApplied` and the state machine (start empty and reapply as the leader announces the commit index), and the leader's `nextIndex` and `matchIndex` (reinitialised on election). The commit index does not need saving because it can always be relearned: the entries are in the log, and the current leader's next message says how far to apply.

### The rules, collected

The log-related parts of Figure 2, in the form the tasks use:

| who | rule |
|---|---|
| AppendEntries receiver | 1. Reply false if `term < currentTerm`. 2. Reply false if the log has no entry at `prevIndex` with term `prevTerm`. 3. If an existing entry conflicts with a new one (same index, different term), delete it and all that follow. 4. Append any new entries not already in the log. 5. If `commit > commitIndex`, set `commitIndex = min(commit, index of last new entry)`. |
| RequestVote receiver | Reply false if `term < currentTerm`. Grant if `votedFor` is null or the candidate, **and** the candidate's log is at least as up to date as the receiver's. |
| All servers | If `commitIndex > lastApplied`, apply the next entry. If any message carries a term greater than `currentTerm`, adopt it and become a follower. |
| Leader | On election, send heartbeats at once and reset `nextIndex` and `matchIndex`. On a client command, append to the log and reply after the entry is applied. Send each follower entries from its `nextIndex`; on success update `nextIndex` and `matchIndex`; on refusal lower `nextIndex` and retry. Commit N when a majority has `matchIndex ≥ N` and `log[N].term == currentTerm`. |

Two small deviations in these tasks: the follower's reply states its new match index (`prevIndex + entries.length`) instead of leaving the leader to infer it, and a refusal reports the follower's log length so the leader can jump back to it instead of stepping one entry at a time. Both are safe optimisations; the leader still never moves `nextIndex` backwards on a success, because replies arrive out of order.

## A worked example

Five nodes. At t=0, n1 leads term 2 and every log is `[1:t1, 2:t1, 3:t2]` with commit index 3 everywhere. Clients send puts to the leader.

| t (ms) | event | logs (index:term) and commit |
|---|---|---|
| 100 | n1 appends `4:t2 (x=6)` and sends it to all; only n2's copy arrives, then **partition** {n1, n2} / {n3, n4, n5} | n1, n2: `…3:t2 4:t2`; n3–n5: `…3:t2`. Index 4 is on 2 of 5: not committed. The client waits |
| 150 | n1 appends `5:t2 (y=1)`; reaches n2 only | n1, n2: `…4:t2 5:t2`. Still 2 of 5. Neither put is acknowledged |
| 520 | n4's election timer fires: term 3, `request_vote {term 3, lastLogIndex 3, lastLogTerm 2}` | |
| 522 | n3 and n5: last entry (3, t2) equals n4's, logs equal length: up to date. Both grant | n4 leads term 3 |
| 523 | n4 appends a no-op `4:t3` and sends `prevIndex 3, prevTerm 2` to everyone | n3, n5 accept; n4 counts 3 of 5 for index 4 (term 3): **commit 4** |
| 600 | a client retries `put x = 9` at n4; it appends `5:t3` | n3, n4, n5: `…4:t3 5:t3` |
| 610 | n3's reply for index 5 is **lost**; n5's arrives | 2 of 5 (n4, n5): not yet |
| 700 | next heartbeat resends index 5 to n3 (its `nextIndex` is still 5); n3 already has `5:t3`, keeps it, replies match 5 | 3 of 5: **commit 5**, apply, reply `put_ok` |
| 1500 | partition heals. n4's heartbeat reaches n1: term 3 > 2, so n1 steps down and becomes a follower | n1 must now be repaired |
| 1501 | n4 sends n1 everything from `nextIndex[n1] = 4` (set when n4 was elected, with its log ending at 3, and never raised since): `prevIndex 3, prevTerm 2`, entries `[4:t3, 5:t3]` | n1 has (3, t2): accept. Index 4 holds t2 where the leader has t3: a conflict, so n1 deletes 4 and 5, then appends both. n1 is `…3:t2 4:t3 5:t3` |
| 1502 | the same repair for n2 | every log is identical; commit 5 everywhere after the next heartbeat |

Here the leader's first guess was already the agreement point. With a longer leader log, n1 would first refuse and the leader would step back, as in the diagram above.

The puts `x=6` and `y=1` that n1 accepted during the partition are gone. That is correct: neither was ever committed, so neither client received `put_ok`, and both learned nothing. The lost reply at 610 only delayed a commit. It could not cause harm, because the leader resends from `nextIndex` and the follower keeps entries that already match.

Now a crash on top. At 2000, n2 grants its vote to n3 in term 4 and crashes the instant the reply is sent. It restarts at 2050. If it had saved `votedFor = n3` before replying, then when n5 asks for its vote in term 4 at 2100, n2 refuses. If it had saved after replying, the crash would have erased the vote, n2 would grant term 4 again, and n3 and n5 could both win term 4. Restarted, n2 also has commit index 0 and an empty store; the next heartbeat from the leader says `commit 5`, and n2 reapplies entries 1–5 from its saved log.

## Why it is correct

Raft's safety argument for the log rests on five properties, each listed in the paper. They build on one another.

1. **Election Safety**: at most one leader per term (from the election track: one vote per term, persisted, and overlapping majorities).
2. **Leader Append-Only**: a leader never deletes or overwrites entries in its own log; it only appends. Only followers truncate.
3. **Log Matching**: if two logs have an entry with the same index and term, they are identical up to that index. A leader makes one entry per index per term, and the consistency check makes every follower append only after agreeing at `prevIndex`.
4. **Leader Completeness**: an entry committed in some term is in the log of every leader of every later term. Sketch: the entry is on a majority; any later winner got votes from a majority; some voter V is in both. V voted only for a log at least as up to date as its own. If the winner's last term equals V's, its log is at least as long and, by Log Matching, contains V's. If it is greater, the leader that created that last entry already held the committed entry, by induction on terms.
5. **State Machine Safety**: if a node has applied the entry at index i, no node ever applies a different entry at i. A node applies only committed entries; by Leader Completeness all future leaders hold that same entry at i; followers only take entries from leaders; so every node's index i holds the same entry once committed.

The current-term commit rule is what makes step 4 hold: counting a majority of *old-term* copies is exactly the Figure 8 hole, where an entry on a majority can still be missing from a candidate whose last term is newer.

All of this assumes: messages can be lost, duplicated, delayed and reordered, but not corrupted or forged; nodes crash and restart but keep their persistent state, and do not behave maliciously; and persistent state is written before the messages that depend on it. Safety never depends on timing. Progress needs a majority of nodes up and able to talk, with message delays well under the election timeout.

## Trade-offs and failure modes

**Every write costs a majority round trip and a disk write.** The leader replies only after a majority has stored the entry, and each of them must persist it first. Write latency is roughly one network round trip to the median follower plus an fsync. Batching many entries per AppendEntries and per fsync is how real systems get throughput.

**A majority must be up.** With three of five nodes down, nothing commits; puts wait or time out. That is the intended behaviour: the alternative is accepting writes that might be lost. The tasks test exactly this.

**The leader is a bottleneck.** All writes go through one node. Systems that need more write throughput run many independent Raft groups, one per shard.

**Unbounded logs.** The log grows with every write, and a restarted node reapplies it all. The next track adds snapshots to compact it.

**Backtracking.** Stepping back one entry per refusal can take many round trips for a follower far behind; reporting the follower's log length, or the conflicting term, shortens it.

**Lying disks break everything.** If a disk acknowledges writes it has not made durable, a node can restart having forgotten a vote or acknowledged entries, and Raft's guarantees silently disappear.

**Uncommitted work is discarded.** Entries on a minority side of a partition are deleted on repair. Clients that sent them get no answer and must treat the outcome as unknown. The next track shows how client sessions make retrying such requests safe.

## In real systems

**etcd** stores Kubernetes' state in a Raft log. Its Raft library (`go.etcd.io/raft`) implements the algorithm as a pure state machine: the application must persist the entries and hard state (term, vote, commit) it is handed before sending the messages it is handed, which is the "save before send" rule turned into an API contract.

**CockroachDB** splits its key space into ranges and runs a separate Raft group for each range, typically with three or five replicas. A cluster can host very many groups at once, so it batches heartbeats between nodes rather than sending them per group.

**TiKV**, the storage layer of TiDB, does the same with regions: many Raft groups (Multi-Raft) placed and moved by a separate placement driver.

**Apache Kafka** replaced ZooKeeper with KRaft, where controller nodes replicate cluster metadata in a Raft-based log; ordinary topic partitions still use Kafka's own in-sync-replica protocol.

## Common misconceptions

- **"Stored on a majority means committed."** Only for entries of the leader's current term. An older-term entry on a majority can still be overwritten (Figure 8); it commits only once a current-term entry after it does.
- **"A follower should truncate its log to match each AppendEntries."** It truncates only from the first *conflicting* entry. Truncating on matching entries lets delayed duplicate messages erase entries the leader has already counted.
- **"The longest log wins the election."** The last term is compared first. A long log of entries from a deposed leader loses to a shorter one ending in a newer term.
- **"The commit index must be on disk."** It is volatile. A restarted node relearns it from the leader and reapplies its log; persisting it adds a write without adding safety.
- **"A deposed leader's uncommitted entries can be kept."** They were never acknowledged and are not on a majority; Raft discards them. Keeping them would put two different entries at one index on different nodes.

## How the tasks build it

[RL-1 Replicate the log](#/c/raft-replicate) adds a log and `nextIndex` to your election code. The tests read every node's log every 100ms and check that no two nodes ever disagree at an index, that logs only grow, and that every acknowledged value appears in client order. Slow links reorder and duplicate messages, and one follower restarts empty and must be refilled.

[RL-2 Commit and apply](#/c/raft-commit) delays every reply until the entry is on a majority and applied to a key-value store. One test crashes two of five nodes, which clients must not notice; another crashes three, during which nothing may be committed or acknowledged at all.

[RL-3 Repair diverged logs](#/c/raft-repair) adds `prevTerm`, conflict deletion, the election restriction and the current-term commit rule. Its scenarios cut off a leader holding uncommitted entries, bring back a stale follower with a higher term, cut off successive leaders, deliver an old AppendEntries after newer ones, and stage the Figure 8 sequence. Committed entries must never change.

[RL-4 Crash recovery](#/c/raft-persist) puts the term, the vote and the log on `node.disk`. The tests restart the leader, the whole cluster at once, and rolling groups of nodes, and crash a node the instant it sends a vote and then ask it again in the same term. No term may have two leaders and no acknowledged put may be lost.

## Further reading

- [In Search of an Understandable Consensus Algorithm (Extended Version)](https://raft.github.io/raft.pdf): sections 5.3 and 5.4 are this track; Figure 2 is the specification; Figure 8 is the old-term commit trap.
- [Students' Guide to Raft (Jon Gjengset)](https://thesquareplanet.com/blog/students-guide-to-raft/): the bugs nearly everyone writes when implementing Figure 2, from timer resets to truncation.
- [Consensus: Bridging Theory and Practice (Diego Ongaro's dissertation)](https://github.com/ongardie/dissertation): the long version, with fast log backtracking, membership changes and a fuller safety proof.
- [etcd-io/raft](https://github.com/etcd-io/raft): a widely used production implementation; its README explains the storage-before-send contract it imposes on applications.
- [The Raft Consensus Algorithm](https://raft.github.io/): visualisation, talks, and a list of implementations to compare against.
