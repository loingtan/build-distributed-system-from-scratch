## The problem

A bank keeps accounts on several machines. Node `n1` holds account `a` with 100 in it; node `n3` holds account `b`, also 100. A client asks to move 130 from `n1.a` to `n3.b`. The simplest implementation sends each node its part: "`n1`, subtract 130 from `a`" and "`n3`, add 130 to `b`".

`n3` adds 130 and replies ok; `b` is now 230. `n1` refuses, because `a` would go to −30. The client is told the transfer failed. But 130 has appeared in `b` out of nowhere: the bank's total went from 200 to 330. Sending the debit first does not help either. If `n1` subtracts and then the message to `n3` is lost, or `n3` crashes, the money leaves one account and never arrives.

The problem is not the order of the messages. Each node decides on its own whether to apply its part, so the nodes can decide differently. What is needed is one decision for the whole transfer, which every node then carries out: all parts happen, or none do. This guarantee is called **atomic commit**, and it gets harder with every kind of failure added: concurrent transfers, lost messages and partitions, and crashes that wipe a node's memory.

## The core idea

### Separate deciding from doing

The **coordinator** is the node that received the client's request and runs the protocol. The **participants** are the nodes whose data the transaction touches. The coordinator may itself be a participant. **Two-phase commit** (2PC) splits the work into two rounds:

1. **Prepare.** The coordinator sends each participant its part of the transaction and asks: can you apply this? Each participant checks and replies with a **vote**, yes or no. It changes nothing yet.
2. **Commit or abort.** If every vote is yes, the coordinator decides commit; if any vote is no, abort. It sends the decision to every participant, including those that voted no. Participants apply their moves on commit and discard them on abort.

```json
{ "type": "prepare", "txn": "t7", "moves": [{ "node": "n1", "account": "a", "delta": -30 }] }
{ "type": "prepare_ok", "vote": "yes" }
{ "type": "commit", "txn": "t7" }
```

The message names between nodes are a design choice; these are the obvious ones. What matters is that a yes vote is not a statement about the present ("I could apply this now") but a **promise** about the future ("I will be able to apply this whenever you tell me to"). The rest of the protocol is about keeping that promise.

<figure>
<svg viewBox="0 0 600 220" role="img" aria-label="Coordinator n1 sends prepare to n2 and n3, both vote yes, n1 decides commit and sends commit to both, which acknowledge">
  <g style="font: 13px var(--mono); fill: var(--ink)">
    <text x="10" y="44">n1</text><text x="10" y="124">n2</text><text x="10" y="204">n3</text>
  </g>
  <g style="stroke: var(--line)"><line x1="40" y1="40" x2="590" y2="40"/><line x1="40" y1="120" x2="590" y2="120"/><line x1="40" y1="200" x2="590" y2="200"/></g>
  <line x1="70" y1="40" x2="120" y2="120" style="stroke: var(--accent); stroke-width: 2"/>
  <line x1="70" y1="40" x2="130" y2="200" style="stroke: var(--accent); stroke-width: 2"/>
  <text x="60" y="28" style="font: 11px var(--mono); fill: var(--accent)">prepare</text>
  <line x1="130" y1="120" x2="190" y2="40" style="stroke: var(--good); stroke-width: 2"/>
  <line x1="140" y1="200" x2="210" y2="40" style="stroke: var(--good); stroke-width: 2"/>
  <text x="150" y="150" style="font: 11px var(--mono); fill: var(--good)">yes</text>
  <rect x="230" y="28" width="70" height="24" rx="4" style="fill: var(--accent-soft); stroke: var(--accent)"/>
  <text x="238" y="45" style="font: 11px var(--sans); fill: var(--ink)">decide</text>
  <line x1="310" y1="40" x2="360" y2="120" style="stroke: var(--m2); stroke-width: 2"/>
  <line x1="310" y1="40" x2="370" y2="200" style="stroke: var(--m2); stroke-width: 2"/>
  <text x="320" y="28" style="font: 11px var(--mono); fill: var(--m2)">commit</text>
  <text x="365" y="140" style="font: 11px var(--sans); fill: var(--muted)">apply</text>
  <line x1="370" y1="120" x2="430" y2="40" style="stroke: var(--line); stroke-width: 2"/>
  <line x1="380" y1="200" x2="450" y2="40" style="stroke: var(--line); stroke-width: 2"/>
  <text x="460" y="28" style="font: 11px var(--mono); fill: var(--muted)">acks</text>
  <g style="font: 11px var(--sans); fill: var(--muted)"><text x="90" y="100">phase 1</text><text x="330" y="100">phase 2</text></g>
</svg>
<figcaption>Two rounds: everyone votes, then everyone carries out one decision. Between its yes and the decision, a participant has made a promise.</figcaption>
</figure>

### Keeping the promise under concurrency

Suppose `n1.a` holds 100 and two transfers each want 60 of it. Both prepares arrive. Both see 100, both vote yes, both commit, and `a` ends at −20. Each check was right when it ran; the second one was checking money already promised to the first.

So between its yes vote and the outcome, a participant must hold on to what it promised. Two ways:

- **Locking:** a prepared transaction locks every account it touches. A second prepare that needs a locked account votes no.
- **Reservations:** money promised to prepared transactions counts as already spent when checking the next one. Two withdrawals of 30 from 100 can both be prepared; a third of 60 cannot.

Either way, the lock or reservation is released on commit and on abort. A lock that is never released shows up later as a transfer that is refused although the money is there.

What should a prepare do when it finds a lock? It could wait. But if transaction T1 holds account `x` and waits for `y`, while T2 holds `y` and waits for `x`, both wait forever: a **deadlock**. The simplest policy is **no-wait**: refuse at once and vote no. Two transactions can then never wait for each other, so deadlock cannot occur. The cost is spurious aborts under contention: a transfer that would have fit, had it waited a millisecond, is refused, and the client must retry.

### When the network fails: the coordinator may give up

Now messages get lost and nodes get cut off. Look at each side separately.

The coordinator, while waiting for votes, may treat a vote that does not arrive within a few hundred milliseconds as **no**. This is safe because, before the coordinator decides, nobody can have committed: commit requires the decision, and the decision does not exist yet. So the coordinator aborts and tells the client.

Once the coordinator has decided, the decision is final. It can tell the client immediately, without waiting for the participants to hear. Delivering the decision is a separate job, and it may take as long as a partition lasts. The coordinator keeps resending commit or abort to each participant until that participant acknowledges. Participants must therefore handle the same outcome twice without harm (apply on the first, ignore the second) and must acknowledge an abort for a transaction they never prepared, since the abort is sent to everyone, including participants whose prepare was lost or who voted no.

One more subtlety: a prepare can be delayed so long that its abort arrives first. The participant must remember the abort and refuse the late prepare; otherwise it would lock accounts for a transaction that is already over, and nobody would ever release them.

### The participant may not

A participant that has voted yes and has not heard the outcome is **in doubt**. It does not know whether the coordinator decided commit (and perhaps told the other participants, who have applied it) or abort. If it times out and aborts on its own, it may abort what another node committed. If it commits on its own, it may commit what the coordinator aborted. Neither is safe.

So the in-doubt participant waits, with its locks held, until it learns the outcome from the coordinator. No timeout can make this wait safe. This is why two-phase commit is called a **blocking** protocol: a coordinator failure at the wrong moment leaves participants stuck, and their locked accounts unusable, until the coordinator returns.

<figure>
<svg viewBox="0 0 600 220" role="img" aria-label="Coordinator n1 decides commit, sends it to n2, which applies it; the commit to n3 is lost and n1 is cut off; n3 is in doubt with its locks held">
  <g style="font: 13px var(--mono); fill: var(--ink)">
    <text x="10" y="44">n1</text><text x="10" y="124">n2</text><text x="10" y="204">n3</text>
  </g>
  <g style="stroke: var(--line)"><line x1="40" y1="40" x2="590" y2="40"/><line x1="40" y1="120" x2="590" y2="120"/><line x1="40" y1="200" x2="590" y2="200"/></g>
  <line x1="70" y1="40" x2="110" y2="120" style="stroke: var(--accent); stroke-width: 2"/>
  <line x1="70" y1="40" x2="120" y2="200" style="stroke: var(--accent); stroke-width: 2"/>
  <line x1="115" y1="120" x2="160" y2="40" style="stroke: var(--good); stroke-width: 2"/>
  <line x1="125" y1="200" x2="170" y2="40" style="stroke: var(--good); stroke-width: 2"/>
  <text x="180" y="30" style="font: 11px var(--sans); fill: var(--ink)">decide commit</text>
  <line x1="210" y1="40" x2="260" y2="120" style="stroke: var(--m2); stroke-width: 2"/>
  <text x="266" y="138" style="font: 11px var(--sans); fill: var(--good)">applied</text>
  <line x1="215" y1="40" x2="265" y2="150" style="stroke: var(--bad); stroke-width: 2; stroke-dasharray: 5 4"/>
  <text x="262" y="166" style="fill: var(--bad); font: 16px var(--sans)">×</text>
  <rect x="290" y="30" width="300" height="20" style="fill: var(--bad); opacity: 0.15"/>
  <text x="380" y="45" style="font: 11px var(--sans); fill: var(--bad)">n1 crashed or cut off</text>
  <rect x="140" y="190" width="450" height="20" style="fill: var(--warn); opacity: 0.2"/>
  <text x="300" y="205" style="font: 11px var(--sans); fill: var(--ink)">in doubt: voted yes, locks held, must wait</text>
</svg>
<figcaption>n2 has committed; n3 cannot know that. If it aborted on a timeout, the two nodes would disagree forever.</figcaption>
</figure>

### When nodes crash: write it down first

A crash erases memory. A participant that voted yes and then crashed has forgotten its promise; a coordinator that crashed has forgotten which transactions it was running and what it decided. The fix is **write-ahead logging**: before a node sends a message that others will rely on, it writes the facts behind that message to durable storage (`node.disk` in this course). The order is the whole point. A yes vote sent before the prepared moves are on disk is a promise the node may forget.

What each role must log:

- **Participant:** the moves of every transaction it voted yes to, until the outcome arrives; and its balances and committed transaction ids.
- **Coordinator:** "I started T with these participants" before the first prepare leaves; then the decision, before sending it, together with the list of participants that have not acknowledged it yet.

**Recovery** after a restart reads the log and finishes what was open:

- A transaction the coordinator **started but never decided** cannot have committed anywhere, because commit messages are only sent after the decision is logged. The coordinator aborts it and tells its participants.
- A **decided** transaction is resent to every participant that has not acknowledged it.
- A participant rebuilds its locks from its prepared transactions and keeps waiting for their outcomes. Locks themselves need not be logged; they follow from the prepared set.

A classic optimisation is **presumed abort**. If a coordinator has no record of a transaction, the answer is abort. Then aborts never need to be forced to disk or acknowledged with care; only commit decisions must be logged before they are sent. In the textbook form, an in-doubt participant asks the coordinator about a transaction, and a coordinator that finds nothing answers abort. The design in this course is coordinator-driven instead (the coordinator pushes outcomes, participants never ask), which is why it logs "started" records: after a crash, those tell it which participants it may have left prepared and must now tell to abort.

## A worked example

Coordinator `n1`, participants `n2` (account `a`, 100) and `n3` (account `b`, 100). Transaction `t7` moves 30 from `n2.a` to `n3.b`. Rpcs time out after 300 ms. The coordinator crashes after sending its decision to only one participant.

| Time (ms) | Node | Action | Disk after the step |
|---|---|---|---|
| 0 | n1 | Receives `transfer t7`. Writes started record, then sends prepare to n2 and n3. | n1: started t7 → [n2, n3] |
| 5 | n2 | 100 − 30 ≥ 0 and `a` unlocked. Writes prepared t7, locks `a`, votes yes. | n2: prepared t7 {a −30} |
| 6 | n3 | Writes prepared t7, locks `b`, votes yes. The vote is **lost**. | n3: prepared t7 {b +30} |
| 305 | n1 | Vote from n3 timed out: counted as no. Decides abort, writes it, replies `committed: false` to the client. | n1: decided t7 abort, waiting [n2, n3] |
| 306 | n1 | Sends abort to n2; n2 releases `a`, records the abort, acknowledges. | n2: aborted t7 |
| 307 | n1 | **Crashes** before the abort to n3 leaves. n3 is in doubt, `b` locked. | n1: decided t7 abort, waiting [n3] |
| 900 | n1 | Restarts. Recovery finds decided t7 with n3 still waiting; resends abort. | |
| 905 | n3 | Releases `b`, records the abort, acknowledges. | n3: aborted t7 |
| 906 | n1 | All acknowledged: deletes the decision record. | n1: (empty) |

Look at the two faults. The lost vote made the coordinator abort a transfer that could have committed. That is allowed: before the decision, aborting is always safe. The crash at 307 ms left `n3` in doubt for 600 ms. During that time `n3` must not decide on its own. Here the outcome happened to be abort, but `n3` could not know that; had both votes arrived, the decision would have been commit, `n2` would have applied it at 306 ms, and an `n3` that timed out and aborted would have destroyed 30.

Now change one detail: `n1` crashes at 3 ms, after writing the started record and sending both prepares but before any vote arrives. On restart, recovery finds t7 started and not decided. Nobody can have committed, since no commit was ever sent, so `n1` decides abort, logs it, and tells both participants, which release their locks.

## Why it is correct

The assumptions: `node.disk` survives crashes; nodes do not lie or corrupt messages; a crashed coordinator eventually restarts, and every partition eventually heals; each transfer's moves sum to zero.

**Agreement: no node commits what another aborts.** Only the coordinator decides. It decides once, writes the decision to disk before sending it, and never changes it; recovery resends the logged decision rather than deciding again. A participant applies a transaction only on receiving the coordinator's commit, and discards it only on the coordinator's abort. An in-doubt participant never decides on its own. So every participant that finishes a transaction does what the single logged decision says.

**Validity: commit only if every participant voted yes.** The coordinator decides commit only when every vote arrived and said yes. A missing vote is treated as no, which is safe because, before the decision exists, no participant can have received a commit.

**Promises are kept.** A participant votes yes only when its moves fit and it has locked the accounts involved. While the lock is held, no other transaction can be prepared on those accounts, so no other commit can change their balances. When commit arrives, the check made at prepare time still holds and no balance goes negative. The prepared moves and the yes vote are written to disk before the vote is sent, and locks are rebuilt from them after a crash, so the promise survives crashes too.

**Recovery is safe.** A started-but-undecided transaction cannot have committed anywhere: commit messages are sent only after the decision is on disk, and there is no decision on disk. Aborting it is therefore consistent with everything any participant has done.

**Conservation.** Every participant applies all of a committed transaction's moves and none of an aborted one's. Since the moves of a transfer sum to zero, the bank's total never changes.

**Liveness, with a caveat.** If the coordinator eventually restarts and the network eventually heals, every resend loop eventually gets an acknowledgement and every transaction finishes. If the coordinator never comes back, participants in doubt wait forever. 2PC guarantees safety under any failures, but progress only once the coordinator is back.

## Trade-offs and failure modes

**Blocking.** The defining weakness. While the coordinator is down or unreachable, in-doubt participants hold their locks, and every other transaction that touches those accounts is refused (with no-wait) or stuck (with waiting). A coordinator that never returns needs manual intervention.

**Latency.** A transaction costs two round trips to the slowest participant, plus forced disk writes: the coordinator's started record and decision, each participant's prepared record. Presumed abort saves some of these on the abort path; real systems batch disk writes across transactions.

**Locks held across the network.** Accounts stay locked from the vote until the outcome arrives, which is at least a round trip and possibly a partition. Under contention, throughput collapses: with no-wait, many transfers are refused; with waiting, queues form and deadlocks need detection.

**Coordinator as a single point.** Every transaction depends on one node's survival. Replicating the coordinator's log with consensus (as Spanner and Paxos Commit do) removes the blocking, at the cost of a consensus round per decision.

**Unbounded memory.** Remembering every aborted transaction id, to refuse late prepares, grows forever. Real systems bound it with transaction timestamps or epochs.

**Heuristic decisions break atomicity.** Production transaction managers let an operator force an in-doubt participant to commit or abort by hand when the coordinator is lost. That ends the blocking but can leave the system inconsistent, which is why such outcomes are reported as "heuristic hazards".

**What 2PC does not do.** It does not provide isolation by itself (the locks do), does not tolerate a participant that lies, and does not survive loss of a node's disk.

## In real systems

**PostgreSQL** implements the participant side. `PREPARE TRANSACTION 'id'` does everything needed to make the transaction committable, writes it to disk, and keeps its locks; `COMMIT PREPARED 'id'` or `ROLLBACK PREPARED 'id'` finishes it later, from any session, even after a server restart. The feature is disabled by default (`max_prepared_transactions = 0`), and the documentation warns that a prepared transaction left hanging holds locks and prevents cleanup indefinitely. The coordinator must be an external transaction manager.

**MySQL** supports the XA standard: `XA PREPARE`, `XA COMMIT` and `XA ROLLBACK`, with `XA RECOVER` listing transactions that are prepared but not finished, which is exactly what a recovering transaction manager needs to find in-doubt work.

**Google Spanner** runs two-phase commit for transactions that span several data partitions. Each participant, and the coordinator, is itself a Paxos-replicated group rather than a single machine, so the coordinator's log survives the loss of any minority of its replicas, and a single machine failure does not block transactions.

**Apache Kafka** transactions follow the same pattern. A transaction coordinator records the producer's intent to commit in its own replicated transaction log, then writes commit (or abort) markers into every partition the transaction touched, then records completion. Consumers in read-committed mode do not see a transaction's messages until its commit marker is written.

## Common misconceptions

- **"A participant that hears nothing can time out and abort, just like the coordinator."** Before the decision the coordinator knows nobody has committed; a participant that voted yes no longer knows that. Its abort could contradict a commit already applied elsewhere.
- **"Two-phase commit is a consensus protocol that tolerates failures."** It reaches agreement only while the coordinator is alive or comes back. Consensus protocols such as Paxos and Raft keep working when any minority fails; plain 2PC blocks when one node, the coordinator, does.
- **"Prepare can apply the change, and abort can undo it."** If changes are visible at prepare, other transactions can read or spend money that may vanish on abort. Prepare only checks and promises; changes happen on commit.
- **"Atomic commit also gives isolation."** Atomicity says all or nothing per transaction. Two concurrent transfers can each be atomic and still overdraw an account together. Isolation needs locks or reservations held from vote to outcome.
- **"If a node writes its log, the order relative to sending does not matter."** It matters completely. A crash between send and write leaves a message in the world that the node no longer remembers. Write first, then send.

## How the tasks build it

[TP-1 Prepare, then commit](#/c/tpc-prepare-commit) introduces atomic commit with transfers between accounts on different nodes. It exercises the two rounds, votes, and applying only on commit. The tests send transfers one at a time, then mix in overdrafts that must change nothing anywhere, and finally three-way transfers that credit two accounts, sometimes on two nodes. Every participant must apply a transaction or none, and the total must stay constant.

[TP-2 Concurrent transfers](#/c/tpc-isolation) makes transfers overlap. It exercises locking or reservations and the no-wait policy. The tests race three withdrawals of 60% of one account, cross two transfers in opposite directions, and run a burst of large transfers. No balance may ever go negative, at least one racer must win, and once things are quiet an affordable transfer must commit, which catches forgotten locks.

[TP-3 Timeouts and partitions](#/c/tpc-timeouts) adds partitions and lost messages. It exercises coordinator timeouts, blocking and in-doubt participants. The tests cut off a participant before the transaction, cut one off right after its yes vote, isolate the coordinator after its first prepare, and drop one message in five. Clients must get answers in time, and within a second of the heal every transaction must be finished and every lock released.

[TP-4 Crash recovery](#/c/tpc-recovery) adds crashes that erase memory. It exercises write-ahead logging, recovery and the logic behind presumed abort. The tests crash a participant the instant it votes yes, crash the coordinator before any vote reaches it, crash it after it has sent the decision to only one participant, and crash nodes at random under load. After restarts, every node must agree on every transaction and no money may be lost.

## Further reading

- [Consensus on Transaction Commit](https://www.microsoft.com/en-us/research/uploads/prod/2004/01/twophase-revised.pdf) — Gray and Lamport. A precise statement of 2PC, why it blocks, and Paxos Commit, which does not.
- [Designing Data-Intensive Applications, chapter 9](https://dataintensive.net/) — Kleppmann on atomic commit, in-doubt transactions and why the coordinator is a single point of blocking.
- [PostgreSQL: PREPARE TRANSACTION](https://www.postgresql.org/docs/current/sql-prepare-transaction.html) — the participant half of 2PC in a real database, and its warning about prepared transactions left hanging.
- [Transaction Management in the R* Distributed Database Management System](https://dl.acm.org/doi/10.1145/7239.7266) — Mohan, Lindsay and Obermarck, 1986. Where presumed abort and presumed commit were introduced.
- [Spanner: Google's Globally-Distributed Database](https://www.usenix.org/conference/osdi12/technical-sessions/presentation/corbett) — Corbett et al., OSDI 2012. Two-phase commit over Paxos groups, so a coordinator failure does not block.
