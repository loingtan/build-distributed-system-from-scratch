## The problem

A key-value store runs on three nodes. `n1` is the **primary**: it accepts every write. `n2` and `n3` are **backups**: they keep copies so that reads can be spread out and the data survives if `n1` fails. The replication code is two lines: when the primary applies a write, it sends the write to each backup, and a backup applies whatever it receives.

A client writes `k3 = 7` and, 5 milliseconds later, `k3 = 8`. The primary applies both and sends both. The link to `n2` is congested for a moment: the first message takes 30 ms, the second 4 ms. `n2` receives "k3 = 8", applies it, then receives "k3 = 7" and applies that. From now on `n2` answers reads of `k3` with 7, while the primary holds 8. No further message will correct it.

A second problem hides in the same design. The primary answers `write_ok` before the backups have the write. If `n1` dies a millisecond after answering, the write exists nowhere else. The client was told it succeeded.

A third: `n3` is unreachable for two seconds. Every write sent to it in that time is lost. When it comes back, it has no idea what it missed, and the primary has no idea what to resend.

This track builds replication that handles all three: ordering, the meaning of an acknowledgement, and catching up. It ends with a different arrangement of the same nodes, a chain, which gives strong reads almost for free.

## The core idea

### One node decides the order

In **primary-backup** replication (also called leader-follower), a single node decides the order of all writes. Backups never decide anything; they reproduce the primary's decisions. A client may send a write to a backup, but the backup forwards it to the primary and relays the answer. Reads can be served by any node from its local copy.

In this track the primary is fixed: `node.nodeIds[0]`. Choosing a new primary when it fails is a separate problem (leader election), handled elsewhere in the course.

The first lesson is that a backup cannot infer order from arrival. The network may reorder messages, especially when they are retried or take different paths. The primary is the only node that knows the true order, so it must put that order into the messages. It keeps a counter, and each write carries a **sequence number**: the value of the counter after incrementing it. A backup stores `{ value, seq }` per key and applies an incoming write only if its sequence number is higher than the one it holds for that key. A late message for an older write is then simply ignored.

<figure>
<svg viewBox="0 0 600 190" role="img" aria-label="The primary sends k3=7 with seq 1 and k3=8 with seq 2; the second arrives first at the backup, which then ignores the late seq 1">
  <g style="font: 13px var(--mono); fill: var(--ink)">
    <text x="10" y="54">n1</text><text x="10" y="154">n2</text>
  </g>
  <g style="stroke: var(--line)"><line x1="40" y1="50" x2="590" y2="50"/><line x1="40" y1="150" x2="590" y2="150"/></g>
  <line x1="100" y1="50" x2="380" y2="150" style="stroke: var(--m1); stroke-width: 2"/>
  <line x1="130" y1="50" x2="220" y2="150" style="stroke: var(--m2); stroke-width: 2"/>
  <text x="70" y="38" style="font: 12px var(--mono); fill: var(--ink)">k3=7 seq 1</text>
  <text x="150" y="38" style="font: 12px var(--mono); fill: var(--ink)">k3=8 seq 2</text>
  <text x="190" y="172" style="font: 12px var(--mono); fill: var(--good)">apply: 8 (seq 2)</text>
  <text x="350" y="172" style="font: 12px var(--mono); fill: var(--muted)">1 &lt; 2: ignore</text>
</svg>
<figcaption>The messages overtake each other. Sequence numbers let the backup recognise the late one and keep 8.</figcaption>
</figure>

### Asynchronous replication and lag

With **asynchronous replication**, the primary applies a write, replies `write_ok`, and only then sends it to the backups. Writes are fast: the client waits for one node only. The cost is that backups trail behind. The delay between a write on the primary and the same write on a backup is **replication lag**. During that window a read from a backup returns an older value. And if the primary fails permanently within that window, the write is lost even though it was acknowledged.

Two properties still hold with sequence numbers in place. A node never goes back to an older value for a key, because it only ever replaces a value with a higher-numbered one. And once writes stop, every node converges on the primary's data: the highest sequence number for each key is the primary's last write to it, and it eventually arrives.

### Synchronous replication and its price

**Synchronous replication** changes what `write_ok` promises: every backup has applied the write. The primary sends the write to each backup as a request, waits for each to confirm, and replies only after all have. A read sent to any node after `write_ok` now returns that value or a newer one. A client that writes through one node and reads through another sees its own write; this guarantee is called **read-your-writes**.

The price is **availability**, the ability to answer requests successfully. If one backup is unreachable, the primary cannot keep its promise. It must not wait forever (the client gets no answer) and it must not pretend (the promise would be false). It retries for a bounded time, then replies with an explicit error:

```json
{ "type": "error", "code": "unavailable" }
```

Adding backups makes this worse, not better: every extra backup is one more node whose absence blocks every write.

Lost messages are handled by retrying. A backup that did not confirm may never have received the request, or its confirmation was lost; the primary cannot tell which. So it resends, and the backup's apply-if-higher check makes a duplicate or a late copy harmless. One subtlety: an `unavailable` error does not mean "not applied". The primary and some backups may already hold the write. It means "the promise could not be kept", and a later read may still see the value.

<figure>
<svg viewBox="0 0 600 220" role="img" aria-label="Asynchronous: the primary replies before replicating. Synchronous: the primary replies after both backups confirm">
  <g style="font: 12px var(--sans); fill: var(--muted)"><text x="40" y="18">asynchronous</text><text x="330" y="18">synchronous</text></g>
  <g style="font: 12px var(--mono); fill: var(--ink)">
    <text x="10" y="54">c</text><text x="10" y="114">n1</text><text x="10" y="174">n2</text>
    <text x="300" y="54">c</text><text x="300" y="114">n1</text><text x="300" y="174">n2</text>
  </g>
  <g style="stroke: var(--line)">
    <line x1="30" y1="50" x2="280" y2="50"/><line x1="30" y1="110" x2="280" y2="110"/><line x1="30" y1="170" x2="280" y2="170"/>
    <line x1="320" y1="50" x2="590" y2="50"/><line x1="320" y1="110" x2="590" y2="110"/><line x1="320" y1="170" x2="590" y2="170"/>
  </g>
  <line x1="50" y1="50" x2="80" y2="110" style="stroke: var(--accent); stroke-width: 2"/>
  <line x1="90" y1="110" x2="120" y2="50" style="stroke: var(--good); stroke-width: 2"/>
  <text x="96" y="40" style="font: 11px var(--mono); fill: var(--good)">write_ok</text>
  <line x1="95" y1="110" x2="190" y2="170" style="stroke: var(--m2); stroke-width: 2"/>
  <text x="150" y="196" style="font: 11px var(--sans); fill: var(--muted)">lag</text>
  <line x1="340" y1="50" x2="370" y2="110" style="stroke: var(--accent); stroke-width: 2"/>
  <line x1="375" y1="110" x2="420" y2="170" style="stroke: var(--m2); stroke-width: 2"/>
  <line x1="430" y1="170" x2="475" y2="110" style="stroke: var(--m2); stroke-width: 2"/>
  <text x="438" y="196" style="font: 11px var(--mono); fill: var(--muted)">replicate_ok</text>
  <line x1="485" y1="110" x2="515" y2="50" style="stroke: var(--good); stroke-width: 2"/>
  <text x="490" y="40" style="font: 11px var(--mono); fill: var(--good)">write_ok</text>
</svg>
<figcaption>Asynchronous replication answers after one node; synchronous waits for the slowest backup.</figcaption>
</figure>

### A log tells a backup what it is missing

A per-key sequence number tells a backup whether one write is newer than another. It cannot tell anybody what is missing. If backup `n3` holds `k1` at seq 40 and `k2` at seq 12, did it miss seq 13 to 39? Maybe they were all writes to `k1`; maybe not.

A **replication log** solves this. The primary numbers every write in a single sequence and keeps them in an array, in order: entry 0, entry 1, entry 2, … The index of an entry is its sequence number. A backup keeps the same array. Now one number describes a backup's whole state: a backup that has 17 entries has exactly entries 0 to 16. The primary knows to send entry 17 next.

The protocol becomes a loop per backup on the primary:

1. Send `{ from, entries }`: a batch of entries starting at index `from`, beginning with what the primary believes the backup has.
2. The backup appends only the entries that extend its log exactly. If `from` is beyond its length there would be a gap, so it appends nothing. If the batch overlaps what it has, it skips the overlap.
3. The backup always replies with `have: log.length`.
4. On a reply, the primary sets its belief to `have` and continues while that is less than its own length. On no reply (lost, partitioned, crashed), it simply asks again.

The reply carries the truth, so the primary's belief never has to be right in advance. After a restart the primary can start from 0 for everyone; the first reply corrects it.

### Durability and crash recovery

**Durability** means an acknowledged write survives crashes. In this course, a crash erases every variable; only `node.disk` survives. So the log lives on disk, and the in-memory store is rebuilt at startup by replaying the log from the beginning. That replay is the whole of **crash recovery**: the node returns with exactly the state it had applied, answers reads immediately, and reports its true `have` to the primary.

Order matters. The primary must append the write to its on-disk log before it replies `write_ok`. A backup must have an entry on disk before it counts it in `have`. Anything acknowledged before it is durable is a promise the node can forget.

### Chain replication

**Chain replication** (van Renesse and Schneider, 2004) arranges the same nodes in a line. Writes enter at the **head**, which assigns them positions in its log, and flow node by node to the **tail**. Each node talks only to its successor. The tail is the last to receive a write, so the tail acknowledges it to the client. Reads go to the tail and are answered from its copy.

<figure>
<svg viewBox="0 0 600 170" role="img" aria-label="Chain of three: a write enters at head n1, passes to n2, then tail n3, which sends write_ok to the client; reads go to the tail">
  <g style="font: 13px var(--mono); fill: var(--ink)">
    <rect x="130" y="60" width="80" height="44" rx="6" style="fill: var(--surface); stroke: var(--accent)"/><text x="150" y="87">n1 head</text>
    <rect x="270" y="60" width="80" height="44" rx="6" style="fill: var(--surface); stroke: var(--line)"/><text x="300" y="87">n2</text>
    <rect x="410" y="60" width="80" height="44" rx="6" style="fill: var(--surface); stroke: var(--good)"/><text x="428" y="87">n3 tail</text>
    <text x="20" y="87">client</text>
  </g>
  <line x1="70" y1="82" x2="130" y2="82" style="stroke: var(--accent); stroke-width: 2"/>
  <line x1="210" y1="82" x2="270" y2="82" style="stroke: var(--accent); stroke-width: 2"/>
  <line x1="350" y1="82" x2="410" y2="82" style="stroke: var(--accent); stroke-width: 2"/>
  <path d="M450 104 Q 260 160 50 100" style="fill: none; stroke: var(--good); stroke-width: 2"/>
  <text x="230" y="150" style="font: 12px var(--mono); fill: var(--good)">write_ok from tail</text>
  <path d="M60 70 Q 260 10 450 60" style="fill: none; stroke: var(--m2); stroke-width: 2; stroke-dasharray: 5 4"/>
  <text x="230" y="30" style="font: 12px var(--mono); fill: var(--m2)">read at tail</text>
</svg>
<figcaption>Writes flow head to tail and are acknowledged by the tail; reads are answered by the tail alone.</figcaption>
</figure>

The key property: everything the tail has, every node has. A write reaches the tail last, so if the tail has it, so does every predecessor. That makes reads at the tail **linearizable**: as soon as a write is acknowledged, every later read sees it, and a read never returns a write that could still vanish. There is no quorum and no coordination at read time. Each hop uses the same log-and-`have` loop as catch-up, so a lost or reordered message between neighbours is repaired the same way.

## A worked example

Primary `n1`, backups `n2` and `n3`, asynchronous replication with a log (the catch-up design). Batches hold up to 50 entries; an unanswered request times out after 100 ms. `n3` is cut off from 1000 ms to 2500 ms.

| Time (ms) | Event | n1 log length | n1's belief: n2 / n3 | n2 / n3 log length |
|---|---|---|---|---|
| 900 | Write `k1 = a` arrives at n1. Appended to disk as entry 40, `write_ok`. Senders start. | 41 | 40 / 40 | 40 / 40 |
| 905 | n2 receives `{from: 40, entries: [k1=a]}`, appends, replies `have: 41`. | 41 | 41 / 40 | 41 / 40 |
| 906 | n3 receives the same batch, appends, but its reply is lost. | 41 | 41 / 40 | 41 / 41 |
| 1000 | Partition: n3 cut off. n1's retry to n3 (from 40) is dropped. | 41 | 41 / 40 | 41 / 41 |
| 1000–2500 | 30 more writes (entries 41–70). n2 follows within milliseconds. Every request to n3 times out; n1 keeps retrying from 40. | 71 | 71 / 40 | 71 / 41 |
| 1700 | A client reads `k1` from n3: answered from its own copy, `a`. Reads of keys written after 1000 return older values (lag). | | | |
| 2500 | Partition heals. | | | |
| 2510 | n3 receives `{from: 40, entries: [40..70]}`. It has 41 entries, so it skips entry 40 and appends 41–70. Replies `have: 71`. | 71 | 71 / 71 | 71 / 71 |

The lost acknowledgement at 906 ms made `n1` believe `n3` was one entry further behind than it was. That cost nothing: the backup skipped the entry it already had and reported its true length. Had the primary crashed instead, at 1500 ms say, it would restart with its log from disk (51 entries by then), rebuild its store, set its belief to 0 for both backups, and the first round of replies would correct that to 51 for `n2` and 41 for `n3`.

Compare synchronous replication in the same partition: every write between 1000 and 2500 ms would have been answered `unavailable` after about 500 ms, because `n3` could not confirm. The data would have stayed consistent everywhere, and nobody could have written.

## Why it is correct

The fixed-primary assumption runs through everything: one node assigns all order, and it never changes. Messages can be lost, delayed, duplicated and reordered, but not corrupted.

**Asynchronous, per-key sequence numbers.** For each key, the primary's sequence numbers increase strictly with each write. A node replaces a key's value only with one carrying a higher number, so the sequence of values a node holds for a key follows the primary's order with some skipped: it never goes back. The primary keeps sending, and (on a connected network) the highest-numbered write for each key eventually arrives, so every node converges on the primary's last value.

**Log replication: the prefix invariant.** At all times, every backup's log is a prefix of the primary's log. Proof sketch: a backup appends entry i only when its length is exactly i and the entry comes from a batch the primary built from its own log at index i. The primary never rewrites an entry. So entry i on the backup equals entry i on the primary. Because the backup's state is the replay of its log, it equals the primary's state at some earlier point: the backup may be behind, but never wrong and never out of order. The sender loop runs until the backup's reported length equals the primary's, and each successful round strictly increases it, so the backup catches up whenever it is reachable long enough.

**Durability.** The primary appends to `node.disk` before replying; a backup appends before reporting `have`. A crash therefore loses nothing that was acknowledged, and replaying the log at startup restores exactly the applied state.

**Synchronous replication.** The primary replies `write_ok` only after every backup confirmed applying that write (or a later one for the key). Any read that starts after `write_ok` is answered from a copy that already has it.

**Chain replication.** By the same prefix argument applied hop by hop, each node's log is a prefix of its predecessor's, so the tail's log is a prefix of every node's log. The moment a write is appended at the tail is a single instant; the tail acknowledges after it, and reads at the tail see exactly the writes appended there before. That instant is the write's linearization point, and the read's is the moment the tail answers. Every acknowledged write is on every node, and the tail never shows a write that is not.

## Trade-offs and failure modes

| | Write latency | A backup down | Primary dies for good | Reads from backups |
|---|---|---|---|---|
| Asynchronous | one node | writes continue | lag window is lost | may be stale |
| Synchronous (all) | slowest backup | writes fail | nothing acknowledged lost | current |
| Chain | sum of hops | chain stalls until reconfigured | nothing acknowledged lost | tail only |

**Failover is the hard part this track skips.** With a fixed primary, a dead primary means no writes. Real systems promote a backup, and then must make sure the old primary, if it was only partitioned, stops accepting writes; otherwise two primaries assign conflicting sequence numbers (**split brain**). That needs leader election with terms or a configuration service. With asynchronous replication, the promoted backup may lack acknowledged writes, which are then lost or must be discarded from the old primary when it returns.

**Synchronous replication couples availability to every backup.** Real systems wait for one backup, or any k of n, or a majority, which weakens "every node has it" to "at least k nodes have it". Reads from a backup that was not among the k are again possibly stale.

**Chain replication needs reconfiguration.** A failed head, middle node or tail must be removed and the chain relinked, which requires an external master that everyone trusts. Reads all go to one node, the tail, which limits read throughput; CRAQ lets any node serve clean reads.

**Logs grow.** Replaying a log of millions of entries on every restart is slow, and a backup that was away for a week may need entries that were deleted. Real systems take snapshots and ship a snapshot plus the log suffix.

**"On disk" is not free.** In this course `node.disk` is durable the instant a line runs. A real disk write is durable only after an fsync, which costs milliseconds; systems batch writes into one fsync (group commit) to amortise it.

## In real systems

**PostgreSQL** streaming replication ships the write-ahead log (WAL) from the primary to standbys, which replay it. Each position in the WAL is a log sequence number (LSN), which plays the role of `have` here. Replication is asynchronous by default. With `synchronous_standby_names` the primary waits for some standbys before a commit returns; the setting can name a priority list (`FIRST n`) or a quorum (`ANY n`), and `synchronous_commit` chooses how far the standby must get (received, flushed or applied). A standby that falls behind catches up from the primary's WAL or from the WAL archive.

**Redis** replicates asynchronously. Each replica tracks a replication offset into the primary's stream; after a short disconnection it asks the primary to continue from that offset (partial resynchronisation), which works if the missing bytes are still in the primary's backlog buffer, and otherwise falls back to a full copy. The `WAIT` command lets a client block until a given number of replicas have acknowledged its writes, without making replication itself synchronous.

**MySQL** semisynchronous replication makes the primary wait, at commit, until at least one replica acknowledges receiving the transaction. If no replica responds within a timeout, the primary falls back to asynchronous replication: an explicit choice of availability over the stronger promise.

**Chain replication** is used in research and production key-value stores, including FAWN-KV and Hibari, and its CRAQ variant spreads reads across all nodes of the chain while keeping them strongly consistent.

## Common misconceptions

- **"The network delivers messages in the order they were sent, so backups can apply in arrival order."** Retries, multiple connections and uneven links all reorder messages. The tests use links of 2 to 50 ms precisely to make messages overtake each other. Order must travel in the messages.
- **"A failed synchronous write did not happen."** An `unavailable` error means the promise could not be kept. The primary and some backups may already have applied the write, and later reads may return it. Treat it like a timeout: unknown.
- **"More synchronous backups means more availability."** More copies means more durability, but if every one must confirm, every one can block writes. Availability falls as backups are added.
- **"With replication, any node can answer reads with the latest data."** Only with synchronous replication, or at the tail of a chain. An asynchronous backup can be behind by the replication lag, and a client that reads after its own write may not see it.
- **"Per-key version numbers are enough to resynchronise a backup."** They tell you which of two writes is newer, not which writes are missing. A single, gap-free log does both.

## How the tasks build it

[RP-1 Asynchronous replication](#/c/repl-async) builds the basic primary-backup store: backups forward writes, the primary answers first and replicates afterwards, and every node serves reads from its own copy. It exercises replication lag and sequence numbers. The tests write through any node and read every node, overwrite the same keys rapidly on uneven links so replication messages overtake each other, and cut a backup off for good. No node may return to an older value, and all must converge.

[RP-2 Synchronous replication](#/c/repl-sync) changes the promise behind `write_ok` to "every node has it". It exercises read-your-writes and the availability cost of waiting for everyone. The tests read the key from every node right after each acknowledgement, drop 15% of messages between nodes while still requiring most writes to succeed, and cut a backup off for 1.5 seconds, during which writes must fail with an honest error within the time limit and afterwards succeed again.

[RP-3 Catching up](#/c/repl-catch-up) replaces per-key numbering with a replication log, kept on disk. It exercises durability and crash recovery. The tests cut a backup off and bring it back, lose a quarter of all messages, crash a backup and crash the primary. Nothing acknowledged may be lost, nothing applied twice, and a restarted node must answer reads at once with no less than it served before.

[RP-4 Chain replication](#/c/repl-chain) rearranges the nodes into a chain with writes at the head, acknowledgements and reads at the tail, and messages only between neighbours. It exercises linearizable reads from the tail. The tests use chains of three to five nodes, uneven links that reorder every hop, and links that lose 20% of messages, and check that every acknowledged write is visible at the tail, reads never go backwards, and requests at the wrong end are refused.

The last three tasks go beyond a single primary into the read anomalies and conflicts of DDIA chapter 6.

[RP-5 Monotonic reads](#/c/repl-monotonic-reads) goes back to an asynchronous primary but lets clients read from any replica, carrying a session token. It exercises monotonic reads and read-your-writes. Every node applies the primary's writes strictly in order, so the token can be one number, the count of applied writes, and a replica behind the token waits before answering. The tests give each backup its own lag (up to 220ms, with reordering), check per client that no key ever reads older than before and that a client always sees its own acknowledged writes, and cut a backup off for good: clients it cannot satisfy may be refused there, but clients with no token must still be served.

[RP-6 Consistent prefix](#/c/repl-consistent-prefix) removes the primary: every node accepts writes, and each key is written once. It exercises consistent prefix reads and causal dependencies. Clients ask questions through one node and answer them through another, over links that are fast in one direction and slow in another. A write is named by its origin and number and carries the origin's applied vector as its dependencies; replicas apply it only after them, and the same vector is the token. The tests check that no node, and no client hopping between nodes, ever sees an answer without its question.

[RP-7 Multi-leader writes](#/c/repl-multi-leader) lets every node write the same keys concurrently. It exercises write conflicts, last write wins and Lamport clocks. Each write gets a timestamp `[counter, node]`, and the highest one wins everywhere; periodic anti-entropy resends what a partition lost. The tests overwrite shared keys through every node, skew the nodes' clocks by up to 400ms, and split off one node for 1.5 seconds while both sides keep writing. Every write must be accepted, a new write must beat what its node already showed, and all nodes must converge on the write with the highest timestamp.

## Further reading

- [Chain Replication for Supporting High Throughput and Availability](https://www.cs.cornell.edu/home/rvr/papers/OSDI04.pdf) — van Renesse and Schneider, OSDI 2004. Short and clear, including how to replace a failed head, middle or tail.
- [PostgreSQL: Log-Shipping Standby Servers](https://www.postgresql.org/docs/current/warm-standby.html) — a production primary-backup system: streaming the log, synchronous standbys and catching up.
- [Redis replication](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/) — replication offsets, the backlog, partial resynchronisation and what asynchronous replication can lose.
- [Object Storage on CRAQ](https://www.usenix.org/legacy/event/usenix09/tech/full_papers/terrace/terrace.pdf) — Terrace and Freedman, USENIX ATC 2009. How to let every chain node serve strongly consistent reads.
- [Designing Data-Intensive Applications, chapter 5](https://dataintensive.net/) — Kleppmann on leaders and followers, replication lag and the anomalies it causes.
