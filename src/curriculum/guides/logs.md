## The problem

A message log runs on two nodes, `n1` and `n2`, so that either can take traffic if the other is busy. Each node keeps, per key, an array of messages, and hands out the array index as the message's position. A producer sends message 5003 for key `k1` to `n1` and gets back "stored at offset 0". At the same moment another producer sends message 5004 for `k1` to `n2` and also gets back "stored at offset 0".

Now a consumer asks `n1` for everything on `k1` from offset 0 and receives `[[0, 5003]]`. It processes the message and records "I have handled `k1` up to offset 0". Later it asks `n2` for everything after offset 0 and receives nothing. Message 5004 was acknowledged to its producer and will never be processed. Two different messages claimed the same position, and a position is the only thing a consumer uses to say what it has seen.

Fixing this by having `n1` use even offsets and `n2` odd ones makes offsets unique but destroys order: `n2` might hand out offset 7 long after `n1` handed out offset 10, so a consumer that has passed 10 never sees 7. The offset has to be decided in one place that both nodes agree on. This track builds the log on one node, then shows how three nodes can share one log by delegating exactly that decision to a store with a strong guarantee.

## The core idea

### The append-only log

An **append-only log** is a sequence of records to which you can only add at the end. Existing records are never modified or removed (until retention deletes old ones, which this track ignores). Each record's position is its **offset**: the first record has offset 0, the next 1, and so on. An offset identifies a record permanently, because nothing ever moves.

The interface has four operations. Each `key` names a separate log.

```json
{ "type": "send", "key": "k1", "msg": 5003 }
{ "type": "send_ok", "offset": 7 }
```

`send` appends a message and returns the offset it was stored at.

```json
{ "type": "poll", "offsets": { "k1": 6, "k2": 0 } }
{ "type": "poll_ok", "msgs": { "k1": [[6, 5001], [7, 5003]], "k2": [] } }
```

`poll` returns, for each key, the messages from the given offset onward as `[offset, msg]` pairs. The consumer chooses where to start. It can reread old messages by asking for a smaller offset; the log does not care.

```json
{ "type": "commit_offsets", "offsets": { "k1": 7 } }
{ "type": "list_committed_offsets", "keys": ["k1", "k2"] }
{ "type": "list_committed_offsets_ok", "offsets": { "k1": 7 } }
```

A **consumer commit** records how far a consumer has processed, so that it (or a replacement after a crash) knows where to resume. Committed offsets only move forward: if commits for 7 and then 5 arrive, perhaps because of a delayed retry, the committed offset stays 7.

This separation is what makes logs useful. Producers do not know who reads. Consumers read at their own pace and keep their own position. Many consumers can read the same log independently, and a consumer that falls behind simply has a smaller offset.

<figure>
<svg viewBox="0 0 600 170" role="img" aria-label="A log of eight records with offsets 0 to 7; a consumer has committed offset 4 and polls from 5; new sends append at offset 8">
  <g style="font: 12px var(--mono); fill: var(--ink)">
    <rect x="30" y="50" width="60" height="40" style="fill: var(--surface); stroke: var(--line)"/><text x="54" y="75">0</text>
    <rect x="90" y="50" width="60" height="40" style="fill: var(--surface); stroke: var(--line)"/><text x="114" y="75">1</text>
    <rect x="150" y="50" width="60" height="40" style="fill: var(--surface); stroke: var(--line)"/><text x="174" y="75">2</text>
    <rect x="210" y="50" width="60" height="40" style="fill: var(--surface); stroke: var(--line)"/><text x="234" y="75">3</text>
    <rect x="270" y="50" width="60" height="40" style="fill: var(--accent-soft); stroke: var(--accent)"/><text x="294" y="75">4</text>
    <rect x="330" y="50" width="60" height="40" style="fill: var(--surface); stroke: var(--m2)"/><text x="354" y="75">5</text>
    <rect x="390" y="50" width="60" height="40" style="fill: var(--surface); stroke: var(--m2)"/><text x="414" y="75">6</text>
    <rect x="450" y="50" width="60" height="40" style="fill: var(--surface); stroke: var(--m2)"/><text x="474" y="75">7</text>
    <rect x="510" y="50" width="60" height="40" style="fill: none; stroke: var(--faint); stroke-dasharray: 4 3"/><text x="534" y="75" style="fill: var(--faint)">8</text>
  </g>
  <g style="font: 12px var(--sans); fill: var(--muted)">
    <text x="250" y="120">committed: 4</text>
    <text x="350" y="120">poll from 5 returns 5, 6, 7</text>
    <text x="480" y="30">next send lands here</text>
  </g>
  <line x1="300" y1="92" x2="300" y2="106" style="stroke: var(--accent)"/>
  <line x1="420" y1="92" x2="420" y2="106" style="stroke: var(--m2)"/>
</svg>
<figcaption>One key's log. Offsets never change; the consumer's position is just a number it stores.</figcaption>
</figure>

On one node all of this is a map from key to array: `send` pushes and returns `length - 1`, `poll` slices, and the committed offset is stored as `max(old, new)`. Because the node handles one message at a time, two sends can never get the same index.

### Sharing one log between nodes

With three nodes and clients sending to any of them, the offset counter must be shared. The tool for that here is `node.kv`, a key-value store that all nodes can reach and that is **linearizable**: every operation appears to take effect at a single instant somewhere between when it was called and when it returned. In practice this means the store behaves like one variable in one memory, even though it is accessed over a network. If one node's write has returned, any read that starts afterwards, from any node, sees it or something newer.

Linearizability alone does not make a counter safe. Consider the obvious approach: read `next/k1` (it says 7), write 8, use offset 7. If `n1` and `n2` both read 7 before either writes, both write 8 and both use offset 7. Each operation was atomic; the pair was not.

**Compare-and-swap** (CAS) closes the gap. `cas(key, from, to)` sets the key to `to` only if it currently holds `from`, and reports whether it did. It is one operation, so linearizability applies to it as a whole. "Set `next/k1` to 8 if it is still 7" can succeed for exactly one of several racing nodes; the others learn that someone got there first.

This is **optimistic concurrency**: assume nobody else is changing the value, do the work, and check at the last moment with a CAS. If the check fails, read again and retry. No locks are held, nobody waits for anybody, and the cost of a conflict is one more round trip.

<figure>
<svg viewBox="0 0 600 230" role="img" aria-label="n1 and n2 both read next equals 7; n1's cas from 7 to 8 succeeds; n2's cas fails, it rereads 8 and its cas from 8 to 9 succeeds">
  <g style="font: 13px var(--mono); fill: var(--ink)">
    <text x="10" y="44">n1</text><text x="10" y="124">kv</text><text x="10" y="204">n2</text>
  </g>
  <g style="stroke: var(--line)"><line x1="40" y1="40" x2="590" y2="40"/><line x1="40" y1="120" x2="590" y2="120"/><line x1="40" y1="200" x2="590" y2="200"/></g>
  <line x1="70" y1="40" x2="110" y2="120" style="stroke: var(--m1); stroke-width: 2"/>
  <line x1="80" y1="200" x2="120" y2="120" style="stroke: var(--m2); stroke-width: 2"/>
  <text x="120" y="110" style="fill: var(--muted); font: 11px var(--mono)">read: 7</text>
  <line x1="170" y1="40" x2="210" y2="120" style="stroke: var(--m1); stroke-width: 2"/>
  <text x="160" y="32" style="fill: var(--ink); font: 11px var(--mono)">cas 7→8</text>
  <text x="214" y="138" style="fill: var(--good); font: 11px var(--mono)">ok, n1 owns 7</text>
  <line x1="200" y1="200" x2="250" y2="120" style="stroke: var(--m2); stroke-width: 2"/>
  <text x="196" y="218" style="fill: var(--ink); font: 11px var(--mono)">cas 7→8</text>
  <text x="270" y="110" style="fill: var(--bad); font: 11px var(--mono)">false</text>
  <line x1="330" y1="200" x2="370" y2="120" style="stroke: var(--m2); stroke-width: 2"/>
  <text x="320" y="218" style="fill: var(--ink); font: 11px var(--mono)">read: 8</text>
  <line x1="440" y1="200" x2="480" y2="120" style="stroke: var(--m2); stroke-width: 2"/>
  <text x="430" y="218" style="fill: var(--ink); font: 11px var(--mono)">cas 8→9</text>
  <text x="484" y="138" style="fill: var(--good); font: 11px var(--mono)">ok, n2 owns 8</text>
</svg>
<figcaption>Two nodes race for the next offset. Exactly one CAS from 7 succeeds; the loser rereads and claims 8.</figcaption>
</figure>

### Where the messages live

Once a node owns an offset, it stores the message under its own key in the shared store, for example `log/k1/7`. A poll from offset 6 then reads `log/k1/6`, `log/k1/7`, `log/k1/8`, … until it finds a missing key.

Two details follow from the order of these steps. First, the claim and the write are two operations, so there is a moment when offset 7 is claimed but `log/k1/7` does not exist yet. Meanwhile offset 8 may already have been claimed and written by another node. A poll that skipped the missing 7 and returned 8 would lead the consumer to commit 8, and message 7 would never be delivered to it. So a poll must **stop at the first missing entry**. The consumer will poll again and get 7 and 8 together once 7 is written.

Second, an entry never changes once written. A node may cache entries it has read or written in local memory forever, which makes polls cheap. The counter `next/k1` is the opposite: it changes on every send, so it must always be read from the store.

Committed offsets need the same care as the counter. "Read `commit/k1`, and if the new offset is larger, write it" has the same race as before: two commits could interleave so that 5 overwrites 7. Use a CAS loop that only ever replaces a smaller value with a larger one.

## A worked example

Three nodes share one log through `kv`. For the whole run the nodes are partitioned from each other, but each can reach `kv`. Key `k1` already holds offsets 0 to 5; `next/k1` is 6.

| Time (ms) | Node | Action | Result |
|---|---|---|---|
| 10 | n1 | Client sends `{k1: 5007}`. `read next/k1` | 6 |
| 11 | n2 | Client sends `{k1: 5008}`. `read next/k1` | 6 |
| 14 | n1 | `cas next/k1 6 → 7` | true: n1 owns offset 6 |
| 15 | n2 | `cas next/k1 6 → 7` | false: someone moved it |
| 18 | n2 | `read next/k1` | 7 |
| 21 | n2 | `cas next/k1 7 → 8` | true: n2 owns offset 7 |
| 24 | n2 | `write log/k1/7 = 5008` | ok. n2 replies `send_ok, offset 7` |
| 25 | n3 | Consumer polls `{k1: 5}`. Reads `log/k1/5` (hit), `log/k1/6` | `null`: n1 has claimed 6 but not written it. Stop. |
| 27 | n3 | Replies `{k1: [[5, 5005]]}` | Offset 7 is not returned, although it exists. |
| 30 | n1 | `write log/k1/6 = 5007`, replies `send_ok, offset 6` | |
| 40 | n3 | Consumer commits `{k1: 5}`. `read commit/k1` returns 4; `cas 4 → 5` | true |
| 60 | n3 | Consumer polls `{k1: 6}`. Reads 6, 7, then `log/k1/8` is missing | `{k1: [[6, 5007], [7, 5008]]}` |
| 70 | n1 | A delayed retry of an old commit `{k1: 3}` arrives. `read commit/k1` returns 5 | 5 ≥ 3: nothing to do. Committed offset stays 5. |

Two faults appear. The first is a lost race: n2's CAS failed, and the retry loop turned that into the next offset with no duplicate. The second is the gap at offset 6: if n3 had skipped it and returned offset 7 at 27 ms, the consumer would have committed 7 and never seen message 5007. Stopping at the gap delayed delivery by 33 ms and lost nothing. The partition between the nodes had no effect at all, because the nodes never talk to each other; everything they share goes through `kv`.

Note the ordering guarantee as well. n1's `send_ok` for offset 6 came after n2's `send_ok` for offset 7. That is allowed: the two sends overlapped in time. If a client had sent a message only after receiving n2's acknowledgement, its offset would have to be larger than 7, and it is, because its read of `next/k1` starts after n2's successful CAS.

## Why it is correct

Assume the shared store is linearizable and always reachable, and nodes do not crash between claiming an offset and writing its entry.

**No two messages share an offset.** Every successful CAS on `next/k1` changes it from some value v to v + 1, and the store starts at missing (treated as 0). Because the store is linearizable, successful CAS operations happen in a single order, and each one starts from the value the previous one left. So successful CAS operations claim 0, 1, 2, … in turn, each exactly once. A node uses an offset only after its CAS for it succeeded.

**Offsets follow acknowledgement order.** If send A is acknowledged before send B begins, A's successful CAS took effect before B's first read of `next/k1`. Linearizability says that read sees A's CAS or something later, so B claims an offset larger than A's.

**No acknowledged message is lost, and polls never skip.** A node replies `send_ok` only after `write log/k1/o` has returned, so any later read of that key from any node sees it. A poll returns a contiguous run of offsets starting where the consumer asked and stopping just before the first missing entry, so it can never return offset o + 1 without o. Caching is safe because an entry, once written, is never written again: its offset was claimed by exactly one node, and that node writes it once.

**Committed offsets never go backwards.** The commit loop only performs a CAS from the current value to a larger one, and stops if the current value is already at least the new one. Linearizability makes the CAS fail if anybody changed the value since it was read.

**Progress.** A CAS fails only because another CAS on the same key succeeded. So whenever some node retries, some other node made progress: the scheme is lock-free. An individual node can in principle lose many races in a row under heavy contention, but the system as a whole always moves forward.

## Trade-offs and failure modes

**Latency.** Every send costs at least three round trips to the shared store (read, CAS, write), and more under contention. A poll costs one read per new entry plus one miss at the end, unless entries are cached. The single-node log answered from memory.

**A hot key is a bottleneck.** All sends to one key serialise on one counter in one store. Throughput per key is bounded by how fast that store can perform CAS operations on one key, and contention makes each node retry more. Real logs scale by splitting a topic into many partitions, each with its own counter and order, and give up order across partitions.

**Holes after a crash.** If a node claims offset 6 and crashes before writing `log/k1/6`, the hole is permanent. Every poll stops at 6 forever, and every later message on that key becomes invisible. A real design needs a way to fill holes: after a timeout, another node writes a "skip" marker into the slot (with a CAS from missing, so it cannot overwrite a real message that arrives late), and consumers step over it.

**The store is doing the hard part.** A linearizable, highly available store is exactly what consensus protocols such as Raft and Paxos provide, at the cost of a majority round trip per operation. Here it is a given. If it is replaced by an eventually consistent store, CAS no longer means anything and duplicate offsets come back.

**Unbounded growth.** An append-only log grows forever. Real systems delete old segments by age or size, or compact a key's history to its latest value. Consumers that fall behind the retention window lose data.

**Commit semantics are a convention.** Whether the committed offset means "last processed" or "next to read" is up to the client protocol. The log just stores the number; a consumer that commits before processing gets at-most-once delivery, after processing at-least-once.

## In real systems

**Apache Kafka** stores each topic partition as an append-only log of segment files. Each partition has one leader broker, which assigns offsets locally, so no CAS is needed: the leader is the single place that decides. Followers copy the leader's log, and consumers can only read up to the **high watermark**, the offset up to which enough replicas have the data. That is a close cousin of "stop at the first gap": a consumer never sees a record that might still disappear. Consumer groups commit their offsets to an internal compacted topic, `__consumer_offsets`.

**Corfu** (from Microsoft Research) is a shared log built very much like this track. Clients obtain the next offset from a **sequencer**, then write the entry directly to storage units at that position. Holes left by clients that crash after taking an offset are filled by a hole-filling protocol that writes a junk entry into the slot. Systems built on it treat the log as the single source of truth for replicated state.

**etcd** gives every change in the store a global, increasing **revision** number, and clients can watch the key space from any revision onward, which turns the store into a log of changes. Its transactions compare a key's revision or value before writing, the same optimistic-concurrency pattern as CAS, and Kubernetes uses this for every object update ("resourceVersion" conflicts are failed CAS operations).

## Common misconceptions

- **"Each node can number its own messages, as long as the numbers are unique."** Unique is not enough. Consumers track one number per key and assume everything below it has been seen. Interleaved per-node numbering lets a later message carry a smaller offset than one a consumer has already passed.
- **"Reading then writing is fine if it is fast."** Speed narrows the window but does not close it. Under load, two nodes will eventually both read the same value before either writes. Only an atomic compare-and-swap (or a lock) makes the pair safe.
- **"A linearizable store makes all my operations atomic."** Each single operation is atomic. A sequence of operations is not, and other nodes can act between them. That is why the claim uses CAS and why polls must handle a claimed but unwritten offset.
- **"Committing an offset removes messages from the log."** A commit only records the consumer's position. The messages stay, other consumers still read them, and the same consumer can reread them by polling from a smaller offset.
- **"Optimistic concurrency can livelock forever."** A failed CAS means another CAS succeeded, so the system always makes progress. One unlucky node can retry several times, but the cluster as a whole never stalls.

## How the tasks build it

[LG-1 Single-node log](#/c/log-single) builds the four operations on one node: per-key logs, offsets as positions, polls from any offset, and consumer commits that never move backwards. The tests drive sends, polls and commits across three keys and check that offsets are unique per key, increase in acknowledgement order, that polls start where asked and are in order, and that no acknowledged message goes missing.

[LG-2 Multi-node log](#/c/log-multi) spreads the same log over three nodes that clients use interchangeably, with a linearizable store available to all of them. It exercises linearizability, compare-and-swap and optimistic retry, and makes you deal with offsets that are claimed before their message exists. The tests check uniqueness and ordering across nodes, that every node can serve every acknowledged message, and, in the second test, that all of this still works when the nodes cannot reach each other at all, only the store.

## Further reading

- [The Log: What every software engineer should know about real-time data's unifying abstraction](https://web.archive.org/web/2025/https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying) — Jay Kreps on why the append-only log sits underneath databases, replication and stream processing (archived copy).
- [Apache Kafka documentation: Design](https://kafka.apache.org/documentation/#design) — offsets, consumer positions, the high watermark and replication in a production log.
- [Linearizability: A Correctness Condition for Concurrent Objects](https://cs.brown.edu/~mph/HerlihyW90/p463-herlihy.pdf) — Herlihy and Wing, 1990. The definition the shared store in LG-2 satisfies.
- [CORFU: A Shared Log Design for Flash Clusters](https://www.usenix.org/conference/nsdi12/technical-sessions/presentation/balakrishnan) — Balakrishnan et al., NSDI 2012. A sequencer plus write-once slots, and how holes are filled.
- [Gossip Glomers: Kafka-Style Log](https://fly.io/dist-sys/5a/) — the Maelstrom challenge whose message format this track follows.
