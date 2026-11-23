## The problem

Two nodes, `n1` and `n2`, run the same order service. Each keeps a counter that starts at 0, and each new order gets the next number. A customer places an order on `n1` and gets order 1. A second later another customer, routed to `n2`, also gets order 1. Both rows are written to the shared database; the second write either fails on a unique-key constraint or, worse, overwrites the first order. Nothing crashed and no message was lost. The bug is that two nodes made the same decision independently, and nothing in their local state told them apart.

The obvious fix is to let one node own the counter. Every node asks `n1` for the next number, and `n1` hands out 1, 2, 3 in turn. Now the numbers are unique, but every order needs a round trip to `n1`. When `n1` is slow, every node is slow. When the network splits and `n2` cannot reach `n1`, `n2` cannot take orders at all, although it is running perfectly well and has customers waiting.

This track is about generating identifiers that are unique across the whole cluster with **no** coordination at all, so that any node that is up can issue one at any moment, whatever the network is doing. Then it adds a second requirement that real databases care about: the ids should sort roughly by the time they were created.

## The core idea

### Partitions and availability

A **network partition** is a failure in which the network splits the nodes into groups that cannot exchange messages with each other, while every node keeps running. Nodes inside a group can talk; messages across the split are lost. From inside, a partition looks exactly like the other side having crashed: you send and hear nothing back. A partition can last milliseconds (a switch reboot) or hours (a misconfigured firewall), and it ends when the network **heals**.

**Availability** is the property that every request to a node that has not crashed gets a successful response. A system can only stay available during a partition if a node can answer using *only the state it has locally*. As soon as answering requires hearing from another node, a partition can make that node wait forever, and the request fails. In this track's brief this is called total availability: any node that is up can answer, whatever the network is doing.

The **CAP theorem** states this trade-off formally: when a partition happens, a system must choose between staying available and staying consistent (every node agreeing on a single up-to-date view). A central counter chooses consistency: ids are perfectly sequential, but the cut-off side stops. For id generation we can choose availability and lose nothing important, because uniqueness does not actually require agreement. It only requires that two nodes never pick the same value.

<figure>
<svg viewBox="0 0 600 250" role="img" aria-label="Left: n2 and n3 must ask n1 for ids and fail during a partition. Right: each node mints ids locally and keeps answering.">
  <text x="20" y="22" style="font: 13px var(--sans); fill: var(--ink)">Central allocator</text>
  <text x="330" y="22" style="font: 13px var(--sans); fill: var(--ink)">Coordination-free</text>
  <g style="fill: var(--surface); stroke: var(--line)">
    <rect x="110" y="40" width="60" height="34" rx="6"/><rect x="40" y="170" width="60" height="34" rx="6"/><rect x="180" y="170" width="60" height="34" rx="6"/>
    <rect x="420" y="40" width="60" height="34" rx="6"/><rect x="350" y="170" width="60" height="34" rx="6"/><rect x="490" y="170" width="60" height="34" rx="6"/>
  </g>
  <g style="font: 13px var(--mono); fill: var(--ink)">
    <text x="130" y="62">n1</text><text x="60" y="192">n2</text><text x="200" y="192">n3</text>
    <text x="440" y="62">n1</text><text x="370" y="192">n2</text><text x="510" y="192">n3</text>
  </g>
  <line x1="20" y1="120" x2="280" y2="120" style="stroke: var(--warn); stroke-width: 2; stroke-dasharray: 8 5"/>
  <line x1="330" y1="120" x2="590" y2="120" style="stroke: var(--warn); stroke-width: 2; stroke-dasharray: 8 5"/>
  <text x="230" y="114" style="font: 12px var(--sans); fill: var(--warn)">partition</text>
  <line x1="75" y1="168" x2="122" y2="80" style="stroke: var(--bad); stroke-width: 2; stroke-dasharray: 5 4"/>
  <text x="96" y="128" style="fill: var(--bad); font: 15px var(--sans)">×</text>
  <line x1="205" y1="168" x2="158" y2="80" style="stroke: var(--bad); stroke-width: 2; stroke-dasharray: 5 4"/>
  <text x="176" y="128" style="fill: var(--bad); font: 15px var(--sans)">×</text>
  <g style="font: 12px var(--mono); fill: var(--good)">
    <text x="486" y="62">n1-41</text><text x="345" y="224">n2-17</text><text x="485" y="224">n3-9</text>
  </g>
  <g style="font: 12px var(--sans); fill: var(--bad)"><text x="30" y="224">waits</text><text x="190" y="224">waits</text></g>
</svg>
<figcaption>With a central allocator, the side of the partition without n1 cannot issue a single id. With local generation, every node keeps answering.</figcaption>
</figure>

### Coordination-free uniqueness

**Coordination** means any step where a node must exchange messages with others before it can act: asking a leader, taking a lock, waiting for a majority to agree. A **coordination-free design** is one where each node acts on local state alone and correctness still holds globally. It is the only kind of design that is fully available under partitions, and it is also the fastest, because no request waits for a network round trip.

For identifiers, coordination-free uniqueness needs two ingredients:

1. Something that separates nodes. In this lab every node has a name (`n1`, `n2`, …) that no other node has. In real systems this is a configured machine number, or a large random number chosen at start-up.
2. Something that separates one node's ids from each other. A local counter that only goes up does it.

The pair `(node id, counter)` is unique across the cluster: two ids from different nodes differ in the first part, and two ids from the same node differ in the second. Written as a string it looks like `n2-17`. No messages are needed, so a partition changes nothing.

A different route is randomness. A random **UUID** (version 4) has 122 random bits; the chance that two of them collide is so small that it can be ignored for practical purposes, even across billions of ids. Random ids need no configured node numbers at all, which is why they are so popular. They have two costs, though: they are big (128 bits), and they carry no order.

### Why order matters

Most databases store rows in a B-tree index ordered by primary key. If keys are random, each new row lands at a random position in the index, so inserts touch pages all over the tree and keep fewer of them in cache. If keys grow with time, new rows always go at the right-hand end, which is cheap. Time-ordered keys also make "the 50 newest orders" a range scan over the largest keys, and let you read roughly when a record was created from its id alone.

So we want ids that are unique, coordination-free, **and** that sort by creation time. The well-known answer is the **Snowflake ID**, designed at Twitter: a single 64-bit integer built from the time, the machine number and a sequence.

### Bit packing

**Bit packing** stores several small fields in one integer by giving each field a fixed range of bit positions. The most significant field decides the sort order first; lower fields only matter when everything above them is equal.

The lab's layout, most significant first:

<figure>
<svg viewBox="0 0 600 170" role="img" aria-label="An id split into three fields: time in milliseconds, 6-bit node number, 12-bit sequence">
  <rect x="20" y="40" width="360" height="44" style="fill: var(--accent-soft); stroke: var(--accent)"/>
  <rect x="380" y="40" width="70" height="44" style="fill: var(--surface); stroke: var(--m2)"/>
  <rect x="450" y="40" width="130" height="44" style="fill: var(--surface); stroke: var(--m3)"/>
  <g style="font: 13px var(--sans); fill: var(--ink)">
    <text x="150" y="67">time in ms</text><text x="398" y="67">node</text><text x="482" y="67">sequence</text>
  </g>
  <g style="font: 12px var(--mono); fill: var(--muted)">
    <text x="22" y="30">most significant</text><text x="400" y="30">6 bits</text><text x="490" y="30">12 bits</text>
    <text x="22" y="106">ms × 2^18</text><text x="380" y="106">node × 2^12</text><text x="470" y="106">+ seq</text>
  </g>
  <text x="20" y="140" style="font: 13px var(--mono); fill: var(--ink)">id = (ms × 2^6 + node) × 2^12 + seq</text>
  <text x="20" y="160" style="font: 12px var(--sans); fill: var(--muted)">n3 at ms 120, seq 0: (120 × 64 + 3) × 4096 + 0 = 31,469,568</text>
</svg>
<figcaption>Time leads, so ids sort by when they were made; the node field keeps nodes apart within one millisecond; the sequence keeps one node's ids apart within one millisecond.</figcaption>
</figure>

Each field has a job. The **time** field puts later ids above earlier ones. Two nodes issuing an id in the same millisecond are kept apart by the **node** field, which is why it sits directly below the time. One node issuing several ids in the same millisecond is kept apart by the **sequence**, a counter that restarts at 0 whenever the millisecond changes and increments while it stays the same. With 12 bits, one node can issue 4,096 ids per millisecond; with 6 node bits, the cluster can have 64 nodes.

In most languages you build this with shifts: `(ms << 18) | (node << 12) | seq`. In JavaScript the shift operators convert their operands to 32-bit integers first and silently discard the high bits, so the result wraps or goes negative. Multiplying by powers of two and adding does the same arithmetic on full-precision numbers. JSON numbers (and JavaScript numbers) represent integers exactly only up to 2^53, so the whole id must stay below that.

### What a clock can and cannot promise

The time field comes from each node's clock, and different machines' clocks disagree. Each is a quartz oscillator that drifts by some parts per million, corrected from time to time by a protocol such as NTP. The difference between two clocks at the same instant is called **clock skew**. A few milliseconds is typical in a data centre; much more is possible when synchronisation fails.

Skew means that if `n1`'s clock is 4 ms ahead of `n2`'s, an id that `n1` issues 2 ms *before* one from `n2` can still be the larger one. Order is reliable only between ids created further apart than the skew. Ids with this weaker property are called **k-sorted**: every id is within some bounded distance *k* of where it would be in perfect creation order. The lab's checker makes the bound explicit: an id requested at least 50 ms after another id was returned must be larger, whichever nodes issued them.

Clocks can also jump *backwards*, for example when NTP steps a fast clock back to the right time. A generator that trusts the clock blindly would then reuse timestamps it has already used and could reissue old ids. The standard defence is to remember the last millisecond used and never go below it: if the clock reads earlier, keep using the last value and continue its sequence.

> Uniqueness comes from the node and sequence fields, which are purely local. Order comes from the clock, which is only approximately shared. That is why the ids are guaranteed unique but only k-sorted.

## A worked example

Three nodes, using the layout above: `id = (ms × 64 + node) × 4096 + seq`. Each node remembers `lastMs` and `seq`. At t = 800 ms the network partitions into `{n1}` and `{n2, n3}`; it heals at t = 2,800 ms.

| t (ms) | Node | Clock reads | Rule applied | `lastMs`, `seq` after | Id |
|---|---|---|---|---|---|
| 120.4 | n1 | 120 | new millisecond | 120, 0 | 31,461,376 |
| 120.7 | n3 | 120 | new millisecond on n3 | 120, 0 | 31,469,568 |
| 120.9 | n1 | 120 | same millisecond: seq + 1 | 120, 1 | 31,461,377 |
| 800 | | | **partition: n1 cut off from n2 and n3** | | |
| 950.3 | n2 | 950 | new millisecond | 950, 0 | 249,044,992 |
| 951.2 | n1 | 951 | new millisecond | 951, 0 | 249,303,040 |
| 951.6 | n1 | 951 | same millisecond: seq + 1 | 951, 1 | 249,303,041 |
| 1,400.2 | n2 | 1,400 | new millisecond | 1,400, 0 | 367,009,792 |
| 1,400.5 | n2 | **1,397** | **clock stepped back 3 ms**: keep 1,400, seq + 1 | 1,400, 1 | 367,009,793 |
| 1,400.6 | n3 | 1,400 | new millisecond on n3 | 1,400, 0 | 367,013,888 |
| 2,800 | | | **network heals** | | |
| 2,900.1 | n1 | 2,900 | new millisecond | 2,900, 0 | 760,221,696 |
| 2,900.3 | n2 | 2,900 | new millisecond | 2,900, 0 | 760,225,792 |

Things to notice:

- At t = 120 three ids share a millisecond. `n1` and `n3` differ in the node field (4,096 × 1 versus 4,096 × 3), and `n1`'s two ids differ in the sequence.
- During the partition every request was answered immediately on both sides. No node sent a single message to issue an id, so the partition was invisible to this workload.
- At t = 1,400.5 `n2`'s clock jumped backwards. Using the raw reading would have produced 366,223,360, smaller than the id it issued 0.3 ms earlier. Worse, if `n2` had issued ids at 1,397 before, it could repeat one exactly. Clamping to `lastMs` and *continuing* the sequence produced 367,009,793, which is new and larger. Note the order of operations: clamp first, then decide whether the millisecond repeated. If a generator resets the sequence to 0 because the reading changed and only then clamps, it issues 367,009,792 a second time.
- Every id issued 50 ms or more after another is larger, across all nodes. Ids within the same millisecond on different nodes (the last two rows) sort by node number, not by which request came first. That is the k in k-sorted.

## Why it is correct

**Uniqueness.** The function `(ms, node, seq) → (ms × 64 + node) × 4096 + seq` is one-to-one as long as `0 ≤ node < 64` and `0 ≤ seq < 4096`, because then each field occupies its own range of bits and no carry from a lower field can reach a higher one. So two ids are equal only if all three fields are equal. Two different nodes never share the node field. On a single node, the generator ensures that the pair `(ms, seq)` never repeats: `ms` never decreases (it is clamped to `lastMs`), and whenever `ms` equals the previous value the sequence strictly increases. When the sequence would reach 4,096, the generator must not wrap to 0 in the same millisecond; the lab's reference borrows the next millisecond instead, and production generators usually wait for the clock to tick. Either way the pair stays fresh. These facts hold whatever the network does, which is why uniqueness survives partitions, message loss and any amount of load.

The assumption behind all of it is that node numbers are unique and stable. If two machines are misconfigured with the same machine number, uniqueness fails silently.

**Order.** Suppose id A was returned by its node at real time *a* and id B was requested at real time *b ≥ a + 50 ms*. If every node's clock is within 50 ms of every other (skew below the slack), then B's millisecond reading is larger than A's. The time field dominates all lower fields (the lower 18 bits sum to less than 2^18), so B > A. Two things can weaken this: skew above the slack, and the generator running ahead of its clock (by borrowing milliseconds after sequence exhaustion, or by clamping after a backward jump). Both are bounded in practice; in the lab every node reads the same simulated clock, so skew is zero.

**Range.** The id stays below 2^53 only while `ms × 2^18 < 2^53`, that is, `ms < 2^35`, about one year of milliseconds. Simulated time starts at 0, so the lab fits easily. A real generator counting from 1970 would overflow 53 bits; real designs subtract a **custom epoch** (a fixed recent date) from the clock and budget the bits accordingly.

## Trade-offs and failure modes

**Bit budget.** Every bit given to one field is taken from another. Twitter's layout uses 41 bits of milliseconds (about 69 years from its epoch), 10 bits of machine id (1,024 machines) and 12 bits of sequence (4,096 ids per millisecond per machine). More node bits allow larger clusters; more sequence bits allow bigger bursts; more time bits allow a longer lifetime. A design that runs out of time bits stops producing ordered ids on a predictable date.

**Assigning node numbers is coordination, done once.** The generator itself never talks to anyone, but someone must hand out machine numbers without repeats, usually from configuration, a deployment tool, or a coordination service such as ZooKeeper at start-up. That is acceptable because it happens rarely and off the request path. Random 128-bit ids avoid even that.

**Clock dependence.** Ordering is only as good as clock synchronisation. A node whose clock is minutes ahead issues ids that sort after everything the healthy nodes produce for those minutes. A clock that jumps back forces the generator either to stall or to run ahead of real time. None of this breaks uniqueness, but it does break the meaning of "larger id means newer".

**Information leakage.** A time-ordered id reveals when it was created and, with a node field, roughly where. Sequential ids also reveal volume: two orders a day apart tell a competitor how many orders you take per day. Public-facing ids are sometimes random for that reason.

**Size and representation.** A 64-bit integer does not fit in a JSON or JavaScript number exactly. APIs that expose Snowflake ids usually send them as strings as well as, or instead of, numbers.

## In real systems

**Twitter Snowflake.** The original: a 64-bit id made of a sign bit left at 0, 41 bits of milliseconds since a custom epoch, 10 bits of worker id and 12 bits of sequence. Generated on many machines with no coordination per id. Because the ids exceed 2^53, Twitter's API returns them in an `id_str` field as well, so JavaScript clients do not corrupt them.

**Discord.** Discord uses Snowflake-style ids for messages, users and channels, with its own epoch (the first second of 2015). Clients extract the creation time of any message from its id, and pagination of message history works by asking for ids before or after a given one.

**MongoDB ObjectId.** A 12-byte value: a 4-byte timestamp in seconds, a 5-byte random value chosen once per process, and a 3-byte counter. It is the same three-part recipe, with a random number instead of a configured machine number, and second rather than millisecond resolution, so ObjectIds are only k-sorted to within a second.

**UUID version 7 (RFC 9562).** A standard 128-bit UUID whose top 48 bits are a Unix timestamp in milliseconds, with most of the rest random. It keeps the "no configuration, practically never collides" benefit of random UUIDs while sorting by creation time, which is why databases increasingly recommend it over version 4 for primary keys.

## Common misconceptions

- **"Unique ids need a central sequence."** Only *gap-free consecutive* ids need one. Uniqueness alone can be built from local state: a unique node number plus a local counter is enough.
- **"Time-ordered ids are sorted by creation time."** They are k-sorted. Ids issued within the clock skew of each other, or within one millisecond on different nodes, can sort either way.
- **"A timestamp makes an id unique."** Many ids can be issued in the same millisecond, on one node or several. Uniqueness comes from the node and sequence fields; the timestamp provides order.
- **"Random UUIDs might collide, so they are unsafe."** A version 4 UUID has 122 random bits. With a good random source, the probability of any collision among a trillion ids is around one in 10^13. The real risks are a broken random number generator or a cloned virtual machine, not chance.
- **"Clocks only move forward."** NTP can step a clock backwards, and virtual machines can pause and resume. A generator must handle a reading earlier than its last one.

## How the tasks build it

[ID-1 Unique IDs](#/c/unique-ids) asks for ids that never collide, across up to five nodes and 1,000 requests, and one test partitions the cluster for two seconds while requests keep arriving on both sides. The starter's plain counter collides immediately across nodes; the partition test rules out any design that asks another node for anything. It exercises coordination-free design and total availability directly.

[ID-2 Sortable IDs](#/c/sortable-ids) adds two constraints: the id must be an integer below 2^53, and an id requested at least 50 ms after another was returned must be larger, whichever node issued it. The burst test sends 2,000 requests in two seconds, so each node issues many ids per millisecond and the sequence field is essential. The partition test checks that ordering did not sneak in any coordination. Bit packing without shifts is the main technical trap.

## Further reading

- [Announcing Snowflake (Twitter engineering blog)](https://blog.twitter.com/engineering/en_us/a/2010/announcing-snowflake) — the original motivation: ids that are unique, roughly sorted and generated without coordination.
- [RFC 9562: Universally Unique IDentifiers (UUIDs)](https://www.rfc-editor.org/rfc/rfc9562) — the current UUID standard, including the time-ordered version 7 and the reasoning behind its layout.
- [Snowflake ID (Wikipedia)](https://en.wikipedia.org/wiki/Snowflake_ID) — the bit layout and a list of systems that adopted variants of it.
- [MongoDB ObjectId reference](https://www.mongodb.com/docs/manual/reference/method/ObjectId/) — a widely deployed three-field id, documented byte by byte.
- [Brewer: CAP twelve years later (InfoQ)](https://www.infoq.com/articles/cap-twelve-years-later-how-the-rules-have-changed/) — the author of the CAP theorem on what the availability / consistency trade-off really means during partitions.
