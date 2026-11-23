## The problem

A shop's product pages read prices from a database that answers in about 50 ms. Traffic is lopsided: the twenty products on the front page get most of the views. Somebody adds an in-memory map in front of the database: look in the map first, and only on a miss read the database and remember the answer. Database load drops by 90% and pages get fast. It looks like a free win.

Here is the same cache a month later, with two web nodes, n1 and n2, each holding its own map.

1. At t=0, n1 reads the price of product p7 from the database: 20 euros. It stores it.
2. At t=10, an admin lowers the price to 15 euros through n2. n2 writes the database and gets OK.
3. At t=20, the admin reloads the page and the request happens to go to n1. n1 finds p7 in its map and shows 20 euros.

The write succeeded, the database is correct, and the admin is looking at the old price, with no time limit on how long that will last. Meanwhile, n1's map has grown to hold every product anyone ever viewed and the process is running out of memory, and on Monday morning when the cache restarts empty, two hundred requests for the front page arrive in the same 50 ms and all go straight to the database at once.

Caching trades correctness and memory for speed. Each of those problems has a standard mechanism, and each mechanism has a race hiding in it.

## The core idea

### Cache-aside

A **cache** keeps copies of recent answers in faster storage, here a hash map in the node's memory. The pattern in the opening is **cache-aside**: the application checks the cache first; on a **hit** it answers from memory, on a **miss** it reads the database, stores the result, and answers. The database knows nothing about the cache. (The alternatives, read-through and write-through caches, put the cache in charge of talking to the database; the ideas below apply to them too.)

The fraction of requests answered from memory is the **hit rate**. With skewed traffic even a small cache gets a high one, because a few **hot keys** carry most of the requests. If the 20 hottest keys out of 10,000 draw 80% of reads, a cache that holds just those 20 avoids 80% of database reads.

Each node keeps its own cache. Two nodes that both see key k1 each read it once; the database sees one read per (node, key), not per key.

### Bounded memory and LRU

A cache with no limit eventually holds everything and exhausts memory. A bounded cache has a **capacity**, say 20 entries, and an **eviction** policy that decides what to drop when a new entry would exceed it. The policy matters because a bad one evicts exactly what you need.

**LRU** (least recently used) evicts the entry that has gone longest without being used. A hit counts as a use: the key moves to the fresh end of the line. Keep the entries in recency order, and evict from the stale end.

<figure>
<svg viewBox="0 0 620 200" role="img" aria-label="LRU list before and after a hit on k3 and an insert of k9 that evicts k1">
  <g style="font: 13px var(--sans); fill: var(--ink)">
    <text x="10" y="40">before</text><text x="10" y="100">get k3 (hit)</text><text x="10" y="160">get k9 (miss)</text>
  </g>
  <g style="fill: var(--surface); stroke: var(--line)">
    <rect x="130" y="20" width="70" height="30"/><rect x="210" y="20" width="70" height="30"/><rect x="290" y="20" width="70" height="30"/><rect x="370" y="20" width="70" height="30"/>
    <rect x="130" y="80" width="70" height="30"/><rect x="210" y="80" width="70" height="30"/><rect x="290" y="80" width="70" height="30"/><rect x="370" y="80" width="70" height="30" style="fill: var(--accent-soft); stroke: var(--accent)"/>
    <rect x="130" y="140" width="70" height="30" style="fill: var(--surface); stroke: var(--bad); stroke-dasharray: 4 3"/><rect x="210" y="140" width="70" height="30"/><rect x="290" y="140" width="70" height="30"/><rect x="370" y="140" width="70" height="30"/><rect x="450" y="140" width="70" height="30" style="fill: var(--accent-soft); stroke: var(--accent)"/>
  </g>
  <g style="font: 13px var(--mono); fill: var(--ink)" text-anchor="middle">
    <text x="165" y="40">k1</text><text x="245" y="40">k3</text><text x="325" y="40">k5</text><text x="405" y="40">k2</text>
    <text x="165" y="100">k1</text><text x="245" y="100">k5</text><text x="325" y="100">k2</text><text x="405" y="100">k3</text>
    <text x="165" y="160" style="fill: var(--bad)">k1 ×</text><text x="245" y="160">k5</text><text x="325" y="160">k2</text><text x="405" y="160">k3</text><text x="485" y="160">k9</text>
  </g>
  <g style="font: 12px var(--sans); fill: var(--muted)">
    <text x="130" y="192">least recently used</text><text x="420" y="192">most recently used</text>
    <text x="460" y="40">capacity 4</text>
  </g>
</svg>
<figcaption>A hit moves the key to the fresh end; a miss that overflows capacity evicts the stalest key, here k1.</figcaption>
</figure>

LRU is good at a common pattern: a few keys used over and over plus a **scan**, a long run of keys each used once (a batch job walking the catalogue, a crawler). Each hot key comes back long before 20 other keys have been touched, so it stays near the fresh end; scan keys pass through and fall off the stale end. **Scan resistance** is the ability to keep the hot set through such a run. **FIFO** eviction, which drops the oldest *insertion* and ignores hits, has none: a hot key is thrown out every 20 misses however popular it is. In the task's traffic, LRU gets about 30% hits, FIFO about 17% and random eviction about 19%.

### Expiry and the staleness bound

The database can change without the cache being told: another service writes the table, a script fixes a row. Without expiry, a cached value can be wrong forever. A **TTL** (time to live) says an entry may be served for at most, say, 1000 ms after it was read; after that it counts as a miss and is read again. Checking the age on lookup is enough; no timer is needed.

A TTL does not make the cache correct. It gives a **staleness bound**: no answer is more than 1000 ms out of date. A shorter TTL means fresher answers and more database reads; a longer one fewer reads and older answers.

Date each entry from when you **sent** the read, not when the answer arrived. The value is at least as fresh as the moment of sending, so the time the reply spent travelling is counted against the TTL instead of being added to it. And do not refresh the date on a hit: a popular key would then never expire, and the bound would be gone.

### Stampedes and single flight

Expiry creates its own failure. A hot key is requested every 4 ms. The moment its entry expires, every request is a miss; while the database takes 150 ms to answer the first read, about 40 more requests arrive, each a miss, each sending its own read. The database, protected until now, suddenly takes the full load for that key, and it is slowest exactly when it is busiest. This is a **cache stampede**, also called a thundering herd or dogpile.

The fix is **single flight**, also called **request coalescing**: at most one database read per key at a time. The first miss starts the read and records it as in progress; later misses for the same key wait for that read and share its answer. In JavaScript this is a map from key to the promise of the read in progress.

<figure>
<svg viewBox="0 0 600 210" role="img" aria-label="Three gets for k1 on n1 share one database read">
  <g style="font: 13px var(--mono); fill: var(--ink)">
    <text x="10" y="44">clients</text><text x="10" y="114">n1</text><text x="10" y="184">db</text>
  </g>
  <g style="stroke: var(--line)"><line x1="70" y1="40" x2="590" y2="40"/><line x1="70" y1="110" x2="590" y2="110"/><line x1="70" y1="180" x2="590" y2="180"/></g>
  <g style="stroke: var(--m1); stroke-width: 2">
    <line x1="100" y1="40" x2="120" y2="110"/><line x1="140" y1="40" x2="160" y2="110"/><line x1="180" y1="40" x2="200" y2="110"/>
  </g>
  <line x1="122" y1="110" x2="200" y2="180" style="stroke: var(--accent); stroke-width: 2.5"/>
  <line x1="330" y1="180" x2="400" y2="110" style="stroke: var(--accent); stroke-width: 2.5"/>
  <rect x="122" y="100" width="278" height="20" style="fill: var(--accent-soft); stroke: none; opacity: 0.6"/>
  <text x="220" y="98" style="fill: var(--ink); font: 12px var(--sans)">inflight[k1] = one promise</text>
  <g style="stroke: var(--good); stroke-width: 2">
    <line x1="402" y1="110" x2="430" y2="40"/><line x1="404" y1="110" x2="450" y2="40"/><line x1="406" y1="110" x2="470" y2="40"/>
  </g>
  <g style="fill: var(--muted); font: 12px var(--sans)">
    <text x="85" y="30">get k1 ×3</text><text x="210" y="160">one read</text><text x="440" y="30">three replies, one value</text>
  </g>
</svg>
<figcaption>The first miss starts the read; the next two find it in progress and wait on the same promise.</figcaption>
</figure>

The subtle part is cleanup. Remove the key from the in-progress map when the read ends, whether it succeeds, fails or times out. A failed read that stays in the map blocks the key forever, because every later miss joins a read that will never finish.

### Invalidation on writes

When writes go through the caching nodes, waiting for a TTL is not good enough: a client told its write succeeded expects to read it back, from any node. This is **read-after-write** consistency. The tool is **invalidation**: when a write completes, delete the key from every cache before acknowledging the write.

Deleting from the local cache is easy. Three races are not.

- **Other nodes.** The node that took the write must tell every peer to drop the key and wait until each confirms, retrying lost messages, before it replies `put_ok`. Otherwise a peer serves the old value after the client was told the write is done.
- **The stale fill race.** A read sent to the database *before* the write can come back *after* the invalidation, carrying the old value. Storing it puts the old value back in the cache, now with a fresh TTL. The fix is a **generation** number per key, bumped by every invalidation. A read remembers the generation when it starts and stores its answer only if the generation is unchanged when it returns.
- **Joining an old read.** With single flight, a get that arrives after the write might join a read that started before it, and return the old value. Invalidation must also drop the key's in-progress read, so later misses start a fresh one.

Delete rather than storing the new value. If two puts for the same key race through two different nodes, each node pushing "its" value to the others could leave the older value on a third node. A deletion carries no value, so it cannot be stale.

## A worked example

Three caching nodes `n1`, `n2`, `n3` in front of `db`. Key `k2` holds `w6`. TTL 1000 ms, database timeout 300 ms. All three nodes start with `k2 = w6` cached; generations are 0.

| t (ms) | event | effect |
|---|---|---|
| 0 | n3's entry for k2 expires; a get for k2 arrives at n3 | miss; n3 starts a read, gen(k2)=0 recorded, inflight[k2] set |
| 5 | another get for k2 at n3 | joins the in-progress read: no second database read |
| 8 | db reads k2 for n3's request: value `w6`; the answer is slow on the way back | |
| 20 | client c1 sends `put k2 w7` to n1 | n1 writes db |
| 50 | db: `write_ok` to n1 | n1 deletes k2 locally, bumps gen to 1, sends `invalidate k2` to n2 and n3 |
| 60 | invalidate to n2 arrives; n2 deletes k2, gen→1, replies | |
| 60 | invalidate to n3 is **lost** | |
| 160 | n1's retry timer (100 ms) fires; resends invalidate to n3 | |
| 165 | n3: deletes k2 (not cached anyway), gen→1, drops inflight[k2], replies | |
| 170 | n1 has both confirmations: replies `put_ok` to c1 | |
| 175 | another get for k2 at n3 | inflight was dropped: starts a fresh read, gen=1 recorded |
| 190 | n3's old read (sent at 0) finally returns `w6` | gen is 1, not 0: answer given to the two waiting gets, **not stored** |
| 215 | the fresh read returns `w7`; gen still 1 | stored; reply `w7` |

The two gets at t=0 and t=5 receive `w6`, which is fine: they were sent before the put was acknowledged. Without the generation check, n3 would have cached `w6` at t=190 and served it until t=1190, after c1 had been told its write succeeded. Without dropping the in-progress read at t=165, the get at t=175 would have joined the old read and returned `w6`.

## Why it is correct

Each mechanism guarantees one thing, under stated assumptions.

**LRU never exceeds capacity** because every insert is followed by evicting from the stale end until the size is back at the limit. It keeps a key that is used at least once in every C distinct accesses (C being the capacity), because such a key is moved to the fresh end before C others can push it out.

**The TTL bounds staleness.** An entry was read at or after its send time s, so the value was current at some moment after s. It is served only until s + TTL. Therefore any served value was the database's current value at some point within the last TTL milliseconds, plus whatever time the reply to the client takes. This holds as long as the node's clock runs at a steady rate.

**Single flight bounds database reads** to one per key per node at a time, because a read is started only when the map has no entry for the key, and the map entry is created in the same uninterrupted step. Because every path out of the read removes the entry, a failure costs at most one timeout before the next miss can try again.

**Invalidation gives read-after-write.** Claim: after `put_ok`, no node serves an older value. When `put_ok` is sent, the database already holds the new value, and every node has processed an invalidation after the write completed. At that moment no cache holds an entry from before the write, since the invalidation deleted it. Could one appear later? Only from a read. A read that started after the invalidation reads the database after the write, so it returns the new value (or newer). A read that started before the invalidation recorded the old generation and is refused when it tries to store. And a get after `put_ok` cannot join such a read, because the invalidation removed it from the in-progress map. The argument assumes the invalidations do reach every peer; if a peer can stay unreachable, the writer must either wait or remove that peer from service before acknowledging.

## Trade-offs and failure modes

**Every cache is a consistency decision.** Cache-aside with a TTL is eventually consistent with a staleness bound; adding invalidation buys read-after-write at the cost of making every write wait for every caching node. One slow or unreachable node now slows or blocks writes.

**Lost invalidations mean stale data.** Production systems often send invalidations without waiting (a message on a bus) and keep a TTL as a backstop, accepting brief staleness when a message is lost. The TTL is what keeps a lost invalidation from becoming a permanent wrong answer.

**Hit rate is not free to raise.** A bigger cache costs memory; a longer TTL costs freshness. And the hit rate on a cold start is zero: a cache that hides a database that cannot handle the full load turns every cache restart into an outage. Plan database capacity for the cold case, or warm caches before taking traffic.

**Single flight concentrates risk.** If the one read in progress is slow, every waiter is slow. With a timeout, every waiter fails together. Some systems refresh hot entries in the background shortly before expiry, or serve the stale value while one request refreshes, so a hot key never misses at all.

**LRU has blind spots.** A scan longer than the cache still flushes items used slightly less often than every C accesses. Policies such as LFU, ARC, 2Q and TinyLFU (used by the Caffeine library) track frequency as well as recency and do better on mixed workloads.

**Per-node caches multiply misses.** Ten nodes each miss every key once. A shared cache tier (Redis, Memcached) avoids that but adds a network hop and its own failure mode; sticky routing of keys to nodes is the other remedy.

## In real systems

**Memcached** is the classic cache-aside store: applications read it first and fill it on a miss. Its eviction is an LRU variant per memory slab class, and keys carry an expiry time. Facebook's "Scaling Memcache at Facebook" paper describes invalidations driven from the database's commit log, and **leases**, tokens given out on a miss that both limit how many clients refill a key at once (stampede protection) and reject a fill that was overtaken by an invalidation, the same race the generation number solves.

**Redis** is often used the same way. With `maxmemory` set, policies such as `allkeys-lru` and `allkeys-lfu` evict using an approximated LRU or LFU that samples a few keys rather than keeping an exact list. Keys can have a TTL with `EXPIRE` or `SET ... EX`.

**Caffeine** (Java) and Go's `groupcache` show the in-process side. Caffeine uses the W-TinyLFU policy for better hit rates than plain LRU and can refresh entries asynchronously before they expire. `groupcache` and the `singleflight` package from Go's extended libraries coalesce concurrent loads of the same key into one.

**HTTP caches and CDNs** apply TTLs through `Cache-Control: max-age`, and many CDNs collapse concurrent misses for the same object into one request to the origin (often called request collapsing). The `stale-while-revalidate` directive lets a cache serve an expired response while it refreshes in the background.

## Common misconceptions

- **"A TTL keeps the cache consistent."** It bounds how stale an answer can be; within that bound, readers see old values. It does not give read-after-write.
- **"On a write, just update the cache with the new value."** Concurrent writes through different nodes can leave the older value cached. Deleting is safe because a deletion carries no value that can be out of date.
- **"Deleting the key on every node is enough."** A read that started before the write can fill the cache with the old value after the deletion. You need a way to reject such fills, such as a generation number or a lease.
- **"Stampedes only happen on a cold start."** They happen whenever a hot key expires, which with a TTL is regularly. Single flight or early refresh is needed even when the cache is warm.
- **"LRU is always the best policy."** It is a good default. Workloads with large scans or stable popularity can do better with frequency-aware policies.

## How the tasks build it

[CA-1 Cache-aside reads](#/c/cache-aside) is the plain pattern: a map, a database read on a miss. The tests use skewed keys on one and two nodes and limit database reads to about one per (node, key).

[CA-2 Bounded memory with LRU](#/c/cache-lru) caps the cache at 20 entries. Traffic mixes 5 hot keys with a scan, and the hot set changes halfway; the tests probe the entry count, require a 25% hit rate that FIFO cannot reach, and check that you are not quietly keeping more than 20 entries.

[CA-3 Expiry with a TTL](#/c/cache-ttl) adds the 1000 ms TTL while the database changes behind the cache. No answer may be more than 1100 ms stale, and no more than 40% of gets may reach the database, so expiring too early fails too.

[CA-4 Stampede protection](#/c/cache-stampede) adds single flight with a 300 ms read timeout. Bursts of 25 gets within 5 ms may cause at most 2 reads, a key requested every 4 ms must cause about one read per expiry, and lost database reads must not block the key afterwards.

[CA-5 Invalidation on writes](#/c/cache-invalidation) adds puts across three nodes with the read-after-write promise. Staged races hold back reads until after a write, slow down invalidations and drop messages, which exposes the stale fill and the old in-progress read.

## Further reading

- [Scaling Memcache at Facebook (NSDI 2013)](https://www.usenix.org/conference/nsdi13/technical-sessions/presentation/nishtala) — invalidation from the commit log, leases against stale sets and thundering herds, at very large scale.
- [Redis: key eviction](https://redis.io/docs/latest/develop/reference/eviction/) — the eviction policies Redis offers and how its approximated LRU and LFU work.
- [Caffeine: Design](https://github.com/ben-manes/caffeine/wiki/Design) — why W-TinyLFU beats LRU on real traces, and how refresh-ahead avoids misses on hot keys.
- [Go singleflight package](https://pkg.go.dev/golang.org/x/sync/singleflight) — a small, widely used implementation of request coalescing.
- [RFC 5861: HTTP Cache-Control Extensions for Stale Content](https://www.rfc-editor.org/rfc/rfc5861) — `stale-while-revalidate` and `stale-if-error`, two standard ways to trade freshness for availability.
