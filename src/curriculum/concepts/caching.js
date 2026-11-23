export default [
  {
    term: 'Cache-aside',
    aliases: ['lazy loading', 'look-aside cache'],
    summary: 'The application checks the cache first and, on a miss, reads the database and stores the answer itself.',
    detail: `In **cache-aside**, the cache is a passive store next to the application. On a request, the application looks in the cache. On a **hit** it answers from there; on a **miss** it reads the database, puts the value into the cache and answers. The database knows nothing about the cache.

For example, a get for k7 misses, costs a 50 ms database read and stores k7; the next hundred gets for k7 are answered from memory in microseconds.

The alternatives are **read-through** (the cache itself loads missing values) and **write-through** (writes go through the cache to the database). Cache-aside is the most common way Memcached and Redis are used.`,
    related: ['Hit rate', 'TTL', 'Invalidation'],
  },
  {
    term: 'Hit rate',
    aliases: ['cache hit rate', 'hit ratio', 'cache hit'],
    summary: 'The fraction of requests answered from the cache without going to the backing store.',
    detail: `**Hit rate** = hits / requests, or equivalently 1 − (database reads / requests). A cache that answers 900 of 1,000 gets from memory has a 90% hit rate and spares the database 90% of the load.

It depends on the traffic's skew, the cache's capacity, the eviction policy and the TTL. Skewed traffic gives high hit rates even with small caches.

A high hit rate is also a risk: the database may be sized for 10% of the traffic, and a cache restart (hit rate zero) then sends it ten times that.`,
    related: ['Hot keys', 'Cache-aside', 'LRU eviction'],
  },
  {
    term: 'Hot keys',
    aliases: ['hot key', 'skew', 'hot spot'],
    summary: 'The few keys that receive a large share of all requests.',
    detail: `Real traffic is rarely uniform. A few products, celebrities or settings are asked for far more than the rest, often roughly following a Zipf distribution. These are **hot keys**.

Skew is why caches work: if 20 keys out of 10,000 carry 80% of reads, caching those 20 avoids 80% of database work. It is also why caches fail in concentrated ways: when a hot key expires, the resulting stampede is large, and in a sharded system a hot key overloads the one node that owns it.`,
    related: ['Hit rate', 'Cache stampede'],
  },
  {
    term: 'LRU eviction',
    aliases: ['LRU', 'least recently used'],
    summary: 'When the cache is full, evict the entry that has gone longest without being used.',
    detail: `**LRU** keeps entries in recency order. Every hit moves the key to the most recently used end; inserting a new key when the cache is full evicts the key at the least recently used end.

With capacity 4 and order k1, k3, k5, k2 (oldest first), a hit on k3 gives k1, k5, k2, k3; a miss on k9 then evicts k1. In JavaScript a \`Map\`, which iterates in insertion order, does the job: delete and re-insert on a hit, evict the first key.

LRU keeps any key used at least once per capacity's worth of other accesses. It compares well with FIFO, which ignores hits, but frequency-aware policies (LFU, ARC, TinyLFU) beat it on some workloads. Redis approximates LRU by sampling.`,
    related: ['Capacity', 'Scan resistance', 'Hit rate'],
  },
  {
    term: 'Capacity',
    aliases: ['cache capacity', 'cache size', 'bounded cache'],
    summary: 'The most entries (or bytes) a cache may hold before it must evict something.',
    detail: `A cache without a **capacity** limit eventually holds every key ever requested and runs out of memory. A bounded cache picks an eviction policy to decide which entry goes when a new one would exceed the limit.

Capacity trades memory for hit rate, with diminishing returns: the hottest keys give most of the benefit, and doubling the cache beyond the hot set may add only a few percent of hits. Capacity can be counted in entries, as in the tasks, or in bytes, as Memcached and Redis's \`maxmemory\` do.`,
    related: ['LRU eviction', 'Hit rate'],
  },
  {
    term: 'Scan resistance',
    aliases: ['scan', 'cache scan'],
    summary: 'A cache\'s ability to keep its hot entries while a long run of one-off keys passes through it.',
    detail: `A **scan** is a sequence of keys each requested once: a batch job walking the catalogue, a report, a crawler. A scan-resistant cache keeps its frequently used entries while the scan passes.

LRU resists short scans as long as hot keys recur within the capacity: they keep returning to the fresh end while scan keys drift to the stale end and fall off. FIFO does not, because it evicts by insertion order however often a key is used. Very long scans can still flush LRU, which is why databases use variants such as midpoint insertion (MySQL's InnoDB buffer pool) or policies like 2Q and ARC.`,
    related: ['LRU eviction', 'Capacity'],
  },
  {
    term: 'TTL',
    aliases: ['time to live', 'time-to-live'],
    summary: 'How long a cached entry may be served after it was read before it must be read again.',
    detail: `A **TTL** of 1000 ms means an entry read at t=500 may be served until t=1500; after that, a lookup treats it as a miss and reads the database again. Checking the age on lookup is enough; no timer is required.

Date the entry from when the read was sent, so time spent in transit counts against the TTL, and do not reset the date on a hit, or a popular key never expires.

A TTL bounds staleness but does not remove it. Redis (\`EXPIRE\`), Memcached (an expiry per item), DNS records and HTTP's \`Cache-Control: max-age\` all use TTLs.`,
    related: ['Staleness bound', 'Expiry', 'Cache stampede'],
  },
  {
    term: 'Staleness bound',
    aliases: ['bounded staleness', 'staleness'],
    summary: 'A guarantee that no answer is older than a fixed amount of time, such as one second.',
    detail: `A **staleness bound** promises that any value served was correct at some moment within the last *T* milliseconds. With a 1000 ms TTL dated from the read's send time, a cache cannot serve a value the database replaced more than 1000 ms ago (plus the time the answer takes to reach the client).

It is weaker than read-after-write: within the bound, a client can write and then read its old value. It is often good enough for product pages, feeds and dashboards, and it is far cheaper than invalidation because it needs no coordination. Azure Cosmos DB offers "bounded staleness" as a named consistency level.`,
    related: ['TTL', 'Read-after-write'],
  },
  {
    term: 'Expiry',
    aliases: ['expiration', 'cache expiry'],
    summary: 'The moment a cached entry stops being valid, after which it is treated as a miss.',
    detail: `An entry **expires** when its TTL runs out. Expiry can be *lazy*, checked when the entry is looked up, or *active*, with a background process removing expired entries to free memory. Redis does both: lazy checks on access plus periodic sampling of keys with a TTL.

Expiry of a hot key is the trigger for a cache stampede, since every request for the key misses at the same moment. Adding a little random jitter to TTLs keeps many keys from expiring together.`,
    related: ['TTL', 'Cache stampede'],
  },
  {
    term: 'Cache stampede',
    aliases: ['thundering herd', 'dogpile', 'dog-pile effect'],
    summary: 'Many requests miss the same key at once and all hit the database together.',
    detail: `When a hot key expires or the cache starts empty, every request for it misses. If the key gets a request every 4 ms and the database takes 150 ms to answer, about 40 requests miss before the first read returns, and each sends its own read. That is a **cache stampede**.

The database, shielded until then, receives a sudden burst for one key and slows down, which lengthens the window and makes the burst bigger.

Defences: single flight (one read per key at a time), refreshing hot entries shortly before they expire, serving the stale value while one request refreshes, and jittering TTLs. Facebook's Memcache leases are a distributed form of single flight.`,
    related: ['Single flight', 'Request coalescing', 'Hot keys', 'Expiry'],
  },
  {
    term: 'Single flight',
    aliases: ['singleflight'],
    summary: 'At most one load per key at a time: concurrent misses for the same key wait for the same load.',
    detail: `With **single flight**, the first miss for a key starts the read and records it as in progress. Later misses find the record and wait for that read instead of starting their own; when it finishes, all of them receive the same answer.

In JavaScript the record is a promise: \`inflight.get(key) ?? start the read and remember its promise\`. In Go, the \`golang.org/x/sync/singleflight\` package does the same with a mutex.

The record must be removed however the read ends, including errors and timeouts. A failed read left in the map blocks the key forever. And with invalidation, a write must also remove it, so that later requests do not join a read that started before the write.`,
    related: ['Request coalescing', 'Cache stampede'],
  },
  {
    term: 'Request coalescing',
    aliases: ['request collapsing', 'coalescing'],
    summary: 'Merging identical concurrent requests into one request to the backend and sharing its answer.',
    detail: `**Request coalescing** is the general name for what single flight does: when several identical requests are outstanding, send one to the backend and fan its response out to all the waiters.

CDNs do this for cache misses on the same object (often called request collapsing), so a popular file that just expired causes one fetch from the origin instead of thousands. The same idea appears in DataLoader-style batching in GraphQL servers, which merges identical lookups made during one request.`,
    related: ['Single flight', 'Cache stampede'],
  },
  {
    term: 'Invalidation',
    aliases: ['cache invalidation', 'invalidate'],
    summary: 'Removing a cached entry when the underlying data changes, so the old value is not served again.',
    detail: `**Invalidation** deletes a key from caches when it is written, instead of waiting for its TTL. To give read-after-write across several caching nodes, the writer must have every node drop the key, and wait for each to confirm, before acknowledging the write.

Delete rather than update: two writes racing through two nodes could each push their own value and leave the older one cached somewhere. A deletion has no value to be out of date.

Invalidation has races: a read sent before the write can return after the deletion and re-insert the old value. Generation numbers or leases close that hole. Many systems send invalidations asynchronously and keep a TTL as a backstop for lost messages.`,
    related: ['Stale fill race', 'Generations', 'Read-after-write'],
  },
  {
    term: 'Stale fill race',
    aliases: ['stale set', 'stale fill'],
    summary: 'A read that began before a write returns after the invalidation and puts the old value back in the cache.',
    detail: `Sequence on node n3: at t=0 a miss sends a database read for k2 and gets the old value w6, but the reply is slow. At t=50 a write of w7 completes and n3 deletes k2. At t=190 the old read returns and, in a naive cache, stores w6. The cache now serves the old value with a fresh TTL, after the write was acknowledged. That is a **stale fill**.

The fix is to let a read store its result only if nothing invalidated the key while it was out: a per-key generation number, a version check, or a lease that the invalidation revokes (Facebook's Memcache leases).`,
    related: ['Generations', 'Invalidation'],
  },
  {
    term: 'Generations',
    aliases: ['generation number', 'generation'],
    summary: 'A per-key counter bumped by every invalidation, used to reject results computed before the latest change.',
    detail: `A **generation** number for a key starts at 0 and increases by one on every invalidation. A read records the generation when it starts. When it returns, it may store its value only if the generation is still the same; otherwise an invalidation happened while it was out, and its value may predate the write.

Example: a read of k2 starts at generation 0; a write invalidates k2, making it generation 1; the read returns and sees 1 ≠ 0, so it answers its waiting callers but does not store.

The same idea, a number that says "this belongs to an older era", appears as Raft terms, fencing tokens and epoch numbers elsewhere in distributed systems.`,
    related: ['Stale fill race', 'Invalidation'],
  },
  {
    term: 'Read-after-write',
    aliases: ['read-after-write consistency'],
    summary: 'Once a write is acknowledged, every later read returns that value or a newer one.',
    detail: `**Read-after-write** consistency means that after a client has been told its write succeeded, a read sent afterwards does not return an older value. In the strongest form it holds for every client and every node; *read-your-writes* is the weaker form that promises it only to the writer.

A cache with only a TTL breaks it: the writer can read the old value from another node's cache for up to a TTL. Keeping it requires invalidating every cache, and closing the stale fill race, before acknowledging the write.`,
    related: ['Invalidation', 'Staleness bound'],
  },
];
