// Track: Caching. See docs/authoring.md for the shape of this file.
import { cacheAside, cacheLru, cacheTtl, cacheStampede, cacheInvalidation } from '../../sim/workloads/caching.js';

export default {
  id: 'caching',
  code: 'CA',
  category: 'scalability',
  title: 'Caching',
  level: 'Intermediate',
  blurb: 'Put a cache in front of a slow database: bounded memory, expiry, invalidation on writes and protection against stampedes.',
  subtracks: [
    {
      title: 'Hits and memory',
      tasks: [
        {
          slug: 'cache-aside',
          title: 'Cache-aside reads',
          level: 'Easy',
          minutes: 10,
          summary: 'Answer repeated reads from memory so the slow database sees each key about once.',
          concepts: ['cache-aside', 'hit rate', 'hot keys'],
          workload: cacheAside,
          tests: [
            { name: 'One node, hot keys', blurb: '300 gets over 30 keys; a few keys get most of the traffic.', nodeCount: 1, duration: 4400, params: { ops: 300, keys: 30 } },
            { name: 'Two nodes', blurb: '400 gets over 40 keys, spread over two nodes that each keep their own cache.', nodeCount: 2, duration: 5600, params: { ops: 400, keys: 40 } },
          ],
          hints: [
            'A `Map` from key to value, at the top of the file, is the whole cache.',
            'On a `get`: if the key is in the map, reply at once. Otherwise read it from the database, store it, then reply.',
            'Values in this task never change, so an entry can be kept forever.',
          ],
        },
        {
          slug: 'cache-lru',
          title: 'Bounded memory with LRU',
          level: 'Medium',
          minutes: 20,
          summary: 'Hold at most 20 entries, and evict the least recently used so the hot keys survive a scan.',
          concepts: ['LRU eviction', 'capacity', 'scan resistance'],
          workload: cacheLru,
          tests: [
            { name: 'Hot keys and a scan', blurb: '300 gets: one in three for 5 hot keys (a different 5 halfway), the rest a scan of keys asked for once.', nodeCount: 1, duration: 6200, params: { perNode: 300 } },
            { name: 'Two nodes', blurb: 'The same pattern on each of two nodes at once.', nodeCount: 2, duration: 6200, params: { perNode: 300 } },
          ],
          hints: [
            'A `Map` iterates in insertion order. Treat its first key as the least recently used and its last as the most recently used.',
            'On a hit, `delete` the key and `set` it again: that moves it to the most recently used end.',
            'After storing a new key, while `cache.size > 20`, delete `cache.keys().next().value`.',
            'Answer `{ "type": "stats" }` with `{ type: "stats_ok", entries: cache.size }`.',
            'If the hit rate is around 17%, hits are not moving keys to the fresh end: the cache is evicting in insertion order (FIFO).',
          ],
        },
      ],
    },
    {
      title: 'Freshness under load',
      tasks: [
        {
          slug: 'cache-ttl',
          title: 'Expiry with a TTL',
          level: 'Easy',
          minutes: 15,
          summary: 'The database changes behind the cache. Never serve an entry more than 1000ms after reading it.',
          concepts: ['TTL', 'staleness bound', 'expiry'],
          workload: cacheTtl,
          tests: [
            { name: 'Database changes', blurb: 'One node, 10 keys; each key changes in the database about every 700ms.', nodeCount: 1, duration: 6000, params: { keys: 10, changeEvery: 700 } },
            { name: 'Two nodes, faster changes', blurb: 'Two nodes, 12 keys changing about every 400ms.', nodeCount: 2, duration: 6000, params: { keys: 12, changeEvery: 400 } },
          ],
          hints: [
            'Store `{ value, at }` instead of the bare value.',
            'Take `at = node.now()` just before you send the read. The value is at least that fresh, whatever the reply\'s journey.',
            'In `lookup`, an entry with `node.now() - at >= 1000` is a miss: delete it and read again.',
            'No timer is needed: check the age when the entry is used.',
          ],
        },
        {
          slug: 'cache-stampede',
          title: 'Stampede protection',
          level: 'Medium',
          minutes: 25,
          summary: 'When a hot key misses, make the waiting requests share one database read.',
          concepts: ['cache stampede', 'single flight', 'request coalescing'],
          workload: cacheStampede,
          tests: [
            { name: 'Bursts', blurb: 'Bursts of 25 gets for one key within 5ms, on cold keys and on keys whose entry just expired.', nodeCount: 2, duration: 4000, params: { scenario: 'bursts' } },
            { name: 'Hot key under load', blurb: 'One key asked for every 4ms on two nodes for 4.5 seconds; it expires every second and each read takes 100–200ms.', nodeCount: 2, duration: 5200, params: { scenario: 'none', steady: { gap: 4, change: 600 }, delay: [100, 200] } },
            { name: 'Lost reads', blurb: 'The database never answers the reads made during two of the five bursts.', nodeCount: 1, duration: 4200, params: { scenario: 'lossy' } },
          ],
          hints: [
            'Keep a second map, `inflight`, from key to the promise of the read in progress.',
            'In `load`: if `inflight` has the key, return that promise. Otherwise start the read, put its promise in the map, and return it. Every waiter `await`s the same promise.',
            'Remove the key from `inflight` when the read ends, on success, error **and** timeout. A failed read that stays in the map blocks the key forever.',
            'Put the read in an `async` function that you call once and keep the promise of: `const p = (async () => { ... })(); inflight.set(key, p);`.',
          ],
        },
        {
          slug: 'cache-invalidation',
          title: 'Invalidation on writes',
          level: 'Hard',
          minutes: 40,
          summary: 'Three caching nodes take writes. After put_ok, no node may serve the old value, whatever the races.',
          concepts: ['invalidation', 'stale fill race', 'generations', 'read-after-write'],
          workload: cacheInvalidation,
          tests: [
            { name: 'Reads and writes', blurb: 'Three nodes; gets every 8ms and puts every 100ms on 6 keys, through any node.', nodeCount: 3, duration: 5000, params: {} },
            { name: 'Staged races', blurb: 'Slow reads overtaken by writes, a get joining an old read, and slowed invalidations.', nodeCount: 3, duration: 4200, params: { staged: 'races', getGap: 25, putGap: 250 } },
            { name: 'Lossy network', blurb: 'One message in ten between nodes is lost, and two invalidations are lost on purpose.', nodeCount: 3, duration: 5000, dropRate: 0.1, params: { staged: 'lossy' } },
          ],
          hints: [
            'Start on one node: when a put\'s write completes, delete the key from the cache before replying.',
            'Then the peers: send each `{ type: "invalidate", key }` with `node.rpc`, retry on `null`, and reply `put_ok` only when every peer has answered. `await Promise.all(node.peers.map(...))` does it.',
            'Keep a generation number per key and bump it in `invalidate`. A read records the generation when it starts and stores its value only if the generation is still the same.',
            '`invalidate` must also delete the key from `inflight`, or a get that arrives after the write can wait for a read that began before it.',
            'Delete the entry rather than storing the new value: two puts racing through two nodes could leave the older value cached on a third.',
          ],
        },
      ],
    },
  ],
  mistakes: [
    { title: 'Counting a hit as nothing', symptom: 'The hot keys keep missing even though the cache is the right size.', fix: 'Eviction by insertion order (FIFO) ignores hits. On every hit, move the key to the most recently used end, so popular keys stay.' },
    { title: 'Refreshing the expiry time on a hit', symptom: 'A popular key serves the same stale value for minutes.', fix: 'Date an entry from when it was read from the database. A hit must not extend its life.' },
    { title: 'Leaving a failed read in the in-flight map', symptom: 'After one database timeout, every later request for that key fails at once.', fix: 'Remove the read from the map when it ends, on success, error and timeout alike.' },
    { title: 'Deleting the entry and nothing else', symptom: 'Rarely, a node serves the old value long after a write was acknowledged.', fix: 'A read that started before the write can store the old value after the delete. Bump a per-key generation on invalidation and drop reads that started under an older one; also forget the read in flight.' },
  ],
  questions: [
    { q: 'Why can a small cache absorb most of the reads of a large database?', a: 'Real access patterns are skewed: a small fraction of keys gets most of the requests. Keeping only those hot keys in memory serves most traffic, so the hit rate depends on how concentrated the traffic is, not on how large the database is.' },
    { q: 'A TTL bounds staleness. Why not set it very short to be safe?', a: 'Every expiry is a database read, so a short TTL moves load back to the database, and every expiry of a hot key is a chance for a stampede. The TTL is a trade between how stale an answer may be and how much load the cache removes.' },
    { q: 'Why do many systems delete a cache entry on write rather than store the new value in it?', a: 'Two concurrent writes can reach the cache in a different order from the database, leaving the older value cached indefinitely. A delete is the same whichever order it arrives in; the next read fetches whatever the database holds then.' },
  ],
  reading: [
    { kind: 'Paper', title: 'Scaling Memcache at Facebook', url: 'https://www.usenix.org/conference/nsdi13/technical-sessions/presentation/nishtala', note: 'Nishtala et al., NSDI 2013: look-aside caching at scale, invalidation, and leases against stale sets and thundering herds.' },
    { kind: 'Blog', title: 'Caching challenges and strategies', url: 'https://aws.amazon.com/builders-library/caching-challenges-and-strategies/', note: 'The Amazon Builders\' Library on TTLs, cold starts, and what happens when a cache disappears.' },
    { kind: 'Docs', title: 'Go: package singleflight', url: 'https://pkg.go.dev/golang.org/x/sync/singleflight', note: 'A small, widely used implementation of request coalescing.' },
  ],
};
