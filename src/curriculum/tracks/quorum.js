// Track: Quorum replication. See docs/authoring.md for the shape of this file.
import { quorumWrites, quorumReads, readRepair, antiEntropy } from '../../sim/workloads/quorum.js';

export default {
  id: 'quorum',
  code: 'QR',
  category: 'convergence',
  title: 'Quorum replication',
  level: 'Advanced',
  blurb: 'Dynamo-style replication with no leader: N replicas, W acknowledgements to write, R answers to read, and repair for what was missed.',
  subtracks: [
    {
      title: 'Quorums',
      tasks: [
        {
          slug: 'quorum-writes',
          title: 'Quorum writes',
          level: 'Medium',
          minutes: 20,
          summary: 'Any node takes a write, sends it to every replica and answers once W have stored it.',
          concepts: ['leaderless replication', 'write quorum', 'coordinator'],
          workload: quorumWrites,
          tests: [
            { name: 'Healthy network', blurb: '40 writes through random nodes; each acknowledged write is looked up on every replica.', nodeCount: 5, duration: 4000, params: { ops: 40 } },
            { name: 'Minority cut off', blurb: 'Two of five nodes are partitioned away: the three still write, the two must refuse.', nodeCount: 5, duration: 6000, params: { ops: 60, fault: 'partition' } },
            { name: 'Nodes crash', blurb: 'Two nodes crash and writes continue; then a third crashes and no quorum is left.', nodeCount: 5, duration: 6000, params: { ops: 60, fault: 'crash' } },
          ],
          hints: [
            'The coordinator is a replica too. Store the value locally and count that as the first acknowledgement.',
            'Send `{ type: "store", key, value }` to every peer with `node.rpc(peer, body, { timeout: 300 })`. Do not `await` them one after another: start them all, then count answers as they arrive.',
            'Wrap the counting in `new Promise((resolve) => ...)`. Resolve with success when the count reaches W, and with failure once every rpc has finished (answered or timed out) and the count is still short.',
            'Make sure exactly one reply goes to the client: `put_ok` on success, `{ type: "error", text }` on failure. With a 300ms rpc timeout the error arrives well inside the 800ms allowed.',
          ],
        },
        {
          slug: 'quorum-reads',
          title: 'Versions and quorum reads',
          level: 'Hard',
          minutes: 35,
          summary: 'Ask R replicas and return the newest version, so a read always sees the last acknowledged write.',
          concepts: ['read quorum', 'W + R > N', 'versions', 'last write wins'],
          workload: quorumReads,
          tests: [
            { name: 'Healthy network', blurb: '120 reads and overwrites on four keys through random nodes.', nodeCount: 5, duration: 5000, params: { ops: 120 } },
            { name: 'Minority cut off', blurb: 'Two of five nodes miss a second of writes, then coordinate reads and writes right after the heal.', nodeCount: 5, duration: 6000, params: { ops: 150, fault: 'partition' } },
            { name: 'Two nodes down', blurb: 'Two of five nodes crash for good; the other three keep reading and writing.', nodeCount: 5, duration: 5000, params: { ops: 120, fault: 'crash' } },
            { name: 'Three replicas', blurb: 'N = 3, W = R = 2, with one node cut off for a while.', nodeCount: 3, duration: 6000, params: { ops: 150, fault: 'partition' } },
            { name: 'Slow links, racing writes', blurb: 'Latency of 5–120ms, and at the end two puts to each key through different nodes at the same moment.', nodeCount: 5, duration: 8000, latency: [5, 120], params: { ops: 120, keys: 4, race: true } },
          ],
          hints: [
            'Store `{ value, counter, origin }` per key. Version A is newer than B if its counter is higher, or the counters are equal and its origin (a node id) is greater. A replica only replaces its copy with a newer version.',
            'Add a node-to-node `read` request that returns the replica\'s entry for a key (or `null`). `get` sends it to everyone, waits for R answers and returns the value of the newest entry.',
            'A coordinator that was cut off does not know how far the counter has moved. If it picks `counter` from its own copy, its write is acknowledged and then loses to an older one.',
            'So `put` has two rounds: first `read` from R replicas and take the highest counter plus one, then `store` to all and wait for W. The first round is a quorum too, so it is guaranteed to see the last acknowledged write.',
          ],
        },
      ],
    },
    {
      title: 'Repair',
      tasks: [
        {
          slug: 'quorum-read-repair',
          title: 'Read repair',
          level: 'Medium',
          minutes: 20,
          summary: 'A read that sees a stale replica writes the newest version back to it.',
          concepts: ['read repair', 'convergence', 'idempotent writes'],
          workload: readRepair,
          tests: [
            { name: 'Five replicas', blurb: 'Two nodes miss a round of writes; each key is then read once and every replica is inspected.', nodeCount: 5, duration: 5600, params: { keys: 8 } },
            { name: 'Three replicas', blurb: 'One node of three misses the writes.', nodeCount: 3, duration: 5600, params: { keys: 8 } },
            { name: 'Slow links', blurb: 'Latency of 5–80ms: the stale replicas often answer after the first R.', nodeCount: 5, duration: 5600, latency: [5, 80], params: { keys: 8 } },
          ],
          hints: [
            'After choosing the newest entry, compare it with what each replica returned. For that you need to know which replica each answer came from.',
            'Send the stale replicas the existing `store` message with the newest entry, version included. Do not mint a new version: this is a copy of an old write, not a new write.',
            'The read answers the client after R replies, but the replica that is behind may be reply number R + 1. Keep collecting until every replica has answered or timed out, then repair.',
            'The coordinator itself may be the stale one. Repair its own copy as well.',
          ],
        },
        {
          slug: 'quorum-anti-entropy',
          title: 'Anti-entropy',
          level: 'Hard',
          minutes: 35,
          summary: 'Replicas compare versions in the background, so missed writes arrive even for keys nobody reads.',
          concepts: ['anti-entropy', 'hinted handoff', 'gossip', 'convergence'],
          workload: antiEntropy,
          tests: [
            { name: 'Minority cut off', blurb: 'Two of five nodes miss a round of writes. No reads until every replica has been inspected.', nodeCount: 5, duration: 5400, params: { keys: 10 } },
            { name: 'Two partitions in a row', blurb: 'As soon as the first partition ends, part of the old majority is cut off instead.', nodeCount: 5, duration: 6800, params: { keys: 10, scenario: 'flapping' } },
            { name: 'Lossy network', blurb: 'One message in ten between nodes is lost, so most writes miss somebody.', nodeCount: 5, duration: 5800, dropRate: 0.1, params: { keys: 10, scenario: 'lossy', lossy: true } },
          ],
          hints: [
            'Two designs pass. Hinted handoff: the coordinator remembers which replicas did not acknowledge a write and keeps retrying them. Anti-entropy: every replica regularly compares its data with a peer. Anti-entropy also covers a coordinator that crashes with its hints, so the reference solution uses it.',
            'With `node.every(200, ...)`, pick one random peer and send it `{ key: version }` for everything you hold. Versions only: the values are the expensive part.',
            'The peer answers with the entries where it is ahead, and the keys where you are ahead. Apply the first, send it the second.',
            'Apply incoming entries through the same version check as `store`. Then a lost, repeated or outdated sync message can never do harm, and you do not need to retry: the next round covers it.',
          ],
        },
      ],
    },
  ],
  mistakes: [
    { title: 'Waiting for all N replicas', symptom: 'Writes fail or time out as soon as one node is down, although W are still reachable.', fix: 'Send to all N, but answer the client when W have acknowledged. Count the coordinator itself as one.' },
    { title: 'Taking the next version from the coordinator\'s own copy', symptom: 'After a partition heals, a write is acknowledged and the next read still returns the older value.', fix: 'The coordinator may have missed writes. Ask R replicas for their version first and use the highest counter plus one.' },
    { title: 'Replicas that overwrite unconditionally', symptom: 'A late store, a read repair or a sync brings an old value back.', fix: 'A replica replaces its copy only when the incoming version is newer. That one check makes every replication message safe to repeat, reorder or lose.' },
    { title: 'Repairing only the first R responders', symptom: 'Some replicas stay stale after a read, mostly the slow ones.', fix: 'Reply to the client after R answers, but keep listening until the rest have answered or timed out, then repair whoever was behind.' },
  ],
  questions: [
    { q: 'With N = 5, why do W = 3 and R = 3 guarantee that a read sees the last acknowledged write, and what does W = 5, R = 1 trade away?', a: 'Any 3 of 5 replicas share at least one member with any other 3, so the read set always contains a replica that stored the write. W = 5, R = 1 still overlaps and makes reads cheap, but a single unreachable node stops every write.' },
    { q: 'A put returns an error because only two of five replicas acknowledged. Is the value gone?', a: 'No. Those two replicas keep it, and read repair or anti-entropy may later spread it to the rest. A failed quorum write is "unknown", not "rolled back"; Dynamo-style stores have no rollback. Clients should treat an error like a timeout and retry.' },
    { q: 'Why is read repair not enough on its own?', a: 'It only fixes keys that are read. A rarely read key can sit on fewer than W healthy replicas for months, and one more failure then loses it. A background process (anti-entropy, usually with Merkle trees to find differences cheaply) restores the replication factor without waiting for a reader.' },
  ],
  reading: [
    { kind: 'Paper', title: 'Dynamo: Amazon\'s highly available key-value store', url: 'https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf', note: 'DeCandia et al., 2007. Quorums, hinted handoff, read repair and Merkle-tree anti-entropy in one system.' },
    { kind: 'Book', title: 'Designing Data-Intensive Applications, chapter 5: Leaderless replication', url: 'https://dataintensive.net/', note: 'Kleppmann on quorum conditions, sloppy quorums and the limits of last-write-wins.' },
    { kind: 'Docs', title: 'Apache Cassandra: Dynamo architecture', url: 'https://cassandra.apache.org/doc/latest/cassandra/architecture/dynamo.html', note: 'How a production system configures consistency levels, hints and repair.' },
  ],
};
