// Track: Primary and backups. See docs/authoring.md for the shape of this file.
import { replAsync, replSync, replCatchUp, replChain, replMonotonic, replConsistentPrefix, replMultiLeader } from '../../sim/workloads/replication.js';

export default {
  id: 'replication',
  code: 'RP',
  category: 'convergence',
  title: 'Primary and backups',
  level: 'Advanced',
  blurb: 'One node takes the writes and the others follow. Build asynchronous and synchronous replication, bring a lagging backup up to date, line the nodes up into a chain, then let clients read from any replica and let every node take writes.',
  subtracks: [
    {
      title: 'One primary',
      tasks: [
        {
          slug: 'repl-async',
          title: 'Asynchronous replication',
          level: 'Medium',
          minutes: 25,
          summary: 'The primary answers at once and the backups follow a moment later, in the right order.',
          concepts: ['primary-backup', 'replication lag', 'sequence numbers'],
          workload: replAsync,
          tests: [
            { name: 'Healthy network', blurb: 'Three clients write through any node while every node is read.', nodeCount: 3, duration: 4000 },
            { name: 'Rapid overwrites', blurb: 'The same keys are overwritten quickly on uneven links (2–40ms), so replication messages overtake each other.', nodeCount: 3, duration: 4000, latency: [2, 40], params: { keysPerClient: 1, gap: [0, 10], rotate: 0.03, writeTo: ['n1'] } },
            { name: 'Backup cut off', blurb: 'From 1.5s the last node is cut off for good. The rest must carry on as if nothing happened.', nodeCount: 4, duration: 4000, params: { cutOff: 1500 } },
          ],
          hints: [
            'Start with the write path. On a backup, `await node.rpc(primary, { type: "write", key, value })` and answer your client with what comes back. The handler must be `async`.',
            'On the primary: apply, `node.reply` with `write_ok`, then `node.send` the write to every peer. Replying before sending is what makes it asynchronous.',
            'Run "Rapid overwrites" and look at the detail of the failing check. Two messages sent 5ms apart can arrive in either order.',
            'Let the primary count its writes: `seq += 1`, and send `seq` along. Store `{ value, seq }` per key. A backup applies a write only if its `seq` is higher than the one it holds for that key.',
          ],
        },
        {
          slug: 'repl-sync',
          title: 'Synchronous replication',
          level: 'Medium',
          minutes: 30,
          summary: 'Say write_ok only when every backup has the write, and see what that costs during a partition.',
          concepts: ['synchronous replication', 'read-your-writes', 'availability'],
          workload: replSync,
          tests: [
            { name: 'Healthy network', blurb: 'After each acknowledged write, the client immediately reads the key from every node.', nodeCount: 3, duration: 4000 },
            { name: 'Lossy links', blurb: '15% of messages between nodes are lost. At least 80% of writes must still succeed.', nodeCount: 3, duration: 4000, dropRate: 0.15, params: { okFraction: 0.8 } },
            { name: 'Backup cut off', blurb: 'The last node is cut off from 1.5s to 3s. Writes must fail honestly, then work again.', nodeCount: 3, duration: 5500, params: { partition: [1500, 3000] } },
          ],
          hints: [
            'Replace `node.send(peer, …)` with `node.rpc(peer, …)`, and have the backup `node.reply(msg, { type: "replicate_ok" })` after it has applied the write.',
            'Wait for all of them: `const acks = await Promise.all(node.peers.map((peer) => replicate(peer, entry)))`. Reply `write_ok` only if every one confirmed.',
            'An rpc that returns `null` was lost or the peer is unreachable; you cannot tell which. Retry in a loop with a short timeout (60ms) until a deadline of about 500ms, then give up.',
            'When you give up, answer `{ type: "error", code: "unavailable" }`. A backup that forwards a write needs a timeout longer than the primary\'s deadline but shorter than the client\'s 1s.',
          ],
        },
        {
          slug: 'repl-catch-up',
          title: 'Catching up',
          level: 'Hard',
          minutes: 40,
          summary: 'A backup that was cut off or crashed returns and gets exactly the writes it missed, in order.',
          concepts: ['replication log', 'durability', 'crash recovery'],
          workload: replCatchUp,
          tests: [
            { name: 'Backup cut off, then back', blurb: 'The last node is cut off from 1s to 2.5s while writes continue elsewhere.', nodeCount: 3, duration: 5000, params: { partition: [1000, 2500] } },
            { name: 'Lossy links', blurb: 'A quarter of all messages between nodes are lost.', nodeCount: 3, duration: 4500, dropRate: 0.25, params: { okFraction: 0.6 } },
            { name: 'Backup crashes', blurb: 'The last node crashes at 1s and restarts at 2.2s with only its disk.', nodeCount: 3, duration: 5000, params: { crash: { node: 'backup', at: 1000, restart: 2200 } } },
            { name: 'Primary crashes', blurb: 'The primary crashes at 1.5s and restarts at 2.3s. Nothing it acknowledged may be lost.', nodeCount: 3, duration: 6000, params: { crash: { node: 'primary', at: 1500, restart: 2300 } } },
          ],
          hints: [
            'Replace the per-key `seq` with a log: an array of `{ key, value }` in the primary\'s order. The index of an entry is its sequence number.',
            'Keep the log on disk (`node.disk.log ??= []`) on every node, and rebuild the `Map` from it at the top of the file. That one loop is your whole crash recovery.',
            'A backup accepts a batch `{ from, entries }` only if `from <= log.length`, appends the entries it does not have yet, and always replies with `have: log.length`.',
            'On the primary, run one loop per backup: while that backup has fewer entries than you, `await node.rpc(...)` the next slice; on a reply, move to `res.have`; on `null`, try again. Start the loops after every write and once at startup.',
          ],
        },
      ],
    },
    {
      title: 'A chain of nodes',
      tasks: [
        {
          slug: 'repl-chain',
          title: 'Chain replication',
          level: 'Hard',
          minutes: 40,
          summary: 'Writes enter at the head and are acknowledged by the tail; reads at the tail are linearizable.',
          concepts: ['chain replication', 'linearizable reads', 'head and tail'],
          workload: replChain,
          tests: [
            { name: 'Healthy network', blurb: 'A chain of three. Writes go to the head, reads to the tail.', nodeCount: 3, duration: 4000 },
            { name: 'Long chain, uneven links', blurb: 'Five nodes and links of 2–50ms, so messages overtake each other on every hop.', nodeCount: 5, duration: 5000, latency: [2, 50], params: { keysPerClient: 1, gap: [0, 10], rotate: 0.03 } },
            { name: 'Lossy links', blurb: 'A chain of four that loses 20% of the messages between nodes.', nodeCount: 4, duration: 5000, dropRate: 0.2 },
          ],
          hints: [
            'Each node needs to know one thing: its successor, `chain[chain.indexOf(node.id) + 1]`. The head stores `msg.src` and `msg.body.msg_id` in the entry so they travel with it.',
            'At the tail, `node.send(entry.client, { type: "write_ok", in_reply_to: entry.msg_id })` is exactly what `node.reply` would have sent.',
            'Order: give every entry its position in the head\'s log. A node appends a batch `{ from, entries }` only if it extends its own log without a gap.',
            'Loss: push to the successor with `node.rpc` in a loop, one request at a time. The reply carries `have: log.length`; resend from there until the successor has everything you have.',
          ],
        },
      ],
    },
    {
      title: 'Reading your replicas',
      tasks: [
        {
          slug: 'repl-monotonic-reads',
          title: 'Monotonic reads',
          level: 'Medium',
          minutes: 35,
          summary: 'Clients read from any replica, yet never see time go backwards and always see their own writes, thanks to a token they carry.',
          concepts: ['monotonic reads', 'session guarantees', 'read-your-writes'],
          workload: replMonotonic,
          tests: [
            { name: 'Lagging replicas', blurb: 'Four clients read and write through random nodes. n2 lags by up to 30ms, n3 by 60–220ms, and replication messages overtake each other.', nodeCount: 3, duration: 4500 },
            { name: 'Four replicas, uneven lag', blurb: 'Four nodes on 2–30ms links; each backup lags by its own amount, the last by up to 220ms.', nodeCount: 4, duration: 4500, latency: [2, 30], params: { clients: 5 } },
            { name: 'Backup cut off', blurb: 'From 1.5s the last node is cut off for good. Clients with a token it cannot meet may be refused there; new clients must still be served.', nodeCount: 3, duration: 4500, params: { cutOff: 1500 } },
          ],
          hints: [
            'Start from the token. If every node applies the primary\'s writes strictly in order, one number describes a node\'s whole state: how many writes it has applied. That number is a good token.',
            'Writes can arrive out of order. Keep the early ones aside (`early.set(seq, entry)`) and apply from there while `early.has(applied + 1)`. The highest `seq` you have seen is not the same thing as how many you have applied.',
            '`write_ok` carries the write\'s own `seq` as its token; `read_ok` carries `applied`. A backup relays the primary\'s token when it forwards a write.',
            'On a read, wait while `applied < token`: `await new Promise((r) => node.after(5, r))` in a loop. Give up after about 600ms with `{ type: "error", code: "unavailable" }`. A token of `null` needs no waiting at all.',
          ],
        },
      ],
    },
    {
      title: 'More than one leader',
      tasks: [
        {
          slug: 'repl-consistent-prefix',
          title: 'Consistent prefix',
          level: 'Hard',
          minutes: 45,
          summary: 'Every node accepts writes. An answer must never be seen before the question it answers, on any node, whichever link is slow.',
          concepts: ['consistent prefix reads', 'causal dependencies', 'session guarantees'],
          workload: replConsistentPrefix,
          tests: [
            { name: 'Fast and slow links', blurb: 'Three nodes. Each direction of each link is either fast (0–25ms extra) or slow (110–230ms extra).', nodeCount: 3, duration: 4500 },
            { name: 'Five regions', blurb: 'Five nodes on mixed links; questions and answers are written and read through random nodes.', nodeCount: 5, duration: 5000, params: { observers: 4 } },
            { name: 'Mostly slow', blurb: 'Most links are slow and a few are fast, so writes from one node routinely overtake writes from another.', nodeCount: 4, duration: 5000, params: { slowShare: 0.7, slow: [80, 260] } },
          ],
          hints: [
            'Name every write by where it was accepted and its number there: `{ origin: node.id, n }`. Keep `applied[origin]`, how many writes from each node you have applied. Apply each origin\'s writes strictly in order.',
            'That is not enough: q7 comes from n1, a7 from n2, and n2 → n3 may be faster than n1 → n3. When a node accepts a write, attach `deps`: a copy of its `applied` vector. A replica applies an entry only when `applied[origin] === n - 1` and its `applied` covers `deps`; otherwise it keeps the entry in a pending list.',
            'Use the `applied` vector as the token. Before a read or a write, wait until your `applied` covers the client\'s token; then the answer reflects everything the client has seen, and a write\'s `deps` include it.',
            'After applying something, walk the pending list again: one entry can unblock another.',
          ],
        },
        {
          slug: 'repl-multi-leader',
          title: 'Multi-leader writes',
          level: 'Hard',
          minutes: 40,
          summary: 'Every node takes writes to the same keys, even when cut off, and all nodes settle on the same winner for each key.',
          concepts: ['multi-leader replication', 'write conflicts', 'last write wins', 'Lamport clock'],
          workload: replMultiLeader,
          tests: [
            { name: 'Concurrent writers', blurb: 'Four clients overwrite three shared keys through every node; links add 5–60ms, so writes cross in flight.', nodeCount: 3, duration: 4500 },
            { name: 'Skewed clocks', blurb: 'The same, but node clocks disagree by up to 400ms either way.', nodeCount: 3, duration: 4500, clockSkew: 400 },
            { name: 'Regions split', blurb: 'The last node is cut off from 1s to 2.5s. Both sides keep accepting writes to the same keys, then must agree after the partition heals.', nodeCount: 3, duration: 5000, params: { partition: [1000, 2500] } },
          ],
          hints: [
            'Store `{ value, ts }` per key with `ts = [counter, node.id]`. When two versions of a key meet, keep the one with the higher counter, or on a tie the higher node id (as strings). Every node applies the same rule, so they agree whatever the arrival order.',
            'The counter is a Lamport clock: `clock += 1` for each write a node accepts, and `clock = Math.max(clock, ts[0])` for every timestamp it receives. Then a write always beats what its node already had.',
            'Reply `{ type: "write_ok", ts }` straight away, then send the write to every peer. Do not use `node.now()` as the counter: clocks are skewed in one test.',
            'Messages sent during the partition are lost. Every 100ms, send each peer your whole store and merge what you receive with the same rule; it is small and makes the nodes converge after any outage.',
          ],
        },
      ],
    },
  ],
  mistakes: [
    { title: 'Applying replication messages in arrival order', symptom: 'A backup shows a newer value and then an older one, or ends on the wrong value.', fix: 'The primary numbers its writes. A backup applies them by number, never by the order they happen to arrive in.' },
    { title: 'Saying write_ok before the promise is kept', symptom: 'A client writes, reads from another node and does not see its write.', fix: 'Decide what write_ok promises (on the primary, on its disk, on every node) and send it only after that has happened.' },
    { title: 'Waiting forever for a backup', symptom: 'Writes get no reply at all while a backup is unreachable.', fix: 'Put a deadline on every wait and turn a missed deadline into an explicit error the client can act on.' },
    { title: 'Counting from zero after a restart', symptom: 'After the primary restarts, backups ignore its new writes.', fix: 'Sequence numbers are part of the data. Keep the log on disk and continue from its length.' },
  ],
  questions: [
    { q: 'The primary of an asynchronously replicated database dies for good and a backup takes over. What can be lost?', a: 'Every write the primary acknowledged but had not yet sent, or that was still in flight. The clients were told those writes succeeded. That window is the replication lag, and it is the price of fast acknowledgements.' },
    { q: 'With fully synchronous replication, why does adding backups make the system less available?', a: 'A write needs every backup, so it fails when any one of them is unreachable. More backups means more things that can be unreachable. Real systems wait for one or a quorum of backups instead of all of them, or drop a silent backup from the set through a separate, agreed configuration.' },
    { q: 'Why can the tail of a chain answer reads on its own, while a backup in primary-backup cannot give the same guarantee?', a: 'A write reaches the tail last, so anything the tail has is on every node and will not be lost or undone, and anything acknowledged is already there. A backup in the middle may be missing an acknowledged write (asynchronous) or hold one that is not yet complete (synchronous).' },
  ],
  reading: [
    { kind: 'Paper', title: 'Chain Replication for Supporting High Throughput and Availability', url: 'https://www.cs.cornell.edu/home/rvr/papers/OSDI04.pdf', note: 'Van Renesse and Schneider, 2004. Short, and covers what this track leaves out: replacing a failed head, middle or tail.' },
    { kind: 'Docs', title: 'PostgreSQL: Log-Shipping Standby Servers', url: 'https://www.postgresql.org/docs/current/warm-standby.html', note: 'A production primary-backup system: streaming the log, synchronous standbys and how a standby catches up.' },
    { kind: 'Book', title: 'Designing Data-Intensive Applications, chapter 5: Replication', url: 'https://dataintensive.net/', note: 'Leaders and followers, replication lag and the anomalies it causes.' },
  ],
};
