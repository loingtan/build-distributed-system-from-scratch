// Track: Sharding. See docs/authoring.md for the shape of this file.
import { shardRouting, shardRing, shardRebalance, shardEpochs, shardKeyRange, shardHotSpot, shardSecondaryIndex } from '../../sim/workloads/sharding.js';

const six = ['n1', 'n2', 'n3', 'n4', 'n5', 'n6'];
const without = (...gone) => six.filter((n) => !gone.includes(n));

// Five-node plans for the last task. flipFlop moves keys away from a node and
// straight back again; churn swaps members around; toggle takes n3 out and
// back four times.
const five = ['n1', 'n2', 'n3', 'n4', 'n5'];
const but = (...gone) => five.filter((n) => !gone.includes(n));
const flipFlop = [but('n3'), five, but('n1'), but('n1', 'n4'), but('n4'), five];
const churn = [but('n2'), but('n2', 'n5'), but('n5'), but('n1', 'n5'), but('n1'), five];
const toggle = [but('n3'), five, but('n3'), five, but('n3'), five, but('n3'), five];

export default {
  id: 'sharding',
  code: 'SH',
  category: 'storage',
  title: 'Sharding',
  level: 'Advanced',
  blurb: 'Split the keys across nodes with consistent hashing, route requests to the owner, and move data when the cluster changes.',
  subtracks: [
    {
      title: 'Who owns a key',
      tasks: [
        {
          slug: 'shard-hash-routing',
          title: 'Hash partitioning',
          level: 'Easy',
          minutes: 20,
          summary: 'Give every key one owner node and forward requests to it.',
          concepts: ['partitioning', 'request routing', 'hash functions'],
          workload: shardRouting,
          tests: [
            { name: 'Three nodes', blurb: '240 keys written and read through random nodes.', nodeCount: 3, duration: 5000, params: { keys: 240 } },
            { name: 'Six nodes', blurb: '480 keys, twelve clients at once.', nodeCount: 6, duration: 5000, params: { keys: 480, clients: 12 } },
            { name: 'Slow links', blurb: 'Links take 10–40ms, so forwarded requests overlap.', nodeCount: 4, duration: 9000, latency: [10, 40], params: { keys: 200, clients: 10 } },
          ],
          hints: [
            'The starter stores a key on whichever node the client happened to pick. Run it and read the first failing check.',
            'Every node needs the same answer to "who owns this key" without asking anyone. `node.nodeIds` is the same list, in the same order, on every node.',
            '`hash(key) % node.nodeIds.length` is an index into that list.',
            'In the `put` handler: if the owner is not `node.id`, `const res = await node.rpc(owner, { type: "put", key, value })`, then `node.reply(msg, { type: "put_ok" })`. The owner runs the same handler and finds that it is the owner.',
            'Do the same for `get`, passing `res.value` back to the client.',
          ],
        },
        {
          slug: 'shard-consistent-hashing',
          title: 'Consistent hashing',
          level: 'Medium',
          minutes: 30,
          summary: 'Place nodes on a hash ring so a membership change moves only the keys it has to.',
          concepts: ['consistent hashing', 'hash ring', 'membership'],
          workload: shardRing,
          tests: [
            { name: 'One node leaves', blurb: 'Six members, then n3 is removed. 2,000 keys are looked up before and after.', nodeCount: 6, duration: 3000, params: { plan: [without('n3')] } },
            { name: 'Leave and rejoin', blurb: 'n5 leaves, n2 leaves, then both come back one at a time.', nodeCount: 6, duration: 3000, params: { plan: [without('n5'), without('n5', 'n2'), without('n5'), six] } },
            { name: 'Churn', blurb: 'Six membership changes, some swapping one node for another, with the list in a different order each time.', nodeCount: 6, duration: 3000, params: { plan: [without('n1'), ['n6', 'n5', 'n4', 'n3'], ['n3', 'n1', 'n6', 'n5', 'n4'], ['n2', 'n4', 'n6', 'n1', 'n3'], ['n5', 'n3', 'n1', 'n2'], six] } },
          ],
          hints: [
            'Run the starter and read the numbers in the failing check: removing one node of six moves about five keys in six.',
            'Think of the hash values as a circle from 0 to 2^32. Each member sits at `hash(id)`. A key sits at `hash(key)` and belongs to the next member clockwise.',
            'Keep an array of `{ pos, id }` sorted by `pos`, rebuilt on every `set_members`. Sorting makes the result independent of the order the member list arrived in.',
            'The owner is the first entry with `pos >= hash(key)`. If there is none, the key is past the last point: wrap round to entry 0.',
            'Build the ring once at startup too, from `node.nodeIds`.',
          ],
        },
        {
          slug: 'shard-virtual-nodes',
          title: 'Virtual nodes',
          level: 'Medium',
          minutes: 20,
          summary: 'Give each node many points on the ring so the load evens out.',
          concepts: ['virtual nodes', 'load balance', 'variance'],
          workload: shardRing,
          tests: [
            { name: 'Six members', blurb: '4,000 keys; every member must own between 0.7× and 1.3× its fair share. Then one leaves.', nodeCount: 6, duration: 3000, params: { keys: 4000, spread: [0.7, 1.3], plan: [without('n3')] } },
            { name: 'Shrinking cluster', blurb: 'From six members down to three, one at a time. The shares must stay even at every size.', nodeCount: 6, duration: 3000, params: { keys: 4000, spread: [0.7, 1.3], plan: [without('n2'), without('n2', 'n5'), without('n2', 'n5', 'n6')] } },
            { name: 'Churn', blurb: 'Six changes; shares stay even and no key moves without a reason.', nodeCount: 6, duration: 3000, params: { keys: 4000, spread: [0.7, 1.3], plan: [without('n1'), ['n6', 'n5', 'n4', 'n3'], ['n3', 'n1', 'n6', 'n5', 'n4'], ['n2', 'n4', 'n6', 'n1', 'n3'], ['n5', 'n3', 'n1', 'n2'], six] } },
          ],
          hints: [
            'The failing check lists how many keys each member owns with one point per node. The arcs between six random points are very uneven.',
            'Put each member on the ring many times: `hash(id + "#" + i)` for `i` from 0 to `POINTS - 1`. Each entry still records the real node id.',
            'A member\'s share is now the sum of many small arcs, and sums of many random pieces are close to their average. Around 100 points a member\'s share typically varies by about ±10% (one standard deviation), so the worst of several members can be off by 30%; the tests need 0.7× to 1.3×, so use 150 or more.',
            'With over a thousand points and thousands of keys per lookup, a linear scan is slow. Binary-search the sorted ring for the first `pos >= hash(key)`.',
          ],
        },
      ],
    },
    {
      title: 'Moving the data',
      tasks: [
        {
          slug: 'shard-rebalancing',
          title: 'Rebalancing',
          level: 'Hard',
          minutes: 40,
          summary: 'When the members change, hand keys to their new owners without losing or reviving anything.',
          concepts: ['rebalancing', 'handoff', 'acknowledged transfer'],
          workload: shardRebalance,
          tests: [
            { name: 'A node leaves', blurb: '200 keys on five members, then n3 is removed and must hand over everything it holds.', nodeCount: 5, duration: 4000, params: { keys: 200, plan: [['n1', 'n2', 'n4', 'n5']] } },
            { name: 'A node joins', blurb: 'Four members hold 200 keys; n5 joins and must receive its share.', nodeCount: 5, duration: 4000, params: { keys: 200, initial: ['n1', 'n2', 'n3', 'n4'], plan: [['n1', 'n2', 'n3', 'n4', 'n5']] } },
            { name: 'Writes during handoff', blurb: 'Three changes; straight after each, 80 keys are overwritten while the handoff is under way.', nodeCount: 5, duration: 7500, params: { keys: 200, rewrite: 80, plan: [['n1', 'n2', 'n4', 'n5'], ['n2', 'n3', 'n4', 'n5'], ['n1', 'n2', 'n3', 'n4', 'n5']] } },
            { name: 'Lossy network', blurb: '10% of messages between nodes are lost, during handoff and during the writes that follow it.', nodeCount: 5, duration: 7500, dropRate: 0.1, params: { keys: 200, rewrite: 80, plan: [['n1', 'n2', 'n4', 'n5'], ['n1', 'n2', 'n3', 'n5'], ['n1', 'n2', 'n3', 'n4', 'n5']] } },
          ],
          hints: [
            'The starter changes the ring and leaves the data where it was. After `set_members`, walk the store and find the keys whose `ownerOf` is no longer this node.',
            'Group those keys by new owner and send each owner one message with its keys and values, e.g. `{ type: "handoff", entries: [[key, value], ...] }`, using `node.rpc`.',
            'Delete a key locally only when the reply arrives. If the rpc resolves to `null`, keep the keys. A `node.every(100, handoff)` timer that re-scans the store gives you retries for free.',
            'On the receiving side, store a handed-over value only if the key is not already there. A value that is already there came from a put after the change, so it is newer.',
            'For the lossy test, forwarded `put` and `get` need retries too: loop a few times with a short rpc timeout (say 8 × 100ms, inside the client\'s 1000ms). Both are safe to repeat.',
          ],
        },
        {
          slug: 'shard-epochs',
          title: 'Changes under load',
          level: 'Hard',
          minutes: 60,
          summary: 'Membership changes overlap and arrive out of order while clients keep reading and writing. Nothing may go back in time.',
          concepts: ['epochs', 'versioned values', 'fencing', 'handoff'],
          workload: shardEpochs,
          tests: [
            { name: 'Out-of-order changes', blurb: 'Six changes 400ms apart, each reaching the nodes over 250ms; one node gets each change only after the next one.', nodeCount: 5, duration: 6500, params: { plan: flipFlop } },
            { name: 'Back-to-back changes', blurb: 'Six changes 150ms apart, so each starts before the previous handoff can finish.', nodeCount: 5, duration: 6000, params: { plan: flipFlop, gap: 150, spread: 300 } },
            { name: 'Slow handoffs', blurb: 'Half of all messages between nodes are held back by up to 100ms while the changes run.', nodeCount: 5, duration: 6500, params: { plan: churn, gap: 300, delay: 100 } },
            { name: 'Back before the handoff', blurb: 'n3 leaves for 150ms at a time, then rejoins for 500ms. Requests n3 sends to other nodes take about 250ms, so it gets its keys back while its handoff is still on the way.', nodeCount: 5, duration: 6000, params: { plan: toggle, gap: [150, 500], spread: 100, slow: { node: 'n3', ms: 250 } } },
          ],
          hints: [
            'Three separate problems, one per failing check. Start with the epochs: in `set_members`, ignore any message whose `epoch` is not greater than the one you have, but still reply `set_members_ok`.',
            'Store `{ value, version }` instead of a bare value. The owner gives each write a version that puts the epoch first, e.g. `epoch * 1000000 + (++seq)`. Wherever two copies meet (a handoff, a collected copy), keep the higher version, not the one that arrived last or first.',
            'A new owner that answers straight away misses writes still sitting on the old owner. Before serving in a new epoch, ask every other node for its copies of the keys you now own (`{ type: "collect", epoch, members }`), merge them, and hold requests until every node has answered.',
            'A node that answers a collect for a newer epoch must adopt that epoch first, so it stops serving those keys itself. More generally: put `epoch` and `members` on every message between nodes and adopt them when they are newer. Then a node that missed a `set_members` catches up from its peers.',
            'If requests time out: a request held while you collect must be routed again when the epoch changes, because this node may no longer own the key. A collect must also start over when a newer epoch turns up halfway through.',
          ],
        },
      ],
    },
    {
      title: 'Ranges and indexes',
      tasks: [
        {
          slug: 'shard-key-range',
          title: 'Key-range partitioning',
          level: 'Medium',
          minutes: 40,
          summary: 'Keep keys in order across nodes so a range scan asks only the nodes that own that range, and split ranges as they grow.',
          concepts: ['range partitioning', 'range scans', 'dynamic splitting'],
          workload: shardKeyRange,
          tests: [
            { name: 'Four nodes', blurb: '400 keys written in random order; then 30 scans, one at a time, each watched for which nodes it involves.', nodeCount: 4, duration: 6000, params: { keys: 400 } },
            { name: 'Six nodes', blurb: '600 keys, twelve clients at once, so splits happen while other puts are being forwarded.', nodeCount: 6, duration: 6000, params: { keys: 600, clients: 12, share: 0.4 } },
            { name: 'Slow links', blurb: 'Links take 10–40ms, so a split announcement can arrive after requests that depend on it.', nodeCount: 4, duration: 9000, latency: [10, 40], params: { keys: 300, clients: 10 } },
          ],
          hints: [
            'Run the starter: one node owns everything. The first failing checks are the range size and the share of keys per node.',
            'After a put, the owner counts the keys it holds in that key\'s range. Over 40: sort them, take the middle key `at`, and add the boundary `[at, newOwner]` to the table.',
            'Boundaries are only ever added, never moved. So a node that hears "a range now starts at `at` and belongs to n3" can just insert it, whatever order the announcements arrive in.',
            'Move the data before you tell everyone: send the upper half\'s keys to the new owner with `node.rpc` (it adds the boundary when the keys arrive), delete them locally, then `node.send` a `split` message to the other nodes.',
            'For `scan`, walk the table from the range holding `from` while a range starts before `to`, and ask only those owners. Do not use timers that send messages: the test counts every message sent while a scan is running.',
          ],
        },
        {
          slug: 'shard-hot-spot',
          title: 'Hot spots',
          level: 'Hard',
          minutes: 45,
          summary: 'One key gets most of the traffic. Notice it and spread its writes over several nodes, while reads stay exact.',
          concepts: ['hot spots', 'skewed workload', 'key salting', 'scatter-gather'],
          workload: shardHotSpot,
          tests: [
            { name: 'One celebrity', blurb: 'Five nodes, 100 counters; 70% of all requests go to one of them, chosen at random.', nodeCount: 5, duration: 4500 },
            { name: 'The celebrity changes', blurb: 'Halfway through, a different counter becomes the hot one.', nodeCount: 5, duration: 6000, params: { shift: true } },
            { name: 'Six nodes, slow links', blurb: 'Links take 5–25ms and twelve clients send at once.', nodeCount: 6, duration: 6000, latency: [5, 25], params: { clients: 12 } },
          ],
          hints: [
            'The failing check shows one node handling far more than the others: the owner of the celebrity. Every increment of that key is forwarded to it.',
            'Let the owner count the requests it serves for each key in a short window (say 200ms, reset by `node.every`). A key past a threshold, such as 20 per window, is hot: remember that and `node.send` a `hot` message to every node.',
            'Salting: an increment of a hot key can go to any of N parts. Put part `i` on node `(hash(key) + i) % N`, so part 0 is the owner\'s own counter and the parts land on different nodes. Pick `i` at random.',
            'A read of a hot key asks every node for its part and adds them up. A node that has not heard yet that the key is hot asks the owner, and the owner, which always knows, collects the parts itself. Never cool a key down; then no part off the owner exists before the owner knows.',
            'Spread only the hot key. Salting every key makes every read a fan-out, and the message budget is there to catch it.',
          ],
        },
        {
          slug: 'shard-secondary-index',
          title: 'Global secondary index',
          level: 'Hard',
          minutes: 45,
          summary: 'Find records by color with an index partitioned by term, updated asynchronously, that settles to the right answer.',
          concepts: ['secondary index', 'term-partitioned index', 'asynchronous index updates', 'versioned values'],
          workload: shardSecondaryIndex,
          tests: [
            { name: 'Steady changes', blurb: 'Sixty cars, six clients recolouring them and asking for colors.', nodeCount: 4, duration: 5000 },
            { name: 'Reordered updates', blurb: 'Half of all messages between nodes are held back by up to 80ms, and cars are often recoloured twice in a row.', nodeCount: 5, duration: 6000, params: { delay: 80, bursts: 0.6 } },
            { name: 'Slow links', blurb: 'Links take 10–50ms.', nodeCount: 4, duration: 7000, latency: [10, 50] },
          ],
          hints: [
            'The starter answers correctly by asking every node. The failing check is about who takes part in a find: only the coordinator and one index node may.',
            'Choose the index node for a color with the same hash trick as for keys, e.g. `node.nodeIds[hash("color:" + color) % N]`. Every node can work it out alone.',
            'When the owner of a record stores a put, it sends "remove key from the old color" and "add key to the new color" to those two index nodes. It need not wait for them before replying.',
            'Those messages can arrive in any order. Number the versions of each record (the owner adds 1 on each put) and store `{ version, present }` per key in the index. Apply an update only if its version is newer than the one stored, and keep removals as `present: false` rather than deleting the entry.',
          ],
        },
      ],
    },
  ],
  mistakes: [
    { title: 'Storing the key where the request landed', symptom: 'A get through another node returns null, and local_keys shows the same key on several nodes.', fix: 'Compute the owner from the key alone and forward the request there. Only the owner touches the store.' },
    { title: 'Using hash(key) % N with a changing N', symptom: 'Removing one node of six gives about 83% of the keys a new owner.', fix: 'Use a hash ring (or rendezvous hashing), where a change only affects the keys next to the node that left or joined.' },
    { title: 'Letting whatever arrives last win', symptom: 'After a burst of changes one node disagrees about the owners, or a read returns a value older than one already acknowledged.', fix: 'Order changes by epoch and values by version. A late set_members or a late handoff copy must never undo something newer.' },
    { title: 'Deleting a key before the new owner has confirmed it', symptom: 'On a lossy network a few keys read as null after a membership change, and no node stores them.', fix: 'Send the handoff as an rpc, delete only on the reply, and retry on a timer until every misplaced key is gone.' },
  ],
  questions: [
    { q: 'With N nodes on a ring and one added, what fraction of keys moves, and where do they come from?', a: 'About 1/(N+1) of the keys, all of them to the new node. With one point per node they all come from a single neighbour; with virtual nodes they come from every node in small slices, which also spreads the cost of the transfer.' },
    { q: 'Why do more virtual nodes give more even load, and what do they cost?', a: 'A node\'s share is the sum of its arcs. The relative spread of a sum of V random arcs shrinks roughly as 1/sqrt(V). The cost is a larger ring to store, sort and search, and more, smaller ranges to track during rebalancing.' },
    { q: 'In the rebalancing task every node learns the new member list before clients write again. What breaks if that is not true?', a: 'Two nodes can both believe they own a key and both accept writes, and "keep what is already there" can no longer tell which value is newer. The last task fixes this the way real systems do: an epoch on every membership change, a version on every value, and a new owner that fences off the old one before it serves.' },
  ],
  reading: [
    { kind: 'Paper', title: 'Dynamo: Amazon\'s highly available key-value store', url: 'https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf', note: 'Section 4.2 covers the ring and virtual nodes; section 6.2 compares partitioning strategies in production.' },
    { kind: 'Paper', title: 'Consistent hashing and random trees', url: 'https://www.cs.princeton.edu/courses/archive/fall09/cos518/papers/chash.pdf', note: 'Karger et al., 1997: the paper that introduced consistent hashing.' },
    { kind: 'Docs', title: 'Redis Cluster specification', url: 'https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/', note: 'A different design: 16,384 fixed hash slots, moved between nodes one slot at a time, with clients redirected to the owner.' },
  ],
};
