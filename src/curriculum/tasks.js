// Curriculum metadata and test definitions. Pure data: this file is loaded by
// the UI, by the simulator worker and by scripts/verify.mjs.
//
// Briefs live in ./briefs/<slug>.md, code in ./code/<slug>.{starter,solution}.js

export const tracks = [
  {
    id: 'messaging',
    title: 'Talking to a node',
    blurb: 'Messages in, messages out. Get comfortable with the node API and meet your first partition.',
    challenges: [
      {
        slug: 'echo',
        title: 'Echo',
        level: 'Warm-up',
        minutes: 5,
        summary: 'Receive a message and reply to it.',
        concepts: ['messages', 'request / reply'],
        workload: 'echo',
        tests: [
          { name: 'One node', blurb: '20 requests to a single node.', nodeCount: 1, duration: 2500, params: { ops: 20 } },
          { name: 'Three nodes', blurb: 'The same code runs on every node.', nodeCount: 3, duration: 3000, params: { ops: 40 } },
        ],
        hints: [
          'Handlers receive `msg` with `msg.src` (who sent it) and `msg.body` (the JSON payload).',
          '`node.reply(msg, body)` sends `body` back to the sender and tags it as the answer to `msg`.',
          'The reply body is `{ type: "echo_ok", echo: msg.body.echo }`.',
        ],
      },
      {
        slug: 'unique-ids',
        title: 'Unique IDs',
        level: 'Easy',
        minutes: 10,
        summary: 'Hand out IDs that never collide, without coordinating.',
        concepts: ['availability', 'partitions', 'coordination-free design'],
        workload: 'uniqueIds',
        tests: [
          { name: 'Three nodes', blurb: '200 requests spread over three nodes.', nodeCount: 3, duration: 3000, params: { ops: 200 } },
          { name: 'During a partition', blurb: 'The network splits for two seconds; every request must still be answered.', nodeCount: 3, duration: 4000, params: { ops: 300, partition: [800, 2800] } },
          { name: 'Five nodes, heavy load', blurb: '1,000 requests.', nodeCount: 5, duration: 4000, params: { ops: 1000 } },
        ],
        hints: [
          'A counter alone fails: every node starts counting from the same number.',
          'What does each node have that no other node has? Look at `node.id`.',
          'Combine the two: the node’s own id plus a local counter can never collide with another node’s ids.',
        ],
      },
    ],
  },
  {
    id: 'broadcast',
    title: 'Broadcast',
    blurb: 'Spread information through a cluster: first naively, then reliably, then cheaply.',
    challenges: [
      {
        slug: 'broadcast-single',
        title: 'Single-node broadcast',
        level: 'Easy',
        minutes: 10,
        summary: 'Remember every value you are told and return them on read.',
        concepts: ['state', 'sets'],
        workload: 'broadcast',
        tests: [
          { name: 'One node', blurb: '30 broadcasts, then a read.', nodeCount: 1, duration: 3000, params: { ops: 30, quiet: 300 } },
        ],
        hints: [
          'Variables you declare at the top of the file live as long as the node does.',
          'A `Set` ignores duplicates for you. Messages are JSON, so send it as an array: `[...seen]`.',
        ],
      },
      {
        slug: 'broadcast-multi',
        title: 'Multi-node broadcast',
        level: 'Medium',
        minutes: 20,
        summary: 'A value told to one node must reach all five.',
        concepts: ['gossip', 'flooding', 'deduplication'],
        workload: 'broadcast',
        tests: [
          { name: 'Five nodes', blurb: 'Values arrive at random nodes; every node must end up with all of them.', nodeCount: 5, duration: 4500, params: { ops: 40 } },
          { name: 'Five nodes, busy', blurb: '150 broadcasts. Watch for message storms.', nodeCount: 5, duration: 5000, params: { ops: 150 } },
        ],
        hints: [
          'When a node hears a new value from a client, it should tell its peers with a message type of your own, e.g. `{ type: "gossip", message }`.',
          'If a node forwards every gossip it receives, messages bounce around forever. Only forward a value the first time you see it.',
          'Keep the client-facing `broadcast` handler and the node-facing `gossip` handler separate; both can call one shared function.',
        ],
      },
      {
        slug: 'broadcast-faults',
        title: 'Fault-tolerant broadcast',
        level: 'Medium',
        minutes: 25,
        summary: 'Keep the guarantee when the network drops messages.',
        concepts: ['retries', 'acknowledgements', 'at-least-once delivery', 'idempotence'],
        workload: 'broadcast',
        tests: [
          { name: 'Healthy network', blurb: 'No faults. Your earlier solution passes this.', nodeCount: 5, duration: 4500, params: { ops: 40 } },
          { name: 'Partition', blurb: 'The cluster splits in two for 2s, then heals. Values must still reach everyone.', nodeCount: 5, duration: 6500, params: { ops: 50, partition: [600, 2600], quiet: 2500 } },
          { name: 'Lossy network', blurb: 'One in three messages between nodes silently disappears.', nodeCount: 5, duration: 6000, dropRate: 0.33, params: { ops: 50, quiet: 2500 } },
        ],
        hints: [
          '`node.send` is fire-and-forget. You cannot know whether a message arrived unless the other side tells you.',
          '`await node.rpc(peer, body, { timeout })` resolves with the reply, or `null` if none came. Have the receiver `node.reply` with an acknowledgement.',
          'Retry in a loop until the peer acknowledges. Retries mean a peer may get the same value twice, which is fine because adding to a set is idempotent.',
        ],
      },
      {
        slug: 'broadcast-efficient',
        title: 'Efficient broadcast',
        level: 'Hard',
        minutes: 35,
        summary: 'Same guarantee, a fraction of the messages.',
        concepts: ['batching', 'topology', 'latency vs. cost'],
        workload: 'broadcast',
        tests: [
          { name: 'Message budget', blurb: 'Nine nodes, slow links (50–100ms). At most 6 node-to-node messages per broadcast, and every value everywhere within 1.5s.', nodeCount: 9, duration: 8000, latency: [50, 100], maxEvents: 150000, params: { ops: 240, quiet: 2500, budget: { msgsPerOp: 6, maxLag: 1500 } } },
          { name: 'Still fault tolerant', blurb: 'A partition, no budget. Efficiency must not cost you correctness.', nodeCount: 9, duration: 9000, latency: [50, 100], maxEvents: 150000, params: { ops: 80, partition: [800, 3000], quiet: 3500 } },
        ],
        hints: [
          'Count what flooding costs: with 9 nodes, one value triggers 8 sends from the first node, then 7 more from each of the others, plus acknowledgements.',
          'Batching: instead of one message per value, keep a per-peer list of values that peer has not acknowledged and flush it on a timer with `node.every`.',
          'Topology: not everyone has to talk to everyone. If every node talks only to one hub (and the hub to everyone), a value needs two hops. What do you give up?',
          'Keep retrying a batch until it is acknowledged, and remove only the values that were in the acknowledged batch.',
        ],
      },
    ],
  },
  {
    id: 'convergence',
    title: 'Clocks and CRDTs',
    blurb: 'Order events without a shared clock, and build state that merges itself.',
    challenges: [
      {
        slug: 'lamport-clock',
        title: 'Lamport clocks',
        level: 'Medium',
        minutes: 15,
        summary: 'Stamp events so that causes always come before effects.',
        concepts: ['logical time', 'causality', 'happens-before'],
        workload: 'lamport',
        tests: [
          { name: 'Three nodes', blurb: 'A mix of local events and notes between nodes.', nodeCount: 3, duration: 3500, params: { ops: 60 } },
          { name: 'One busy node', blurb: 'n1 does most of the work; the quiet nodes must catch up when they hear from it.', nodeCount: 4, duration: 4000, params: { ops: 100, skew: true } },
        ],
        hints: [
          'Each node keeps one integer. Every event adds one to it: handling an event request is one event, and a notify (sending the note and replying) is also one event, so the note and the reply carry the same stamp.',
          'Put the clock value in the note you send. The same value goes in the notify_ok reply.',
          'On receiving a note, set `clock = Math.max(clock, note.ts) + 1`. That is the whole algorithm.',
        ],
      },
      {
        slug: 'g-counter',
        title: 'Grow-only counter',
        level: 'Medium',
        minutes: 25,
        summary: 'A counter every node can add to, that converges after any partition.',
        concepts: ['CRDT', 'eventual consistency', 'merge functions'],
        workload: 'counter',
        tests: [
          { name: 'Healthy network', blurb: 'Three nodes, adds and reads.', nodeCount: 3, duration: 4500, params: { ops: 80 } },
          { name: 'Partition', blurb: 'Adds keep arriving on both sides of a split.', nodeCount: 3, duration: 6500, params: { ops: 120, partition: [600, 3000], quiet: 2000 } },
          { name: 'Lossy network', blurb: 'Five nodes; 40% of messages are lost.', nodeCount: 5, duration: 6500, dropRate: 0.4, params: { ops: 120, quiet: 2500 } },
        ],
        hints: [
          'Sending each delta to peers breaks when a message is lost (the add vanishes) or retried (it is counted twice).',
          'Instead of one number, keep one number per node: how much was added at n1, at n2, and so on. A node only ever increases its own entry.',
          'Send your whole table to peers regularly. To merge a table you receive, take the larger value for each entry. Merging twice changes nothing, so duplicates and reordering are harmless.',
          'The counter’s value is the sum of all entries.',
        ],
      },
      {
        slug: 'pn-counter',
        title: 'Counter with decrements',
        level: 'Medium',
        minutes: 15,
        summary: 'Extend the counter so that it can go down as well as up.',
        concepts: ['CRDT composition', 'monotonicity'],
        workload: 'counter',
        tests: [
          { name: 'Healthy network', blurb: 'Positive and negative deltas.', nodeCount: 3, duration: 4500, params: { ops: 80, negative: true } },
          { name: 'Partition', blurb: 'Increments and decrements on both sides of a split.', nodeCount: 3, duration: 6500, params: { ops: 120, negative: true, partition: [600, 3000], quiet: 2000 } },
          { name: 'Lossy network', blurb: 'Five nodes; 40% of messages are lost.', nodeCount: 5, duration: 6500, dropRate: 0.4, params: { ops: 120, negative: true, quiet: 2500 } },
        ],
        hints: [
          'Why does the grow-only merge break? `max` assumes entries only grow. After a decrement the newer value is the smaller one, and `max` throws it away.',
          'You cannot make one table work, but you can use two grow-only tables.',
          'Track increments in one table and decrements in another. Both only grow, both merge with `max`, and the value is sum(increments) − sum(decrements).',
        ],
      },
    ],
  },
  {
    id: 'logs',
    title: 'Replicated logs',
    blurb: 'Append-only logs are the backbone of Kafka-style systems. Build one, then share it.',
    challenges: [
      {
        slug: 'log-single',
        title: 'Single-node log',
        level: 'Medium',
        minutes: 20,
        summary: 'Append messages per key, hand out offsets, serve polls.',
        concepts: ['append-only log', 'offsets', 'consumer commits'],
        workload: 'log',
        tests: [
          { name: 'One node', blurb: 'Sends, polls and commits across three keys.', nodeCount: 1, duration: 4000, params: { ops: 120, quiet: 300 } },
        ],
        hints: [
          'Keep one array per key. The offset of a message is simply its index in that array.',
          'A poll for `{ k1: 3 }` returns `{ k1: [[3, msg], [4, msg], …] }`: everything from offset 3 onward, as `[offset, msg]` pairs.',
          'Committed offsets only ever move forward: store `Math.max(old, new)`.',
        ],
      },
      {
        slug: 'log-multi',
        title: 'Multi-node log',
        level: 'Hard',
        minutes: 40,
        summary: 'Three nodes serve one log, with help from a linearizable store.',
        concepts: ['linearizability', 'compare-and-swap', 'optimistic concurrency'],
        workload: 'log',
        kv: true,
        tests: [
          { name: 'Three nodes', blurb: 'Clients send to any node and poll from any node.', nodeCount: 3, duration: 6000, kv: true, params: { ops: 110, quiet: 1500 } },
          { name: 'Nodes cannot see each other', blurb: 'The nodes are partitioned from one another the whole time, but all can reach the store.', nodeCount: 3, duration: 6000, kv: true, params: { ops: 110, quiet: 1500, partition: [50, 5900] } },
        ],
        hints: [
          'Per-node arrays fail at once: two nodes both hand out offset 0 for the same key. The offset has to come from somewhere shared: `node.kv`.',
          'To claim an offset, read the next free one and `cas` it from that value to value + 1. If the cas fails, somebody else got there first; read again and retry.',
          '`node.kv.read` returns `null` for a missing key, and `cas(key, null, 1)` succeeds only if the key is still missing.',
          'Store each message under its own key such as `log/k1/7`. Entries never change once written, so a node may cache them locally to make polls cheap.',
        ],
      },
    ],
  },
  {
    id: 'transactions',
    title: 'Transactions',
    blurb: 'Group reads and writes together, then replicate them without giving up availability.',
    challenges: [
      {
        slug: 'txn-single',
        title: 'Single-node transactions',
        level: 'Easy',
        minutes: 10,
        summary: 'Execute a list of reads and writes against a key-value store.',
        concepts: ['transactions', 'serial execution'],
        workload: 'txn',
        tests: [
          { name: 'One node', blurb: '80 transactions from three clients.', nodeCount: 1, duration: 3500, params: { ops: 80 } },
        ],
        hints: [
          'A transaction is a list of `[op, key, value]`. For `"r"` the value arrives as null and you fill it in; for `"w"` you store it.',
          'Reply with the same list, reads filled in. A key that was never written reads as `null`.',
          'A handler runs to completion before the next message is handled, so a synchronous handler is automatically serial.',
        ],
      },
      {
        slug: 'txn-replicated',
        title: 'Totally-available transactions',
        level: 'Hard',
        minutes: 40,
        summary: 'Three replicas that always answer and always converge.',
        concepts: ['total availability', 'last-writer-wins', 'anti-entropy'],
        workload: 'txn',
        tests: [
          { name: 'Healthy network', blurb: 'Writes on any node must show up on all of them.', nodeCount: 3, duration: 5000, params: { ops: 80 } },
          { name: 'Partition', blurb: 'Both sides keep accepting writes to the same keys, then the network heals.', nodeCount: 3, duration: 7000, params: { ops: 120, partition: [600, 3200], quiet: 2200 } },
          { name: 'Lossy network', blurb: 'Five nodes; a third of the messages are lost.', nodeCount: 5, duration: 7000, dropRate: 0.33, params: { ops: 120, quiet: 2500 } },
        ],
        hints: [
          'Start from your single-node solution: apply the transaction locally and reply at once. Replication happens afterwards, in the background.',
          'Two sides of a partition will write different values to the same key. For the replicas to converge, every node must pick the same winner, whatever order it hears about the writes in.',
          'Tag each write with a Lamport timestamp and the writer’s node id. The larger (timestamp, id) pair wins. That rule gives the same answer on every node.',
          'Simplest replication: every few hundred ms, send your whole store (values with their tags) to every peer and let them merge. Lost messages are repaired by the next round.',
        ],
      },
    ],
  },
  {
    id: 'consensus',
    title: 'Consensus',
    blurb: 'The capstone: get a cluster to agree on who is in charge, even when it is split.',
    challenges: [
      {
        slug: 'leader-election',
        title: 'Leader election',
        level: 'Capstone',
        minutes: 60,
        summary: 'Implement the election half of Raft: terms, votes, heartbeats.',
        concepts: ['Raft', 'terms', 'majority quorum', 'split brain'],
        workload: 'election',
        tests: [
          { name: 'Stable network', blurb: 'Three nodes. One leader, quickly, and no needless re-elections.', nodeCount: 3, duration: 5000, params: { scenario: 'stable' } },
          { name: 'Leader cut off', blurb: 'Five nodes. The leader is isolated; the rest must replace it, and the old leader must stand down when the network heals.', nodeCount: 5, duration: 9500, params: { scenario: 'isolate-leader' } },
          { name: 'Repeated partitions', blurb: 'Five nodes, two different splits, back to back.', nodeCount: 5, duration: 14000, params: { scenario: 'churn' } },
        ],
        hints: [
          'Every node starts as a follower with a randomised election timer (say 300–600ms). `node.after` returns a handle; cancel and restart it whenever you hear from a valid leader.',
          'When the timer fires: add one to your term, vote for yourself, and ask every peer for its vote. A peer grants it only if it has not voted for someone else in that term.',
          'With votes from a majority of the whole cluster (count yourself), become leader and send heartbeats every 100ms or so.',
          'The rule that keeps everything consistent: whenever you see a term higher than yours, in any message or reply, adopt it and become a follower. Ignore anything from a lower term.',
          'Randomised timeouts matter. If all nodes time out together they all vote for themselves and nobody wins.',
        ],
      },
    ],
  },
];

export const challenges = tracks.flatMap((track, ti) =>
  track.challenges.map((c, ci) => ({ ...c, trackId: track.id, trackTitle: track.title, number: `${ti + 1}.${ci + 1}` })),
);

export const bySlug = Object.fromEntries(challenges.map((c) => [c.slug, c]));
