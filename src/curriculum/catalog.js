// The site's structure: categories → tracks → subtracks → tasks.
//
// Task definitions (tests, hints) live in ./tasks.js and in `extra` below;
// this file arranges them into tracks and adds the material shown on each
// track page: common mistakes, review questions and further reading.

import { challenges as base } from './tasks.js';
import moduleTracks from './tracks/index.js';

const extra = [
  {
    slug: 'idempotent-requests',
    title: 'Idempotent requests',
    level: 'Easy',
    minutes: 10,
    summary: 'Clients retry. Apply each request once, however often it arrives.',
    concepts: ['idempotence', 'retries', 'deduplication'],
    workload: 'dedup',
    tests: [
      { name: 'Retrying clients', blurb: 'Half of all requests arrive two or three times.', nodeCount: 1, duration: 3500, params: { ops: 60 } },
      { name: 'Heavy retries', blurb: '200 requests on slow links (20–80ms), so retries overlap the originals.', nodeCount: 1, duration: 5000, latency: [20, 80], params: { ops: 200 } },
    ],
    hints: [
      'Every `add` carries `msg.body.request_id`. A retry has the same id as the original.',
      'Keep a `Set` of the ids you have applied. Only add the delta if the id is new.',
      'Reply `add_ok` in both cases. The client is retrying precisely because it has not seen an answer.',
    ],
  },
  {
    slug: 'sortable-ids',
    title: 'Sortable IDs',
    level: 'Medium',
    minutes: 20,
    summary: 'Pack time, node and sequence into one integer, Snowflake style.',
    concepts: ['Snowflake IDs', 'k-sorted', 'bit packing'],
    workload: 'uniqueIds',
    tests: [
      { name: 'Three nodes', blurb: '300 requests; ids must be integers that sort by time.', nodeCount: 3, duration: 3500, params: { ops: 300, sortable: true } },
      { name: 'Burst', blurb: '2,000 requests in two seconds: many ids per millisecond on each node.', nodeCount: 3, duration: 2400, params: { ops: 2000, sortable: true } },
      { name: 'During a partition', blurb: 'Still no coordination allowed.', nodeCount: 5, duration: 4000, params: { ops: 400, sortable: true, partition: [800, 2800] } },
    ],
    hints: [
      'Put the time in the most significant position: `Math.floor(node.now())` multiplied by a large power of two.',
      'Below the time, leave room for the node number (`Number(node.id.slice(1))`) and for a sequence that restarts at zero each millisecond.',
      'Use `*` and `2 ** n`, not `<<`. Shifts work on 32-bit integers and will wrap.',
      'Two requests can reach one node in the same millisecond. Remember the last millisecond you used and bump the sequence when it repeats.',
    ],
  },
  {
    slug: 'vector-clock',
    title: 'Vector clocks',
    level: 'Medium',
    minutes: 20,
    summary: 'Tell "happened before" apart from "happened independently".',
    concepts: ['vector clocks', 'concurrency detection', 'causality'],
    workload: 'vector',
    tests: [
      { name: 'Three nodes', blurb: 'Events and notes; every clock is checked entry by entry.', nodeCount: 3, duration: 3500, params: { ops: 60 } },
      { name: 'Five nodes', blurb: 'More nodes, more interleavings.', nodeCount: 5, duration: 4500, params: { ops: 140 } },
    ],
    hints: [
      'Start with an object holding a zero for every id in `node.nodeIds`.',
      'An event or a notify adds one to `clock[node.id]` and nothing else.',
      'On a note: for each entry keep `Math.max(mine, theirs)`, and only then add one to your own entry.',
    ],
  },
  {
    slug: 'failure-detector',
    title: 'Failure detector',
    level: 'Medium',
    minutes: 25,
    summary: 'Use heartbeats to notice an unreachable node, without false alarms.',
    concepts: ['heartbeats', 'timeouts', 'suspicion'],
    workload: 'detector',
    tests: [
      { name: 'One node cut off', blurb: 'Five nodes. One is isolated for three seconds, then returns.', nodeCount: 5, duration: 8000, params: { isolate: [2000, 5000] } },
      { name: 'Lossy network', blurb: 'One message in ten is lost. Nobody has failed, so nobody may be suspected.', nodeCount: 5, duration: 8000, dropRate: 0.1, params: {} },
    ],
    hints: [
      '`node.every(100, …)` to send a heartbeat to each peer; in the heartbeat handler store `node.now()` for `msg.src`.',
      'A peer is suspected if `node.now() - lastHeard` is above a threshold. Compute it when `status` arrives rather than storing a flag, and recovery takes care of itself.',
      'Choose the threshold as several heartbeat intervals. With 10% loss, one missed heartbeat is common and five in a row are vanishingly rare.',
    ],
  },
];

const pool = Object.fromEntries([...base, ...extra].map((c) => [c.slug, c]));

export const categories = [
  { id: 'foundations', title: 'Foundations', blurb: 'Messages, identity, spreading information and telling time.' },
  { id: 'convergence', title: 'Replication', blurb: 'Replicas that accept writes independently and still agree in the end.' },
  { id: 'storage', title: 'Storage', blurb: 'Logs and the coordination needed to share them.' },
  { id: 'consensus', title: 'Consensus', blurb: 'Detecting failure and agreeing on who is in charge.' },
  { id: 'processing', title: 'Batch and streams', blurb: 'Processing data at scale: MapReduce over files, and streams of events that never end.' },
  { id: 'scalability', title: 'Scalability', blurb: 'Queues and the machinery for spreading load.' },
  { id: 'operations', title: 'Operations', blurb: 'Keeping a system up when its dependencies misbehave.' },
];

const trackDefs = [
  {
    id: 'messaging',
    code: 'MS',
    category: 'foundations',
    title: 'Messaging',
    level: 'Beginner',
    blurb: 'Handle a request, send a reply, and cope with a client that asks twice. Everything later is built from these moves.',
    subtracks: [{ title: 'Request and reply', tasks: ['echo', 'idempotent-requests'] }],
    mistakes: [
      { title: 'Replying with node.send instead of node.reply', symptom: 'The client reports a timeout although your handler ran.', fix: 'A reply must carry in_reply_to so the client can match it to its request. node.reply adds it.' },
      { title: 'Treating a retry as a new request', symptom: 'Totals drift upwards under load.', fix: 'Deduplicate on the request id before applying the effect.' },
      { title: 'Dropping the duplicate silently', symptom: 'The client retries forever.', fix: 'Skip the work but always send the acknowledgement.' },
    ],
    questions: [
      { q: 'A client sees a timeout. What does it know about whether the server applied the request?', a: 'Nothing. The request may have been lost, the server may be slow, or the reply may have been lost. That ambiguity is why retries need idempotence.' },
      { q: 'What is the difference between at-most-once, at-least-once and exactly-once delivery?', a: 'At-most-once never retries, so messages can be lost. At-least-once retries until acknowledged, so messages can be duplicated. "Exactly-once" is at-least-once delivery combined with idempotent processing.' },
      { q: 'How long must a server remember request ids?', a: 'For as long as a client might still retry. Real systems bound this with expiry times or per-client sequence numbers, where remembering only the highest number seen is enough.' },
    ],
    reading: [
      { kind: 'Docs', title: 'Maelstrom protocol', url: 'https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md', note: 'The message format this lab is modelled on.' },
      { kind: 'Blog', title: 'Stripe: designing robust APIs with idempotency', url: 'https://stripe.com/blog/idempotency', note: 'Idempotency keys in a production payments API.' },
    ],
  },
  {
    id: 'identifiers',
    code: 'ID',
    category: 'foundations',
    title: 'Identifiers',
    level: 'Beginner',
    blurb: 'Mint ids that never collide, on any node, during any partition. Then make them sort by time.',
    subtracks: [{ title: 'Coordination-free IDs', tasks: ['unique-ids', 'sortable-ids'] }],
    mistakes: [
      { title: 'A plain counter on every node', symptom: 'Two nodes hand out the same id.', fix: 'Include something only this node has, such as its node id.' },
      { title: 'Asking a central node for the next id', symptom: 'Requests time out during a partition.', fix: 'Generate ids from local state only.' },
      { title: 'Building the number with bit shifts', symptom: 'Ids come out negative or repeat.', fix: 'JavaScript shifts are 32-bit. Use multiplication by powers of two.' },
      { title: 'No sequence within a millisecond', symptom: 'One node issues duplicates under a burst.', fix: 'Add a sequence field that increments while the millisecond is unchanged.' },
    ],
    questions: [
      { q: 'Why not use random UUIDs everywhere?', a: 'They are unique without coordination, but they are large and unordered, which scatters inserts across a database index. Time-ordered ids keep new rows together.' },
      { q: 'What happens to a Snowflake-style generator if the machine clock jumps backwards?', a: 'It could reissue ids from the repeated interval. Generators either refuse to issue ids until the clock catches up, or keep using the last timestamp they saw.' },
      { q: 'Why are such ids called k-sorted rather than sorted?', a: 'Clocks on different machines disagree slightly, so ids created within that margin of each other can sort either way. Order is only reliable beyond the skew.' },
    ],
    reading: [
      { kind: 'Reference', title: 'Snowflake ID', url: 'https://en.wikipedia.org/wiki/Snowflake_ID', note: 'The bit layout and its descendants.' },
      { kind: 'Spec', title: 'RFC 9562: UUID version 7', url: 'https://www.rfc-editor.org/rfc/rfc9562', note: 'The standardised time-ordered UUID.' },
    ],
  },
  {
    id: 'broadcast',
    code: 'BR',
    category: 'foundations',
    title: 'Broadcast',
    level: 'Intermediate',
    blurb: 'Tell one node, and every node finds out. First by flooding, then despite a broken network, then on a message budget.',
    subtracks: [
      { title: 'Flooding', tasks: ['broadcast-single', 'broadcast-multi'] },
      { title: 'Reliability and cost', tasks: ['broadcast-faults', 'broadcast-efficient'] },
    ],
    mistakes: [
      { title: 'Forwarding every gossip message', symptom: 'The run stops with a message storm.', fix: 'Forward a value only the first time you see it.' },
      { title: 'Putting a Set in a message', symptom: 'The node throws: messages must be plain JSON.', fix: 'Spread it into an array first.' },
      { title: 'Waiting for peers before acknowledging the client', symptom: 'Broadcasts time out during a partition.', fix: 'Acknowledge at once and replicate in the background.' },
      { title: 'Clearing the whole pending list on an acknowledgement', symptom: 'A few values never arrive.', fix: 'Remove only the values that were in the acknowledged batch; more may have been queued since.' },
    ],
    questions: [
      { q: 'Why is retrying a gossip message safe?', a: 'Adding a value to a set is idempotent, so a duplicate changes nothing. At-least-once delivery plus an idempotent operation gives the effect of exactly-once.' },
      { q: 'What does a star topology cost compared with a full mesh?', a: 'Far fewer messages, but an extra hop of latency, and the hub becomes a bottleneck: while it is unreachable nothing propagates.' },
      { q: 'What does batching trade away?', a: 'Latency. A value waits for the next flush, so fewer and fuller messages mean slower propagation.' },
    ],
    reading: [
      { kind: 'Paper', title: 'Epidemic algorithms for replicated database maintenance', url: 'http://bitsavers.trailing-edge.com/pdf/xerox/parc/techReports/CSL-89-1_Epidemic_Algorithms_for_Replicated_Database_Maintenance.pdf', note: 'Demers et al., the origin of gossip protocols (Xerox PARC report, free PDF).' },
      { kind: 'Challenge', title: 'Gossip Glomers', url: 'https://fly.io/dist-sys/', note: 'The Maelstrom challenges that inspired this track.' },
    ],
  },
  {
    id: 'time',
    code: 'TM',
    category: 'foundations',
    title: 'Logical time',
    level: 'Intermediate',
    blurb: 'Order events across machines whose clocks cannot be trusted, and detect when two events are unrelated.',
    subtracks: [{ title: 'Logical clocks', tasks: ['lamport-clock', 'vector-clock'] }],
    mistakes: [
      { title: 'Forgetting the receive rule', symptom: 'A quiet node stamps an effect earlier than its cause.', fix: 'On receive, take the maximum of both clocks and then add one.' },
      { title: 'Ticking twice for one notify', symptom: 'The note and the reply carry different timestamps.', fix: 'One event, one tick; use the same value for both.' },
      { title: 'Incrementing the sender’s entry on receive', symptom: 'Vector clocks run ahead of the true count.', fix: 'A node only ever increments its own entry. Other entries change by merging.' },
    ],
    questions: [
      { q: 'If Lamport timestamp A is smaller than B, did A happen before B?', a: 'Not necessarily. Lamport clocks guarantee the converse only: if A caused B then A is smaller. Two unrelated events can have any timestamps.' },
      { q: 'How do you tell from two vector clocks that the events were concurrent?', a: 'Neither is less than or equal to the other in every entry: each has at least one entry larger than the other’s.' },
      { q: 'What is the cost of vector clocks?', a: 'One entry per node on every message and every stored version. Systems with many or changing participants need pruning or a different scheme.' },
    ],
    reading: [
      { kind: 'Paper', title: 'Time, Clocks, and the Ordering of Events in a Distributed System', url: 'https://lamport.azurewebsites.net/pubs/time-clocks.pdf', note: 'Lamport, 1978.' },
      { kind: 'Book', title: 'Designing Data-Intensive Applications, chapter 8', url: 'https://dataintensive.net/', note: 'Kleppmann on unreliable clocks.' },
    ],
  },
  {
    id: 'crdts',
    code: 'CR',
    category: 'convergence',
    title: 'Counters and CRDTs',
    level: 'Intermediate',
    blurb: 'State that every replica can change on its own and that merges back together without conflicts.',
    subtracks: [{ title: 'Convergent counters', tasks: ['g-counter', 'pn-counter'] }],
    mistakes: [
      { title: 'Replicating deltas', symptom: 'The total is too low after a partition, or too high after retries.', fix: 'Replicate state and merge it; merging can be repeated safely.' },
      { title: 'Merging by adding', symptom: 'The value grows on every gossip round.', fix: 'Take the maximum per entry. Merge must be idempotent.' },
      { title: 'One table for increments and decrements', symptom: 'Decrements are lost when states merge.', fix: 'max only works on values that never shrink. Keep two grow-only tables.' },
    ],
    questions: [
      { q: 'Which three properties must a state-based CRDT merge have?', a: 'Commutative, associative and idempotent. Together they mean replicas converge whatever the order, grouping or duplication of merges.' },
      { q: 'Why does a G-Counter keep one entry per node rather than one number?', a: 'With one number, two replicas that each added 1 cannot be merged: max loses an increment and sum double-counts. Per-node entries make every increment attributable.' },
      { q: 'What do you give up by choosing a CRDT?', a: 'Invariants across replicas. A PN-Counter cannot enforce "never below zero", because two replicas can each spend the last unit before they hear from one another.' },
    ],
    reading: [
      { kind: 'Site', title: 'crdt.tech', url: 'https://crdt.tech/', note: 'Papers, talks and implementations.' },
      { kind: 'Paper', title: 'A comprehensive study of Convergent and Commutative Replicated Data Types', url: 'https://inria.hal.science/inria-00555588', note: 'Shapiro et al., the reference catalogue.' },
    ],
  },
  {
    id: 'transactions',
    code: 'TX',
    category: 'convergence',
    title: 'Transactions',
    level: 'Advanced',
    blurb: 'Group reads and writes, then replicate them while staying available through a partition.',
    subtracks: [{ title: 'From serial to available', tasks: ['txn-single', 'txn-replicated'] }],
    mistakes: [
      { title: 'Keeping whichever write arrived last', symptom: 'Replicas swap values after a partition and still disagree.', fix: 'Pick the winner by a tag every replica orders the same way, such as (timestamp, node id).' },
      { title: 'Ties on the timestamp', symptom: 'Occasional divergence on one key.', fix: 'Break ties with the node id so the order is total.' },
      { title: 'Not advancing the clock on merge', symptom: 'A later local write loses to an older remote one.', fix: 'Apply the Lamport receive rule when merging remote entries.' },
    ],
    questions: [
      { q: 'What does last-writer-wins silently do to concurrent writes?', a: 'It discards all but one, even though every client was told its write succeeded.' },
      { q: 'Why can a totally available system not be serializable?', a: 'During a partition each side must answer without seeing the other’s writes, so no single serial order can explain what both sides observed.' },
      { q: 'What is anti-entropy?', a: 'A background process in which replicas repeatedly compare or exchange state, so that anything missed is eventually repaired without tracking individual messages.' },
    ],
    reading: [
      { kind: 'Reference', title: 'Jepsen: consistency models', url: 'https://jepsen.io/consistency', note: 'A map of isolation and consistency levels and which are achievable when.' },
      { kind: 'Paper', title: 'Highly Available Transactions: Virtues and Limitations', url: 'https://www.vldb.org/pvldb/vol7/p181-bailis.pdf', note: 'Bailis et al.' },
    ],
  },
  {
    id: 'logs',
    code: 'LG',
    category: 'storage',
    title: 'Replicated logs',
    level: 'Advanced',
    blurb: 'The append-only log behind Kafka-style systems: offsets, polls, commits, and then three nodes serving one log.',
    subtracks: [{ title: 'Append-only logs', tasks: ['log-single', 'log-multi'] }],
    mistakes: [
      { title: 'Per-node offset counters', symptom: 'Two messages share an offset.', fix: 'Claim offsets through the shared store with compare-and-swap.' },
      { title: 'Read then write instead of compare-and-swap', symptom: 'Rare duplicate offsets under load.', fix: 'Another node can write between your read and your write. cas makes the pair atomic.' },
      { title: 'Polling past a gap', symptom: 'A message is skipped and never delivered.', fix: 'An offset can be claimed before its message is stored. Stop at the first missing entry.' },
    ],
    questions: [
      { q: 'What does linearizable mean for the shared store?', a: 'Every operation appears to take effect at one instant between its call and its return, so the store behaves like a single variable.' },
      { q: 'Why is compare-and-swap enough to build a counter many nodes can share?', a: 'Each node reads the value and attempts to replace it only if unchanged. Exactly one of several racing nodes succeeds; the rest retry with the new value.' },
      { q: 'Why may log entries be cached forever but the next-offset key may not?', a: 'Entries are immutable once written. The next-offset key changes on every append.' },
    ],
    reading: [
      { kind: 'Blog', title: 'The Log: what every software engineer should know', url: 'https://web.archive.org/web/2025/https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying', note: 'Jay Kreps on logs as the unifying abstraction (archived copy; the original page is gone).' },
      { kind: 'Docs', title: 'Apache Kafka: design', url: 'https://kafka.apache.org/documentation/#design', note: 'Offsets, consumer groups and replication in practice.' },
    ],
  },
  {
    id: 'election',
    code: 'EL',
    category: 'consensus',
    title: 'Failure and leadership',
    level: 'Advanced',
    blurb: 'Notice that a node has gone quiet, then elect exactly one leader to replace it: the first half of Raft.',
    subtracks: [{ title: 'Detecting and electing', tasks: ['failure-detector', 'leader-election'] }],
    mistakes: [
      { title: 'Fixed election timeouts', symptom: 'Every node becomes a candidate together and nobody wins.', fix: 'Randomise the timeout so one node usually goes first.' },
      { title: 'Counting a majority of reachable nodes', symptom: 'Both sides of a partition elect a leader.', fix: 'A majority is of the whole cluster, always.' },
      { title: 'Voting twice in a term', symptom: 'Two leaders in the same term.', fix: 'Record who you voted for and reset it only when the term changes.' },
      { title: 'A deposed leader keeps leading', symptom: 'Two nodes claim leadership after the network heals.', fix: 'Step down on seeing any higher term, including in replies.' },
    ],
    questions: [
      { q: 'Why can two majorities of one cluster never elect different leaders in the same term?', a: 'Any two majorities share at least one node, and a node votes once per term.' },
      { q: 'Can a failure detector tell a crashed node from a slow one?', a: 'No. In an asynchronous network both are silent. A detector can only suspect, and must tolerate being wrong.' },
      { q: 'An old leader is cut off but still believes it leads. Why is that safe?', a: 'It leads an older term and cannot reach a majority, so nothing it proposes can be committed. It steps down when it sees the newer term.' },
    ],
    reading: [
      { kind: 'Paper', title: 'In Search of an Understandable Consensus Algorithm', url: 'https://raft.github.io/raft.pdf', note: 'Ongaro and Ousterhout: the Raft paper.' },
      { kind: 'Visual', title: 'The Secret Lives of Data: Raft', url: 'https://thesecretlivesofdata.com/raft/', note: 'An animated walkthrough of election and replication.' },
      { kind: 'Paper', title: 'SWIM', url: 'https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf', note: 'Scalable failure detection and membership.' },
    ],
  },
];

// Tracks come from two places: the ones above, and one-file-per-track modules
// in ./tracks. A module with no tasks yet is listed as "coming soon".
const allDefs = [...trackDefs, ...moduleTracks];

export const planned = [
  ...allDefs.filter((t) => t.subtracks.every((sub) => sub.tasks.length === 0)).map(({ category, title, blurb }) => ({ category, title, blurb })),
];

// Numbered in category order, so the numbers read 01, 02, 03… down the tracks page.
const categoryRank = (t) => categories.findIndex((c) => c.id === t.category);

export const tracks = allDefs
  .filter((t) => t.subtracks.some((sub) => sub.tasks.length > 0))
  .sort((a, b) => categoryRank(a) - categoryRank(b))
  .map((t, ti) => {
  let n = 0;
  return {
    ...t,
    number: String(ti + 1).padStart(2, '0'),
    subtracks: t.subtracks.map((s) => ({
      ...s,
      tasks: s.tasks.map((task) => ({ ...(typeof task === 'string' ? pool[task] : task), id: `${t.code}-${++n}`, number: `${t.code}-${n}`, trackId: t.id, trackTitle: t.title })),
    })),
  };
});

for (const t of tracks) {
  t.tasks = t.subtracks.flatMap((s) => s.tasks);
  t.minutes = t.tasks.reduce((sum, c) => sum + c.minutes, 0);
}

export const challenges = tracks.flatMap((t) => t.tasks);
export const bySlug = Object.fromEntries(challenges.map((c) => [c.slug, c]));
export const trackById = Object.fromEntries(tracks.map((t) => [t.id, t]));
