// Capstone projects: guided paths through tasks from several tracks that
// together make up one recognisable system. A stage lists whole tracks
// and/or single tasks; anything not built yet is skipped.

import { tracks, bySlug, trackById } from './catalog.js';

const defs = [
  {
    id: 'mini-dynamo',
    title: 'Mini-Dynamo',
    level: 'Advanced',
    tagline: 'A leaderless, always-writable key-value store.',
    blurb: 'Amazon’s Dynamo chose availability over consistency: any node accepts a write, replicas disagree for a while, and the system repairs itself. Build the pieces in the order the design needs them.',
    builds: [
      'Keys spread over nodes with consistent hashing',
      'Quorum reads and writes with tunable N, R and W',
      'Version tracking that tells a stale value from a conflict',
      'Failure detection and gossip, with no coordinator',
      'Data types that merge instead of conflicting',
    ],
    tradeoff: 'Every node can always take a write, so two clients can update the same key on different sides of a partition. Dynamo accepts that and makes reconciliation the application’s problem. You gain uptime and lose the idea of one current value.',
    tags: ['consistent hashing', 'quorums', 'vector clocks', 'gossip', 'CRDTs'],
    stages: [
      { title: 'Placing keys', blurb: 'Decide which nodes own which keys, and keep that stable as nodes come and go.', tracks: ['sharding'] },
      { title: 'Replicating without a leader', blurb: 'Write to several replicas, read from several, and repair the ones that missed out.', tracks: ['quorum'] },
      { title: 'Versions and conflicts', blurb: 'Know whether one version replaces another or the two are siblings.', tasks: ['lamport-clock', 'vector-clock'] },
      { title: 'Membership', blurb: 'Notice unreachable nodes and spread what you know, cheaply.', tasks: ['failure-detector', 'broadcast-faults', 'broadcast-efficient'], tracks: ['membership'] },
      { title: 'Merging state', blurb: 'For some data, conflicts can be designed away: counters, sets, registers and maps that merge.', tracks: ['crdts', 'crdt-collections'] },
    ],
  },
  {
    id: 'mini-kafka',
    title: 'Mini-Kafka',
    level: 'Advanced',
    tagline: 'A durable, replicated commit log with consumers.',
    blurb: 'Kafka is an append-only log that many producers write and many consumers read at their own pace. Build the log, make it durable, replicate it, and give consumers delivery guarantees.',
    builds: [
      'An append-only log with offsets, polls and committed positions',
      'One log served by several brokers',
      'Durability: a write-ahead log that survives a crash',
      'A leader with followers that stay in sync',
      'At-least-once delivery, redelivery and deduplication for consumers',
    ],
    tradeoff: 'A log gives every consumer the same order, which is what makes replay and exactly-once processing possible. The price is that order is per partition: scale out by adding partitions and you give up ordering across them.',
    tags: ['append-only log', 'offsets', 'replication', 'WAL', 'delivery guarantees'],
    stages: [
      { title: 'The log', blurb: 'Offsets, polling and commits; then several nodes serving one log.', tracks: ['logs'] },
      { title: 'Durability', blurb: 'What is acknowledged must survive a crash.', tracks: ['storage-engine'] },
      { title: 'Replicas', blurb: 'A leader takes the writes; followers keep up.', tracks: ['replication'] },
      { title: 'Producers and consumers', blurb: 'Retries, acknowledgements and what happens to a job that keeps failing.', tasks: ['idempotent-requests'], tracks: ['queues'] },
    ],
  },
  {
    id: 'mini-etcd',
    title: 'Mini-etcd',
    level: 'Advanced',
    tagline: 'A consistent store built on Raft.',
    blurb: 'etcd keeps the configuration that clusters such as Kubernetes cannot afford to get wrong. It is a key-value state machine driven by a Raft log. Build Raft from failure detection up to crash recovery.',
    builds: [
      'A failure detector and randomised leader election',
      'A replicated log that commits on a majority',
      'A state machine applied in log order',
      'Recovery of term, vote and log after a crash',
      'A linearizable key-value API with client sessions',
    ],
    tradeoff: 'A majority must be reachable for anything to happen. During a partition the minority side stops accepting writes: the opposite choice to Dynamo. You get one agreed history and pay for it in availability.',
    tags: ['Raft', 'leader election', 'log replication', 'linearizability'],
    stages: [
      { title: 'Who is in charge', blurb: 'Suspect failures, then elect exactly one leader per term.', tracks: ['election'] },
      { title: 'One log, many nodes', blurb: 'Replicate, commit, repair, recover.', tracks: ['raft-log'] },
      { title: 'The other consensus', blurb: 'Paxos, the algorithm Raft was designed to explain: the same guarantees, built from ballots instead of terms.', tracks: ['paxos'] },
      { title: 'A database on the log', blurb: 'Turn the log into a store that behaves like one machine: sessions, exactly-once writes, reads that are never stale.', tracks: ['linearizable-kv'] },
      { title: 'On disk', blurb: 'The log has to survive the machine.', tracks: ['storage-engine'] },
      { title: 'Clients', blurb: 'A retried request must not be applied twice.', tasks: ['idempotent-requests'] },
    ],
  },
  {
    id: 'mini-bank',
    title: 'Mini-Bank',
    level: 'Advanced',
    tagline: 'Transactions that span several nodes.',
    blurb: 'Moving money between accounts on different machines is the classic distributed transaction: both sides change, or neither does. Build transactions on one node, then across nodes, then make them survive failure.',
    builds: [
      'Serial transactions on one node',
      'Atomic commit across nodes with two-phase commit',
      'Aborts, locks and timeouts',
      'Recovery after the coordinator crashes',
      'A dependency layer that fails gracefully',
    ],
    tradeoff: 'Two-phase commit gives atomicity but blocks: a participant that has voted yes cannot decide alone, and waits for a coordinator that may be down. Systems that need to stay available use weaker guarantees instead, which you build in the Transactions track.',
    tags: ['transactions', 'two-phase commit', 'atomicity', 'recovery'],
    stages: [
      { title: 'Transactions', blurb: 'From serial execution to replicas that stay available.', tracks: ['transactions'] },
      { title: 'Atomic commit', blurb: 'All nodes commit, or none do.', tracks: ['two-phase-commit'] },
      { title: 'Ordering', blurb: 'Timestamps that do not rely on synchronised clocks.', tasks: ['lamport-clock'] },
      { title: 'When dependencies fail', blurb: 'Timeouts, retries and circuit breakers.', tracks: ['resilience'] },
    ],
  },
  {
    id: 'mini-gateway',
    title: 'Mini-Gateway',
    level: 'Intermediate',
    tagline: 'The layer in front of every busy service.',
    blurb: 'An API gateway or edge proxy decides which backend serves each request, answers what it can from a cache, protects the services behind it from overload, and keeps running while it is being upgraded.',
    builds: [
      'Load balancing with health checks and sticky routing',
      'A cache with eviction, expiry, invalidation and stampede protection',
      'Timeouts, retries with jitter and a circuit breaker',
      'Rate limiting and bounded concurrency',
      'Upgrades rolled out node by node without dropping requests',
    ],
    tradeoff: 'Every layer in front of a service adds a place where requests can be delayed, served stale or refused. A gateway earns its keep by failing fast and predictably on behalf of the backends, so a slow dependency costs you a few errors instead of an outage.',
    tags: ['load balancing', 'caching', 'circuit breakers', 'rate limiting', 'rolling upgrades'],
    stages: [
      { title: 'Spreading the load', blurb: 'Pick a backend for every request, and stop picking dead ones.', tracks: ['load-balancing'] },
      { title: 'Answering from memory', blurb: 'Serve hot data from a cache without serving wrong data.', tracks: ['caching'] },
      { title: 'Protecting the backends', blurb: 'Fail fast when a dependency is slow, broken or overloaded.', tracks: ['resilience'] },
      { title: 'Shipping changes', blurb: 'Upgrade the gateway itself while it keeps serving.', tracks: ['rolling-changes'] },
    ],
  },
];

export const projects = defs
  .map((p) => {
    const seen = new Set();
    const stages = p.stages
      .map((s) => {
        const tasks = [
          ...(s.tasks ?? []).map((slug) => bySlug[slug]),
          ...(s.tracks ?? []).flatMap((id) => trackById[id]?.tasks ?? []),
        ].filter((t) => t && !seen.has(t.slug) && seen.add(t.slug));
        return { ...s, tasks };
      })
      .filter((s) => s.tasks.length > 0);
    const tasks = stages.flatMap((s) => s.tasks);
    return { ...p, stages, tasks, hours: Math.max(1, Math.round(tasks.reduce((sum, t) => sum + t.minutes, 0) / 60)) };
  })
  .filter((p) => p.tasks.length > 0);

export const projectById = Object.fromEntries(projects.map((p) => [p.id, p]));
export { tracks };
