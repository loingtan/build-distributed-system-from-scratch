export default [
  {
    term: 'Heartbeats',
    aliases: ['heartbeat'],
    summary: 'Small periodic "I am alive" messages that let other nodes notice when a node goes silent.',
    detail: `A **heartbeat** carries little or no data; its arrival is the information. Each node records when it last heard from each peer, and a peer silent for longer than a timeout becomes suspected.

Example: n1 sends a heartbeat to each peer every 100ms; n2 suspects n1 after 500ms without one. With 10% message loss, five consecutive losses happen about once in 100,000 intervals, so false alarms are rare.

In Raft the leader's heartbeat is an AppendEntries message with no entries; it also resets followers' election timers and carries the leader's term, so stale leaders learn of newer terms.`,
    related: ['Timeouts', 'Suspicion', 'Failure detector'],
  },
  {
    term: 'Timeouts',
    aliases: ['timeout'],
    summary: 'How long a node waits for a message before acting as if it will not come.',
    detail: `A **timeout** turns silence into a decision: retry, suspect a peer, or start an election. In an asynchronous network there is no timeout that is always right, because messages can be delayed arbitrarily.

Short timeouts react quickly and misfire on ordinary delays; long ones misfire less and react slowly. A common rule is several times the normal interval or round trip: suspect after 5 missed heartbeats, or set Raft's election timeout to about ten times the round-trip time.`,
    related: ['Heartbeats', 'Suspicion'],
  },
  {
    term: 'Suspicion',
    aliases: ['suspected', 'suspect'],
    summary: 'A belief, not a fact, that a peer has failed, held because it has been silent for too long.',
    detail: `A crashed node, a slow node and a node behind a broken link all look the same from outside: silent. A failure detector can therefore only **suspect**, and must withdraw suspicion when the peer speaks again.

Computing suspicion on demand (\`now − lastHeard > timeout\`) instead of storing a flag makes recovery automatic. Systems such as SWIM add an intermediate "suspect" state and ask other nodes to probe before declaring a node dead, to reduce false positives caused by one bad link.`,
    related: ['Failure detector', 'Heartbeats', 'Timeouts'],
  },
  {
    term: 'Failure detector',
    aliases: ['failure detection'],
    summary: 'A component that tells each node which peers it currently suspects have failed.',
    detail: `A **failure detector** is judged on two properties: **completeness** (a node that has really failed is eventually suspected) and **accuracy** (a live node is not suspected). On an asynchronous network no detector can guarantee both, so real ones trade them through the timeout.

Variants: fixed-timeout heartbeats (simple); the phi accrual detector used by Cassandra and Akka, which outputs a continuous suspicion level from the observed distribution of heartbeat gaps; and SWIM-style probing, used by Consul's Serf and memberlist, which scales to large clusters.

A failure detector alone cannot elect a leader safely: in a partition each side suspects the other. Election needs a majority vote.`,
    related: ['Heartbeats', 'Suspicion', 'Split brain'],
  },
  {
    term: 'Raft',
    aliases: ['Raft consensus'],
    summary: 'A consensus algorithm that elects one leader per term and has it replicate a log to a majority.',
    detail: `**Raft** (Ongaro and Ousterhout, 2014) was designed to be easier to understand and implement than Paxos while giving the same guarantees. It separates three concerns: leader election (terms, votes, randomised timeouts), log replication (AppendEntries, commit on a majority), and safety rules that connect them (the election restriction and committing only current-term entries).

Its rules fit on one page, Figure 2 of the paper. A Raft cluster of 2f + 1 nodes keeps working with up to f failed or unreachable nodes.

Raft runs inside etcd (and therefore Kubernetes), Consul, CockroachDB, TiKV, Kafka's KRaft controller, RabbitMQ quorum queues and many more.`,
    related: ['Terms', 'Majority quorum', 'Replicated log'],
  },
  {
    term: 'Terms',
    aliases: ['term', 'Raft term'],
    summary: 'Numbered periods in Raft, each starting with an election and having at most one leader.',
    detail: `Every Raft node stores a **current term** that only increases. A candidate increments it to start an election; every message carries the sender's term. A node that sees a higher term adopts it, clears its vote and becomes a follower; a request with a lower term is rejected, and the reply carries the higher term so the sender learns it is stale.

Terms are a logical clock for leadership. Example: n1 leads term 1 and is cut off; n3 wins term 2. When n1 hears anything from term 2 it steps down at once, even though it never saw the election.

A term may end with no leader (a split vote); the next timeout starts a new term. Log entries record the term in which they were created, which the next stages of Raft rely on.`,
    related: ['Raft', 'Epochs', 'Split brain'],
  },
  {
    term: 'Majority quorum',
    aliases: ['majority', 'majorities'],
    summary: 'Any set of more than half of a cluster\'s nodes; any two such sets always share a node.',
    detail: `In a cluster of N nodes a **majority** is ⌊N/2⌋ + 1 nodes: 2 of 3, 3 of 5, 4 of 7. Two majorities together have more than N members, so they must overlap in at least one node. That overlap is the basis of Raft's safety: a node votes once per term, so two candidates cannot both collect a majority in the same term; and a committed entry on a majority is always seen by the majority that elects the next leader.

The majority is always of the whole configured cluster, never of the nodes currently reachable; otherwise both sides of a partition could proceed. With 2f + 1 nodes, f may fail and a majority remains.`,
    related: ['Raft', 'Split brain', 'Commit index'],
  },
  {
    term: 'Split brain',
    aliases: ['split-brain'],
    summary: 'Two parts of a cluster each acting as if in charge, typically after a partition, so their decisions conflict.',
    detail: `**Split brain** happens when each side of a partition, or a stale leader and its replacement, accepts writes independently. When the network heals there are two incompatible histories.

Example: five nodes split 2 + 3. If each side elects a leader among the nodes it can reach, both accept writes. Requiring votes from a majority of all five means only the side with three nodes can elect a leader.

Prevention relies on majorities and on fencing stale leaders with terms or epochs. A stale leader may still *believe* it leads; what matters is that nothing it does can take effect without a majority that has moved on.`,
    related: ['Majority quorum', 'Terms', 'Fencing'],
  },
  {
    term: 'Election timeout',
    aliases: ['randomized election timeout', 'randomised election timeout', 'election timer'],
    summary: 'How long a Raft follower waits without hearing from a leader before starting an election, randomised per node.',
    detail: `Each follower restarts its **election timer** whenever it hears from the current leader or grants a vote, choosing a fresh random duration from a range such as 300–600ms. If the timer fires, it becomes a candidate.

The randomness prevents **split votes**: with equal timeouts, every follower would stand at once, vote for itself, and nobody would win, repeatedly. With random ones, one node usually stands first and collects the votes before others wake.

It must be much larger than the heartbeat interval and the round-trip time. The Raft paper suggests 150–300ms; etcd defaults to a 1000ms election timeout with 100ms heartbeats.`,
    related: ['Heartbeats', 'Terms', 'Raft'],
  },
  {
    term: 'Election Safety',
    aliases: ['one leader per term'],
    summary: 'Raft\'s guarantee that at most one leader is elected in any given term.',
    detail: `**Election Safety** follows from two facts: each node grants at most one vote per term (and remembers it, on disk, across crashes), and a candidate needs votes from a majority of the whole cluster. If two candidates both won term t, some node in the overlap of their majorities would have voted twice in t.

It does not say there is only one node *believing* it is leader at any instant: an old leader in an older term may not have noticed yet. It only rules out two leaders of the same term, which is enough because everything a leader does carries its term.`,
    related: ['Majority quorum', 'Terms', 'Split brain'],
  },
  {
    term: 'Pre-Vote',
    aliases: ['prevote', 'pre-vote'],
    summary: 'A Raft extension where a node checks it could win before incrementing its term, so rejoining nodes do not disrupt.',
    detail: `A node cut off from the cluster keeps timing out and raising its term. When it rejoins, its high term forces the working leader to step down for no reason. With **Pre-Vote**, a would-be candidate first asks peers whether they would grant a vote (without anyone changing term); peers that still hear from a leader, or whose logs are more up to date, say no. Only with a majority of yeses does it start a real election.

Described in Ongaro's dissertation, it is implemented in etcd's Raft library, among others.`,
    related: ['Election timeout', 'Terms'],
  },
];
