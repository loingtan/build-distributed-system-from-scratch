export default [
  {
    term: 'State machine',
    summary: 'The deterministic store a Raft log drives; every node applies the same entries in order and computes the same replies.',
    detail: `In a linearizable key-value store the state machine is the store **plus** everything replies depend on, such as the client session table. One \`apply(entry)\` function changes it and returns the reply (\`put_ok\`, \`get_ok\` with a value, \`cas_ok\` or \`cas_failed\`), and it is the only place the state changes.`,
    related: ['Replicated log', 'Client sessions'],
  },
  {
    term: 'Linearizability',
    summary: 'Every operation appears to take effect at one instant between its request and its reply, as if on a single machine.',
    detail: `A store is **linearizable** if there is one total order of all operations that explains every reply, and in which an operation that finished before another started comes first. Overlapping operations may be ordered either way.

Example: client A's \`put x=3\` returns at t=10; client B sends \`get x\` at t=12. B must see 3 or something newer, never the value before 3.

Putting every operation through a consensus log gives it directly: the commit order is the order, and each operation takes effect when its entry is applied. Checkers such as Jepsen's Knossos search for such an order in a recorded history.`,
    related: ['Stale reads', 'Read-index'],
  },
  {
    term: 'Compare-and-set',
    summary: 'Change a key only if it currently holds an expected value; otherwise fail and leave it unchanged.',
    detail: `\`cas x from 7 to 8\` sets x to 8 if x is 7 and replies \`cas_ok\`; otherwise it replies \`cas_failed\`. Clients use it to update shared values safely: read, compute, cas, and retry on failure.

Unlike put, cas is not idempotent: a retried cas that already succeeded fails the second time, telling the client "no" about a change that happened. That is why it needs client sessions.`,
    related: ['Client sessions', 'Exactly-once'],
  },
  {
    term: 'Client sessions',
    aliases: ['session table', 'client session'],
    summary: 'A table in the state machine recording each client\'s last applied sequence number and reply, to recognise retries.',
    detail: `Each client sends a \`client_id\` and a \`seq\` that only grows; a retry repeats both. When a write entry is applied, the state machine compares \`seq\` with \`sessions[client_id].seq\`: greater means apply and record \`{ seq, reply }\`; equal means a duplicate, so return the saved reply without touching the store; smaller means a leftover the client has given up on.

The table must be changed only in \`apply\`, so every node and every future leader makes the same decision from the same log, and it must be included in snapshots. Raft's paper (section 8) describes exactly this; real systems also expire old sessions, ideally through the log.`,
    related: ['Exactly-once', 'Deduplication', 'Snapshots'],
  },
  {
    term: 'Exactly-once',
    aliases: ['exactly once'],
    summary: 'Each request takes effect once however many copies arrive, and every copy gets the original reply.',
    detail: `Networks deliver at-least-once when clients retry; **exactly-once** effect is built on top by recognising duplicates. In a Raft store, a request and its retries may all end up in the log; the session table makes only the first applied copy change state.

It is "exactly once" in effect, not in delivery: the request may be received and logged several times.`,
    related: ['Client sessions', 'Deduplication'],
  },
  {
    term: 'Deduplication',
    summary: 'Recognising repeated copies of a request and making them harmless.',
    detail: `In a replicated state machine, deduplicate **when the entry is applied**, not when the request arrives. Two copies can both be in the log before either is applied, and a new leader may hold the original entry, committed by a crashed predecessor, without having applied it yet. Only an apply-time check against replicated state gives the same answer on every node.`,
    related: ['Client sessions', 'Exactly-once'],
  },
  {
    term: 'Read-index',
    aliases: ['ReadIndex', 'read index'],
    summary: 'Serving a read without a log entry: note the commit index, confirm leadership with a majority, wait until applied.',
    detail: `To answer a get, the leader (1) makes sure an entry of its own term is committed, usually its no-op; (2) records its commit index as the read index; (3) confirms it is still leader with a round of AppendEntries sent after the read arrived and answered by a majority in its term; (4) waits until it has applied up to the read index, then answers from its store.

One heartbeat round can confirm every read that arrived before it was sent. It saves the disk write and log growth of logging reads, but still costs a majority round trip. etcd serves linearizable reads this way.`,
    related: ['No-op entry', 'Stale reads', 'Leader lease'],
  },
  {
    term: 'Stale reads',
    aliases: ['stale read'],
    summary: 'A read that returns a value older than a write that had already completed before the read began.',
    detail: `Typical causes in a Raft store: a follower answers before it has applied a committed write; a leader cut off by a partition answers while a successor commits newer writes; a newly elected leader answers before it knows which entries are committed.

Example: \`put x=2\` is acknowledged at t=1000; a get sent at t=1010 returns 1. That violates linearizability. Reads through the log or read-index prevent it; serializable or "stale" read modes (etcd, Consul) accept it for speed.`,
    related: ['Linearizability', 'Read-index'],
  },
  {
    term: 'No-op entry',
    aliases: ['no-op', 'noop entry', 'noop'],
    summary: 'An empty log entry a new Raft leader appends at once, so an entry of its own term commits quickly.',
    detail: `A new leader holds every committed entry but may count replicas only for entries of its own term, so it cannot tell which older entries are committed until one of its own commits. Appending \`{ term, op: "noop" }\` on election fixes that without waiting for a client write; \`apply\` ignores it.

Read-index reads must wait for it: until the no-op commits, the leader's commit index may be behind writes its predecessor acknowledged.`,
    related: ['Read-index', 'Current-term commit rule'],
  },
  {
    term: 'Log compaction',
    summary: 'Replacing an applied prefix of the log with a snapshot, so the log and recovery time stay bounded.',
    detail: `Once entries are applied, the log is only needed to rebuild the state machine. **Log compaction** folds them into a snapshot and deletes them. Each node compacts independently, for example once 25 applied entries accumulate, because every node's state at a given index is identical.

Afterwards indexes are offset: entry i is at position \`i − snapshot.index − 1\`, and the term of \`snapshot.index\` comes from the snapshot. Other approaches include LSM-style incremental compaction for very large states.`,
    related: ['Snapshots', 'InstallSnapshot', 'Log truncation'],
  },
  {
    term: 'Snapshots',
    aliases: ['snapshot'],
    summary: 'A saved copy of the state machine as of a log index, with that index and its term.',
    detail: `A Raft snapshot holds \`{ index, term, store, sessions }\`: the state after applying entries 1…index. Only applied (therefore committed) entries may be included, and the session table must be, or a node restarted from the snapshot applies old duplicates again.

On restart a node loads the snapshot and starts at \`commitIndex = snapshot.index\`. Large state machines write snapshots in the background (copy-on-write) rather than inside a request handler.`,
    related: ['Log compaction', 'InstallSnapshot', 'Client sessions'],
  },
  {
    term: 'InstallSnapshot',
    aliases: ['install_snapshot', 'InstallSnapshot RPC'],
    summary: 'The Raft message a leader sends instead of entries when a follower needs entries already compacted away.',
    detail: `When \`nextIndex[peer] ≤ snapshot.index\`, the entries the follower needs are gone, so the leader sends its snapshot. A follower receiving a snapshot past its commit index takes its store and sessions and sets its commit index; it keeps log entries after the snapshot only if its own entry at \`snapshot.index\` has \`snapshot.term\`, and otherwise empties its log. A snapshot at or behind its commit index is ignored.

On reply the leader sets \`matchIndex\` to \`snapshot.index\` and \`nextIndex\` just past it. Real implementations send large snapshots in chunks.`,
    related: ['Snapshots', 'nextIndex'],
  },
  {
    term: 'Leader lease',
    aliases: ['lease read', 'leases'],
    summary: 'Letting a leader answer reads locally for a time window during which no other leader can be elected.',
    detail: `With a **lease**, followers promise not to elect a new leader until some time after they last heard from the current one, so the leader can serve reads with no round trip until its lease expires. Safety depends on bounded clock drift and on no process pause outlasting the margin.

Read-index relies on messages alone. Consul's default read mode and CockroachDB's leaseholders use leases.`,
    related: ['Read-index', 'Stale reads'],
  },
];
