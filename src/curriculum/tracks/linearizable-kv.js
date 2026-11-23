// Track: Linearizable key-value store. See docs/authoring.md for the shape of this file.
import { kvOps, kvSessions, kvReads, kvSnapshots } from '../../sim/workloads/linearizable-kv.js';

export default {
  id: 'linearizable-kv',
  code: 'KV',
  category: 'consensus',
  title: 'Linearizable key-value store',
  level: 'Advanced',
  blurb: 'A key-value store on top of your Raft log that behaves like a single machine: client sessions, exactly-once writes and reads that are never stale.',
  subtracks: [
    {
      title: 'A store on the log',
      tasks: [
        {
          slug: 'kv-state-machine',
          title: 'Get, put and cas through the log',
          level: 'Medium',
          minutes: 30,
          summary: 'Turn the Raft log into a key-value store whose every operation takes effect at one instant, in one order.',
          concepts: ['state machine', 'linearizability', 'compare-and-set'],
          workload: kvOps,
          tests: [
            { name: 'Healthy network', blurb: 'Three nodes, four clients mixing get, put and cas on four keys.', nodeCount: 3, duration: 6000, params: { scenario: 'stable' } },
            { name: 'Leader crashes', blurb: 'Five nodes; the leader crashes at 3s and restarts at 4.5s.', nodeCount: 5, duration: 8000, params: { scenario: 'leader-crash' } },
            { name: 'Leader cut off', blurb: 'The leader is cut off alone from 3s to 6s while one client keeps reading from it.', nodeCount: 5, duration: 9500, params: { scenario: 'leader-cut' } },
          ],
          hints: [
            'Give each entry an `op`: `{ term, op: "put", key, value }`, `{ term, op: "get", key }` or `{ term, op: "cas", key, from, to }`.',
            'Write one `apply(entry)` that changes the store and returns the reply: `put_ok`, `get_ok` with the value, or `cas_ok` / `cas_failed`. Call it in `commitTo` and send its result to the request waiting on that index.',
            'Every client handler starts the same way: not the leader, reply `not_leader`; otherwise push the entry, remember the request under its index, and replicate.',
            'The starter\'s get is the bug the tests look for. A follower applies a committed entry only when the next heartbeat tells it to, and a cut-off leader never hears of newer writes at all.',
            'In `adoptTerm`, forget the waiting requests without replying. Their entries are in the log and may still commit, so `not_leader` would be a lie.',
          ],
        },
        {
          slug: 'kv-sessions',
          title: 'Exactly-once with client sessions',
          level: 'Hard',
          minutes: 35,
          summary: 'Clients retry with a client id and a sequence number; every request takes effect once, even across leader changes.',
          concepts: ['client sessions', 'exactly-once', 'deduplication'],
          workload: kvSessions,
          tests: [
            { name: 'Lost replies and duplicates', blurb: 'One reply in ten is lost, a quarter of writes arrive twice, and old puts are delivered again later.', nodeCount: 3, duration: 7000, params: { scenario: 'stable', dropReplies: 0.1, dup: 0.25, lateDup: 0.15 } },
            { name: 'Reply lost, leader crashes', blurb: 'Twice, the leader commits a cas, its reply is lost and it crashes on the spot; the client retries on the next leader.', nodeCount: 3, duration: 9500, params: { scenario: 'lost-reply', lateDup: 0.1 } },
            { name: 'Partition, then a full restart', blurb: 'The leader is cut off from 2.8s to 4.3s; at 6.8s every node crashes, and old requests keep arriving after the restart.', nodeCount: 5, duration: 10500, params: { scenario: 'cut-restart', dropReplies: 0.05, dup: 0.2, lateDup: 0.15 } },
          ],
          hints: [
            'Carry `client_id` and `seq` into the log entry, so that `apply` can see them.',
            'Add `const sessions = {}` next to the store. It is part of the state machine: it changes only in `apply`, and after a restart it is rebuilt by applying the log again, like the store.',
            'In `apply`, for put and cas: if `seq <= sessions[client_id].seq`, leave the store alone and return the saved reply when the seqs are equal (nothing when it is older). Otherwise apply the entry and save `{ seq, reply }`.',
            'Checking the table when the request arrives is not enough. Two copies can both be in the log before either is applied, and a new leader may hold the original entry without having applied it yet.',
            'A cas answered `cas_failed` whose new value later shows up was applied twice: once for real, and again by a copy that then found the value changed.',
          ],
        },
      ],
    },
    {
      title: 'Making it fast and small',
      tasks: [
        {
          slug: 'kv-read-index',
          title: 'Reads that are never stale',
          level: 'Hard',
          minutes: 35,
          summary: 'Take gets off the log without ever returning a stale value: confirm leadership with a majority before answering.',
          concepts: ['read-index', 'stale reads', 'no-op entry'],
          workload: kvReads,
          tests: [
            { name: 'Read-heavy, healthy', blurb: 'Three nodes, seven requests in ten are gets; the log may only grow with writes.', nodeCount: 3, duration: 6500, params: { scenario: 'stable', reads: 0.7, cas: 0.15, lateDup: 0.05 } },
            { name: 'Leader cut off', blurb: 'The leader is cut off alone from 3s to 6s while one client keeps reading from it.', nodeCount: 5, duration: 9500, params: { scenario: 'leader-cut', reads: 0.6, cas: 0.2 } },
            { name: 'New leader, nothing to write', blurb: 'The leader crashes the instant it acknowledges a write; for 2.5s clients only read, starting with that key.', nodeCount: 3, duration: 9500, params: { scenario: 'noop', reads: 0.6, cas: 0.2 } },
          ],
          hints: [
            'In `becomeLeader`, push `{ term, op: "noop" }` and remember its index. `apply` ignores it.',
            'On a get, keep `{ msg, index: Math.max(commitIndex, noopIndex), confirmed: false }` in a list and start a round of `append_entries` at once.',
            'A follower that answers that round with your term, success or not, still follows you. When you plus those followers make a majority, mark every read that was in the list when the round started as confirmed.',
            'Answer a read once it is confirmed and `commitIndex >= index`. Check after every confirmation and after every commit.',
            'When the leader steps down, answer its waiting reads with `not_leader`. A read has no effect, so for a read that reply is true.',
          ],
        },
        {
          slug: 'kv-snapshots',
          title: 'Compact the log with snapshots',
          level: 'Hard',
          minutes: 45,
          summary: 'Keep node.disk small: fold applied entries into a snapshot, and send it to followers that fall too far behind.',
          concepts: ['log compaction', 'snapshots', 'InstallSnapshot'],
          workload: kvSnapshots,
          tests: [
            { name: 'Many writes', blurb: 'Three nodes and a write-heavy load: hundreds of entries, but node.disk must stay under 6,000 bytes.', nodeCount: 3, duration: 8000, params: { scenario: 'stable', reads: 0.3, cas: 0.3, gap: 30, lateDup: 0.05 } },
            { name: 'A follower falls behind', blurb: 'A follower is cut off from 2.5s to 4.5s; at 6s the leader crashes, and the follower that fell behind must help commit.', nodeCount: 3, duration: 10500, params: { scenario: 'lagging-follower', reads: 0.3, cas: 0.3, gap: 30 } },
            { name: 'Whole cluster restarts', blurb: 'Every node crashes at 4.5s and restarts at 5s; for 1.5s clients only read while old requests from before the crash arrive again.', nodeCount: 3, duration: 9000, params: { scenario: 'full-restart', reads: 0.3, cas: 0.3, gap: 30, lateDup: 0.15 } },
          ],
          hints: [
            'Keep `node.disk.snap = { index, term, store, sessions }` (index 0 and term 0 at first) and write helpers: `lastIndex()` is `snap.index + log.length`, plus `termAt(i)` and `entryAt(i)`. Then replace every `log.length` and `log[i - 1]` with them.',
            'After applying, if `commitIndex - snap.index` passes your threshold, save deep copies of store and sessions (`JSON.parse(JSON.stringify(x))`) with the index and term of `commitIndex`, then `log.splice(0, commitIndex - snap.index)`.',
            'On restart, take `store` and `sessions` from the snapshot and start at `commitIndex = snap.index`.',
            'In `sendEntries`, when `nextIndex[peer] <= snap.index`, send `{ type: "install_snapshot", term, snapshot }` instead. On a reply in your term, set `nextIndex` to `snapshot.index + 1` and `matchIndex` to `snapshot.index`.',
            'A follower given a snapshot past its commit index keeps the entries after it if its own entry at `snapshot.index` has `snapshot.term`, and otherwise empties its log; then it takes the store and sessions and sets `commitIndex`. If an `append_entries` starts inside its snapshot, it skips the entries it already covers.',
          ],
        },
      ],
    },
  ],
  mistakes: [
    { title: 'Answering reads from local state', symptom: 'A get returns a value older than a write that had already finished: from a follower that has not applied it yet, or from a leader that has been cut off.', fix: 'Put reads through the log, or have the leader confirm with a majority, after the read arrived, that it still leads (read-index).' },
    { title: 'Deduplicating when the request arrives', symptom: 'Two copies of one cas get different answers, or a retry sent to a new leader is applied a second time.', fix: 'Decide when the entry is applied, from a session table that is part of the replicated state machine.' },
    { title: 'Serving reads straight after an election', symptom: 'Just after a leader change, a get misses the last write the old leader acknowledged.', fix: 'Commit a no-op entry of the new term first; only then does the leader know what is committed.' },
    { title: 'Snapshotting the store but not the sessions', symptom: 'After a restart, an old duplicate request is applied again.', fix: 'The session table is part of the state machine, so it goes into the snapshot.' },
  ],
  questions: [
    { q: 'Why can a leader not simply answer gets from its own state machine?', a: 'Being leader is a belief, not a fact. A leader cut off from the others keeps believing it while a majority elects a successor and commits new writes. Before answering it must hear from a majority, after the read arrived, that no newer term exists. Putting the read in the log forces the same check.' },
    { q: 'A client sent a cas, heard nothing, and sent it again. What does the session table let the store promise?', a: 'That the cas takes effect at most once whatever became of the first copy, and that the retry gets the original answer. Without it, a retry of a cas that did apply fails, and the client is told "no" about a change that happened.' },
    { q: 'Leader leases answer reads with no round trip at all. What do they assume that read-index does not?', a: 'Time. A lease is safe only if clocks run at nearly the same rate and no other node can become leader until the lease has expired, so followers must refuse to elect anyone while they still hear from the leaseholder. Read-index relies on messages alone.' },
  ],
  reading: [
    { kind: 'Paper', title: 'In Search of an Understandable Consensus Algorithm (Extended Version)', url: 'https://raft.github.io/raft.pdf', note: 'Section 8 is this track: client sessions, the no-op entry and read-only queries. Section 7 covers snapshots.' },
    { kind: 'Paper', title: 'Linearizability: A Correctness Condition for Concurrent Objects', url: 'https://www.cs.cmu.edu/~wing/publications/HerlihyWing90.pdf', note: 'Herlihy and Wing (1990): the definition, and why it can be checked one object at a time.' },
    { kind: 'Docs', title: 'Jepsen: Linearizability', url: 'https://jepsen.io/consistency/models/linearizable', note: 'A short, precise statement of the model and how it relates to the others.' },
  ],
};
