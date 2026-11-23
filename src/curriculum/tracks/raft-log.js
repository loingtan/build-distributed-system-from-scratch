// Track: Raft log replication. See docs/authoring.md for the shape of this file.
import { raftAppend, raftKv } from '../../sim/workloads/raft-log.js';

export default {
  id: 'raft-log',
  code: 'RL',
  category: 'consensus',
  title: 'Raft log replication',
  level: 'Advanced',
  blurb: 'The second half of Raft: replicate a log, commit on a majority, and keep it safe through leader changes and crashes.',
  subtracks: [
    {
      title: 'One leader, one log',
      tasks: [
        {
          slug: 'raft-replicate',
          title: 'Replicate the log',
          level: 'Medium',
          minutes: 30,
          summary: 'The leader takes appends and gets the same entries, in the same order, onto every follower.',
          concepts: ['replicated log', 'AppendEntries', 'nextIndex'],
          workload: raftAppend,
          tests: [
            { name: 'Healthy network', blurb: 'Three nodes, two clients appending; every log must end up identical.', nodeCount: 3, duration: 6000, params: { scenario: 'stable' } },
            { name: 'Slow links', blurb: 'Five nodes on 20–60ms links, so messages overtake one another all the time.', nodeCount: 5, duration: 7000, latency: [20, 60], params: { scenario: 'stable', gap: 60 } },
            { name: 'A follower restarts', blurb: 'One follower crashes at 2.6s and comes back empty at 3.8s; the leader must refill it.', nodeCount: 3, duration: 7000, params: { scenario: 'follower-restart' } },
          ],
          hints: [
            'Only the leader may take an `append`. Everyone else replies `{ type: "not_leader", leader }` with the leader they currently believe in.',
            'Do not send each new entry once and hope. Keep `nextIndex[peer]` on the leader and, on every tick and after every append, send that peer everything from `nextIndex[peer]` on. With nothing new the list is empty, and the message is your heartbeat.',
            'Include `prevIndex`, the index just before the entries you send. A follower whose log is shorter than `prevIndex` must refuse, or it would store entries with a hole before them.',
            'Messages are duplicated by your own retries and can overtake each other. The follower should work out the index of every entry it receives (`prevIndex + 1 + i`) and store only the ones past the end of its log.',
            'Have the follower reply with how far it now matches (`prevIndex + entries.length`) or, on refusal, its log length. The leader moves `nextIndex[peer]` forward on success (never backward: use `Math.max`) and back to the follower\'s length on refusal.',
          ],
        },
        {
          slug: 'raft-commit',
          title: 'Commit and apply',
          level: 'Hard',
          minutes: 35,
          summary: 'Acknowledge a write only once a majority stores it, and apply committed entries to a key-value store.',
          concepts: ['commit index', 'majority', 'state machine'],
          workload: raftKv,
          tests: [
            { name: 'Healthy network', blurb: 'Three nodes, puts followed by gets; every node must end with the same store.', nodeCount: 3, duration: 6000, params: { scenario: 'stable' } },
            { name: 'Minority down', blurb: 'Two of five nodes crash for 1.5s. Clients must not notice.', nodeCount: 5, duration: 7500, params: { scenario: 'minority-down' } },
            { name: 'Majority down', blurb: 'Three of five nodes crash for 2s. Nothing may be committed or acknowledged until they return.', nodeCount: 5, duration: 9500, params: { scenario: 'majority-down' } },
          ],
          hints: [
            'Do not reply to `put` in the handler. Keep the request (a `Map` from log index to message works) and reply when that index commits.',
            'The leader needs `matchIndex[peer]`: the highest index it knows that peer stores. Update it from successful `append_entries` replies.',
            'An index is committed when the leader plus the peers with `matchIndex[peer] >= index` add up to a majority of `node.nodeIds.length`. Not of the nodes that happen to answer.',
            'Write one function that applies entries `commitIndex + 1 .. n` to the store in order and answers waiting clients. Applying before replying is what makes a `get` after `put_ok` see the write.',
            'Followers learn the commit index from the leader: add `commit: commitIndex` to `append_entries`. A follower commits up to `Math.min(commit, prevIndex + entries.length)`, never past what that message covered.',
          ],
        },
      ],
    },
    {
      title: 'Surviving failure',
      tasks: [
        {
          slug: 'raft-repair',
          title: 'Repair diverged logs',
          level: 'Hard',
          minutes: 45,
          summary: 'After partitions and leader changes, make every log match the new leader without losing a committed entry.',
          concepts: ['log matching', 'election restriction', 'conflict resolution'],
          workload: raftKv,
          tests: [
            { name: 'Leader cut off', blurb: 'The leader and one follower are partitioned away with writes that can never commit; the other three carry on.', nodeCount: 5, duration: 11500, params: { scenario: 'cut-leader' } },
            { name: 'Stale follower returns', blurb: 'A follower misses 3.5s of writes, then comes back with a higher term and tries to get elected.', nodeCount: 3, duration: 12000, params: { scenario: 'stale-follower' } },
            { name: 'Successive leaders cut off', blurb: 'The leader is cut off with uncommitted entries; as it rejoins, its successor is cut off too, so a third leader must repair both. Links are slow (5–40ms).', nodeCount: 5, duration: 14500, latency: [5, 40], params: { scenario: 'churn' } },
            { name: 'Delayed message', blurb: 'An old message from the leader reaches a follower after newer ones; then the leader is cut off.', nodeCount: 3, duration: 12000, params: { scenario: 'reorder' } },
            { name: 'Figure 8', blurb: 'The sequence from Figure 8 of the Raft paper: an old-term entry reaches a majority, then a node with a newer term takes over.', nodeCount: 5, duration: 14000, params: { scenario: 'figure8' } },
          ],
          hints: [
            'Send `prevTerm` along with `prevIndex`. The follower accepts only if it has an entry at `prevIndex` **with that term**. If two logs agree on the term at an index, they agree on everything up to it.',
            'When a follower refuses, the leader steps `nextIndex[peer]` back and tries again, until it finds the last point where the two logs agree.',
            'On the follower, an existing entry whose term differs from the incoming entry at the same index is a leftover from a deposed leader. Delete it and everything after it, then append. **Do not truncate when the entries match**: a late, duplicate message would cut off newer entries the leader already counted (the *Delayed message* test).',
            'The election restriction: `request_vote` carries the candidate\'s last log index and last log term. A voter refuses if its own last term is higher, or the terms are equal and its own log is longer. Now that some nodes can never win, reset the election timer only when you hear from the current leader, grant a vote, or stand yourself; not merely on seeing a higher term, or stale candidates keep the one node that could win from ever standing.',
            'A leader may only count copies towards a commit for entries of **its own term** (the *Figure 8* test). Older entries become committed when a newer entry after them commits. Appending a no-op entry when elected is a fine way to get there quickly.',
          ],
        },
        {
          slug: 'raft-persist',
          title: 'Crash recovery',
          level: 'Hard',
          minutes: 30,
          summary: 'Keep the term, the vote and the log on disk, so a restarted node keeps every promise it made.',
          concepts: ['durability', 'persistent state', 'recovery'],
          workload: raftKv,
          tests: [
            { name: 'Leader crashes and returns', blurb: 'The leader dies at 3s and restarts at 4.5s under a new leader.', nodeCount: 3, duration: 8500, params: { scenario: 'leader-restart', persist: true } },
            { name: 'Whole cluster restarts', blurb: 'Every node crashes at the same moment and restarts half a second later.', nodeCount: 3, duration: 9500, params: { scenario: 'full-restart', persist: true } },
            { name: 'Rolling crashes', blurb: 'One node, then two, then all five crash and restart while clients keep writing.', nodeCount: 5, duration: 13500, params: { scenario: 'rolling', persist: true } },
            { name: 'A vote is a promise', blurb: 'A node votes, crashes the instant its answer is sent, restarts, and is asked again in the same term.', nodeCount: 3, duration: 11000, params: { scenario: 'double-vote', persist: true } },
          ],
          hints: [
            '`node.disk` is a plain object that survives a crash. `node.disk.log ??= []` creates the log on first start and finds it again after a restart.',
            'Three things must be durable: the current term, who you voted for in it, and the log. Use the disk copy of the log directly, and write term and vote to disk every time either changes.',
            'Save before you send. A vote that is granted and then forgotten can be granted again to someone else in the same term.',
            'Do not persist the commit index or the store. After a restart start from `commitIndex = 0` and an empty store; the leader\'s next message tells you how far to apply again.',
            'After the whole cluster restarts, the new leader holds entries from an older term that it may not count as committed by themselves. They commit with the first write of the new term.',
          ],
        },
      ],
    },
  ],
  mistakes: [
    { title: 'Acknowledging on append', symptom: 'A client is told its write succeeded, the leader is cut off, and the write is gone.', fix: 'Reply only when the entry is stored on a majority of the whole cluster and applied.' },
    { title: 'Checking only the length of a follower\'s log', symptom: 'After a leader change two nodes hold different entries at the same index, forever.', fix: 'Send the term of the previous entry too. The follower refuses unless index and term both match, and deletes entries that conflict.' },
    { title: 'Voting for anyone with a newer term', symptom: 'A node that sat out a partition wins the election and overwrites committed entries.', fix: 'A voter refuses any candidate whose log is behind its own: compare last term first, then length.' },
    { title: 'Persisting the vote after replying', symptom: 'A node crashes right after voting, restarts, votes again in the same term, and one term gets two leaders.', fix: 'Term and vote go to disk before the reply is sent.' },
  ],
  questions: [
    { q: 'A leader has stored an entry on a majority. Is it committed?', a: 'Only if the entry is from the leader\'s own term. An entry from an older term can sit on a majority and still be overwritten by a leader elected without it, so it counts as committed only once a current-term entry after it is stored on a majority.' },
    { q: 'Why does the election restriction guarantee that a new leader holds every committed entry?', a: 'A committed entry is on a majority. A candidate needs votes from a majority. The two majorities share a node, and that node refuses any candidate whose log is behind its own.' },
    { q: 'Why is the commit index not written to disk?', a: 'It can be recomputed. A restarted node holds its log; the leader tells it how much is committed, and it applies those entries again from the start.' },
  ],
  reading: [
    { kind: 'Paper', title: 'In Search of an Understandable Consensus Algorithm (Extended Version)', url: 'https://raft.github.io/raft.pdf', note: 'Sections 5.3, 5.4 and Figure 2 are this track; Figure 8 is the old-term commit trap.' },
    { kind: 'Blog', title: 'Students\' Guide to Raft', url: 'https://thesquareplanet.com/blog/students-guide-to-raft/', note: 'Jon Gjengset on the bugs nearly everyone writes when implementing Figure 2.' },
    { kind: 'Paper', title: 'Consensus: Bridging Theory and Practice', url: 'https://github.com/ongardie/dissertation', note: 'Diego Ongaro\'s dissertation: the long version, with client interaction and log compaction.' },
  ],
};
