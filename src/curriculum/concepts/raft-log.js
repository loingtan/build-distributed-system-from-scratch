export default [
  {
    term: 'Replicated log',
    aliases: ['log replication', 'Raft log'],
    summary: 'An ordered list of commands that every node stores identically, so every copy of the state machine ends the same.',
    detail: `A **replicated log** is the core of Raft and Multi-Paxos. Each entry holds a command and the term in which a leader created it; its index is its position, starting at 1. The leader appends entries and copies them to followers; once an entry is committed, every node applies it, in index order, to its own state machine.

Because the state machine is deterministic, the same log gives the same state everywhere. Example: logs \`[put x=5, put y=1, put x=9]\` on three nodes all end with x = 9, y = 1.

The log is also a recovery mechanism: a node that restarts replays it, and a follower that falls behind is sent the entries it is missing.`,
    related: ['AppendEntries', 'Commit index', 'State machine', 'Log matching'],
  },
  {
    term: 'AppendEntries',
    aliases: ['append_entries', 'AppendEntries RPC'],
    summary: 'The Raft message a leader sends to replicate log entries; with no entries it doubles as the heartbeat.',
    detail: `**AppendEntries** carries the leader's term, \`prevLogIndex\` and \`prevLogTerm\` (the entry just before the new ones), the new entries, and \`leaderCommit\`, the leader's commit index.

The receiver (Figure 2): reply false if the term is older than its own; reply false if it has no entry at \`prevLogIndex\` with term \`prevLogTerm\`; delete any existing entry that conflicts with a new one (same index, different term) and everything after it; append entries not already present; and set its commit index to \`min(leaderCommit, index of last new entry)\`.

Because it is sent on every heartbeat with everything from the follower's \`nextIndex\`, lost messages need no special handling: the next one covers them.`,
    related: ['nextIndex', 'Log matching', 'Conflict resolution', 'Heartbeats'],
  },
  {
    term: 'nextIndex',
    aliases: ['next index'],
    summary: 'For each follower, the index of the next log entry the Raft leader will send it.',
    detail: `A new leader sets \`nextIndex\` for every follower to its own last index + 1, optimistically assuming they are up to date. Each AppendEntries sends entries from \`nextIndex\` on, with \`prevLogIndex = nextIndex − 1\`.

If the follower refuses (its log does not match at \`prevLogIndex\`), the leader lowers \`nextIndex\` and retries until the logs agree; on success it raises it past the entries sent. Replies can arrive out of order, so a success must never move it backwards.

Example: leader log ends at 8; follower has 5 matching entries. nextIndex starts at 9, refusals walk it back to 6 (or straight there if the follower reports its length), then entries 6–8 are sent.`,
    related: ['matchIndex', 'AppendEntries'],
  },
  {
    term: 'matchIndex',
    aliases: ['match index'],
    summary: 'For each follower, the highest log index the Raft leader knows is identical on that follower.',
    detail: `\`matchIndex\` starts at 0 when a node becomes leader and rises only from successful AppendEntries replies. It is pessimistic where \`nextIndex\` is optimistic: usually \`matchIndex = nextIndex − 1\` once a follower has caught up, but not before.

The leader uses it to commit: index N is committed when the leader plus the followers with \`matchIndex ≥ N\` form a majority and the entry at N is from the leader's current term.`,
    related: ['nextIndex', 'Commit index'],
  },
  {
    term: 'Commit index',
    aliases: ['commitIndex', 'committed entry'],
    summary: 'The highest log index known to be committed: safely on a majority and never to be lost or replaced.',
    detail: `In Raft an entry is **committed** once the leader that created it has stored it on a majority of the whole cluster; entries before a committed entry are committed too. The **commit index** is the highest such index a node knows of. Only committed entries are applied to the state machine, and a leader acknowledges a client only after its entry is committed and applied.

A leader may advance its commit index to N only if N is stored on a majority **and** the entry at N has the leader's current term (the Figure 8 rule). Followers learn the commit index from \`leaderCommit\` in AppendEntries, capped at the last entry that message covered.

The commit index is volatile: a restarted node starts at 0 and relearns it from the leader.`,
    related: ['Majority', 'matchIndex', 'Current-term commit rule'],
  },
  {
    term: 'Majority',
    aliases: ['majority quorum'],
    summary: 'More than half of all the nodes in the cluster: ⌊N/2⌋ + 1, such as 3 of 5.',
    detail: `Raft needs a **majority** of the whole configured cluster both to elect a leader and to commit an entry. Any two majorities share a node, so a new leader's voters always include a node holding every committed entry.

The count is always of the full cluster, never of the nodes that happen to answer. With 3 of 5 nodes down, nothing commits until enough return.`,
    related: ['Commit index', 'Election restriction'],
  },
  {
    term: 'State machine',
    aliases: ['replicated state machine', 'state machine replication'],
    summary: 'A deterministic program that applies commands in order; replicas fed the same log reach the same state.',
    detail: `In consensus, the **state machine** is whatever the log drives: a key-value store, a lock service, a configuration database. It must be deterministic: the same commands in the same order must produce the same state and the same outputs, so no wall-clock reads, random numbers or iteration over unordered maps inside it.

**State machine replication** runs a copy on every node and feeds each one the same committed log. The replies to clients are computed by applying the command, so every replica would give the same reply. Everything that affects replies, such as a client session table, must be part of the state machine.`,
    related: ['Replicated log', 'Commit index'],
  },
  {
    term: 'Log matching',
    aliases: ['Log Matching Property', 'consistency check'],
    summary: 'Raft\'s property that two logs with an entry of the same index and term are identical up to that index.',
    detail: `The **Log Matching Property** holds because a leader creates at most one entry per index in its term and never moves entries, and because of the **consistency check**: a follower accepts AppendEntries only if it has an entry at \`prevLogIndex\` whose term is \`prevLogTerm\`. By induction, a follower only ever extends a log that agrees with the leader's.

Its practical consequence is that checking one entry suffices. If a follower has (index 4, term 2) and the leader's entry 4 also has term 2, entries 1–4 agree, and the leader can send 5 onwards. If not, the leader steps back until they agree.`,
    related: ['AppendEntries', 'Conflict resolution', 'nextIndex'],
  },
  {
    term: 'Election restriction',
    aliases: ['up-to-date check', 'at least as up-to-date'],
    summary: 'A Raft voter refuses a candidate whose log is less up to date than its own, so leaders hold all committed entries.',
    detail: `A vote request carries the candidate's last log index and last log term. The voter compares last entries: a later **term** is more up to date; with equal last terms, the **longer** log is. It grants only if the candidate is at least as up to date, and only if it has not voted for someone else this term.

Example: a voter whose log ends (7, t3) refuses a candidate ending (9, t2): the candidate's extra entries came from an older leader. It grants one ending (5, t4).

Since a committed entry is on a majority and a winner needs a majority of votes, some voter has the entry and would refuse a candidate lacking it. This gives **Leader Completeness**: every leader holds every committed entry.`,
    related: ['Log matching', 'Majority', 'Terms'],
  },
  {
    term: 'Conflict resolution',
    aliases: ['log conflict', 'conflicting entries'],
    summary: 'A Raft follower deletes an entry that differs in term from the leader\'s at the same index, and all after it.',
    detail: `After a leader change, a follower may hold entries a deposed leader wrote that the new leader does not have. When an accepted AppendEntries reaches an index where the follower's entry has a **different term**, that entry was never committed: the follower deletes it and every entry after it, then appends the leader's.

Only conflicts trigger deletion. If the existing entry has the **same** term, it is identical (Log Matching) and must be kept, even if the message is shorter than the follower's log; otherwise a delayed duplicate message could erase entries the leader already counted toward a commit.

Leaders never delete from their own logs (Leader Append-Only).`,
    related: ['Log matching', 'AppendEntries'],
  },
  {
    term: 'Current-term commit rule',
    aliases: ['Figure 8', 'commit only current term'],
    summary: 'A Raft leader counts replicas only for entries of its own term; older entries commit along with them.',
    detail: `An entry from an older term can be stored on a majority and still be overwritten: Figure 8 of the Raft paper shows a leader re-replicating its term-2 entry to a majority in term 4, crashing, and a node whose last entry is from term 3 winning and overwriting it.

So a leader sets its commit index to N only if a majority holds N **and** \`log[N].term\` equals its current term. Once such an entry commits, all earlier entries are committed too. New leaders often append a no-op entry immediately so that earlier entries commit without waiting for a client write.`,
    related: ['Commit index', 'Election restriction', 'No-op entry'],
  },
  {
    term: 'Durability',
    summary: 'An acknowledged write, vote or log entry survives a crash because it was stored before the reply.',
    detail: `In Raft, durability applies to promises as well as data: a follower that acknowledged an entry, or a node that granted a vote, must still hold that entry or vote after a restart. Write to stable storage first, reply second.`,
    related: ['Persistent state', 'Recovery'],
  },
  {
    term: 'Persistent state',
    aliases: ['persisted state', 'durable state'],
    summary: 'In Raft: the current term, the vote in that term, and the log, written to disk before replying.',
    detail: `Figure 2 lists exactly three things every Raft node must keep on stable storage, updated before it responds to any message: \`currentTerm\`, \`votedFor\` and \`log[]\`.

- Lose \`currentTerm\` and a node's term goes backwards, letting it accept a stale leader.
- Lose \`votedFor\` and a node that voted, crashed and restarted can vote again in the same term: two leaders.
- Lose log entries and an entry counted toward a commit may vanish from the majority.

Everything else (role, commit index, last applied, the state machine, \`nextIndex\`, \`matchIndex\`) is volatile and rebuilt after a restart.`,
    related: ['Durability', 'Recovery', 'Commit index'],
  },
  {
    term: 'Recovery',
    summary: 'Rebuilding a node\'s working state after a restart from what it saved on disk.',
    detail: `A restarted Raft node loads its term, vote and log, starts as a follower with commit index 0 and an empty state machine, and waits. The leader's next AppendEntries tells it the commit index, and it reapplies its log up to there. With snapshots (next track), it starts from the snapshot instead of from an empty state.`,
    related: ['Persistent state', 'Durability'],
  },
  {
    term: 'Leader Completeness',
    summary: 'Raft\'s guarantee that every committed entry is present in the log of every later leader.',
    detail: `**Leader Completeness** is what makes it safe for a leader to overwrite followers' logs with its own. It follows from the election restriction and the current-term commit rule: a committed entry is on a majority, the winning candidate's voters include a node from that majority, and that node refuses any candidate whose log is less up to date than its own.

It implies **State Machine Safety**: once any node has applied an entry at index i, no node ever applies a different entry at i.`,
    related: ['Election restriction', 'Current-term commit rule', 'Log matching'],
  },
];
