// Durable state. On first start the disk is empty; after a crash it holds
// what was saved. Losing any of these three breaks a promise made earlier.
node.disk.log ??= [];
let term = node.disk.term ?? 0;
let votedFor = node.disk.votedFor ?? null;

// Call after changing term or votedFor, before sending anything.
function save() {
  node.disk.term = term;
  node.disk.votedFor = votedFor;
}

// Everything below is rebuilt after a restart.
let role = 'follower';
let leader = null;
let votes = 0;
let electionTimer = null;

const log = node.disk.log;  // entries { term, key, value }; index 1 is log[0]. Changes are durable at once.
let commitIndex = 0;        // relearned from the leader after a restart
const store = {};           // rebuilt by applying the log again as commitIndex advances
const nextIndex = {};       // leader only: the next index to send to each peer
const matchIndex = {};      // leader only: how much of each peer's log is known to match ours
const waiting = new Map();  // leader only: index -> the client request to answer when it commits

const majority = Math.floor(node.nodeIds.length / 2) + 1;
const lastLogTerm = () => (log.length ? log[log.length - 1].term : 0);

function resetElectionTimer() {
  if (electionTimer) electionTimer.cancel();
  electionTimer = node.after(300 + node.random() * 300, startElection);
}

function adoptTerm(newTerm) {
  const wasLeader = role === 'leader';
  term = newTerm;
  role = 'follower';
  votedFor = null;
  save();
  leader = null;
  // No longer leader: whoever is waiting on us should ask someone else.
  for (const client of waiting.values()) node.reply(client, { type: 'not_leader', leader: null });
  waiting.clear();
  // Hearing of a newer term is not hearing from a leader: keep the election
  // timer running (a deposed leader starts one). Otherwise candidates whose
  // logs are too stale to win keep resetting it, and the node that could win
  // never stands.
  if (wasLeader) resetElectionTimer();
}

function startElection() {
  term += 1;
  role = 'candidate';
  votedFor = node.id;
  save();
  leader = null;
  votes = 1;
  resetElectionTimer();

  const electionTerm = term;
  for (const peer of node.peers) {
    const ask = { type: 'request_vote', term: electionTerm, lastLogIndex: log.length, lastLogTerm: lastLogTerm() };
    node.rpc(peer, ask, { timeout: 200 }).then((res) => {
      if (!res) return;
      if (res.term > term) return adoptTerm(res.term);
      if (role !== 'candidate' || term !== electionTerm || !res.granted) return;
      votes += 1;
      if (votes >= majority) becomeLeader();
    });
  }
}

function becomeLeader() {
  role = 'leader';
  leader = node.id;
  electionTimer.cancel();
  for (const peer of node.peers) {
    nextIndex[peer] = log.length + 1;
    matchIndex[peer] = 0; // nothing is known until the peer says so
  }
  replicate();
}

function replicate() {
  for (const peer of node.peers) sendEntries(peer);
}

function sendEntries(peer) {
  const sentTerm = term;
  const prevIndex = nextIndex[peer] - 1;
  const prevTerm = prevIndex > 0 ? log[prevIndex - 1].term : 0;
  const entries = log.slice(prevIndex);
  node.rpc(peer, { type: 'append_entries', term, prevIndex, prevTerm, entries, commit: commitIndex }, { timeout: 100 }).then((res) => {
    if (!res) return;
    if (res.term > term) return adoptTerm(res.term);
    if (role !== 'leader' || term !== sentTerm) return;
    if (res.success) {
      nextIndex[peer] = Math.max(nextIndex[peer], res.matchIndex + 1);
      matchIndex[peer] = Math.max(matchIndex[peer], res.matchIndex);
      advanceCommit();
    } else {
      // The follower's log differs from ours at or before prevIndex.
      // Step back one entry (or straight to the end of its log) and try again.
      nextIndex[peer] = Math.max(1, Math.min(nextIndex[peer] - 1, res.logLength + 1));
      sendEntries(peer);
    }
  });
}

// Leader: the highest index stored on a majority of the whole cluster is
// committed, but only if that entry is from our own term. An older entry on
// a majority can still be overwritten; it commits along with a newer one.
function advanceCommit() {
  for (let n = log.length; n > commitIndex; n--) {
    if (log[n - 1].term !== term) break;
    let copies = 1; // our own
    for (const peer of node.peers) if (matchIndex[peer] >= n) copies += 1;
    if (copies >= majority) {
      commitTo(n);
      break;
    }
  }
}

// Apply entries in order, up to index. This is the only place the state machine changes.
function commitTo(index) {
  while (commitIndex < index) {
    const entry = log[commitIndex];
    commitIndex += 1;
    store[entry.key] = entry.value;
    const client = waiting.get(commitIndex);
    if (client) {
      waiting.delete(commitIndex);
      node.reply(client, { type: 'put_ok' });
    }
  }
}

node.every(100, () => {
  if (role === 'leader') replicate();
});

node.on('request_vote', (msg) => {
  const b = msg.body;
  if (b.term > term) adoptTerm(b.term);
  // The election restriction: never vote for a log that is behind ours.
  // A later last term wins; with equal last terms the longer log wins.
  const upToDate = b.lastLogTerm > lastLogTerm() || (b.lastLogTerm === lastLogTerm() && b.lastLogIndex >= log.length);
  const granted = b.term === term && upToDate && (votedFor === null || votedFor === msg.src);
  if (granted) {
    votedFor = msg.src;
    save(); // the vote is on disk before the candidate hears about it
    resetElectionTimer();
  }
  node.reply(msg, { type: 'request_vote_ok', term, granted });
});

node.on('append_entries', (msg) => {
  const b = msg.body;
  if (b.term < term) return node.reply(msg, { type: 'append_entries_ok', term, success: false, logLength: log.length });
  if (b.term > term) adoptTerm(b.term);
  role = 'follower';
  leader = msg.src;
  resetElectionTimer();

  // Log matching: accept the entries only if our log is identical to the
  // leader's up to prevIndex. Same index and same term means same history.
  const attaches = b.prevIndex === 0 || (b.prevIndex <= log.length && log[b.prevIndex - 1].term === b.prevTerm);
  if (!attaches) {
    return node.reply(msg, { type: 'append_entries_ok', term, success: false, logLength: log.length });
  }
  b.entries.forEach((entry, i) => {
    const index = b.prevIndex + 1 + i;
    // An entry of ours that conflicts with the leader's was never committed. Drop it and all after it.
    if (index <= log.length && log[index - 1].term !== entry.term) log.length = index - 1;
    if (index > log.length) log.push(entry);
  });
  // The leader's commit index may be ahead of what this message covered.
  const matched = b.prevIndex + b.entries.length;
  commitTo(Math.min(b.commit, matched));
  node.reply(msg, { type: 'append_entries_ok', term, success: true, matchIndex: matched });
});

node.on('put', (msg) => {
  if (role !== 'leader') return node.reply(msg, { type: 'not_leader', leader });
  log.push({ term, key: msg.body.key, value: msg.body.value });
  waiting.set(log.length, msg); // answered in commitTo, not here
  replicate();
});

node.on('get', (msg) => {
  node.reply(msg, { type: 'get_ok', value: store[msg.body.key] ?? null });
});

node.on('log', (msg) => {
  node.reply(msg, { type: 'log_ok', entries: log, commit: commitIndex });
});

node.on('status', (msg) => {
  node.reply(msg, { type: 'status_ok', role, term, leader });
});

resetElectionTimer();
