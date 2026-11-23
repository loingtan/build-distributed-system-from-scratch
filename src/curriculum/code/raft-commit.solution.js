let term = 0;
let role = 'follower';
let votedFor = null;
let leader = null;
let votes = 0;
let electionTimer = null;

const log = [];             // entries { term, key, value }; index 1 is log[0]
let commitIndex = 0;        // entries 1..commitIndex are committed and applied
const store = {};           // the state machine: key -> value
const nextIndex = {};       // leader only: the next index to send to each peer
const matchIndex = {};      // leader only: how much of each peer's log is known to match ours
const waiting = new Map();  // leader only: index -> the client request to answer when it commits

const majority = Math.floor(node.nodeIds.length / 2) + 1;

function resetElectionTimer() {
  if (electionTimer) electionTimer.cancel();
  electionTimer = node.after(300 + node.random() * 300, startElection);
}

function adoptTerm(newTerm) {
  term = newTerm;
  role = 'follower';
  votedFor = null;
  leader = null;
  // No longer leader: whoever is waiting on us should ask someone else.
  for (const client of waiting.values()) node.reply(client, { type: 'not_leader', leader: null });
  waiting.clear();
  resetElectionTimer();
}

function startElection() {
  term += 1;
  role = 'candidate';
  votedFor = node.id;
  leader = null;
  votes = 1;
  resetElectionTimer();

  const electionTerm = term;
  for (const peer of node.peers) {
    node.rpc(peer, { type: 'request_vote', term: electionTerm }, { timeout: 200 }).then((res) => {
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
  const entries = log.slice(prevIndex);
  node.rpc(peer, { type: 'append_entries', term, prevIndex, entries, commit: commitIndex }, { timeout: 100 }).then((res) => {
    if (!res) return;
    if (res.term > term) return adoptTerm(res.term);
    if (role !== 'leader' || term !== sentTerm) return;
    if (res.success) {
      nextIndex[peer] = Math.max(nextIndex[peer], res.matchIndex + 1);
      matchIndex[peer] = Math.max(matchIndex[peer], res.matchIndex);
      advanceCommit();
    } else {
      nextIndex[peer] = Math.min(nextIndex[peer], res.logLength + 1);
      sendEntries(peer);
    }
  });
}

// Leader: the highest index stored on a majority of the whole cluster is committed.
function advanceCommit() {
  for (let n = log.length; n > commitIndex; n--) {
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
  if (msg.body.term > term) adoptTerm(msg.body.term);
  const granted = msg.body.term === term && (votedFor === null || votedFor === msg.src);
  if (granted) {
    votedFor = msg.src;
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

  if (log.length < b.prevIndex) {
    return node.reply(msg, { type: 'append_entries_ok', term, success: false, logLength: log.length });
  }
  b.entries.forEach((entry, i) => {
    const index = b.prevIndex + 1 + i;
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
