// This is the log replication solution, with the client protocol changed
// from append to put / get. It still answers the client as soon as the
// leader has the entry, and nothing is ever committed or applied.
let term = 0;
let role = 'follower';
let votedFor = null;
let leader = null;
let votes = 0;
let electionTimer = null;

const log = [];        // entries { term, key, value }; index 1 is log[0]
let commitIndex = 0;   // entries 1..commitIndex are committed and applied
const store = {};      // the state machine: key -> value
const nextIndex = {};  // leader only: the next index to send to each peer
// TODO: the leader also needs, for each peer, how much of that peer's log
// is known to match its own (Raft calls it matchIndex), and somewhere to
// keep client requests that are waiting for their entry to commit.

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
  // Assume every follower is up to date; their answers will correct us.
  for (const peer of node.peers) nextIndex[peer] = log.length + 1;
  replicate();
}

function replicate() {
  for (const peer of node.peers) sendEntries(peer);
}

// Send one follower everything from its nextIndex on. With nothing new to
// send this is an empty message, which is the heartbeat.
function sendEntries(peer) {
  const sentTerm = term;
  const prevIndex = nextIndex[peer] - 1;
  const entries = log.slice(prevIndex);
  node.rpc(peer, { type: 'append_entries', term, prevIndex, entries }, { timeout: 100 }).then((res) => {
    if (!res) return; // lost: the next tick sends it again
    if (res.term > term) return adoptTerm(res.term);
    if (role !== 'leader' || term !== sentTerm) return;
    if (res.success) {
      // Replies can arrive out of order; never move backwards on a success.
      nextIndex[peer] = Math.max(nextIndex[peer], res.matchIndex + 1);
      // TODO: remember how far this peer matches, then find the highest index
      // stored on a majority of the whole cluster (count yourself). Everything
      // up to it is committed: apply it and answer the clients that wait.
    } else {
      // The follower has a gap before prevIndex. Go back to what it has.
      nextIndex[peer] = Math.min(nextIndex[peer], res.logLength + 1);
      sendEntries(peer);
    }
  });
}

// TODO: one function that applies entries commitIndex+1 .. index to the
// store, in order, used by leader and followers alike.

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

  // The entries start right after prevIndex. If we do not have that much
  // yet, storing them would leave a hole.
  if (log.length < b.prevIndex) {
    return node.reply(msg, { type: 'append_entries_ok', term, success: false, logLength: log.length });
  }
  // Messages can be duplicated or overtaken: store only what is new.
  b.entries.forEach((entry, i) => {
    const index = b.prevIndex + 1 + i;
    if (index > log.length) log.push(entry);
  });
  // TODO: the leader must also send its commit index, so that followers
  // can apply entries too. Never commit past what this message covered.
  node.reply(msg, { type: 'append_entries_ok', term, success: true, matchIndex: b.prevIndex + b.entries.length });
});

node.on('put', (msg) => {
  if (role !== 'leader') return node.reply(msg, { type: 'not_leader', leader });
  log.push({ term, key: msg.body.key, value: msg.body.value });
  // TODO: too early. Reply only once this entry is committed and applied.
  node.reply(msg, { type: 'put_ok' });
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
