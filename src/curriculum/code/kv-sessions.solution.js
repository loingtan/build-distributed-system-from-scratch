// Durable state: the current term, the vote, and the log.
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

const log = node.disk.log;  // entries { term, op, key, value | from, to, client_id, seq }; index 1 is log[0]
let commitIndex = 0;        // relearned from the leader after a restart
const store = {};           // the state machine: rebuilt by applying the log again
const sessions = {};        // part of the state machine too: client_id -> { seq, reply } of its last write
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
  // These requests are in our log and may still commit under the next
  // leader. "not_leader" would tell the client they did not happen, so say
  // nothing: the client times out and knows the outcome is unknown.
  waiting.clear();
  // Hearing of a newer term is not hearing from a leader: keep the election
  // timer running (a deposed leader starts one).
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
    matchIndex[peer] = 0;
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
      nextIndex[peer] = Math.max(1, Math.min(nextIndex[peer] - 1, res.logLength + 1));
      sendEntries(peer);
    }
  });
}

// Only entries of our own term are counted; older ones commit along with them.
function advanceCommit() {
  for (let n = log.length; n > commitIndex; n--) {
    if (log[n - 1].term !== term) break;
    let copies = 1;
    for (const peer of node.peers) if (matchIndex[peer] >= n) copies += 1;
    if (copies >= majority) {
      commitTo(n);
      break;
    }
  }
}

// The state machine. Every node applies the same entries in the same order,
// so every node computes the same result for each one. The result is the reply.
function apply(entry) {
  const current = store[entry.key] ?? null;
  if (entry.op === 'get') return { type: 'get_ok', value: current };
  if (entry.op !== 'put' && entry.op !== 'cas') return null;

  // Exactly once. The same request can be in the log several times: client
  // retries, duplicated messages, a retry sent to a new leader. Deciding here,
  // when the entry is applied, means every node (and every future leader)
  // makes the same decision, from the same log.
  const last = sessions[entry.client_id];
  if (last && entry.seq <= last.seq) {
    return entry.seq === last.seq ? last.reply : null; // a leftover: the client has moved on
  }
  let reply;
  if (entry.op === 'put') {
    store[entry.key] = entry.value;
    reply = { type: 'put_ok' };
  } else if (current === entry.from) {
    store[entry.key] = entry.to;
    reply = { type: 'cas_ok' };
  } else {
    reply = { type: 'cas_failed' };
  }
  sessions[entry.client_id] = { seq: entry.seq, reply };
  return reply;
}

function commitTo(index) {
  while (commitIndex < index) {
    const entry = log[commitIndex];
    commitIndex += 1;
    const result = apply(entry);
    const client = waiting.get(commitIndex);
    if (client) {
      waiting.delete(commitIndex);
      if (result) node.reply(client, result);
    }
  }
}

node.every(100, () => {
  if (role === 'leader') replicate();
});

node.on('request_vote', (msg) => {
  const b = msg.body;
  if (b.term > term) adoptTerm(b.term);
  const upToDate = b.lastLogTerm > lastLogTerm() || (b.lastLogTerm === lastLogTerm() && b.lastLogIndex >= log.length);
  const granted = b.term === term && upToDate && (votedFor === null || votedFor === msg.src);
  if (granted) {
    votedFor = msg.src;
    save();
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

  const attaches = b.prevIndex === 0 || (b.prevIndex <= log.length && log[b.prevIndex - 1].term === b.prevTerm);
  if (!attaches) {
    return node.reply(msg, { type: 'append_entries_ok', term, success: false, logLength: log.length });
  }
  b.entries.forEach((entry, i) => {
    const index = b.prevIndex + 1 + i;
    if (index <= log.length && log[index - 1].term !== entry.term) log.length = index - 1;
    if (index > log.length) log.push(entry);
  });
  const matched = b.prevIndex + b.entries.length;
  commitTo(Math.min(b.commit, matched));
  node.reply(msg, { type: 'append_entries_ok', term, success: true, matchIndex: matched });
});

// Every client operation, reads included, becomes a log entry. The leader
// answers once that entry is committed and applied. A retry becomes a new
// entry too; apply() recognises it.
function propose(msg, entry) {
  if (role !== 'leader') return node.reply(msg, { type: 'not_leader', leader });
  log.push({ term, ...entry });
  waiting.set(log.length, msg);
  replicate();
}

// The session fields travel with the entry, so the decision can be made at apply time.
const session = (b) => ({ client_id: b.client_id, seq: b.seq });
node.on('put', (msg) => propose(msg, { op: 'put', key: msg.body.key, value: msg.body.value, ...session(msg.body) }));
node.on('get', (msg) => propose(msg, { op: 'get', key: msg.body.key }));
node.on('cas', (msg) => propose(msg, { op: 'cas', key: msg.body.key, from: msg.body.from ?? null, to: msg.body.to, ...session(msg.body) }));

node.on('log', (msg) => {
  node.reply(msg, { type: 'log_ok', entries: log, commit: commitIndex });
});

node.on('status', (msg) => {
  node.reply(msg, { type: 'status_ok', role, term, leader });
});

resetElectionTimer();
