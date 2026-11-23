// Durable state: term, vote, the snapshot, and the log entries after it.
node.disk.log ??= [];
node.disk.snap ??= { index: 0, term: 0, store: {}, sessions: {} };
let term = node.disk.term ?? 0;
let votedFor = node.disk.votedFor ?? null;

// Call after changing term or votedFor, before sending anything.
function save() {
  node.disk.term = term;
  node.disk.votedFor = votedFor;
}

const SNAPSHOT_EVERY = 25; // applied entries kept in the log before they are folded into the snapshot
const clone = (v) => JSON.parse(JSON.stringify(v));

// Everything below is rebuilt after a restart, starting from the snapshot.
let role = 'follower';
let leader = null;
let votes = 0;
let electionTimer = null;

// The log no longer starts at index 1. The snapshot stands for entries
// 1..snap.index; log[0] is entry snap.index + 1.
const log = node.disk.log;
const base = () => node.disk.snap.index;
const lastIndex = () => base() + log.length;
const termAt = (i) => (i === base() ? node.disk.snap.term : log[i - base() - 1].term); // base() <= i <= lastIndex()
const entryAt = (i) => log[i - base() - 1];
const lastLogTerm = () => termAt(lastIndex());

let commitIndex = base();                     // the snapshot is committed and applied
let store = clone(node.disk.snap.store);       // the state machine, starting from the snapshot
let sessions = clone(node.disk.snap.sessions); // client_id -> { seq, reply }: part of the state machine
const nextIndex = {};
const matchIndex = {};
const waiting = new Map();  // leader only: index -> the client request to answer when it commits
const reads = [];           // leader only: gets waiting for { confirmed, index }
let termStart = 0;          // leader only: index of this term's no-op entry

const majority = Math.floor(node.nodeIds.length / 2) + 1;

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
  waiting.clear(); // these may still commit: say nothing
  for (const r of reads) node.reply(r.msg, { type: 'not_leader', leader: null });
  reads.length = 0;
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
    const ask = { type: 'request_vote', term: electionTerm, lastLogIndex: lastIndex(), lastLogTerm: lastLogTerm() };
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
    nextIndex[peer] = lastIndex() + 1;
    matchIndex[peer] = 0;
  }
  log.push({ term, op: 'noop' });
  termStart = lastIndex();
  replicate();
}

// Every round is also a leadership check for the reads that arrived before it.
function replicate() {
  const pending = reads.filter((r) => !r.confirmed);
  let acks = 1;
  const ack = () => {
    acks += 1;
    if (acks !== majority) return;
    for (const r of pending) r.confirmed = true;
    serveReads();
  };
  for (const peer of node.peers) sendEntries(peer, pending.length ? ack : null);
}

function sendEntries(peer, onAck) {
  // The entries this follower needs next are gone from our log: send the snapshot.
  if (nextIndex[peer] <= base()) return sendSnapshot(peer, onAck);
  const sentTerm = term;
  const prevIndex = nextIndex[peer] - 1;
  const prevTerm = termAt(prevIndex);
  const entries = log.slice(prevIndex - base());
  node.rpc(peer, { type: 'append_entries', term, prevIndex, prevTerm, entries, commit: commitIndex }, { timeout: 100 }).then((res) => {
    if (!res) return;
    if (res.term > term) return adoptTerm(res.term);
    if (role !== 'leader' || term !== sentTerm) return;
    if (onAck) onAck();
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

function sendSnapshot(peer, onAck) {
  const sentTerm = term;
  const snapshot = node.disk.snap;
  node.rpc(peer, { type: 'install_snapshot', term, snapshot }, { timeout: 100 }).then((res) => {
    if (!res) return;
    if (res.term > term) return adoptTerm(res.term);
    if (role !== 'leader' || term !== sentTerm) return;
    if (onAck) onAck();
    nextIndex[peer] = Math.max(nextIndex[peer], snapshot.index + 1);
    matchIndex[peer] = Math.max(matchIndex[peer], snapshot.index);
    advanceCommit();
  });
}

function advanceCommit() {
  for (let n = lastIndex(); n > commitIndex; n--) {
    if (termAt(n) !== term) break;
    let copies = 1;
    for (const peer of node.peers) if (matchIndex[peer] >= n) copies += 1;
    if (copies >= majority) {
      commitTo(n);
      break;
    }
  }
}

function apply(entry) {
  const current = store[entry.key] ?? null;
  if (entry.op !== 'put' && entry.op !== 'cas') return null; // a no-op
  const last = sessions[entry.client_id];
  if (last && entry.seq <= last.seq) {
    return entry.seq === last.seq ? last.reply : null;
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
    const entry = entryAt(commitIndex + 1);
    commitIndex += 1;
    const result = apply(entry);
    const client = waiting.get(commitIndex);
    if (client) {
      waiting.delete(commitIndex);
      if (result) node.reply(client, result);
    }
  }
  serveReads();
  compact();
}

// Fold applied entries into the snapshot. The snapshot is the state machine
// as it stood after entry commitIndex: the store and the session table (or a
// restarted node would apply an old duplicate again). Only committed entries
// are ever dropped, and every node compacts on its own.
function compact() {
  if (commitIndex - base() < SNAPSHOT_EVERY) return;
  const drop = commitIndex - base();
  node.disk.snap = { index: commitIndex, term: termAt(commitIndex), store: clone(store), sessions: clone(sessions) };
  log.splice(0, drop);
}

function serveReads() {
  for (let i = 0; i < reads.length; ) {
    const r = reads[i];
    if (r.confirmed && commitIndex >= r.index) {
      node.reply(r.msg, { type: 'get_ok', value: store[r.msg.body.key] ?? null });
      reads.splice(i, 1);
    } else {
      i += 1;
    }
  }
}

node.every(100, () => {
  if (role === 'leader') replicate();
});

node.on('request_vote', (msg) => {
  const b = msg.body;
  if (b.term > term) adoptTerm(b.term);
  const upToDate = b.lastLogTerm > lastLogTerm() || (b.lastLogTerm === lastLogTerm() && b.lastLogIndex >= lastIndex());
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
  if (b.term < term) return node.reply(msg, { type: 'append_entries_ok', term, success: false, logLength: lastIndex() });
  if (b.term > term) adoptTerm(b.term);
  role = 'follower';
  leader = msg.src;
  resetElectionTimer();

  const matched = b.prevIndex + b.entries.length;
  // Entries up to our snapshot are committed, so they match the leader's.
  if (matched <= base()) return node.reply(msg, { type: 'append_entries_ok', term, success: true, matchIndex: matched });
  let prevIndex = b.prevIndex;
  let prevTerm = b.prevTerm;
  let entries = b.entries;
  if (prevIndex < base()) {
    entries = entries.slice(base() - prevIndex);
    prevIndex = base();
    prevTerm = node.disk.snap.term;
  }
  const attaches = prevIndex <= lastIndex() && termAt(prevIndex) === prevTerm;
  if (!attaches) {
    return node.reply(msg, { type: 'append_entries_ok', term, success: false, logLength: lastIndex() });
  }
  entries.forEach((entry, i) => {
    const index = prevIndex + 1 + i;
    if (index <= lastIndex() && termAt(index) !== entry.term) log.length = index - base() - 1;
    if (index > lastIndex()) log.push(entry);
  });
  commitTo(Math.min(b.commit, matched));
  node.reply(msg, { type: 'append_entries_ok', term, success: true, matchIndex: matched });
});

// A follower too far behind gets the leader's snapshot instead of entries.
node.on('install_snapshot', (msg) => {
  const b = msg.body;
  if (b.term < term) return node.reply(msg, { type: 'install_snapshot_ok', term });
  if (b.term > term) adoptTerm(b.term);
  role = 'follower';
  leader = msg.src;
  resetElectionTimer();

  const s = b.snapshot;
  if (s.index > commitIndex) {
    // Keep what follows the snapshot if our log agrees with it there.
    if (s.index <= lastIndex() && termAt(s.index) === s.term) log.splice(0, s.index - base());
    else log.length = 0;
    node.disk.snap = s;
    store = clone(s.store);
    sessions = clone(s.sessions);
    commitIndex = s.index;
  }
  node.reply(msg, { type: 'install_snapshot_ok', term });
});

function propose(msg, entry) {
  if (role !== 'leader') return node.reply(msg, { type: 'not_leader', leader });
  log.push({ term, ...entry });
  waiting.set(lastIndex(), msg);
  replicate();
}

const session = (b) => ({ client_id: b.client_id, seq: b.seq });
node.on('put', (msg) => propose(msg, { op: 'put', key: msg.body.key, value: msg.body.value, ...session(msg.body) }));
node.on('cas', (msg) => propose(msg, { op: 'cas', key: msg.body.key, from: msg.body.from ?? null, to: msg.body.to, ...session(msg.body) }));

node.on('get', (msg) => {
  if (role !== 'leader') return node.reply(msg, { type: 'not_leader', leader });
  reads.push({ msg, index: Math.max(commitIndex, termStart), confirmed: false });
  replicate();
});

node.on('status', (msg) => {
  node.reply(msg, { type: 'status_ok', role, term, leader });
});

resetElectionTimer();
