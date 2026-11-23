// This is the leader election solution. Elections already work.
// Your job: make the leader's log appear, in the same order, on every node.
let term = 0;
let role = 'follower';
let votedFor = null;
let leader = null;
let votes = 0;
let electionTimer = null;

const log = [];        // entries { term, value }; index 1 is log[0]
const nextIndex = {};  // leader only: the next index to send to each peer

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
  // TODO: start nextIndex[peer] at log.length + 1 for every peer.
  sendHeartbeats();
}

// TODO: turn the heartbeat into append_entries. For each peer send
//   { type: 'append_entries', term, prevIndex, entries }
// where entries is everything from nextIndex[peer] on and prevIndex is the
// index just before them. An empty entries list is still a heartbeat.
// Use node.rpc so you see the answer: on success move nextIndex[peer]
// forward; on failure move it back and send again.
function sendHeartbeats() {
  for (const peer of node.peers) node.send(peer, { type: 'heartbeat', term });
}

node.every(100, () => {
  if (role === 'leader') sendHeartbeats();
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

// TODO: a follower receiving append_entries does everything the heartbeat
// handler does, and then:
//   - if its log is shorter than prevIndex, it refuses (there would be a hole)
//   - otherwise it stores the entries it does not have yet, at their indexes
//   - it replies with success and how far its log now matches the leader's
node.on('heartbeat', (msg) => {
  if (msg.body.term < term) return;
  if (msg.body.term > term) adoptTerm(msg.body.term);
  role = 'follower';
  leader = msg.src;
  resetElectionTimer();
});

node.on('append', (msg) => {
  // TODO: if this node is not the leader, reply { type: 'not_leader', leader }.
  // Otherwise push { term, value: msg.body.value } onto the log,
  // reply { type: 'append_ok' } and start replicating.
});

node.on('log', (msg) => {
  node.reply(msg, { type: 'log_ok', entries: log });
});

node.on('status', (msg) => {
  node.reply(msg, { type: 'status_ok', role, term, leader });
});

resetElectionTimer();
