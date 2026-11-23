let term = 0;
let role = 'follower';
let votedFor = null;
let leader = null;
let votes = 0;
let electionTimer = null;

const majority = Math.floor(node.nodeIds.length / 2) + 1;

function resetElectionTimer() {
  if (electionTimer) electionTimer.cancel();
  electionTimer = node.after(300 + node.random() * 300, startElection);
}

// The one rule that keeps the cluster consistent: a higher term always wins.
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
  resetElectionTimer(); // if this election stalls, try again later

  const electionTerm = term;
  for (const peer of node.peers) {
    node.rpc(peer, { type: 'request_vote', term: electionTerm }, { timeout: 200 }).then((res) => {
      if (!res) return;
      if (res.term > term) return adoptTerm(res.term);
      // Ignore answers to an election we have already moved on from.
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
  sendHeartbeats();
}

function sendHeartbeats() {
  for (const peer of node.peers) node.send(peer, { type: 'heartbeat', term });
}

node.every(100, () => {
  if (role === 'leader') sendHeartbeats();
});

node.on('request_vote', (msg) => {
  if (msg.body.term > term) adoptTerm(msg.body.term);
  // One vote per term: this is what makes two leaders in a term impossible.
  const granted = msg.body.term === term && (votedFor === null || votedFor === msg.src);
  if (granted) {
    votedFor = msg.src;
    resetElectionTimer();
  }
  node.reply(msg, { type: 'request_vote_ok', term, granted });
});

node.on('heartbeat', (msg) => {
  if (msg.body.term < term) return; // a deposed leader that has not noticed yet
  if (msg.body.term > term) adoptTerm(msg.body.term);
  role = 'follower';
  leader = msg.src;
  resetElectionTimer();
});

node.on('status', (msg) => {
  node.reply(msg, { type: 'status_ok', role, term, leader });
});

resetElectionTimer();
