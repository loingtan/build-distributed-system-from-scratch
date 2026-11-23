// Raft leader election. The state every node keeps:
let term = 0;            // the latest term this node has seen
let role = 'follower';   // 'follower' | 'candidate' | 'leader'
let votedFor = null;     // who this node voted for in `term`
let leader = null;       // who this node believes leads `term`
let electionTimer = null;

const majority = Math.floor(node.nodeIds.length / 2) + 1;

// (Re)start the countdown to an election. Call this whenever a valid
// leader is heard from. The timeout is random so nodes rarely tie.
function resetElectionTimer() {
  if (electionTimer) electionTimer.cancel();
  electionTimer = node.after(300 + node.random() * 300, startElection);
}

function startElection() {
  // TODO: term += 1, become a candidate, vote for yourself,
  // restart the timer, then ask every peer:
  //   const res = await node.rpc(peer, { type: 'request_vote', term })
  // With votes from a majority (in the same term), become leader.
}

node.on('request_vote', (msg) => {
  // TODO: if msg.body.term is newer than ours, adopt it and step down.
  // Grant the vote only for the current term, and only once per term.
  // reply { type: 'request_vote_ok', term, granted }
});

node.on('heartbeat', (msg) => {
  // TODO: ignore heartbeats from an older term. Otherwise accept the
  // sender as leader and reset the election timer.
});

// TODO: while leader, send { type: 'heartbeat', term } to every peer
// regularly (node.every), more often than the election timeout.

node.on('status', (msg) => {
  node.reply(msg, { type: 'status_ok', role, term, leader });
});

resetElectionTimer();
