// Single-decree Paxos. Every node plays all three roles: proposer, acceptor
// and learner.
//
// This starter "decides" the moment a client asks: it takes the client's
// value as chosen and tells everyone. With one proposer that looks fine.
// With two proposers at once, two nodes choose different values.
// Replace it with the two phases of Paxos.
const N = node.nodeIds.length;
const majority = Math.floor(N / 2) + 1;
const me = node.nodeIds.indexOf(node.id);

// Acceptor: the highest ballot promised, and the last proposal accepted.
let promised = 0;
let accepted = null; // { ballot, value }

// Learner: the chosen value, once known.
let chosen = null;

// Proposer.
let myValue = null;  // what a client asked this node to propose
let round = 0;
const waiting = [];  // propose requests to answer once a value is chosen

// Ballots must be unique per node and grow: round * N + index works.
function nextBallot() {
  round += 1;
  return round * N + me;
}

// TODO: acceptor handlers.
//   prepare { ballot }        -> promise not to accept lower ballots; report `accepted`
//   accept  { ballot, value } -> accept unless a higher ballot was promised

function learn(value) {
  if (chosen !== null) return;
  chosen = value;
  for (const msg of waiting) node.reply(msg, { type: 'propose_ok', value: chosen });
  waiting.length = 0;
  // TODO: messages get lost. Keep telling each peer until it confirms.
  for (const peer of node.peers) node.send(peer, { type: 'decided', value: chosen });
}

node.on('decided', (msg) => learn(msg.body.value));

async function propose() {
  // TODO: phase 1 (prepare) to a majority, pick the accepted value with the
  // highest ballot (or myValue if none), phase 2 (accept) to a majority.
  // Retry with a higher ballot, after a random pause, until a value is chosen.
  learn(myValue); // WRONG: nobody else was asked
}

node.on('propose', (msg) => {
  if (chosen !== null) return node.reply(msg, { type: 'propose_ok', value: chosen });
  waiting.push(msg);
  if (myValue === null) myValue = msg.body.value;
  propose();
});

node.on('get', (msg) => node.reply(msg, { type: 'get_ok', value: chosen }));
