// Multi-Paxos: one Paxos instance per log slot. A leader runs phase 1 once,
// for every slot at the same time, then runs only phase 2 for each command.
//
// This starter has a fixed leader, n1, that numbers the commands and tells
// everyone. Nobody accepts or promises anything, so when n1 dies the log
// stops, and nothing records which slots were really chosen.
const N = node.nodeIds.length;
const majority = Math.floor(N / 2) + 1;
const me = node.nodeIds.indexOf(node.id);

// Acceptor. One promise covers every slot.
let promised = 0;
const accepted = {}; // slot -> { ballot, value }

// Learner. log holds the values of slots 1..log.length, all chosen.
const chosen = {}; // slot -> value
const log = [];

// Proposer / leader.
let role = node.id === 'n1' ? 'leader' : 'follower'; // TODO: elect with phase 1
let ballot = 0;        // the ballot this node leads with
let leader = 'n1';     // who this node believes leads
let nextSlot = 1;      // leader only: the next free slot
const waiting = new Map(); // leader only: slot -> the append to answer once it is chosen

// TODO: acceptor handlers.
//   prepare { ballot, from }      -> promise ballot for every slot; reply with
//                                    everything accepted in slots >= from
//   accept  { ballot, slot, value } -> accept unless a higher ballot was promised

// TODO: election. A node that hears nothing from a leader for a random
// 300-600ms runs phase 1 with a higher ballot. With a majority of promises
// it leads: it re-proposes, for every slot mentioned, the accepted value with
// the highest ballot, fills holes with null, and sends heartbeats every 100ms.

function learn(slot, value) {
  if (chosen[slot] !== undefined) return;
  chosen[slot] = value;
  while (chosen[log.length + 1] !== undefined) log.push(chosen[log.length + 1]);
  for (const [s, msg] of waiting) {
    if (s > log.length) continue;
    waiting.delete(s);
    if (log[s - 1] === msg.body.value) node.reply(msg, { type: 'append_ok', slot: s });
    else node.reply(msg, { type: 'not_leader', leader: null });
  }
}

node.on('decide', (msg) => learn(msg.body.slot, msg.body.value));

function propose(slot, value) {
  // TODO: phase 2. Send accept { ballot, slot, value } to every node and
  // count the yeses (yours too); the slot is chosen with a majority.
  learn(slot, value); // WRONG: nobody else stored it
  for (const peer of node.peers) node.send(peer, { type: 'decide', slot, value });
}

node.on('append', (msg) => {
  if (role !== 'leader') return node.reply(msg, { type: 'not_leader', leader });
  const slot = nextSlot++;
  waiting.set(slot, msg);
  propose(slot, msg.body.value);
});

node.on('log', (msg) => node.reply(msg, { type: 'log_ok', entries: log }));
