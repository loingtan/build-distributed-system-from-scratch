// Single-decree Paxos. Every node plays all three roles: proposer, acceptor
// and learner. One value is chosen, once, for good.
const N = node.nodeIds.length;
const majority = Math.floor(N / 2) + 1;
const me = node.nodeIds.indexOf(node.id);

// Acceptor: the highest ballot promised, and the last proposal accepted.
let promised = 0;
let accepted = null; // { ballot, value }

// Learner: the chosen value, once known.
let chosen = null;
const informed = new Set(); // peers that confirmed they know it

// Proposer.
let myValue = null;  // what a client asked this node to propose
let round = 0;
let highest = 0;     // the highest ballot seen in any reply
let running = false;
const waiting = [];  // propose requests to answer once a value is chosen

// Ballots are unique per node: round * N + index. A node's next ballot is
// above every ballot it has seen.
function nextBallot() {
  round = Math.max(round, Math.floor(highest / N)) + 1;
  return round * N + me;
}

// ---------------------------------------------------------------- acceptor

function onPrepare(ballot) {
  if (ballot > promised) promised = ballot;
  return { ok: ballot >= promised, promised, accepted, chosen };
}

function onAccept(ballot, value) {
  if (ballot >= promised) {
    promised = ballot;
    accepted = { ballot, value };
    return { ok: true, promised, chosen };
  }
  return { ok: false, promised, chosen };
}

node.on('prepare', (msg) => node.reply(msg, { type: 'prepare_ok', ...onPrepare(msg.body.ballot) }));
node.on('accept', (msg) => node.reply(msg, { type: 'accept_ok', ...onAccept(msg.body.ballot, msg.body.value) }));

// ----------------------------------------------------------------- learner

function learn(value) {
  if (chosen !== null) return;
  chosen = value;
  for (const msg of waiting) node.reply(msg, { type: 'propose_ok', value: chosen });
  waiting.length = 0;
  announce();
}

// Keep telling every peer until it confirms: messages get lost.
function announce() {
  if (chosen === null) return;
  for (const peer of node.peers) {
    if (informed.has(peer)) continue;
    node.rpc(peer, { type: 'decided', value: chosen }, { timeout: 200 }).then((res) => res && informed.add(peer));
  }
}
node.every(250, announce);

node.on('decided', (msg) => {
  learn(msg.body.value);
  node.reply(msg, { type: 'decided_ok' });
});

// ---------------------------------------------------------------- proposer

// Send `body` to every acceptor (this node included) and resolve with the
// positive replies as soon as a majority has said yes, or everyone answered
// or timed out.
function ask(body, local) {
  return new Promise((resolve) => {
    const oks = [];
    let pending = node.peers.length;
    let done = false;
    const see = (res) => {
      if (!res) return;
      if (res.chosen !== null && res.chosen !== undefined) learn(res.chosen);
      if (res.promised > highest) highest = res.promised;
      if (res.ok) oks.push(res);
    };
    const finish = () => {
      if (!done && (oks.length >= majority || pending === 0)) {
        done = true;
        resolve(oks);
      }
    };
    see(local);
    for (const peer of node.peers) {
      node.rpc(peer, body, { timeout: 200 }).then((res) => {
        pending -= 1;
        see(res);
        finish();
      });
    }
    finish();
  });
}

async function propose() {
  if (running) return;
  running = true;
  let attempt = 0;
  while (chosen === null) {
    const ballot = nextBallot();

    // Phase 1: ask a majority to promise to ignore lower ballots, and to
    // report what they have already accepted.
    const promises = await ask({ type: 'prepare', ballot }, onPrepare(ballot));
    if (chosen !== null) break;
    if (promises.length >= majority) {
      // If any acceptor in the majority accepted something, a value may
      // already be chosen: propose the one with the highest ballot.
      let value = myValue;
      let best = 0;
      for (const p of promises) {
        if (p.accepted && p.accepted.ballot > best) {
          best = p.accepted.ballot;
          value = p.accepted.value;
        }
      }

      // Phase 2: ask them to accept (ballot, value).
      const accepts = await ask({ type: 'accept', ballot, value }, onAccept(ballot, value));
      if (chosen !== null) break;
      if (accepts.length >= majority) {
        learn(value);
        break;
      }
    }

    // Someone else holds a higher ballot, or messages were lost. Wait a
    // random, growing time so that duelling proposers stop pre-empting
    // each other.
    attempt += 1;
    await new Promise((r) => node.after(20 + node.random() * 40 * Math.min(attempt, 5), r));
  }
  running = false;
}

// ------------------------------------------------------------------ clients

node.on('propose', (msg) => {
  if (chosen !== null) return node.reply(msg, { type: 'propose_ok', value: chosen });
  waiting.push(msg);
  if (myValue === null) myValue = msg.body.value;
  propose();
});

node.on('get', (msg) => node.reply(msg, { type: 'get_ok', value: chosen }));
