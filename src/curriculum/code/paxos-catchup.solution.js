// Multi-Paxos with catch-up and crash recovery. The acceptor state (the
// promise and every slot's accepted value) is on node.disk, written before
// any reply. What a node has learned is not: a node that is behind, after
// lost messages, a partition or a restart, asks the leader for it.
const N = node.nodeIds.length;
const majority = Math.floor(N / 2) + 1;
const me = node.nodeIds.indexOf(node.id);

// Acceptor. One promise covers every slot. Both survive a crash.
let promised = node.disk.promised ?? 0;
node.disk.accepted ??= {};
const accepted = node.disk.accepted; // slot -> { ballot, value }, durable as soon as it is set

function promise(b) {
  promised = b;
  node.disk.promised = b;
}

// Learner. log holds the values of slots 1..log.length, all chosen.
const chosen = {}; // slot -> value
const log = [];

// Proposer / leader.
let role = 'follower'; // 'follower' | 'candidate' | 'leader'
let ballot = 0;        // the ballot this node leads with
let leader = null;     // who this node believes leads
let round = 0;
let highest = 0;       // the highest ballot seen anywhere
let nextSlot = 1;      // leader only: the next free slot
const waiting = new Map(); // leader only: slot -> the append to answer once it is chosen
let electionTimer = null;

function nextBallot() {
  round = Math.max(round, Math.floor(Math.max(highest, promised) / N)) + 1;
  return round * N + me;
}

function resetElectionTimer() {
  if (electionTimer) electionTimer.cancel();
  electionTimer = node.after(300 + node.random() * 300, campaign);
}

function stepDown() {
  if (role === 'leader') {
    for (const msg of waiting.values()) node.reply(msg, { type: 'not_leader', leader: null });
    waiting.clear();
  }
  role = 'follower';
  resetElectionTimer();
}

// A message carried ballot b from `from`. A higher ballot than ours means
// someone else is (trying to be) leader.
function heardBallot(b, from) {
  if (b > highest) highest = b;
  if (role !== 'follower' && b > ballot && from !== node.id) stepDown();
}

// ---------------------------------------------------------------- acceptor

function onPrepare(b, from) {
  if (b > promised) promise(b);
  const list = [];
  for (const s of Object.keys(accepted)) {
    if (Number(s) >= from) list.push([Number(s), accepted[s].ballot, accepted[s].value]);
  }
  return { ok: b >= promised, promised, accepted: list };
}

function onAccept(b, slot, value) {
  if (b < promised) return { ok: false, promised };
  if (b > promised) promise(b);
  accepted[slot] = { ballot: b, value };
  return { ok: true, promised };
}

node.on('prepare', (msg) => {
  const { ballot: b, from } = msg.body;
  heardBallot(b, msg.src);
  const res = onPrepare(b, from);
  if (res.ok) resetElectionTimer(); // give the candidate time to win
  node.reply(msg, { type: 'prepare_ok', ...res });
});

node.on('accept', (msg) => {
  const { ballot: b, slot, value } = msg.body;
  heardBallot(b, msg.src);
  const res = onAccept(b, slot, value);
  if (res.ok) {
    leader = msg.src;
    resetElectionTimer();
  }
  node.reply(msg, { type: 'accept_ok', ...res });
});

node.on('heartbeat', (msg) => {
  const b = msg.body.ballot;
  heardBallot(b, msg.src);
  if (b >= promised) {
    if (b > promised) promise(b);
    leader = msg.src;
    resetElectionTimer();
    if (msg.body.known > log.length) catchUp(msg.src);
  }
  node.reply(msg, { type: 'heartbeat_ok', ok: b >= promised, promised });
});

// ----------------------------------------------------------------- learner

function learn(slot, value) {
  if (chosen[slot] !== undefined) return;
  chosen[slot] = value;
  while (chosen[log.length + 1] !== undefined) log.push(chosen[log.length + 1]);
  // Answer every waiting client whose slot is now in the log.
  for (const [s, msg] of waiting) {
    if (s > log.length) continue;
    waiting.delete(s);
    if (log[s - 1] === msg.body.value) node.reply(msg, { type: 'append_ok', slot: s });
    else node.reply(msg, { type: 'not_leader', leader: null });
  }
}

node.on('decide', (msg) => learn(msg.body.slot, msg.body.value));

// The leader's heartbeat says how many slots it knows. A node that knows
// fewer asks for the rest; one request at a time, retried on the next
// heartbeat if it is lost.
let fetching = false;
function catchUp(from) {
  if (fetching) return;
  fetching = true;
  node.rpc(from, { type: 'catchup', from: log.length + 1 }, { timeout: 300 }).then((res) => {
    fetching = false;
    if (!res) return;
    res.entries.forEach((value, i) => learn(res.from + i, value));
  });
}

node.on('catchup', (msg) => {
  const from = msg.body.from;
  node.reply(msg, { type: 'catchup_ok', from, entries: log.slice(from - 1) });
});

// ------------------------------------------------------------------ leader

// Send body to every acceptor (this node's answer is `local`) and resolve
// with the positive replies once a majority said yes or all have answered.
function ask(body, local) {
  return new Promise((resolve) => {
    const oks = [];
    let pending = node.peers.length;
    let done = false;
    const see = (res) => {
      if (!res) return;
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
      node.rpc(peer, body, { timeout: 250 }).then((res) => {
        pending -= 1;
        see(res);
        finish();
      });
    }
    finish();
  });
}

// Phase 1, once, for every slot from our first unknown one on.
async function campaign() {
  if (role === 'leader') return;
  role = 'candidate';
  leader = null;
  const b = nextBallot();
  ballot = b;
  resetElectionTimer();
  const from = log.length + 1;
  const promises = await ask({ type: 'prepare', ballot: b, from }, onPrepare(b, from));
  if (role !== 'candidate' || ballot !== b) return;
  if (promises.length < majority || highest > b) {
    role = 'follower';
    return; // the election timer tries again later
  }
  role = 'leader';
  leader = node.id;
  if (electionTimer) electionTimer.cancel();

  // For every slot a promise mentions, the accepted value with the highest
  // ballot may already be chosen: re-propose it. Holes get a no-op (null).
  const best = {};
  let top = log.length;
  for (const p of promises) {
    for (const [s, bb, v] of p.accepted) {
      if (!best[s] || bb > best[s].ballot) best[s] = { ballot: bb, value: v };
      top = Math.max(top, s);
    }
  }
  for (const s of Object.keys(chosen)) top = Math.max(top, Number(s));
  nextSlot = top + 1;
  heartbeat();
  for (let s = from; s <= top; s++) {
    const value = chosen[s] !== undefined ? chosen[s] : best[s] ? best[s].value : null;
    propose(s, value);
  }
}

// Phase 2 for one slot, retried while we lead with the same ballot.
async function propose(slot, value) {
  const b = ballot;
  while (role === 'leader' && ballot === b) {
    const oks = await ask({ type: 'accept', ballot: b, slot, value }, onAccept(b, slot, value));
    if (role !== 'leader' || ballot !== b) return;
    if (oks.length >= majority) {
      learn(slot, value);
      for (const peer of node.peers) node.send(peer, { type: 'decide', slot, value });
      return;
    }
    if (highest > b) return stepDown();
    await new Promise((r) => node.after(50, r));
  }
}

function heartbeat() {
  if (role !== 'leader') return;
  const b = ballot;
  for (const peer of node.peers) {
    node.rpc(peer, { type: 'heartbeat', ballot: b, known: log.length }, { timeout: 250 }).then((res) => {
      if (res && !res.ok) heardBallot(res.promised, peer);
    });
  }
}
node.every(100, heartbeat);

// ------------------------------------------------------------------ clients

node.on('append', (msg) => {
  if (role !== 'leader') return node.reply(msg, { type: 'not_leader', leader });
  const slot = nextSlot++;
  waiting.set(slot, msg);
  propose(slot, msg.body.value);
});

node.on('log', (msg) => node.reply(msg, { type: 'log_ok', entries: log }));

resetElectionTimer();
