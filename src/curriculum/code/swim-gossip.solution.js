// SWIM, part 3: dissemination by piggybacking.
//
// A node that finds a member dead tells nobody directly. It puts the news in
// a small buffer, and every ping, ack, ping_req and ping_req_ok it sends
// carries the freshest entries of that buffer. Whoever learns something new
// buffers it too, so the news spreads like an epidemic at no extra message
// cost. Each entry rides on a limited number of messages, then is dropped.
const PERIOD_MS = 200;
const ACK_TIMEOUT_MS = 60;
const INDIRECT_K = 3;
const INDIRECT_TIMEOUT_MS = 120;
const MAX_SENDS = 3 * Math.ceil(Math.log2(node.nodeIds.length + 1));
const MAX_PIGGYBACK = 8;

const status = {};
for (const id of node.nodeIds) status[id] = 'alive';

// Updates still being spread: { id, status, sends }.
let buffer = [];

function remember(id, st) {
  buffer = buffer.filter((u) => u.id !== id);
  buffer.push({ id, status: st, sends: 0 });
}

// The entries to attach to one outgoing message: the least-sent first.
function piggyback() {
  buffer.sort((a, b) => a.sends - b.sends);
  const out = buffer.slice(0, MAX_PIGGYBACK);
  for (const u of out) u.sends++;
  buffer = buffer.filter((u) => u.sends < MAX_SENDS);
  return out.map((u) => ({ id: u.id, status: u.status }));
}

// Without incarnation numbers, "dead" is final: it beats "alive".
function learn(updates) {
  for (const u of updates || []) {
    if (u.id === node.id || status[u.id] === undefined) continue;
    if (u.status === 'dead' && status[u.id] !== 'dead') {
      status[u.id] = 'dead';
      remember(u.id, 'dead');
    }
  }
}

function shuffled(list) {
  const out = [...list];
  for (let i = out.length - 1; i > 0; i--) {
    const k = Math.floor(node.random() * (i + 1));
    [out[i], out[k]] = [out[k], out[i]];
  }
  return out;
}

// Dead members are no longer probed.
let order = [];
function nextTarget() {
  order = order.filter((p) => status[p] !== 'dead');
  if (order.length === 0) order = shuffled(node.peers.filter((p) => status[p] !== 'dead'));
  return order.pop();
}

async function ping(target) {
  const ack = await node.rpc(target, { type: 'ping', updates: piggyback() }, { timeout: ACK_TIMEOUT_MS });
  if (ack) learn(ack.updates);
  return !!ack;
}

async function probe(target) {
  if (await ping(target)) return true;
  const helpers = shuffled(node.peers.filter((p) => p !== target && status[p] !== 'dead')).slice(0, INDIRECT_K);
  const replies = await Promise.all(helpers.map((h) => node.rpc(h, { type: 'ping_req', target, updates: piggyback() }, { timeout: INDIRECT_TIMEOUT_MS })));
  for (const r of replies) if (r) learn(r.updates);
  return replies.some((r) => r && r.ok);
}

node.every(PERIOD_MS, async () => {
  const target = nextTarget();
  if (!target) return;
  if (!(await probe(target)) && status[target] !== 'dead') {
    status[target] = 'dead';
    remember(target, 'dead');
  }
});

node.on('ping', (msg) => {
  learn(msg.body.updates);
  node.reply(msg, { type: 'ack', updates: piggyback() });
});

node.on('ping_req', async (msg) => {
  learn(msg.body.updates);
  const ok = await ping(msg.body.target);
  node.reply(msg, { type: 'ping_req_ok', ok, updates: piggyback() });
});

node.on('members', (msg) => {
  node.reply(msg, { type: 'members_ok', members: status });
});
