// SWIM, part 4: suspicion and incarnation numbers.
//
// A failed probe no longer kills a member; it makes it a suspect. A suspect
// that is alive hears the rumour about itself and refutes it by bumping its
// incarnation number and spreading "alive" at the new number. A suspect
// nobody vouches for within SUSPECT_MS is declared dead.
//
// Every update carries (status, incarnation). Of two updates about the same
// member, the one with the higher incarnation wins; at equal incarnations
// dead beats suspect beats alive. That order is all the merge logic needed.
const PERIOD_MS = 200;
const ACK_TIMEOUT_MS = 60;
const INDIRECT_K = 3;
const INDIRECT_TIMEOUT_MS = 120;
const SUSPECT_MS = 2500;
const MAX_SENDS = 3 * Math.ceil(Math.log2(node.nodeIds.length + 1));
const MAX_PIGGYBACK = 8;
const RANK = { alive: 0, suspect: 1, dead: 2 };

// Survives a crash: a restarted node comes back with a higher incarnation
// than any rumour about its previous life.
node.disk.incarnation = (node.disk.incarnation ?? -1) + 1;
let incarnation = node.disk.incarnation;

const members = {}; // id -> { status, inc }
for (const id of node.nodeIds) members[id] = { status: 'alive', inc: 0 };
members[node.id].inc = incarnation;

let buffer = []; // { id, status, inc, sends }

function remember(id, status, inc) {
  buffer = buffer.filter((u) => u.id !== id);
  buffer.push({ id, status, inc, sends: 0 });
}
remember(node.id, 'alive', incarnation);

// Entries about the receiver go first, so a suspect hears about itself.
function piggyback(dest) {
  buffer.sort((a, b) => (a.id === dest ? -1 : 0) - (b.id === dest ? -1 : 0) || a.sends - b.sends);
  const out = buffer.slice(0, MAX_PIGGYBACK);
  for (const u of out) u.sends++;
  buffer = buffer.filter((u) => u.sends < MAX_SENDS);
  return out.map((u) => ({ id: u.id, status: u.status, inc: u.inc }));
}

function apply(u) {
  if (u.id === node.id) {
    // Someone thinks we are suspect or dead: refute with a higher incarnation.
    if (u.status !== 'alive' && u.inc >= incarnation) {
      incarnation = u.inc + 1;
      node.disk.incarnation = incarnation;
      members[node.id] = { status: 'alive', inc: incarnation };
      remember(node.id, 'alive', incarnation);
    }
    return;
  }
  const cur = members[u.id];
  if (!cur || !(u.status in RANK)) return;
  if (u.inc < cur.inc || (u.inc === cur.inc && RANK[u.status] <= RANK[cur.status])) return;
  members[u.id] = { status: u.status, inc: u.inc };
  remember(u.id, u.status, u.inc);
  if (u.status === 'suspect') {
    node.after(SUSPECT_MS, () => {
      const m = members[u.id];
      if (m.status === 'suspect' && m.inc === u.inc) apply({ id: u.id, status: 'dead', inc: u.inc });
    });
  }
}

function learn(updates) {
  for (const u of updates || []) apply(u);
}

function shuffled(list) {
  const out = [...list];
  for (let i = out.length - 1; i > 0; i--) {
    const k = Math.floor(node.random() * (i + 1));
    [out[i], out[k]] = [out[k], out[i]];
  }
  return out;
}

const live = (p) => members[p].status !== 'dead';

let order = [];
function nextTarget() {
  order = order.filter(live);
  if (order.length === 0) order = shuffled(node.peers.filter(live));
  return order.pop();
}

async function ping(target) {
  const ack = await node.rpc(target, { type: 'ping', updates: piggyback(target) }, { timeout: ACK_TIMEOUT_MS });
  if (ack) learn(ack.updates);
  return !!ack;
}

async function probe(target) {
  if (await ping(target)) return true;
  const helpers = shuffled(node.peers.filter((p) => p !== target && live(p))).slice(0, INDIRECT_K);
  const replies = await Promise.all(helpers.map((h) => node.rpc(h, { type: 'ping_req', target, updates: piggyback(h) }, { timeout: INDIRECT_TIMEOUT_MS })));
  for (const r of replies) if (r) learn(r.updates);
  return replies.some((r) => r && r.ok);
}

node.every(PERIOD_MS, async () => {
  const target = nextTarget();
  if (!target) return;
  const inc = members[target].inc;
  if (!(await probe(target))) apply({ id: target, status: 'suspect', inc });
});

node.on('ping', (msg) => {
  learn(msg.body.updates);
  node.reply(msg, { type: 'ack', updates: piggyback(msg.src) });
});

node.on('ping_req', async (msg) => {
  learn(msg.body.updates);
  const ok = await ping(msg.body.target);
  node.reply(msg, { type: 'ping_req_ok', ok, updates: piggyback(msg.src) });
});

node.on('members', (msg) => {
  const view = {};
  for (const id of node.nodeIds) view[id] = members[id].status;
  node.reply(msg, { type: 'members_ok', members: view });
});
