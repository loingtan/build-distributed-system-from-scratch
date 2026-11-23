// SWIM, part 2: indirect probes.
//
// A missing ack may mean the target is down, or only that the link between
// us is bad. Before suspecting, ask K other members to ping the target on
// our behalf. Any one of them getting an ack proves the target is alive.
const PERIOD_MS = 200;
const ACK_TIMEOUT_MS = 60;
const INDIRECT_K = 3;
const INDIRECT_TIMEOUT_MS = 120; // > ACK_TIMEOUT_MS plus two network hops

const status = {};
for (const id of node.nodeIds) status[id] = 'alive';

function shuffled(list) {
  const out = [...list];
  for (let i = out.length - 1; i > 0; i--) {
    const k = Math.floor(node.random() * (i + 1));
    [out[i], out[k]] = [out[k], out[i]];
  }
  return out;
}

let order = [];
function nextTarget() {
  if (order.length === 0) order = shuffled(node.peers);
  return order.pop();
}

async function probe(target) {
  if (await node.rpc(target, { type: 'ping' }, { timeout: ACK_TIMEOUT_MS })) return true;
  const helpers = shuffled(node.peers.filter((p) => p !== target)).slice(0, INDIRECT_K);
  const replies = await Promise.all(helpers.map((h) => node.rpc(h, { type: 'ping_req', target }, { timeout: INDIRECT_TIMEOUT_MS })));
  return replies.some((r) => r && r.ok);
}

node.every(PERIOD_MS, async () => {
  const target = nextTarget();
  status[target] = (await probe(target)) ? 'alive' : 'suspect';
});

node.on('ping', (msg) => {
  node.reply(msg, { type: 'ack' });
});

// Ping the target for someone else and tell them how it went.
node.on('ping_req', async (msg) => {
  const ack = await node.rpc(msg.body.target, { type: 'ping' }, { timeout: ACK_TIMEOUT_MS });
  node.reply(msg, { type: 'ping_req_ok', ok: !!ack });
});

node.on('members', (msg) => {
  node.reply(msg, { type: 'members_ok', members: status });
});
