// Direct and indirect probing, from the previous task. Each node finds out
// about a crash only when it probes the crashed node itself, which in a big
// cluster can take many periods, and it only ever says "suspect".
//
// TODO: when a probe fails (direct and indirect), mark the target 'dead'
// and stop probing it. Then spread the news: keep a buffer of recent
// updates { id, status } and attach a few of them ("updates": [...]) to
// every ping, ack, ping_req and ping_req_ok you send. When you receive an
// update you did not know, apply it and buffer it too. Drop an entry after
// it has been sent about 3 * log2(N + 1) times.
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
