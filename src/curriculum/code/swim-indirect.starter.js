// Direct probing, from the previous task. One lossy link between two live
// nodes makes them suspect each other, although everyone else can reach both.
//
// TODO: when the direct ping gets no ack, send { type: "ping_req", target }
// to INDIRECT_K other members. Each pings the target itself and replies
// whether it got an ack. Suspect the target only if nobody did.
const PERIOD_MS = 200;
const ACK_TIMEOUT_MS = 60;
const INDIRECT_K = 3;

const status = {};
for (const id of node.nodeIds) status[id] = 'alive';

// Round robin over a shuffled list: random enough that no two nodes probe in
// lockstep, yet every member is probed at least once per pass.
let order = [];
function nextTarget() {
  if (order.length === 0) {
    order = [...node.peers];
    for (let i = order.length - 1; i > 0; i--) {
      const k = Math.floor(node.random() * (i + 1));
      [order[i], order[k]] = [order[k], order[i]];
    }
  }
  return order.pop();
}

node.every(PERIOD_MS, async () => {
  const target = nextTarget();
  const ack = await node.rpc(target, { type: 'ping' }, { timeout: ACK_TIMEOUT_MS });
  // A later successful probe clears the suspicion: nothing is final yet.
  status[target] = ack ? 'alive' : 'suspect';
});

node.on('ping', (msg) => {
  node.reply(msg, { type: 'ack' });
});

node.on('members', (msg) => {
  node.reply(msg, { type: 'members_ok', members: status });
});
