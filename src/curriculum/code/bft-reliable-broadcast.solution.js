// Bracha's reliable broadcast, tolerating f = 1 Byzantine node out of n = 4.
// One broadcast per sender; the sender of a propose is msg.src.
const F = 1;

const echoed = {};    // sender -> true once we have sent our echo for it
const readied = {};   // sender -> true once we have sent our ready for it
const delivered = {}; // sender -> the value we delivered
const echoes = {};    // sender -> value -> { node: true }
const readies = {};   // sender -> value -> { node: true }
let proposed = false;

function toAll(body) {
  for (const id of node.nodeIds) node.send(id, body);
}

// Record that `from` vouched for (sender, value); return how many distinct
// nodes have vouched for exactly that value.
function add(table, sender, value, from) {
  table[sender] ??= {};
  table[sender][value] ??= {};
  table[sender][value][from] = true;
  return Object.keys(table[sender][value]).length;
}

const valid = (sender, value) => node.nodeIds.includes(sender) && typeof value === 'string';

function sendReady(sender, value) {
  if (readied[sender]) return;
  readied[sender] = true;
  toAll({ type: 'ready', sender, value });
}

node.on('broadcast', (msg) => {
  if (!proposed) {
    proposed = true;
    toAll({ type: 'propose', value: msg.body.value });
  }
  node.reply(msg, { type: 'broadcast_ok' });
});

// Echo the first value a sender proposes to us, and only that one.
node.on('propose', (msg) => {
  const sender = msg.src;
  if (echoed[sender] || typeof msg.body.value !== 'string') return;
  echoed[sender] = true;
  toAll({ type: 'echo', sender, value: msg.body.value });
});

// 2f+1 matching echoes: any two such groups share an honest node, and an
// honest node echoes one value only, so no other value can get there.
node.on('echo', (msg) => {
  const { sender, value } = msg.body;
  if (!valid(sender, value)) return;
  if (add(echoes, sender, value, msg.src) >= 2 * F + 1) sendReady(sender, value);
});

// f+1 matching readies: at least one honest node is ready, so join it
// (this is what makes delivery all-or-nothing). 2f+1: deliver.
node.on('ready', (msg) => {
  const { sender, value } = msg.body;
  if (!valid(sender, value)) return;
  const count = add(readies, sender, value, msg.src);
  if (count >= F + 1) sendReady(sender, value);
  if (count >= 2 * F + 1 && !(sender in delivered)) delivered[sender] = value;
});

node.on('read', (msg) => {
  node.reply(msg, { type: 'read_ok', delivered });
});
