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

node.on('broadcast', (msg) => {
  if (!proposed) {
    proposed = true;
    toAll({ type: 'propose', value: msg.body.value });
  }
  node.reply(msg, { type: 'broadcast_ok' });
});

node.on('propose', (msg) => {
  // TODO: the sender is msg.src. Echo the first value each sender proposes,
  // once, to every node: { type: 'echo', sender, value }.
});

node.on('echo', (msg) => {
  // TODO: count distinct nodes per (sender, value) with add(echoes, ...).
  // On 2f+1 matching echoes, send { type: 'ready', sender, value } to every
  // node (at most one ready per sender).
});

node.on('ready', (msg) => {
  // TODO: count distinct nodes per (sender, value) with add(readies, ...).
  // On f+1 matching readies, send your own ready if you have not yet.
  // On 2f+1 matching readies, deliver: delivered[sender] = value.
});

node.on('read', (msg) => {
  node.reply(msg, { type: 'read_ok', delivered });
});
