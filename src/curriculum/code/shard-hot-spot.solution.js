const HOT_LIMIT = 20;  // requests for one key within one window make it hot
const WINDOW = 200;    // ms

// A deterministic string hash (FNV-1a with a final mix). The same string gives
// the same 32-bit number (0 .. 4294967295) on every node, every time.
function hash(str) {
  let h = 2166136261;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  h ^= h >>> 16;
  h = Math.imul(h, 0x85ebca6b);
  h ^= h >>> 13;
  h = Math.imul(h, 0xc2b2ae35);
  h ^= h >>> 16;
  return h >>> 0;
}

const N = node.nodeIds.length;
const ownerOf = (key) => node.nodeIds[hash(key) % N];

// A counter may be split into N parts. Part i of a key lives on the node
// (hash(key) + i) % N, so part 0 is on the owner and the parts of one key
// land on N different nodes. This node keeps its own parts here.
const parts = new Map(); // key -> this node's part of the counter
const partNode = (key, i) => node.nodeIds[(hash(key) + i) % N];
const bump = (key, delta) => parts.set(key, (parts.get(key) ?? 0) + delta);

// Keys known to be hot. Only the owner decides, and a key never cools down,
// so every part that is not on the owner was written after the owner knew.
const hot = new Set();

// The owner counts the requests it serves per key, per window.
let counts = new Map();
node.every(WINDOW, () => {
  counts = new Map();
});
function noticeRequest(key) {
  if (hot.has(key)) return;
  const c = (counts.get(key) ?? 0) + 1;
  counts.set(key, c);
  if (c >= HOT_LIMIT) {
    hot.add(key);
    for (const peer of node.peers) node.send(peer, { type: 'hot', key });
  }
}

node.on('hot', (msg) => {
  hot.add(msg.body.key);
});

// Sum every part: ask each of the other N - 1 nodes for its part.
async function sumParts(key) {
  const got = await Promise.all(
    node.nodeIds.map(async (id) => {
      if (id === node.id) return parts.get(key) ?? 0;
      const res = await node.rpc(id, { type: 'read_part', key });
      return res ? res.value : null;
    }),
  );
  return got.includes(null) ? null : got.reduce((a, b) => a + b, 0);
}

node.on('incr', async (msg) => {
  const { key, delta } = msg.body;
  // A hot key: any part will do, so pick one at random (salting).
  const dest = hot.has(key) ? partNode(key, Math.floor(node.random() * N)) : ownerOf(key);
  if (dest !== node.id) {
    const res = await node.rpc(dest, { type: 'incr_part', key, delta, route: !hot.has(key) });
    if (res) node.reply(msg, { type: 'incr_ok' });
    return;
  }
  if (dest === ownerOf(key)) noticeRequest(key);
  bump(key, delta);
  node.reply(msg, { type: 'incr_ok' });
});

// route: the sender treated the key as cold, so this is the owner, which
// counts the request.
node.on('incr_part', (msg) => {
  const { key, delta, route } = msg.body;
  if (route) noticeRequest(key);
  bump(key, delta);
  node.reply(msg, { type: 'incr_part_ok' });
});

node.on('read_part', (msg) => {
  node.reply(msg, { type: 'read_part_ok', value: parts.get(msg.body.key) ?? 0 });
});

// owner_read: a node that thinks the key is cold asks the owner, and the owner
// knows best: if the key is hot, it collects all the parts itself.
async function readAsOwner(key) {
  noticeRequest(key);
  if (!hot.has(key)) return parts.get(key) ?? 0;
  return sumParts(key);
}

node.on('owner_read', async (msg) => {
  const value = await readAsOwner(msg.body.key);
  if (value !== null) node.reply(msg, { type: 'owner_read_ok', value });
});

node.on('get', async (msg) => {
  const { key } = msg.body;
  let value;
  if (hot.has(key)) value = await sumParts(key);
  else if (ownerOf(key) === node.id) value = await readAsOwner(key);
  else {
    const res = await node.rpc(ownerOf(key), { type: 'owner_read', key });
    value = res ? res.value : null;
  }
  if (value !== null) node.reply(msg, { type: 'get_ok', value });
});
