const store = new Map(); // the keys stored on this node

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

const POINTS = 200; // virtual nodes per member

let members = [...node.nodeIds]; // the nodes that currently own keys
let ring = [];                   // [{ pos, id }] sorted by pos

// Many points per member. Each member's share of the ring is the sum of many
// small arcs, so the shares even out.
function buildRing() {
  ring = [];
  for (const id of members) {
    for (let i = 0; i < POINTS; i++) ring.push({ pos: hash(id + '#' + i), id });
  }
  ring.sort((a, b) => a.pos - b.pos || (a.id < b.id ? -1 : 1));
}

// The owner of a key is the node at the first point at or after the key's
// hash, going clockwise; past the last point, wrap round to the first.
function ownerOf(key) {
  const h = hash(key);
  let lo = 0;
  let hi = ring.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (ring[mid].pos < h) lo = mid + 1;
    else hi = mid;
  }
  return ring[lo === ring.length ? 0 : lo].id;
}

// ---- handoff: move keys this node holds but no longer owns

const busy = new Set(); // owners with a handoff in flight

function handoff() {
  const batches = new Map(); // new owner -> [[key, value], ...]
  for (const [key, value] of store) {
    const owner = ownerOf(key);
    if (owner === node.id) continue;
    if (!batches.has(owner)) batches.set(owner, []);
    batches.get(owner).push([key, value]);
  }
  for (const [owner, entries] of batches) {
    if (busy.has(owner)) continue;
    busy.add(owner);
    node.rpc(owner, { type: 'handoff', entries }, { timeout: 150 }).then((res) => {
      busy.delete(owner);
      if (!res) return; // lost on the way: the timer below tries again
      // Only now is it safe to forget the keys: the new owner has them.
      for (const [key, value] of entries) {
        if (store.get(key) === value && ownerOf(key) !== node.id) store.delete(key);
      }
    });
  }
}

node.on('handoff', (msg) => {
  for (const [key, value] of msg.body.entries) {
    // A value already here arrived by a put after the change, so it is newer.
    if (!store.has(key)) store.set(key, value);
  }
  node.reply(msg, { type: 'handoff_ok' });
});

node.every(100, handoff);

node.on('set_members', (msg) => {
  members = msg.body.members;
  buildRing();
  node.reply(msg, { type: 'set_members_ok' });
  handoff();
});

node.on('lookup', (msg) => {
  node.reply(msg, { type: 'lookup_ok', owners: msg.body.keys.map(ownerOf) });
});

// ---- routing, with retries: put and get are safe to repeat

async function forward(owner, body) {
  for (let attempt = 0; attempt < 8; attempt++) {
    const res = await node.rpc(owner, body, { timeout: 100 });
    if (res) return res;
  }
  return null;
}

node.on('put', async (msg) => {
  const { key, value } = msg.body;
  const owner = ownerOf(key);
  if (owner === node.id) {
    store.set(key, value);
    return node.reply(msg, { type: 'put_ok' });
  }
  const res = await forward(owner, { type: 'put', key, value });
  if (res) node.reply(msg, { type: 'put_ok' });
});

node.on('get', async (msg) => {
  const { key } = msg.body;
  const owner = ownerOf(key);
  if (owner === node.id) {
    return node.reply(msg, { type: 'get_ok', value: store.has(key) ? store.get(key) : null });
  }
  const res = await forward(owner, { type: 'get', key });
  if (res) node.reply(msg, { type: 'get_ok', value: res.value });
});

node.on('local_keys', (msg) => {
  node.reply(msg, { type: 'local_keys_ok', keys: [...store.keys()] });
});

buildRing();
