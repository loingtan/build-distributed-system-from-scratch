const store = new Map(); // the keys stored on this node: key -> value
// TODO: store { value, version } instead, so two copies can be compared.

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
      for (const [key, value] of entries) {
        if (store.get(key) === value && ownerOf(key) !== node.id) store.delete(key);
      }
    });
  }
}

node.on('handoff', (msg) => {
  for (const [key, value] of msg.body.entries) {
    // TODO: "a value already here is newer" stops being true once changes
    // overlap: this node may still hold an old copy from an earlier epoch.
    // Keep whichever copy has the higher version instead.
    if (!store.has(key)) store.set(key, value);
  }
  node.reply(msg, { type: 'handoff_ok' });
});

node.every(100, handoff);

// TODO: set_members now carries an epoch, and an older one can arrive after
// a newer one. Ignore it unless msg.body.epoch is greater than the epoch you
// have (but reply set_members_ok either way).
node.on('set_members', (msg) => {
  members = msg.body.members;
  buildRing();
  node.reply(msg, { type: 'set_members_ok' });
  handoff();
});

node.on('lookup', (msg) => {
  node.reply(msg, { type: 'lookup_ok', owners: msg.body.keys.map(ownerOf) });
});

// ---- routing

// No messages are lost in this task, so one attempt with a long timeout is
// enough. (A retry could reach the owner after a newer write and undo it.)
// While two nodes disagree about the members they can pass a request back and
// forth; the hop count stops that turning into an endless loop.
function forward(owner, body, hops) {
  if (hops >= 4) return Promise.resolve(null);
  return node.rpc(owner, { ...body, hops: hops + 1 }, { timeout: 900 });
}

// TODO: a node that has just become the owner of a key answers from whatever
// it has, which may be nothing yet: the old owner's handoff is still on its
// way, or the old owner has not even heard of the change. Before serving in
// a new epoch, get the newest copy of every key you now own, and hold
// requests until you have.
node.on('put', async (msg) => {
  const { key, value } = msg.body;
  const owner = ownerOf(key);
  if (owner === node.id) {
    store.set(key, value);
    return node.reply(msg, { type: 'put_ok' });
  }
  const res = await forward(owner, { type: 'put', key, value }, msg.body.hops || 0);
  if (res) node.reply(msg, { type: 'put_ok' });
});

node.on('get', async (msg) => {
  const { key } = msg.body;
  const owner = ownerOf(key);
  if (owner === node.id) {
    return node.reply(msg, { type: 'get_ok', value: store.has(key) ? store.get(key) : null });
  }
  const res = await forward(owner, { type: 'get', key }, msg.body.hops || 0);
  if (res) node.reply(msg, { type: 'get_ok', value: res.value });
});

node.on('local_keys', (msg) => {
  node.reply(msg, { type: 'local_keys_ok', keys: [...store.keys()] });
});

buildRing();
