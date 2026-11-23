const store = new Map(); // key -> { value, version }

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

let epoch = 0;                   // the newest membership this node knows of
let members = [...node.nodeIds]; // the members of that epoch
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

// ---- epochs: adopt a membership only if it is newer than the one we have

let ready = true;  // has this node collected every key it owns in this epoch?
let waiting = [];  // requests held back until it has
let seq = 0;

function wake() {
  const held = waiting;
  waiting = [];
  for (const resume of held) resume();
}

// Every message between nodes carries the sender's epoch and members, so a
// node that missed a set_members catches up from whoever it talks to.
function learn(e, m) {
  if (typeof e !== 'number' || e <= epoch) return; // stale or already known
  epoch = e;
  members = m;
  buildRing();
  ready = false;
  wake(); // held requests re-route under the new members
  collect(e, m);
}

// Before serving in epoch e, ask every other node for its copies of the keys
// this node now owns. A node that answers has moved to epoch e (or later), so
// it will never again accept a write for these keys as their owner. After the
// last answer, this node holds the newest value of every key it owns.
async function collect(e, m) {
  await Promise.all(
    node.peers.map(async (peer) => {
      while (epoch === e) {
        const res = await node.rpc(peer, { type: 'collect', epoch: e, members: m }, { timeout: 400 });
        if (!res) continue;
        if (res.epoch > e) return learn(res.epoch, res.members); // someone knows a newer one
        for (const [key, value, version] of res.entries) merge(key, value, version);
        return;
      }
    }),
  );
  if (epoch === e) {
    ready = true;
    wake();
  }
}

node.on('collect', (msg) => {
  learn(msg.body.epoch, msg.body.members);
  const entries = [];
  if (msg.body.epoch === epoch) {
    for (const [key, e] of store) if (ownerOf(key) === msg.src) entries.push([key, e.value, e.version]);
  }
  node.reply(msg, { type: 'collect_ok', epoch, members, entries });
});

// The newest version wins, whatever order copies arrive in.
function merge(key, value, version) {
  const cur = store.get(key);
  if (!cur || version > cur.version) store.set(key, { value, version });
}

// ---- handoff: move keys this node holds but no longer owns

const busy = new Set(); // owners with a handoff in flight

function handoff() {
  const batches = new Map(); // owner -> [[key, value, version], ...]
  for (const [key, e] of store) {
    const owner = ownerOf(key);
    if (owner === node.id) continue;
    if (!batches.has(owner)) batches.set(owner, []);
    batches.get(owner).push([key, e.value, e.version]);
  }
  for (const [owner, entries] of batches) {
    if (busy.has(owner)) continue;
    busy.add(owner);
    node.rpc(owner, { type: 'handoff', epoch, members, entries }, { timeout: 400 }).then((res) => {
      busy.delete(owner);
      if (!res) return; // the timer tries again
      if (!res.accepted) return learn(res.epoch, res.members);
      for (const [key, value, version] of entries) {
        const cur = store.get(key);
        if (cur && cur.version === version && ownerOf(key) !== node.id) store.delete(key);
      }
    });
  }
}

node.on('handoff', (msg) => {
  learn(msg.body.epoch, msg.body.members);
  // From an older epoch the sender may be wrong about who owns these keys,
  // and this node may already have answered the real owner's collect.
  if (msg.body.epoch < epoch) return node.reply(msg, { type: 'handoff_ok', accepted: false, epoch, members });
  for (const [key, value, version] of msg.body.entries) merge(key, value, version);
  node.reply(msg, { type: 'handoff_ok', accepted: true });
});

node.every(100, handoff);

node.on('set_members', (msg) => {
  learn(msg.body.epoch, msg.body.members);
  node.reply(msg, { type: 'set_members_ok' });
});

node.on('lookup', (msg) => {
  node.reply(msg, { type: 'lookup_ok', owners: msg.body.keys.map(ownerOf) });
});

// ---- routing

// Serve a put or get here if this node owns the key and has collected it;
// wait if it owns the key but has not; otherwise pass it to the owner.
async function serve(type, key, value) {
  for (;;) {
    const owner = ownerOf(key);
    if (owner !== node.id) {
      return node.rpc(owner, { type, key, value, epoch, members }, { timeout: 900 });
    }
    if (!ready) {
      await new Promise((resume) => waiting.push(resume));
      continue; // the members may have changed while we waited
    }
    if (type === 'put') {
      // The epoch goes first: a write in a later epoch beats any earlier one.
      store.set(key, { value, version: epoch * 1000000 + ++seq });
      return { type: 'put_ok' };
    }
    const e = store.get(key);
    return { type: 'get_ok', value: e ? e.value : null };
  }
}

node.on('put', async (msg) => {
  learn(msg.body.epoch, msg.body.members);
  const res = await serve('put', msg.body.key, msg.body.value);
  if (res) node.reply(msg, { type: 'put_ok' });
});

node.on('get', async (msg) => {
  learn(msg.body.epoch, msg.body.members);
  const res = await serve('get', msg.body.key);
  if (res) node.reply(msg, { type: 'get_ok', value: res.value });
});

node.on('local_keys', (msg) => {
  node.reply(msg, { type: 'local_keys_ok', keys: [...store.keys()] });
});

buildRing();
