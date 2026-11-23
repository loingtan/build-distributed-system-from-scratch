const MAX_KEYS = 40; // a range holding more keys than this is split

const store = new Map(); // the keys this node owns

// The range table, the same on every node once the splits have been announced:
// [[from, owner], ...] sorted by from. A range runs from its from up to the
// next entry's from; the first starts at "" and the last runs to the end.
// At first one range holds everything, owned by the first node.
const table = [['', node.nodeIds[0]]];

// The range holding key: the last entry whose from <= key (binary search).
function rangeIndex(key) {
  let lo = 0;
  let hi = table.length - 1;
  while (lo < hi) {
    const mid = (lo + hi + 1) >> 1;
    if (table[mid][0] <= key) lo = mid;
    else hi = mid - 1;
  }
  return lo;
}
const ownerOf = (key) => table[rangeIndex(key)][1];
const rangeEnd = (i) => (i + 1 < table.length ? table[i + 1][0] : null);
const inRange = (key, from, to) => key >= from && (to === null || key < to);

// A split adds one boundary: from `from` up to the next boundary now belongs
// to `owner`. Boundaries are only ever added, never moved, so it does not
// matter in which order a node hears about two splits.
function addBoundary(from, owner) {
  const i = rangeIndex(from);
  if (table[i][0] === from) return;
  table.splice(i + 1, 0, [from, owner]);
}

// Who gets the upper half: the node that owns the fewest ranges.
function leastLoaded() {
  const count = new Map(node.nodeIds.map((id) => [id, 0]));
  for (const [, owner] of table) count.set(owner, count.get(owner) + 1);
  return node.nodeIds.reduce((best, id) => (count.get(id) < count.get(best) ? id : best));
}

// Called by the owner after a put. If the range is now too big, cut it at its
// middle key, hand the upper half to another node, then tell everyone.
async function maybeSplit(key) {
  const i = rangeIndex(key);
  const from = table[i][0];
  const to = rangeEnd(i);
  const keys = [...store.keys()].filter((k) => inRange(k, from, to)).sort();
  if (keys.length <= MAX_KEYS) return;
  const at = keys[keys.length >> 1];
  const owner = leastLoaded();
  addBoundary(at, owner);
  if (owner !== node.id) {
    const entries = keys.filter((k) => k >= at).map((k) => [k, store.get(k)]);
    for (const [k] of entries) store.delete(k);
    await node.rpc(owner, { type: 'take', from: at, entries });
  }
  for (const peer of node.peers) if (peer !== owner) node.send(peer, { type: 'split', from: at, owner });
}

node.on('split', (msg) => addBoundary(msg.body.from, msg.body.owner));

// The new owner learns of the split from the data itself. A key already here
// came from a put after the split, so it is newer.
node.on('take', (msg) => {
  addBoundary(msg.body.from, node.id);
  for (const [k, v] of msg.body.entries) if (!store.has(k)) store.set(k, v);
  node.reply(msg, { type: 'take_ok' });
});

node.on('put', async (msg) => {
  const { key, value } = msg.body;
  const owner = ownerOf(key);
  if (owner !== node.id) {
    const res = await node.rpc(owner, { type: 'put', key, value });
    if (res) node.reply(msg, { type: 'put_ok' });
    return;
  }
  store.set(key, value);
  await maybeSplit(key);
  node.reply(msg, { type: 'put_ok' });
});

node.on('get', async (msg) => {
  const { key } = msg.body;
  const owner = ownerOf(key);
  if (owner === node.id) return node.reply(msg, { type: 'get_ok', value: store.has(key) ? store.get(key) : null });
  const res = await node.rpc(owner, { type: 'get', key });
  if (res) node.reply(msg, { type: 'get_ok', value: res.value });
});

// What this node itself stores in [from, to).
function localScan(from, to) {
  return [...store].filter(([k]) => inRange(k, from, to));
}

node.on('scan_local', (msg) => {
  node.reply(msg, { type: 'scan_local_ok', entries: localScan(msg.body.from, msg.body.to) });
});

// Ask only the owners of the ranges that overlap [from, to): that is what
// keeping keys in order buys over hashing them.
node.on('scan', async (msg) => {
  const { from, to } = msg.body;
  const owners = new Set();
  for (let i = rangeIndex(from); i < table.length && table[i][0] < to; i++) owners.add(table[i][1]);
  const parts = await Promise.all(
    [...owners].map(async (owner) => {
      if (owner === node.id) return localScan(from, to);
      const res = await node.rpc(owner, { type: 'scan_local', from, to });
      return res ? res.entries : null;
    }),
  );
  if (parts.includes(null)) return;
  const entries = parts.flat().sort((a, b) => (a[0] < b[0] ? -1 : 1));
  node.reply(msg, { type: 'scan_ok', entries });
});

node.on('ranges', (msg) => node.reply(msg, { type: 'ranges_ok', ranges: table }));

node.on('local_keys', (msg) => node.reply(msg, { type: 'local_keys_ok', keys: [...store.keys()] }));
