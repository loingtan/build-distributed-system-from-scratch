// Multi-value registers, one per key. Each key holds a list of versions
// { value, vv }. vv is a version vector: for each node, how many of that
// node's writes to this key the version has seen, itself included.
//
// A write replaces every version this replica holds: its vector is their
// merge plus one for this node, so it dominates all of them. Merging two
// replicas keeps every version that no other version dominates. Versions
// written without seeing each other dominate neither way and stay as
// siblings until a later write has seen them all.
const registers = {}; // key -> [{ value, vv }]

function dominates(x, y) {
  // x has seen everything y has, and something more.
  let more = false;
  for (const id of new Set([...Object.keys(x), ...Object.keys(y)])) {
    const a = x[id] || 0;
    const b = y[id] || 0;
    if (a < b) return false;
    if (a > b) more = true;
  }
  return more;
}

const same = (x, y) => !dominates(x, y) && !dominates(y, x) && JSON.stringify(Object.entries(x).sort()) === JSON.stringify(Object.entries(y).sort());

function merge(mine, theirs) {
  const out = [];
  for (const v of [...mine, ...theirs]) {
    if (mine.concat(theirs).some((w) => dominates(w.vv, v.vv))) continue;
    if (out.some((w) => same(w.vv, v.vv))) continue;
    out.push(v);
  }
  return out;
}

node.on('write', (msg) => {
  const key = msg.body.key;
  const vv = {};
  for (const v of registers[key] || []) {
    for (const [id, n] of Object.entries(v.vv)) vv[id] = Math.max(vv[id] || 0, n);
  }
  vv[node.id] = (vv[node.id] || 0) + 1;
  registers[key] = [{ value: msg.body.value, vv }];
  node.reply(msg, { type: 'write_ok' });
});

node.on('read', (msg) => {
  const versions = registers[msg.body.key] || [];
  node.reply(msg, { type: 'read_ok', values: versions.map((v) => v.value) });
});

node.on('merge', (msg) => {
  for (const [key, versions] of Object.entries(msg.body.registers)) {
    registers[key] = merge(registers[key] || [], versions);
  }
});

node.every(100, () => {
  for (const peer of node.peers) node.send(peer, { type: 'merge', registers });
});
