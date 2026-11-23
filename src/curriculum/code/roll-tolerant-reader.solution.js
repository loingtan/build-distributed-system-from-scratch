// A user directory spread over the cluster. Each key lives on one node, its
// home; any node accepts a request and forwards it there.
//
// Every node runs this same file. The harness upgrades a node by stopping it,
// setting node.disk.version = 2 and starting it again, one node at a time,
// while clients keep sending requests.
const version = node.disk.version ?? 1;

node.disk.users ??= {};
const users = node.disk.users; // key -> { name }

function home(key) {
  let sum = 0;
  for (let i = 0; i < key.length; i++) sum += key.charCodeAt(i);
  return node.nodeIds[sum % node.nodeIds.length];
}

// ---------------------------------------------------------------- version 1
// Already running in production. Do not change it: a node that has not been
// upgraded yet runs exactly this code, and you cannot edit it there.
function runVersion1() {
  node.on('put', async (msg) => {
    const { key, name } = msg.body;
    const owner = home(key);
    if (owner === node.id) {
      const rec = (users[key] ??= {});
      rec.name = name;
      node.reply(msg, { type: 'put_ok' });
      return;
    }
    const res = await node.rpc(owner, { type: 'fwd_put', key, name }, { timeout: 300 });
    node.reply(msg, res ? { type: 'put_ok' } : { type: 'error', code: 'unavailable' });
  });

  node.on('get', async (msg) => {
    const { key } = msg.body;
    const owner = home(key);
    if (owner === node.id) {
      node.reply(msg, { type: 'get_ok', name: users[key] ? users[key].name : null });
      return;
    }
    const res = await node.rpc(owner, { type: 'fwd_get', key }, { timeout: 300 });
    node.reply(msg, res ? { type: 'get_ok', name: res.name } : { type: 'error', code: 'unavailable' });
  });

  // Version 1 reads only the fields it knows (key, name) and ignores the rest.
  node.on('fwd_put', (msg) => {
    const rec = (users[msg.body.key] ??= {});
    rec.name = msg.body.name;
    node.reply(msg, { type: 'fwd_put_ok' });
  });

  node.on('fwd_get', (msg) => {
    const rec = users[msg.body.key];
    node.reply(msg, { type: 'fwd_get_ok', name: rec ? rec.name : null });
  });
}

// ---------------------------------------------------------------- version 2
// Version 2 records who made each change: get_ok carries `by`, the client id
// whose put wrote the current name, or null when that is unknown.
function runVersion2() {
  runVersion1();

  // The record keeps `name` where version 1 put it and gains `by` beside it.
  // A missing writer is stored as null: unknown, not "same as before".
  function store(key, name, by) {
    const rec = (users[key] ??= {});
    rec.name = name;
    rec.by = by ?? null;
  }

  node.on('put', async (msg) => {
    const { key, name } = msg.body;
    const owner = home(key);
    if (owner === node.id) {
      store(key, name, msg.src);
      node.reply(msg, { type: 'put_ok' });
      return;
    }
    // Same message as version 1, every field it reads unchanged, plus `by`.
    // A version-1 home ignores the extra field.
    const res = await node.rpc(owner, { type: 'fwd_put', key, name, by: msg.src }, { timeout: 300 });
    node.reply(msg, res ? { type: 'put_ok' } : { type: 'error', code: 'unavailable' });
  });

  node.on('get', async (msg) => {
    const { key } = msg.body;
    const owner = home(key);
    if (owner === node.id) {
      const rec = users[key];
      node.reply(msg, { type: 'get_ok', name: rec ? rec.name : null, by: rec ? rec.by ?? null : null });
      return;
    }
    const res = await node.rpc(owner, { type: 'fwd_get', key }, { timeout: 300 });
    // A version-1 home does not send `by`: the writer is unknown.
    node.reply(msg, res ? { type: 'get_ok', name: res.name, by: res.by ?? null } : { type: 'error', code: 'unavailable' });
  });

  // A version-1 forwarder sends no `by`.
  node.on('fwd_put', (msg) => {
    store(msg.body.key, msg.body.name, msg.body.by);
    node.reply(msg, { type: 'fwd_put_ok' });
  });

  // `name` stays where a version-1 forwarder looks for it.
  node.on('fwd_get', (msg) => {
    const rec = users[msg.body.key];
    node.reply(msg, { type: 'fwd_get_ok', name: rec ? rec.name : null, by: rec ? rec.by ?? null : null });
  });
}

if (version === 1) runVersion1();
else runVersion2();
