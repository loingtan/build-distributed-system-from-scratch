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
  // TODO: replace the handlers that change. Calling node.on again for a type
  // replaces the earlier handler.
  //  - The node a client's put arrives at knows the client (msg.src); the
  //    home only learns it if the forwarded message says so.
  //  - Version-1 nodes are still running. They read key and name, ignore
  //    fields they do not know, and have no handler for message types they
  //    do not know.
  //  - A message from a version-1 node has no `by` at all.
}

if (version === 1) runVersion1();
else runVersion2();
