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
// Version 2 can answer gets from the platform team's new read cache, behind
// the flag cache_reads, which is off until set_flag turns it on:
//
//   -> { type: 'set_flag', flag: 'cache_reads', on: true }   <- { type: 'set_flag_ok' }
//
// With the flag on, every get asks the cache first:
//
//   node.rpc('cache', { type: 'read', key }, { timeout: 100 })
//   <- { type: 'read_ok', name }  or  { type: 'error', code: 'unavailable' }
//
// When the cache fails, answer from the normal path instead. When 5 of the
// node's last 20 cache reads failed, the node turns the flag off by itself.
// Turning the flag on starts a fresh count. Flags live on node.disk.
function runVersion2() {
  runVersion1();
  // TODO: set_flag, and a get handler that uses the cache while the flag is
  // on. Calling node.on again for a type replaces the earlier handler.
}

if (version === 1) runVersion1();
else runVersion2();
