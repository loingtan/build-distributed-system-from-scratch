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
  runVersion1(); // puts and forwarding are unchanged

  node.disk.flags ??= {}; // survives a restart, like the data
  let recent = []; // outcomes of the last 20 cache reads: true = ok

  node.on('set_flag', (msg) => {
    node.disk.flags[msg.body.flag] = !!msg.body.on;
    if (msg.body.on) recent = []; // a fresh start for the guard
    node.reply(msg, { type: 'set_flag_ok' });
  });

  async function fromCache(key) {
    const res = await node.rpc('cache', { type: 'read', key }, { timeout: 100 });
    const ok = !!res && res.type === 'read_ok';
    recent.push(ok);
    if (recent.length > 20) recent.shift();
    if (node.disk.flags.cache_reads && recent.filter((x) => !x).length >= 5) {
      // Automatic rollback: a flag change like any other, so on disk.
      node.disk.flags.cache_reads = false;
      node.log('cache_reads: 5 of the last 20 cache reads failed, turning it off');
    }
    return ok ? res : null;
  }

  node.on('get', async (msg) => {
    const { key } = msg.body;
    // Looked up on every get, so set_flag takes effect at once.
    if (node.disk.flags.cache_reads) {
      const hit = await fromCache(key);
      if (hit) {
        node.reply(msg, { type: 'get_ok', name: hit.name });
        return;
      }
      // The cache failed: fall through. The client never notices.
    }
    const owner = home(key);
    if (owner === node.id) {
      node.reply(msg, { type: 'get_ok', name: users[key] ? users[key].name : null });
      return;
    }
    const res = await node.rpc(owner, { type: 'fwd_get', key }, { timeout: 300 });
    node.reply(msg, res ? { type: 'get_ok', name: res.name } : { type: 'error', code: 'unavailable' });
  });
}

if (version === 1) runVersion1();
else runVersion2();
