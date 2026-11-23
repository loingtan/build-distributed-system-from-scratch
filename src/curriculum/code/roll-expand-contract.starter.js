// A user directory spread over the cluster. Each key lives on one node, its
// home; any node accepts a request and forwards it there.
//
// Every node runs this same file. The harness upgrades a node by stopping it,
// setting node.disk.version = 2 and starting it again, one node at a time,
// while clients keep sending requests. In this task it may also roll a node
// back by restarting it with node.disk.version = 1.
const version = node.disk.version ?? 1;

node.disk.users ??= {};
const users = node.disk.users; // key -> a record; see below

function home(key) {
  let sum = 0;
  for (let i = 0; i < key.length; i++) sum += key.charCodeAt(i);
  return node.nodeIds[sum % node.nodeIds.length];
}

// The request handling is the same in both versions; only the storage
// differs. store.write(key, name) saves a name, store.read(key) returns it
// (or null).
function serve(store) {
  node.on('put', async (msg) => {
    const { key, name } = msg.body;
    const owner = home(key);
    if (owner === node.id) {
      store.write(key, name);
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
      node.reply(msg, { type: 'get_ok', name: store.read(key) });
      return;
    }
    const res = await node.rpc(owner, { type: 'fwd_get', key }, { timeout: 300 });
    node.reply(msg, res ? { type: 'get_ok', name: res.name } : { type: 'error', code: 'unavailable' });
  });

  node.on('fwd_put', (msg) => {
    store.write(msg.body.key, msg.body.name);
    node.reply(msg, { type: 'fwd_put_ok' });
  });

  node.on('fwd_get', (msg) => {
    node.reply(msg, { type: 'fwd_get_ok', name: store.read(msg.body.key) });
  });
}

// ---------------------------------------------------------------- version 1
// Already running in production. Do not change it: a node that has not been
// upgraded, or has been rolled back, runs exactly this code.
//
// A record is { name: "Ada Lovelace" }. Version 1 updates a record in place:
// it sets name and leaves every other field as it was.
const v1Store = {
  write(key, name) {
    const rec = (users[key] ??= {});
    rec.name = name;
  },
  read(key) {
    const rec = users[key];
    return rec ? rec.name : null;
  },
};

function runVersion1() {
  serve(v1Store);
}

// ---------------------------------------------------------------- version 2
// Version 2 keeps first and last names apart. Names are always two words.
//
//   before finalize: { name: "Ada Lovelace", first: "Ada", last: "Lovelace" }
//   after finalize:  { first: "Ada", last: "Lovelace" }
//
// The harness sends { type: "finalize" } to every node once every node runs
// version 2 for good; version 1 never runs again after that. Reply
// { type: "finalize_ok" }.
const v2Store = {
  // TODO: while version 1 may still run (until finalize), a record must stay
  // readable by version 1, and version 1 may change name behind your back
  // after a rollback.
  write(key, name) {
    v1Store.write(key, name);
  },
  read(key) {
    return v1Store.read(key);
  },
};

function runVersion2() {
  serve(v2Store);
  // TODO: a "finalize" handler. Rewrite every record to the final shape, and
  // make sure the node still knows it has finalized after a restart.
}

if (version === 1) runVersion1();
else runVersion2();
