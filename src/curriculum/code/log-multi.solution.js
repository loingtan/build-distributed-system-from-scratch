// All shared state lives in the linearizable store:
//   next/<key>          the next free offset for that key
//   log/<key>/<offset>  one message
//   commit/<key>        the committed offset

// Entries never change once written, so reads of them can be cached forever.
const cache = new Map();

// Claim an offset with compare-and-swap. If another node claims it first the
// cas fails and we try again with the value it left behind.
async function claimOffset(key) {
  for (;;) {
    const current = await node.kv.read('next/' + key);
    const offset = current ?? 0;
    if (await node.kv.cas('next/' + key, current, offset + 1)) return offset;
  }
}

async function entry(key, offset) {
  const id = 'log/' + key + '/' + offset;
  if (cache.has(id)) return cache.get(id);
  const value = await node.kv.read(id);
  if (value !== null) cache.set(id, value);
  return value;
}

node.on('send', async (msg) => {
  const { key, msg: value } = msg.body;
  const offset = await claimOffset(key);
  await node.kv.write('log/' + key + '/' + offset, value);
  cache.set('log/' + key + '/' + offset, value);
  node.reply(msg, { type: 'send_ok', offset });
});

node.on('poll', async (msg) => {
  const msgs = {};
  for (const [key, from] of Object.entries(msg.body.offsets)) {
    msgs[key] = [];
    // Stop at the first gap: an offset may be claimed but not written yet.
    for (let offset = from; ; offset++) {
      const value = await entry(key, offset);
      if (value === null) break;
      msgs[key].push([offset, value]);
    }
  }
  node.reply(msg, { type: 'poll_ok', msgs });
});

node.on('commit_offsets', async (msg) => {
  for (const [key, offset] of Object.entries(msg.body.offsets)) {
    for (;;) {
      const current = await node.kv.read('commit/' + key);
      if (current !== null && current >= offset) break;
      if (await node.kv.cas('commit/' + key, current, offset)) break;
    }
  }
  node.reply(msg, { type: 'commit_offsets_ok' });
});

node.on('list_committed_offsets', async (msg) => {
  const offsets = {};
  for (const key of msg.body.keys) {
    const value = await node.kv.read('commit/' + key);
    if (value !== null) offsets[key] = value;
  }
  node.reply(msg, { type: 'list_committed_offsets_ok', offsets });
});
