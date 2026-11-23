// Cache-aside: look in memory first; on a miss, read the database and keep
// the answer for next time.
const cache = new Map();   // key -> value read from the database

node.on('get', async (msg) => {
  const key = msg.body.key;
  if (cache.has(key)) {
    // A hit: no database call at all.
    node.reply(msg, { type: 'get_ok', value: cache.get(key) });
    return;
  }
  const res = await node.rpc('db', { type: 'read', key });
  if (!res || res.type !== 'read_ok') {
    node.reply(msg, { type: 'error', code: 'unavailable' });
    return;
  }
  cache.set(key, res.value);
  node.reply(msg, { type: 'get_ok', value: res.value });
});
