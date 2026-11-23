// Every get goes straight to the database, which answers in 30-60ms.
// Most clients ask for the same few keys, so most of those reads are repeats.
//
// TODO: keep the values you have read in memory (a Map is enough) and answer
// repeated gets from there. Only a key you have never read needs the database.

node.on('get', async (msg) => {
  const key = msg.body.key;
  const res = await node.rpc('db', { type: 'read', key });
  if (!res || res.type !== 'read_ok') {
    node.reply(msg, { type: 'error', code: 'unavailable' });
    return;
  }
  node.reply(msg, { type: 'get_ok', value: res.value });
});
