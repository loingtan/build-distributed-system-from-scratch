// A service that answers "get" by asking the upstream for the value.
// It works as long as the upstream answers quickly, every time.

node.on('get', async (msg) => {
  // node.rpc waits up to 1000ms by default and resolves to null if no
  // reply came. Pass { timeout: ms } as the third argument to change that.
  const res = await node.rpc('upstream', { type: 'fetch', key: msg.body.key });

  // TODO: the client must hear back within 300ms, whatever the upstream does.
  // If there is no answer in time, reply { type: 'error', code: 'timeout' }.
  node.reply(msg, { type: 'get_ok', value: res.value });
});
