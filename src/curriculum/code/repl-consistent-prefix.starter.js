// There is no primary any more: every node accepts writes, applies them and
// sends them to the others. Clients still carry a token (msg.body.token, null
// at first) and send back the last one they were given.
const store = new Map(); // key -> value

// TODO: a single number no longer describes a node's state, because writes
// come from several origins. Name each write by { origin, n } (n counts the
// writes accepted at that origin) and keep applied[origin] for every node.
// That vector makes a good token.

node.on('write', (msg) => {
  const { key, value } = msg.body;
  // TODO: first wait until this node has seen everything the client's token
  // says the client has seen.
  store.set(key, value);
  // TODO: answer with a token.
  node.reply(msg, { type: 'write_ok', token: null });
  // TODO: the write depends on everything this node has applied so far.
  // Send that along (deps), with the write's origin and number.
  for (const peer of node.peers) node.send(peer, { type: 'replicate', key, value });
});

node.on('replicate', (msg) => {
  // TODO: this applies each write the moment it arrives. An answer can arrive
  // over a fast link before its question arrives over a slow one. Hold a
  // write back until its origin's earlier writes and its deps are applied.
  store.set(msg.body.key, msg.body.value);
});

node.on('read', (msg) => {
  // TODO: wait until this node covers msg.body.token, then answer with a token.
  const key = msg.body.key;
  node.reply(msg, { type: 'read_ok', value: store.has(key) ? store.get(key) : null, token: null });
});
