// A first attempt: keep a total and tell peers about each delta.
// It passes on a healthy network. Run the other tests to see it break.
let total = 0;

node.on('add', (msg) => {
  total += msg.body.delta;
  for (const peer of node.peers) node.send(peer, { type: 'added', delta: msg.body.delta });
  node.reply(msg, { type: 'add_ok' });
});

node.on('added', (msg) => {
  total += msg.body.delta;
});

node.on('read', (msg) => {
  node.reply(msg, { type: 'read_ok', value: total });
});
