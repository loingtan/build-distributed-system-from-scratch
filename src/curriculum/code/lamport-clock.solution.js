let clock = 0;

node.on('event', (msg) => {
  clock += 1;
  node.reply(msg, { type: 'event_ok', ts: clock });
});

node.on('notify', (msg) => {
  clock += 1;
  node.send(msg.body.to, { type: 'note', ts: clock });
  node.reply(msg, { type: 'notify_ok', ts: clock });
});

// The receive rule: jump past the sender's stamp, so that anything we do
// from now on is stamped later than the event that caused it.
node.on('note', (msg) => {
  clock = Math.max(clock, msg.body.ts) + 1;
});
