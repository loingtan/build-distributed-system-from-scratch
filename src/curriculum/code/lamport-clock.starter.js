let clock = 0;

// A local event: stamp it and report the stamp.
node.on('event', (msg) => {
  // TODO: advance the clock
  node.reply(msg, { type: 'event_ok', ts: clock });
});

// Send a note to another node: msg.body.to is its id.
node.on('notify', (msg) => {
  // TODO: advance the clock, send { type: 'note', ts } to msg.body.to,
  // and reply { type: 'notify_ok', ts } with the same ts.
});

// A note from another node arrives.
node.on('note', (msg) => {
  // TODO: move the clock past msg.body.ts
});
