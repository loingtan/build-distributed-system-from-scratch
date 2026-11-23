// One counter per node, e.g. { n1: 0, n2: 0, n3: 0 }.
const clock = {};
for (const id of node.nodeIds) clock[id] = 0;

node.on('event', (msg) => {
  // TODO: count this event in our own entry
  node.reply(msg, { type: 'event_ok', clock });
});

node.on('notify', (msg) => {
  // TODO: count the event, send { type: 'note', clock } to msg.body.to,
  // and reply { type: 'notify_ok', clock } with the same clock.
});

node.on('note', (msg) => {
  // TODO: merge msg.body.clock into ours (entry by entry, keep the larger),
  // then count the receive as an event of our own.
});
