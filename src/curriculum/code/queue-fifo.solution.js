// The queue: oldest job at the front, newest at the back.
const queue = [];

node.on('enqueue', (msg) => {
  const { id, payload } = msg.body;
  queue.push({ id, payload });
  node.reply(msg, { type: 'enqueue_ok' });
});

// A handler runs to completion before the next message is looked at, so two
// consumers asking at the same moment can never be given the same job.
node.on('dequeue', (msg) => {
  const job = queue.length > 0 ? queue.shift() : null;
  node.reply(msg, { type: 'dequeue_ok', job });
});
