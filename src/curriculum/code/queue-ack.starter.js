// The FIFO queue from the last task. A dequeued job is forgotten at once,
// so a consumer that crashes halfway through takes the job with it.
//
// TODO: keep a handed-out job until its consumer sends { type: 'ack', id }.
// If no ack arrives within VISIBILITY ms, make the job available again.
const VISIBILITY = 500;

const queue = [];

node.on('enqueue', (msg) => {
  const { id, payload } = msg.body;
  queue.push({ id, payload });
  node.reply(msg, { type: 'enqueue_ok' });
});

node.on('dequeue', (msg) => {
  const job = queue.length > 0 ? queue.shift() : null;
  // TODO: remember the job as "in flight" and start a timer with node.after.
  node.reply(msg, { type: 'dequeue_ok', job });
});

// TODO: node.on('ack', ...) -> forget the job for good, reply { type: 'ack_ok' }
