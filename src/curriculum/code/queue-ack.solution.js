// A job is in exactly one place: waiting in ready, or out with a consumer
// in inFlight. It is only forgotten when the consumer acknowledges it.
const VISIBILITY = 500;

const ready = [];            // jobs waiting, oldest first
const inFlight = new Map();  // id -> { job, timer }

node.on('enqueue', (msg) => {
  const { id, payload } = msg.body;
  ready.push({ id, payload });
  node.reply(msg, { type: 'enqueue_ok' });
});

node.on('dequeue', (msg) => {
  if (ready.length === 0) {
    node.reply(msg, { type: 'dequeue_ok', job: null });
    return;
  }
  const job = ready.shift();
  // If the consumer stays silent for VISIBILITY ms, assume it died and put
  // the job back at the front: it is the oldest work there is.
  const timer = node.after(VISIBILITY, () => {
    inFlight.delete(job.id);
    ready.unshift(job);
  });
  inFlight.set(job.id, { job, timer });
  node.reply(msg, { type: 'dequeue_ok', job });
});

node.on('ack', (msg) => {
  const entry = inFlight.get(msg.body.id);
  if (entry) {
    entry.timer.cancel();
    inFlight.delete(msg.body.id);
  }
  node.reply(msg, { type: 'ack_ok' });
});
