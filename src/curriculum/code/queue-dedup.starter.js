// The at-least-once queue from the last task. Two things are still wrong:
//
// 1. A producer that sees no enqueue_ok sends the same enqueue again (same
//    id). Each copy becomes a job of its own here, even if the first copy
//    was processed long ago.
// 2. An ack only looks in inFlight. A slow consumer may ack after the
//    timeout, when the job is back in ready or out with someone else.
//
// TODO: make enqueue and ack idempotent. You will need to remember every
// id you have ever accepted, and what became of it.
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
