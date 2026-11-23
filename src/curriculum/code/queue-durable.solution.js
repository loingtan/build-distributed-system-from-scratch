// Everything the broker must not forget lives on node.disk, as plain JSON:
//   disk.jobs   id -> { job, state, deliveries, deadline }
//   disk.ready  ids waiting, oldest first (may hold stale ids)
//   disk.dead   jobs that used up their attempts
// A handler changes the disk first and replies afterwards, so anything a
// client has been told is already safe.
const VISIBILITY = 500;
const MAX_DELIVERIES = 3;

const disk = node.disk;
disk.jobs ??= {};
disk.ready ??= [];
disk.dead ??= [];

// Timers do not survive a crash, so the disk holds the moment each hand-out
// expires and the timer is only a reminder to go and look.
function watch(id, deadline) {
  node.after(Math.max(0, deadline - node.now()), () => {
    const entry = disk.jobs[id];
    // Stale reminder: the job was acknowledged, or handed out again since.
    if (entry.state !== 'inflight' || entry.deadline !== deadline) return;
    if (entry.deliveries >= MAX_DELIVERIES) {
      entry.state = 'dead';
      disk.dead.push(entry.job);
    } else {
      entry.state = 'ready';
      disk.ready.unshift(id);
    }
  });
}

// After a restart: pick up the hand-outs that were running when we died.
for (const id of Object.keys(disk.jobs)) {
  if (disk.jobs[id].state === 'inflight') watch(id, disk.jobs[id].deadline);
}

node.on('enqueue', (msg) => {
  const { id, payload } = msg.body;
  if (!disk.jobs[id]) {
    disk.jobs[id] = { job: { id, payload }, state: 'ready', deliveries: 0, deadline: 0 };
    disk.ready.push(id);
  }
  node.reply(msg, { type: 'enqueue_ok' });
});

node.on('dequeue', (msg) => {
  let entry = null;
  while (disk.ready.length > 0 && !entry) {
    const candidate = disk.jobs[disk.ready.shift()];
    if (candidate.state === 'ready') entry = candidate;
  }
  if (!entry) {
    node.reply(msg, { type: 'dequeue_ok', job: null });
    return;
  }
  entry.state = 'inflight';
  entry.deliveries += 1;
  entry.deadline = node.now() + VISIBILITY;
  watch(entry.job.id, entry.deadline);
  node.reply(msg, { type: 'dequeue_ok', job: entry.job });
});

node.on('ack', (msg) => {
  const entry = disk.jobs[msg.body.id];
  if (entry && (entry.state === 'ready' || entry.state === 'inflight')) entry.state = 'done';
  node.reply(msg, { type: 'ack_ok' });
});

node.on('dead_letters', (msg) => {
  node.reply(msg, { type: 'dead_letters_ok', jobs: disk.dead });
});
