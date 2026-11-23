// A derived view kept up to date by change data capture. The database's
// changelog is topic "users" on the log service; each record is
//   { offset, key, before, after }   (before null: insert, after null: delete)
//
// The view (rows, counts) and the offset of the next change to apply live
// together on node.disk. They are changed in the same handler run, so a
// crash never separates them: after a restart the node resumes exactly
// where its saved view ends, without skipping or repeating a change.
const disk = node.disk;
disk.offset ??= 0;   // the next change to apply = how many have been applied
disk.rows ??= {};    // user -> city, a copy of the table
disk.counts ??= {};  // city -> number of users there

function apply(change) {
  const { key, before, after } = change;
  if (before !== null) {
    disk.counts[before] -= 1;
    if (disk.counts[before] === 0) delete disk.counts[before];
  }
  if (after !== null) {
    disk.counts[after] = (disk.counts[after] ?? 0) + 1;
    disk.rows[key] = after;
  } else {
    delete disk.rows[key];
  }
}

// One poll at a time: a slow reply must not overlap the next poll, or the
// same changes would arrive twice.
let polling = false;
async function poll() {
  if (polling) return;
  polling = true;
  try {
    const res = await node.rpc('log', { type: 'poll', topic: 'users', offset: disk.offset, max: 20 }, { timeout: 200 });
    if (!res) return; // lost or slow: ask again on the next tick
    if (res.type !== 'poll_ok') {
      node.log('poll failed', res);
      return;
    }
    for (const change of res.records) {
      // Apply strictly in order, each change once.
      if (change.offset !== disk.offset) continue;
      apply(change);
      disk.offset = change.offset + 1;
    }
  } finally {
    polling = false;
  }
}
node.every(25, poll);

node.on('view', (msg) => {
  node.reply(msg, { type: 'view_ok', offset: disk.offset, rows: disk.rows, counts: disk.counts });
});
