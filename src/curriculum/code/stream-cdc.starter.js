// A derived view kept up to date by change data capture. The database's
// changelog is topic "users" on the log service; each record is
//   { offset, key, before, after }   (before null: insert, after null: delete)
//
// This version keeps everything in variables. It works until the node
// crashes: after a restart it starts again from offset 0, and the log no
// longer has those changes.
//
// TODO: keep the view and the offset on node.disk, and change them together
// so that a crash never leaves one ahead of the other. Also check what
// happens when a poll reply is slow and the next tick polls again.
let offset = 0;     // the next change to apply = how many have been applied
const rows = {};    // user -> city, a copy of the table
const counts = {};  // city -> number of users there

function apply(change) {
  const { key, before, after } = change;
  if (before !== null) {
    counts[before] -= 1;
    if (counts[before] === 0) delete counts[before];
  }
  if (after !== null) {
    counts[after] = (counts[after] ?? 0) + 1;
    rows[key] = after;
  } else {
    delete rows[key];
  }
}

node.every(25, async () => {
  const res = await node.rpc('log', { type: 'poll', topic: 'users', offset, max: 20 }, { timeout: 200 });
  if (!res) return;
  if (res.type !== 'poll_ok') {
    node.log('poll failed', res);
    return;
  }
  for (const change of res.records) {
    apply(change);
    offset = change.offset + 1;
  }
});

node.on('view', (msg) => {
  node.reply(msg, { type: 'view_ok', offset, rows, counts });
});
