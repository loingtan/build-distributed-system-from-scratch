// Everything a node must still know after a crash lives on node.disk.
const disk = node.disk;
disk.balances ??= { a: 100, b: 100 };
disk.committed ??= [];   // ids of the transactions this node has applied
disk.aborted ??= {};     // ids this node has been told were aborted
disk.prepared ??= {};    // participant: txn -> its moves, voted yes, outcome unknown
disk.started ??= {};     // coordinator: txn -> participants, no decision yet
disk.decided ??= {};     // coordinator: txn -> { commit, waiting: participants that have not acknowledged }

// Locks are not stored: they follow from the prepared transactions.
const locks = new Map();
for (const txn of Object.keys(disk.prepared)) {
  for (const m of disk.prepared[txn]) locks.set(m.account, txn);
}

// ---- coordinator

node.on('transfer', async (msg) => {
  const { txn, moves } = msg.body;
  const participants = [...new Set(moves.map((m) => m.node))];

  // Written before the first prepare leaves, so that after a crash this
  // node knows whom it may have left prepared.
  disk.started[txn] = participants;

  const votes = await Promise.all(
    participants.map((p) => node.rpc(p, { type: 'prepare', txn, moves: moves.filter((m) => m.node === p) }, { timeout: 300 })),
  );
  const commit = votes.every((v) => v && v.vote === 'yes');

  decide(txn, commit);
  node.reply(msg, { type: 'transfer_ok', txn, committed: commit });
});

// The decision exists once it is on disk, and only then is it sent.
function decide(txn, commit) {
  disk.decided[txn] = { commit, waiting: disk.started[txn] };
  delete disk.started[txn];
  finish(txn);
}

function finish(txn) {
  for (const p of [...disk.decided[txn].waiting]) tell(p, txn);
}

async function tell(participant, txn) {
  const entry = disk.decided[txn];
  for (;;) {
    const ack = await node.rpc(participant, { type: entry.commit ? 'commit' : 'abort', txn }, { timeout: 300 });
    if (ack) break;
  }
  entry.waiting = entry.waiting.filter((p) => p !== participant);
  if (entry.waiting.length === 0) delete disk.decided[txn];
}

// Recovery. A transaction that was started but never decided cannot have
// committed anywhere, so it is safe to abort it now. A decided one is
// simply sent out again to whoever has not acknowledged.
for (const txn of Object.keys(disk.started)) decide(txn, false);
for (const txn of Object.keys(disk.decided)) finish(txn);

// ---- participant

node.on('prepare', (msg) => {
  const { txn, moves } = msg.body;
  const free = !disk.aborted[txn] && moves.every((m) => !locks.has(m.account));
  const fits = moves.every((m) => disk.balances[m.account] + m.delta >= 0);
  if (free && fits) {
    // On disk first, then the vote: a yes must survive a crash.
    disk.prepared[txn] = moves;
    for (const m of moves) locks.set(m.account, txn);
  }
  node.reply(msg, { type: 'prepare_ok', vote: free && fits ? 'yes' : 'no' });
});

function release(txn) {
  const moves = disk.prepared[txn];
  if (!moves) return null;
  delete disk.prepared[txn];
  for (const m of moves) locks.delete(m.account);
  return moves;
}

node.on('commit', (msg) => {
  const moves = release(msg.body.txn);
  if (moves) {
    for (const m of moves) disk.balances[m.account] += m.delta;
    disk.committed.push(msg.body.txn);
  }
  node.reply(msg, { type: 'commit_ok' });
});

node.on('abort', (msg) => {
  release(msg.body.txn);
  disk.aborted[msg.body.txn] = true;
  node.reply(msg, { type: 'abort_ok' });
});

node.on('status', (msg) => {
  node.reply(msg, { type: 'status_ok', balances: disk.balances, committed: disk.committed });
});
