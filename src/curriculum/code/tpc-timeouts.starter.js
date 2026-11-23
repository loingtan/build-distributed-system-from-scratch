// This is two-phase commit for a network that never loses a message.
//
// TODO, coordinator: a participant that cannot be reached must not keep the
// client waiting. Give `prepare` a short timeout and count silence as a no.
// Answer the client as soon as the decision is made, then keep sending the
// outcome to each participant until it acknowledges.
//
// TODO, participant: the outcome can now arrive twice, and an abort can
// arrive for a transaction this node never prepared. Both must be harmless.
// Do not add a timeout that decides on the participant's side.

// Every node is a bank branch with two accounts.
const balances = { a: 100, b: 100 };
const committed = [];          // ids of the transactions this node has applied
const prepared = new Map();    // txn -> this node's moves, voted yes, waiting for the outcome
const locks = new Map();       // account -> the prepared txn that holds it

// ---- coordinator

node.on('transfer', async (msg) => {
  const { txn, moves } = msg.body;
  const participants = [...new Set(moves.map((m) => m.node))];

  const votes = await Promise.all(
    participants.map((p) => node.rpc(p, { type: 'prepare', txn, moves: moves.filter((m) => m.node === p) })),
  );
  const commit = votes.every((v) => v && v.vote === 'yes');

  await Promise.all(participants.map((p) => node.rpc(p, { type: commit ? 'commit' : 'abort', txn })));
  node.reply(msg, { type: 'transfer_ok', txn, committed: commit });
});

// ---- participant

// A yes vote is a promise that the moves can still be applied later. So the
// accounts are locked from the vote until the outcome arrives, and a second
// transaction that wants a locked account is refused straight away. Refusing
// instead of waiting means two transactions can never wait for each other.
node.on('prepare', (msg) => {
  const { txn, moves } = msg.body;
  const free = moves.every((m) => !locks.has(m.account));
  const fits = moves.every((m) => balances[m.account] + m.delta >= 0);
  if (free && fits) {
    prepared.set(txn, moves);
    for (const m of moves) locks.set(m.account, txn);
  }
  node.reply(msg, { type: 'prepare_ok', vote: free && fits ? 'yes' : 'no' });
});

// Forget a prepared transaction and unlock its accounts.
function release(txn) {
  const moves = prepared.get(txn);
  if (!moves) return null;
  prepared.delete(txn);
  for (const m of moves) locks.delete(m.account);
  return moves;
}

node.on('commit', (msg) => {
  const moves = release(msg.body.txn);
  if (moves) {
    for (const m of moves) balances[m.account] += m.delta;
    committed.push(msg.body.txn);
  }
  node.reply(msg, { type: 'commit_ok' });
});

// An abort also reaches nodes that voted no: release() then finds nothing.
node.on('abort', (msg) => {
  release(msg.body.txn);
  node.reply(msg, { type: 'abort_ok' });
});

node.on('status', (msg) => {
  node.reply(msg, { type: 'status_ok', balances, committed });
});
