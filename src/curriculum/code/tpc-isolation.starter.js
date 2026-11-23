// This is two-phase commit without isolation. It is correct as long as
// transactions arrive one at a time.
//
// TODO: between a yes vote and the outcome, nothing stops a second
// transaction from being promised the same money. Make `prepare` reserve
// what it promises (lock the accounts, or set the amount aside), and make
// both `commit` and `abort` give it back.

// Every node is a bank branch with two accounts.
const balances = { a: 100, b: 100 };
const committed = [];          // ids of the transactions this node has applied
const prepared = new Map();    // txn -> this node's moves, voted yes, waiting for the outcome

// ---- coordinator: the node the client happened to ask

node.on('transfer', async (msg) => {
  const { txn, moves } = msg.body;
  const participants = [...new Set(moves.map((m) => m.node))];

  // Phase 1: ask everyone. Nobody changes a balance yet.
  const votes = await Promise.all(
    participants.map((p) => node.rpc(p, { type: 'prepare', txn, moves: moves.filter((m) => m.node === p) })),
  );
  const commit = votes.every((v) => v && v.vote === 'yes');

  // Phase 2: one decision, sent to everyone, including those who voted no.
  await Promise.all(participants.map((p) => node.rpc(p, { type: commit ? 'commit' : 'abort', txn })));
  node.reply(msg, { type: 'transfer_ok', txn, committed: commit });
});

// ---- participant: a node that holds one of the accounts

node.on('prepare', (msg) => {
  const { txn, moves } = msg.body;
  const fits = moves.every((m) => balances[m.account] + m.delta >= 0);
  if (fits) prepared.set(txn, moves);
  node.reply(msg, { type: 'prepare_ok', vote: fits ? 'yes' : 'no' });
});

node.on('commit', (msg) => {
  const moves = prepared.get(msg.body.txn);
  if (moves) {
    for (const m of moves) balances[m.account] += m.delta;
    committed.push(msg.body.txn);
    prepared.delete(msg.body.txn);
  }
  node.reply(msg, { type: 'commit_ok' });
});

node.on('abort', (msg) => {
  prepared.delete(msg.body.txn);
  node.reply(msg, { type: 'abort_ok' });
});

node.on('status', (msg) => {
  node.reply(msg, { type: 'status_ok', balances, committed });
});
