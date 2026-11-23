// Every node is a bank branch with two accounts.
const balances = { a: 100, b: 100 };
const committed = [];   // ids of the transactions this node has applied

// A client sends `transfer` to any node. That node is the coordinator for
// this transaction; the nodes named in the moves are its participants.
// (A node can send messages to itself, so the coordinator may treat itself
// like any other participant.)
node.on('transfer', async (msg) => {
  const { txn, moves } = msg.body;
  const participants = [...new Set(moves.map((m) => m.node))];

  // One phase: tell every participant to apply its moves, and hope.
  // TODO: this is the bug. A participant that can apply does so at once,
  // even when another participant refuses. Split it into two rounds:
  //   1. 'prepare': each participant checks its moves and votes yes or no,
  //      without changing any balance.
  //   2. 'commit' to everyone if every vote was yes, otherwise 'abort'.
  const answers = await Promise.all(
    participants.map((p) => node.rpc(p, { type: 'apply', txn, moves: moves.filter((m) => m.node === p) })),
  );
  const ok = answers.every((a) => a && a.applied);
  node.reply(msg, { type: 'transfer_ok', txn, committed: ok });
});

node.on('apply', (msg) => {
  const { txn, moves } = msg.body;
  const fits = moves.every((m) => balances[m.account] + m.delta >= 0);
  if (fits) {
    for (const m of moves) balances[m.account] += m.delta;
    committed.push(txn);
  }
  node.reply(msg, { type: 'apply_ok', applied: fits });
});

node.on('status', (msg) => {
  node.reply(msg, { type: 'status_ok', balances, committed });
});
