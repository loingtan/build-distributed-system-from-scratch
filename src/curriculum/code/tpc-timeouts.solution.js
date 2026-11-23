// Every node is a bank branch with two accounts.
const balances = { a: 100, b: 100 };
const committed = [];          // ids of the transactions this node has applied
const aborted = new Set();     // ids this node has been told were aborted
const prepared = new Map();    // txn -> this node's moves, voted yes, waiting for the outcome
const locks = new Map();       // account -> the prepared txn that holds it

// ---- coordinator

node.on('transfer', async (msg) => {
  const { txn, moves } = msg.body;
  const participants = [...new Set(moves.map((m) => m.node))];

  // A vote that does not arrive in time counts as no. Before the decision
  // the coordinator is free to give up; nobody has been promised a commit.
  const votes = await Promise.all(
    participants.map((p) => node.rpc(p, { type: 'prepare', txn, moves: moves.filter((m) => m.node === p) }, { timeout: 300 })),
  );
  const commit = votes.every((v) => v && v.vote === 'yes');

  // This line is the decision. From here on it never changes, so the client
  // can be told now, without waiting for a participant that is cut off.
  node.reply(msg, { type: 'transfer_ok', txn, committed: commit });
  for (const p of participants) tell(p, txn, commit);
});

// Deliver the outcome, however long it takes. A prepared participant is
// waiting for exactly this message and cannot do anything else.
async function tell(participant, txn, commit) {
  for (;;) {
    const ack = await node.rpc(participant, { type: commit ? 'commit' : 'abort', txn }, { timeout: 300 });
    if (ack) return;
  }
}

// ---- participant

node.on('prepare', (msg) => {
  const { txn, moves } = msg.body;
  // A prepare that arrives after its own abort must not lock anything.
  const free = !aborted.has(txn) && moves.every((m) => !locks.has(m.account));
  const fits = moves.every((m) => balances[m.account] + m.delta >= 0);
  if (free && fits) {
    prepared.set(txn, moves);
    for (const m of moves) locks.set(m.account, txn);
  }
  node.reply(msg, { type: 'prepare_ok', vote: free && fits ? 'yes' : 'no' });
});

// Note what is missing: a timer. After voting yes this node waits for the
// coordinator, with its accounts locked, for as long as it takes.

function release(txn) {
  const moves = prepared.get(txn);
  if (!moves) return null;
  prepared.delete(txn);
  for (const m of moves) locks.delete(m.account);
  return moves;
}

// The outcome may arrive more than once, because the coordinator retries
// until it sees an acknowledgement. The second time release() finds nothing.
node.on('commit', (msg) => {
  const moves = release(msg.body.txn);
  if (moves) {
    for (const m of moves) balances[m.account] += m.delta;
    committed.push(msg.body.txn);
  }
  node.reply(msg, { type: 'commit_ok' });
});

node.on('abort', (msg) => {
  release(msg.body.txn);
  aborted.add(msg.body.txn);
  node.reply(msg, { type: 'abort_ok' });
});

node.on('status', (msg) => {
  node.reply(msg, { type: 'status_ok', balances, committed });
});
