// Exactly-once results from at-least-once parts. Topic "payments" holds
// { offset, account, amount }; for each payment the node sends the sink
//   { type: 'emit', id, offset, account, balance }
// and the sink ignores an id it has already stored.
//
// This version keeps the offset and the balances in variables and makes up
// a fresh id for every output. After a crash it starts again from offset 0,
// which the log has deleted by then.
//
// TODO: save the offset and the balances on node.disk, together, and only
// once the outputs they cover have been acknowledged. Give each output an
// id that a replay of the same payment will reproduce.
let offset = 0;
const balances = {};

async function emit(body) {
  // Retry until acknowledged: the sink may be down or slow.
  while (true) {
    const res = await node.rpc('sink', body, { timeout: 400 });
    if (res && res.type === 'emit_ok') return;
  }
}

let busy = false;
node.every(25, async () => {
  if (busy) return;
  busy = true;
  try {
    const res = await node.rpc('log', { type: 'poll', topic: 'payments', offset, max: 20 }, { timeout: 200 });
    if (!res || res.type !== 'poll_ok') return;
    for (const p of res.records) {
      if (p.offset !== offset) continue;
      balances[p.account] = (balances[p.account] ?? 0) + p.amount;
      offset = p.offset + 1;
      await emit({ type: 'emit', id: node.id + '-' + Math.floor(node.random() * 1e9), offset: p.offset, account: p.account, balance: balances[p.account] });
    }
  } finally {
    busy = false;
  }
});
