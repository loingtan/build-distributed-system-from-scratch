// Exactly-once results from at-least-once parts. Topic "payments" holds
// { offset, account, amount }; for each payment the node sends the sink
//   { type: 'emit', id, offset, account, balance }
// and the sink ignores an id it has already stored.
//
// Two rules make every payment count exactly once:
//   1. The checkpoint (offset + balances) is one value on node.disk, written
//      in one assignment, and only after the sink has acknowledged every
//      output it covers. A crash rewinds to a consistent point and replays.
//   2. A replayed payment is sent with the same id as the first time (the id
//      comes from the payment's offset), so the sink drops the repeat.
const disk = node.disk;
disk.checkpoint ??= { offset: 0, balances: {} };

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
    const { offset, balances } = disk.checkpoint;
    const res = await node.rpc('log', { type: 'poll', topic: 'payments', offset, max: 20 }, { timeout: 200 });
    if (!res || res.type !== 'poll_ok') return;
    const next = { ...balances };
    let upTo = offset;
    for (const p of res.records) {
      if (p.offset !== upTo) continue;
      next[p.account] = (next[p.account] ?? 0) + p.amount;
      await emit({ type: 'emit', id: 'payment-' + p.offset, offset: p.offset, account: p.account, balance: next[p.account] });
      upTo = p.offset + 1;
    }
    // Every output up to upTo is acknowledged: commit state and offset together.
    if (upTo > offset) disk.checkpoint = { offset: upTo, balances: next };
  } finally {
    busy = false;
  }
});
