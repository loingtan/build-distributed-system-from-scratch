// A stream-table join. Topic "customers" is a changelog:
//   { offset, kind: 'change', customer, tier, ts }   strictly increasing ts
//   { offset, kind: 'watermark', ts }                 no later change has ts <= this
// Topic "orders" holds { offset, order, customer, amount, ts }.
//
// This version joins each order with the table as it is at the moment the
// order is read (processing time). The customers log lags behind the
// orders, so an order often sees a tier that is too old, and an order read
// late sees one that is too new.
//
// TODO: join each order with the tier its customer had at the order's ts.
// Keep every version of each customer, and hold an order back until the
// customers log has passed its ts.
const table = {}; // customer -> current tier
let customersOffset = 0;
let ordersOffset = 0;

async function emit(body) {
  while (!(await node.rpc('sink', body, { timeout: 300 }))) {}
}

async function read(topic, offset) {
  const res = await node.rpc('log', { type: 'poll', topic, offset, max: 20 }, { timeout: 200 });
  return res && res.type === 'poll_ok' ? res.records.filter((r) => r.offset >= offset) : [];
}

let busy = false;
node.every(25, async () => {
  if (busy) return;
  busy = true;
  try {
    for (const r of await read('customers', customersOffset)) {
      if (r.offset !== customersOffset) continue;
      customersOffset += 1;
      if (r.kind === 'change') table[r.customer] = r.tier;
    }
    for (const o of await read('orders', ordersOffset)) {
      if (o.offset !== ordersOffset) continue;
      ordersOffset += 1;
      await emit({ type: 'enriched', order: o.order, customer: o.customer, amount: o.amount, tier: table[o.customer] ?? null });
    }
  } finally {
    busy = false;
  }
});
