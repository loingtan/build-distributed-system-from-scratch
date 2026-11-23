// A stream-table join in event time. Topic "customers" is a changelog:
//   { offset, kind: 'change', customer, tier, ts }   strictly increasing ts
//   { offset, kind: 'watermark', ts }                 no later change has ts <= this
// Topic "orders" holds { offset, order, customer, amount, ts }.
//
// Each order is joined with the customer's tier as it was at the order's
// ts. That needs two things: every version of every customer (not just the
// latest), and patience: an order waits until the customers log has passed
// its ts, because the changelog lags behind the orders.
const versions = {};       // customer -> [{ ts, tier }], oldest first
let tableTime = -Infinity; // every change with ts <= tableTime has been read
const waiting = [];        // orders read but not yet joinable
let customersOffset = 0;
let ordersOffset = 0;

function tierAt(customer, ts) {
  let tier = null;
  for (const v of versions[customer] ?? []) if (v.ts <= ts) tier = v.tier;
  return tier;
}

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
      if (r.kind === 'change') {
        (versions[r.customer] ??= []).push({ ts: r.ts, tier: r.tier });
      }
      // Changes come in ts order, so a change at ts also means nothing
      // earlier is still to come; a watermark promises the same.
      tableTime = Math.max(tableTime, r.ts);
    }
    for (const r of await read('orders', ordersOffset)) {
      if (r.offset !== ordersOffset) continue;
      ordersOffset += 1;
      waiting.push(r);
    }
    // Join every order the table has caught up with.
    const ready = waiting.filter((o) => o.ts <= tableTime);
    for (const o of ready) {
      await emit({ type: 'enriched', order: o.order, customer: o.customer, amount: o.amount, tier: tierAt(o.customer, o.ts) });
      waiting.splice(waiting.indexOf(o), 1);
    }
  } finally {
    busy = false;
  }
});
