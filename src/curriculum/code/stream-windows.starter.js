// Event-time tumbling windows with a watermark and allowed lateness.
// Topic "clicks" holds { offset, kind: 'event', key, ts } and
// { offset, kind: 'watermark', ts }.
//
// The polling loop is done: it hands every record to handle(), once and in
// log order.
const WINDOW = 1000;
const LATENESS = 500;

let offset = 0;
let watermark = -Infinity;
let dropped = 0;

async function emit(body) {
  // The sink is reliable, but ask again if a reply goes missing.
  while (!(await node.rpc('sink', body, { timeout: 300 }))) {}
}

async function handle(record) {
  // TODO: an event belongs to the window [start, start + WINDOW) that holds
  // its ts. Count it there, unless that window has already closed: then
  // count it in `dropped` instead.
  //
  // TODO: a watermark record moves `watermark` forward. Every window whose
  // end + LATENESS is now <= watermark closes: send one
  //   { type: 'window', key, start, end, count }
  // per key with events in it, then forget the window.
}

let polling = false;
node.every(25, async () => {
  if (polling) return;
  polling = true;
  try {
    const res = await node.rpc('log', { type: 'poll', topic: 'clicks', offset, max: 20 }, { timeout: 200 });
    if (!res || res.type !== 'poll_ok') return;
    for (const record of res.records) {
      if (record.offset !== offset) continue;
      offset += 1;
      await handle(record);
    }
  } finally {
    polling = false;
  }
});

node.on('stats', (msg) => {
  node.reply(msg, { type: 'stats_ok', dropped });
});
