// Event-time tumbling windows with a watermark and allowed lateness.
// Topic "clicks" holds { offset, kind: 'event', key, ts } and
// { offset, kind: 'watermark', ts }.
//
// Windows come from each event's own ts, never from node.now(). A window
// [start, start + 1000) stays open until the watermark reaches
// end + 500; events that arrive before that still count, even if they are
// behind the watermark. Once closed, its counts go to the sink and the
// window is forgotten; anything that turns up for it later is dropped.
const WINDOW = 1000;
const LATENESS = 500;

let offset = 0;
let watermark = -Infinity;
let dropped = 0;
const open = {}; // window start -> { key: count }

async function emit(body) {
  // The sink is reliable, but ask again if a reply goes missing.
  while (!(await node.rpc('sink', body, { timeout: 300 }))) {}
}

async function handle(record) {
  if (record.kind === 'event') {
    const start = Math.floor(record.ts / WINDOW) * WINDOW;
    if (watermark >= start + WINDOW + LATENESS) {
      dropped += 1; // its window has already been emitted
      return;
    }
    open[start] ??= {};
    open[start][record.key] = (open[start][record.key] ?? 0) + 1;
  } else if (record.kind === 'watermark') {
    watermark = Math.max(watermark, record.ts);
    const closing = Object.keys(open).map(Number).filter((s) => s + WINDOW + LATENESS <= watermark);
    for (const start of closing) {
      const counts = open[start];
      delete open[start];
      for (const key of Object.keys(counts)) {
        await emit({ type: 'window', key, start, end: start + WINDOW, count: counts[key] });
      }
    }
  }
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
