// Workloads for the stream-processing track. The harness plays the upstream
// log (service "log", topics with offsets, optional retention) and, where a
// task has output, the downstream system (service "sink"). The learner's
// node is the stream processor in the middle.
//
// Everything a checker needs is recorded where the harness can see it: what
// the log served (in the service handler) and what reached the sink (in the
// sink handler), so the node-side design is the learner's.

import { check, j, plural, sample } from '../workloads.js';

const ms = (t) => `${Math.round(t)}ms`;
const isInt = (x) => Number.isInteger(x);

// ------------------------------------------------------------ the upstream log

// A Kafka-like log: each topic is an array of records with offsets 0, 1, 2, …
// Records appear at the time the harness appends them. With a retention,
// records older than that many ms are deleted and polling below the earliest
// remaining offset is refused with code "truncated".
function makeLog(sim, { retention = Infinity } = {}) {
  const topics = new Map();
  const polls = []; // { t, topic, offset, count }
  const truncations = []; // { t, topic, asked, earliest }

  const topic = (name) => {
    if (!topics.has(name)) topics.set(name, { records: [], at: [], start: 0 });
    return topics.get(name);
  };
  const append = (name, fields) => {
    const tp = topic(name);
    const rec = { offset: tp.records.length, ...fields };
    tp.records.push(rec);
    tp.at.push(sim.now());
    return rec;
  };
  const earliest = (tp) => {
    if (retention === Infinity) return 0;
    while (tp.start < tp.records.length && tp.at[tp.start] < sim.now() - retention) tp.start++;
    return tp.start;
  };

  sim.service('log', (msg, reply) => {
    const b = msg.body;
    if (b.type !== 'poll') {
      reply({ type: 'error', code: 'bad_request', text: 'the log understands { type: "poll", topic, offset, max }' });
      return;
    }
    const tp = topics.get(b.topic);
    if (!tp) {
      reply({ type: 'error', code: 'unknown_topic', text: `there is no topic ${j(b.topic)}; topics: ${[...topics.keys()].join(', ')}` });
      return;
    }
    if (!isInt(b.offset) || b.offset < 0) {
      reply({ type: 'error', code: 'bad_request', text: `offset must be a whole number >= 0, not ${j(b.offset)}` });
      return;
    }
    const first = earliest(tp);
    if (b.offset < first) {
      truncations.push({ t: sim.now(), topic: b.topic, asked: b.offset, earliest: first });
      reply({ type: 'error', code: 'truncated', earliest: first, text: `records before offset ${first} have been deleted` });
      return;
    }
    const max = isInt(b.max) && b.max > 0 ? Math.min(b.max, 50) : 20;
    const records = tp.records.slice(b.offset, b.offset + max);
    polls.push({ t: sim.now(), topic: b.topic, offset: b.offset, count: records.length });
    reply({ type: 'poll_ok', records });
  });

  // The highest offset of `name` handed to the node by time t (or -1).
  const servedBy = (name, t) => {
    let hi = -1;
    for (const p of polls) if (p.topic === name && p.t <= t && p.count > 0) hi = Math.max(hi, p.offset + p.count - 1);
    return hi;
  };

  return { topic, append, polls, truncations, servedBy, retention };
}

function truncationCheck(log) {
  const tr = log.truncations[0];
  return check(
    'The processor never asks for records the log has deleted',
    !tr,
    tr &&
      `At ${ms(tr.t)} the node polled ${tr.topic} from offset ${tr.asked}, but everything before offset ${tr.earliest} had been deleted (the log keeps records for ${log.retention}ms). ` +
        `After a restart, start from the offset saved on node.disk, not from 0: the log is not kept forever, so whatever you derived from it must be kept too.`,
  );
}

// Crashes: the node goes down at each time and restarts `down` ms later.
function scheduleCrashes(sim, node, crashes, down) {
  const downs = [];
  for (const t of crashes) {
    sim.at(t, () => {
      sim.crash(node);
      downs.push([t, t + down]);
    });
    sim.at(t + down, () => sim.restart(node));
  }
  return downs;
}

// ------------------------------------------------------- 1. change data capture

const CITIES = ['Lisbon', 'Oslo', 'Lima', 'Hanoi', 'Accra'];

// The harness is the primary database: a users table (user -> city) whose
// every committed change is appended to topic "users" as
// { offset, key, before, after } (before null = insert, after null = delete).
// The node keeps a derived view: a copy of the table plus a count of users per
// city, and answers { type: "view" } with { offset, rows, counts }.
//
// params:
//   writes     how many changes the database commits
//   users      how many distinct keys
//   burst      changes are committed in groups of this size
//   crashes    times at which the node crashes; it restarts `down` ms later
//   slow       fraction of poll replies held back 60–160 ms (so polls overlap)
//   lossy      fraction of poll requests and replies lost
//   retention  ms the log keeps a change
export function cdc(sim, params = {}) {
  const { writes = 80, users = 8, burst = 1, crashes = [], down = 200, slow = 0, lossy = 0, retention = 1500 } = params;
  const node = sim.nodeIds[0];
  const log = makeLog(sim, { retention });
  log.topic('users');

  // Precompute the database history so the checker knows every snapshot.
  const changes = [];
  const snaps = [{}];
  const db = {};
  for (let i = 0; i < writes; i++) {
    const key = 'u' + (1 + Math.floor(sim.rand() * users));
    const before = db[key] ?? null;
    let after;
    if (before === null) after = sim.pick(CITIES);
    else if (sim.rand() < 0.2) after = null;
    else after = sim.pick(CITIES.filter((c) => c !== before));
    if (after === null) delete db[key];
    else db[key] = after;
    changes.push({ key, before, after });
    snaps.push({ ...db });
  }

  const start = 100;
  const stopWrites = sim.duration - 1100;
  const gap = (stopWrites - start) / writes;
  changes.forEach((c, i) => {
    const when = start + Math.floor(i / burst) * burst * gap + (i % burst);
    sim.at(when, () => {
      c.offset = log.append('users', { key: c.key, before: c.before, after: c.after }).offset;
      c.at = sim.now();
    });
  });

  sim.intercept = ({ src, dest }) => {
    if (src === node && dest === 'log' && lossy && sim.rand() < lossy) return 'drop';
    if (src === 'log' && dest === node) {
      if (lossy && sim.rand() < lossy) return 'drop';
      if (slow && sim.rand() < slow) return 60 + sim.rand() * 100;
    }
    return undefined;
  };

  const downs = scheduleCrashes(sim, node, crashes, down);

  const views = [];
  const view = async (client, timeout = 300) => {
    const entry = { invoke: sim.now(), res: null, done: null };
    views.push(entry);
    entry.res = await sim.clientRpc(node, { type: 'view' }, { client, timeout });
    entry.done = sim.now();
    return entry;
  };
  for (let t = 300; t < stopWrites + 400; t += 170) sim.at(t, () => view('c1'));
  let final = null;
  sim.at(sim.duration - 350, async () => {
    final = await view('c2');
  });

  const describe = (k) => {
    const c = changes[k];
    return `change ${k} (${c.key}: ${c.before ?? 'none'} → ${c.after ?? 'deleted'})`;
  };
  const countBy = (rows) => {
    const out = {};
    for (const v of Object.values(rows)) out[v] = (out[v] ?? 0) + 1;
    return out;
  };
  const sameCounts = (a, b) => {
    const keys = new Set([...Object.keys(a), ...Object.keys(b)]);
    for (const k of keys) if ((a[k] ?? 0) !== (b[k] ?? 0)) return k;
    return null;
  };
  const rowDiff = (got, want) => {
    const keys = [...new Set([...Object.keys(got), ...Object.keys(want)])].sort();
    for (const k of keys) if ((got[k] ?? null) !== (want[k] ?? null)) return k;
    return null;
  };
  // Which change last touched `key` at or before offset k (exclusive end).
  const lastTouch = (key, k) => {
    for (let i = k - 1; i >= 0; i--) if (changes[i].key === key) return i;
    return -1;
  };

  return () => {
    const margin = sim.real ? 100 : 20;
    const excused = (v) => downs.some(([a, b]) => v.invoke >= a - margin && v.invoke <= b);
    const lost = views.filter((v) => !v.res && !excused(v) && v !== final);
    const checks = [
      check(
        crashes.length ? 'Every view request gets a reply while the node is up' : 'Every view request gets a reply',
        lost.length === 0,
        lost.length && `${plural(lost.length, 'view request')} got no reply within 300ms; the first was sent at ${ms(lost[0].invoke)}. Answer view at once from what you have applied so far; do not wait for the log.`,
      ),
    ];

    const answered = views.filter((v) => v.res).sort((a, b) => a.done - b.done);
    const bad = answered.find((v) => {
      const r = v.res;
      return r.type !== 'view_ok' || !isInt(r.offset) || r.offset < 0 || !r.rows || typeof r.rows !== 'object' || Array.isArray(r.rows) || !r.counts || typeof r.counts !== 'object' || Array.isArray(r.counts);
    });
    checks.push(check('view_ok carries offset, rows and counts', !bad, bad && `The node replied ${j(bad.res)} to a view. Expected { type: "view_ok", offset: <next offset to apply>, rows: { user: city }, counts: { city: n } }.`));
    if (bad) return checks;

    // The view must equal the database as of the offset it reports.
    let mismatch = null;
    let countProblem = null;
    let backwards = null;
    let prev = null;
    for (const v of answered) {
      const { offset, rows, counts } = v.res;
      const committed = changes.filter((c) => c.at !== undefined && c.at <= v.done).length;
      if (offset > committed) {
        mismatch ??= `At ${ms(v.done)} the view reported offset ${offset}, but only ${committed} changes had been committed. offset is the number of changes applied (the next offset to read).`;
        prev = v;
        continue;
      }
      const want = snaps[offset];
      const key = rowDiff(rows, want);
      if (key !== null && !mismatch) {
        const lt = lastTouch(key, offset);
        mismatch =
          `At ${ms(v.done)} the view said offset ${offset} with ${key} = ${j(rows[key] ?? null)}, but after the first ${offset} changes the database has ${key} = ${j(want[key] ?? null)}` +
          (lt >= 0 ? ` (set by ${describe(lt)}).` : ' (never written).') +
          ' A change was skipped, applied twice, applied out of order, or the rows and the offset came from different moments.';
        const restart = downs.filter(([, b]) => b <= v.done).pop();
        if (restart) mismatch += ` The node restarted at ${ms(restart[1])}: rows and offset must come back from node.disk together.`;
      }
      const ck = sameCounts(counts, countBy(rows));
      if (ck !== null && !countProblem) {
        countProblem = `At ${ms(v.done)} the view's counts say ${ck} has ${counts[ck] ?? 0}, but its rows put ${countBy(rows)[ck] ?? 0} users there. Counts must agree with rows: on each change take one off the before city and add one to the after city, exactly once per change.`;
      }
      if (prev && offset < prev.res.offset) {
        backwards ??= `The view said offset ${prev.res.offset} at ${ms(prev.done)} and offset ${offset} at ${ms(v.done)}.` + (downs.some(([a]) => a > prev.done && a < v.done) ? ' The node crashed in between: progress that was shown to a reader must survive a crash.' : '');
      }
      prev = v;
    }
    checks.push(
      check('The view equals the database at the offset it reports', !mismatch, mismatch),
      check('Counts per city agree with the rows', !countProblem, countProblem),
      check('The offset never goes backwards', !backwards, backwards),
      truncationCheck(log),
    );

    let finalProblem = null;
    if (!final || !final.res) finalProblem = `The final view request at ${ms(sim.duration - 350)} got no reply.`;
    else if (final.res.offset !== changes.length) {
      finalProblem = `At the end the view had applied ${final.res.offset} of ${changes.length} changes; the last change was committed at ${ms(changes[changes.length - 1].at)}, ${ms(sim.duration - 350 - changes[changes.length - 1].at)} before the final view. Keep polling from your offset.`;
    } else if (rowDiff(final.res.rows, snaps[changes.length]) !== null) {
      const k = rowDiff(final.res.rows, snaps[changes.length]);
      finalProblem = `At the end the view has ${k} = ${j(final.res.rows[k] ?? null)}, the database ${j(snaps[changes.length][k] ?? null)}.`;
    }
    checks.push(check('At the end the view has applied every change and equals the database', !finalProblem, finalProblem));
    return checks;
  };
}

// ---------------------------------------------------------- 2. event-time windows

export const WINDOW = 1000;
export const LATENESS = 500;
const PAGES = ['home', 'cart', 'search', 'checkout'];

// Topic "clicks": { offset, kind: "event", key, ts } and
// { offset, kind: "watermark", ts }. Event time runs at twice the speed of
// the test clock and starts at 40000, so it has nothing to do with node.now().
// The node counts events per key in 1000-wide tumbling windows, closes a
// window once the watermark reaches end + 500, sends each non-empty
// (key, window) to the sink once, and counts the events it had to drop.
//
// params:
//   events     how many click events
//   keys       how many distinct keys
//   late       fraction of events delayed 130–450 ms (late, usually still counted)
//   veryLate   fraction delayed 450–1200 ms (often too late)
export function windows(sim, params = {}) {
  const { events = 120, keys = 3, late = 0, veryLate = 0 } = params;
  const node = sim.nodeIds[0];
  const log = makeLog(sim);
  log.topic('clicks');
  const T0 = 40000;
  const start = 100;
  const streamEnd = sim.duration - 900;
  const toTs = (t) => T0 + Math.round(2 * (t - start));
  const pages = PAGES.slice(0, keys);

  // Generate events in event time, then deliver them in arrival order.
  const items = [];
  for (let i = 0; i < events; i++) {
    const e = start + sim.rand() * (streamEnd - start - 250);
    const r = sim.rand();
    const d = r < veryLate ? 450 + sim.rand() * 750 : r < veryLate + late ? 130 + sim.rand() * 320 : sim.rand() * 80;
    const arrive = Math.min(e + d, streamEnd - 5);
    items.push({ at: arrive, seq: i, rec: { kind: 'event', key: sim.pick(pages), ts: toTs(e) } });
  }
  let lastW = -Infinity;
  for (let a = start + 100; a < streamEnd; a += 100) {
    const w = toTs(a) - 200;
    if (w > lastW) items.push({ at: a + 0.5, seq: events + a, rec: { kind: 'watermark', ts: w } });
    lastW = w;
  }
  const finalTs = toTs(streamEnd) + 5000;
  items.push({ at: streamEnd + 20, seq: 1e9, rec: { kind: 'watermark', ts: finalTs } });
  items.sort((x, y) => x.at - y.at || x.seq - y.seq);

  // The expected outcome, by the rules in the brief, in log order.
  const expected = new Map(); // "key|start" -> { key, start, count, late, closeOffset }
  const dropped = [];
  let wm = -Infinity;
  const open = new Map();
  items.forEach((it, offset) => {
    const r = it.rec;
    if (r.kind === 'event') {
      const s = Math.floor(r.ts / WINDOW) * WINDOW;
      if (wm >= s + WINDOW + LATENESS) {
        dropped.push({ offset, ...r, wm });
        return;
      }
      const id = r.key + '|' + s;
      if (!open.has(id)) open.set(id, { key: r.key, start: s, count: 0, late: 0 });
      const w = open.get(id);
      w.count++;
      if (r.ts < wm) w.late++;
    } else {
      wm = Math.max(wm, r.ts);
      for (const [id, w] of open) {
        if (w.start + WINDOW + LATENESS <= wm) {
          expected.set(id, { ...w, closeOffset: offset, closeTs: wm });
          open.delete(id);
        }
      }
    }
  });

  items.forEach((it) => sim.at(it.at, () => log.append('clicks', it.rec)));

  const got = []; // { t, body }
  sim.service('sink', (msg, reply) => {
    got.push({ t: sim.now(), body: msg.body });
    reply({ type: 'window_ok' });
  });

  let stats = null;
  sim.at(sim.duration - 250, async () => {
    stats = await sim.clientRpc(node, { type: 'stats' }, { client: 'c1', timeout: 200 });
  });

  return () => {
    const malformed = got.find(({ body: b }) => b.type !== 'window' || typeof b.key !== 'string' || !isInt(b.start) || !isInt(b.end) || !isInt(b.count) || b.end !== b.start + WINDOW || b.start % WINDOW !== 0);
    const checks = [
      check(
        'Sink messages are { type: "window", key, start, end, count }',
        !malformed,
        malformed && `The sink received ${j(malformed.body)} at ${ms(malformed.t)}. start must be a multiple of ${WINDOW} (the window's event-time start), end = start + ${WINDOW}, count a whole number.`,
      ),
    ];
    if (malformed) return checks;

    const seen = new Map();
    for (const g of got) {
      const id = g.body.key + '|' + g.body.start;
      if (!seen.has(id)) seen.set(id, []);
      seen.get(id).push(g);
    }
    const label = (w) => `${w.key} [${w.start}, ${w.start + WINDOW})`;
    const unexpected = [...seen.keys()].filter((id) => !expected.has(id));
    const missing = [...expected.keys()].filter((id) => !seen.has(id));
    const twice = [...seen.entries()].filter(([, v]) => v.length > 1);
    const wrong = [];
    const early = [];
    for (const [id, w] of expected) {
      const g = seen.get(id);
      if (!g) continue;
      if (g[0].body.count !== w.count) wrong.push({ w, count: g[0].body.count });
      const served = log.servedBy('clicks', g[0].t);
      if (served < w.closeOffset) early.push({ w, t: g[0].t, served });
    }
    const exampleUnexpected = unexpected.length && seen.get(unexpected[0])[0];
    checks.push(
      check(
        'Results only for windows that have events, by event time',
        unexpected.length === 0,
        unexpected.length &&
          `${plural(unexpected.length, 'result')} for windows with no events in them, e.g. ${j(exampleUnexpected.body)} at ${ms(exampleUnexpected.t)}. Windows come from each event's ts (event time, starting around ${T0}), never from node.now().`,
      ),
      check(
        'Every non-empty window is emitted',
        missing.length === 0,
        missing.length &&
          `${plural(missing.length, 'window')} never reached the sink (${sample(missing.map((id) => label(expected.get(id))))}). ${label(expected.get(missing[0]))} holds ${plural(expected.get(missing[0]).count, 'event')} and closed when the watermark reached ${expected.get(missing[0]).closeTs} (log offset ${expected.get(missing[0]).closeOffset}).`,
      ),
      check(
        'Each window is emitted once',
        twice.length === 0,
        twice.length && `${label(twice[0][1][0].body)} was sent ${twice[0][1].length} times (counts ${twice[0][1].map((g) => g.body.count).join(', ')}). Emit a window once, when it closes, then forget it.`,
      ),
    );
    let wrongDetail = null;
    if (wrong.length) {
      const { w, count } = wrong[0];
      const lateDropped = dropped.filter((d) => d.key === w.key && Math.floor(d.ts / WINDOW) * WINDOW === w.start).length;
      wrongDetail =
        `${plural(wrong.length, 'window')} had the wrong count. ${label(w)} was emitted with count ${count}; expected ${w.count}` +
        (w.late ? `, of which ${w.late} arrived late (ts below the watermark at the time) but before the window closed at watermark ${w.start + WINDOW + LATENESS}: they still count` : '') +
        (lateDropped ? `. ${plural(lateDropped, 'more event')} for it came after it closed and must be dropped, not counted` : '') +
        '.';
    }
    checks.push(check('Counts are exact, including late events within the allowed lateness', !wrong.length, wrongDetail));
    checks.push(
      check(
        `A window is emitted only once the watermark reaches end + ${LATENESS}`,
        !early.length,
        early.length &&
          `${label(early[0].w)} reached the sink at ${ms(early[0].t)}, but the first watermark >= ${early[0].w.start + WINDOW + LATENESS} is at log offset ${early[0].w.closeOffset} and the node had only read up to offset ${early[0].served}. Events up to ${LATENESS} behind the watermark can still arrive for it.`,
      ),
    );
    let statsProblem = null;
    if (!stats) statsProblem = `The stats request at ${ms(sim.duration - 250)} got no reply.`;
    else if (stats.type !== 'stats_ok' || !isInt(stats.dropped)) statsProblem = `The node replied ${j(stats)} to stats; expected { type: "stats_ok", dropped: <number> }.`;
    else if (stats.dropped !== dropped.length) {
      const d = dropped[0];
      statsProblem =
        `stats says dropped: ${stats.dropped}; expected ${dropped.length}.` +
        (d ? ` For example the event at offset ${d.offset} (${d.key}, ts ${d.ts}) arrived when the watermark was already ${d.wm}, past its window's end + ${LATENESS} = ${Math.floor(d.ts / WINDOW) * WINDOW + WINDOW + LATENESS}.` : ' No event arrived after its window had closed.');
    }
    checks.push(check('Events for closed windows are dropped and counted', !statsProblem, statsProblem));
    return checks;
  };
}

// -------------------------------------------------------- 3. stream-table join

const TIERS = ['bronze', 'silver', 'gold'];

// Topic "customers": a changelog { offset, kind: "change", customer, tier, ts }
// in strictly increasing ts, plus { offset, kind: "watermark", ts } records
// promising that no later change has ts <= that. It lags 100–350 ms behind.
// Topic "orders": { offset, order, customer, amount, ts }, slightly out of order.
// The node sends each order to the sink once, enriched with the customer's
// tier as of the order's ts (null if the customer had no row yet).
//
// params:
//   orders     how many orders
//   customers  how many customers (two of them appear only halfway through)
//   changes    how many tier changes after the initial rows
export function join(sim, params = {}) {
  const { orders: orderCount = 100, customers = 5, changes: changeCount = 25 } = params;
  const log = makeLog(sim);
  log.topic('customers');
  log.topic('orders');
  const T0 = 70000;
  const start = 100;
  const streamEnd = sim.duration - 1000;
  const toTs = (t) => T0 + Math.round(t - start);
  const ids = Array.from({ length: customers }, (_, i) => 'cust-' + (i + 1));

  // Customer changes, in event time.
  const changes = [];
  let e = start;
  const early = ids.slice(0, customers - 2);
  for (const c of early) {
    e += 5 + Math.floor(sim.rand() * 30);
    changes.push({ customer: c, tier: sim.pick(TIERS), e });
  }
  const span = (streamEnd - 500 - e) / changeCount;
  const current = Object.fromEntries(changes.map((c) => [c.customer, c.tier]));
  for (let i = 0; i < changeCount; i++) {
    e = Math.max(e + 1, Math.round(start + 200 + (i + sim.rand()) * span));
    const late = ids.slice(customers - 2);
    const c = i === Math.floor(changeCount / 3) ? late[0] : i === Math.floor(changeCount / 2) ? late[1] : sim.pick(ids);
    const tier = sim.pick(TIERS.filter((t) => t !== current[c]));
    current[c] = tier;
    changes.push({ customer: c, tier, e });
  }
  // The changelog lags, but stays in order.
  let prevAt = 0;
  for (const c of changes) {
    c.ts = toTs(c.e);
    c.at = Math.max(prevAt, c.e + 100 + sim.rand() * 250);
    prevAt = c.at;
  }
  for (const c of changes) sim.at(c.at, () => log.append('customers', { kind: 'change', customer: c.customer, tier: c.tier, ts: c.ts }));
  for (let a = start + 450; a < streamEnd + 400; a += 80) {
    sim.at(a + 0.5, () => log.append('customers', { kind: 'watermark', ts: toTs(a - 400) }));
  }
  sim.at(streamEnd + 450, () => log.append('customers', { kind: 'watermark', ts: toTs(streamEnd) + 100000 }));

  // Orders, in event time, delivered slightly out of order.
  const orders = [];
  for (let i = 0; i < orderCount; i++) {
    const oe = start + 20 + sim.rand() * (streamEnd - start - 60);
    orders.push({ customer: sim.pick(ids), amount: 5 + Math.floor(sim.rand() * 196), e: oe, at: oe + sim.rand() * 60 });
  }
  orders.sort((a, b) => a.e - b.e);
  orders.forEach((o, i) => {
    o.order = 'ord-' + (i + 1);
    o.ts = toTs(o.e);
  });
  const arrival = [...orders].sort((a, b) => a.at - b.at);
  for (const o of arrival) sim.at(o.at, () => log.append('orders', { order: o.order, customer: o.customer, amount: o.amount, ts: o.ts }));

  const tierAt = (customer, ts) => {
    let found = null;
    for (const c of changes) if (c.customer === customer && c.ts <= ts) found = c;
    return found;
  };
  const nextChange = (customer, ts) => changes.find((c) => c.customer === customer && c.ts > ts);

  const got = [];
  sim.service('sink', (msg, reply) => {
    got.push({ t: sim.now(), body: msg.body });
    reply({ type: 'enriched_ok' });
  });

  return () => {
    const byId = new Map(orders.map((o) => [o.order, o]));
    const malformed = got.find(({ body: b }) => b.type !== 'enriched' || typeof b.order !== 'string' || !(b.tier === null || typeof b.tier === 'string'));
    const checks = [
      check(
        'Sink messages are { type: "enriched", order, customer, amount, tier }',
        !malformed,
        malformed && `The sink received ${j(malformed.body)} at ${ms(malformed.t)}. tier is a string, or null when the customer had no row yet.`,
      ),
    ];
    if (malformed) return checks;

    const seen = new Map();
    for (const g of got) {
      if (!seen.has(g.body.order)) seen.set(g.body.order, []);
      seen.get(g.body.order).push(g);
    }
    const unknown = got.find((g) => !byId.has(g.body.order));
    const altered = got.find((g) => {
      const o = byId.get(g.body.order);
      return o && (g.body.customer !== o.customer || g.body.amount !== o.amount);
    });
    const missing = orders.filter((o) => !seen.has(o.order));
    const twice = [...seen.values()].find((v) => v.length > 1);
    checks.push(
      check(
        'Orders reach the sink unchanged',
        !unknown && !altered,
        unknown ? `The sink received ${j(unknown.body)}, but no such order was in the log.` : altered && `${altered.body.order} reached the sink as ${j(altered.body)}; in the log it is ${j({ order: altered.body.order, customer: byId.get(altered.body.order).customer, amount: byId.get(altered.body.order).amount })}.`,
      ),
      check(
        'Every order is emitted',
        missing.length === 0,
        missing.length && `${plural(missing.length, 'order')} never reached the sink (${sample(missing.map((o) => o.order))}). ${missing[0].order} has ts ${missing[0].ts}; the customers log passed that time by ${ms(Math.max(...changes.map((c) => c.at)) + 400)} at the latest.`,
      ),
      check('Each order is emitted once', !twice, twice && `${twice[0].body.order} reached the sink ${twice.length} times.`),
    );

    let wrong = null;
    let wrongCount = 0;
    for (const o of orders) {
      const g = seen.get(o.order);
      if (!g) continue;
      const want = tierAt(o.customer, o.ts);
      const wantTier = want ? want.tier : null;
      if (g[0].body.tier === wantTier) continue;
      wrongCount++;
      if (wrong) continue;
      const nx = nextChange(o.customer, o.ts);
      wrong =
        `${o.order} (${o.customer}, ts ${o.ts}) was joined with tier ${j(g[0].body.tier)} at ${ms(g[0].t)}, but at ts ${o.ts} ${o.customer} ` +
        (want ? `was ${want.tier} (since the change at ts ${want.ts}, appended to the customers log at ${ms(want.at)})` : 'had no row yet') +
        (nx ? `; it became ${nx.tier} only at ts ${nx.ts}` : '') +
        '. Join against the table as of the order\'s event time: keep each customer\'s versions, and hold an order until the customers log has passed its ts.';
    }
    if (wrongCount > 1) wrong += ` (${wrongCount} orders in all.)`;
    checks.push(check("Each order carries its customer's tier as of the order's ts", !wrong, wrong));
    return checks;
  };
}

// ------------------------------------------------------ 4. exactly-once output

// Topic "payments": { offset, account, amount }, kept for `retention` ms. The
// node keeps a running balance per account and sends, for every payment,
// { type: "emit", id, offset, account, balance } to the sink. The sink stores
// an emit under its id and ignores ids it already holds. The node crashes;
// the sink sometimes drops requests (outages) or answers slowly.
//
// params:
//   payments   how many payments
//   accounts   how many accounts
//   burst      payments arrive in groups of this size
//   crashes    times at which the node crashes; it restarts `down` ms later
//   outages    [[from, to]] during which the sink drops every request
//   slow       [[from, to]] during which sink replies take 150–300 ms
//   retention  ms the log keeps a payment
export function exactlyOnce(sim, params = {}) {
  const { payments = 120, accounts = 4, burst = 8, crashes = [], down = 200, outages = [], slow = [], retention = 2000 } = params;
  const node = sim.nodeIds[0];
  const log = makeLog(sim, { retention });
  log.topic('payments');
  const names = Array.from({ length: accounts }, (_, i) => 'acct-' + (i + 1));

  const list = [];
  const bal = {};
  for (let i = 0; i < payments; i++) {
    const account = sim.pick(names);
    const amount = sim.rand() < 0.25 ? -(1 + Math.floor(sim.rand() * 40)) : 1 + Math.floor(sim.rand() * 100);
    bal[account] = (bal[account] ?? 0) + amount;
    list.push({ offset: i, account, amount, balance: bal[account] });
  }
  const start = 100;
  const streamEnd = sim.duration - 1500;
  const gap = (streamEnd - start) / payments;
  list.forEach((p, i) => {
    sim.at(start + Math.floor(i / burst) * burst * gap + (i % burst), () => {
      log.append('payments', { account: p.account, amount: p.amount });
      p.at = sim.now();
    });
  });

  const within = (spans, t) => spans.some(([a, b]) => t >= a && t < b);
  sim.intercept = ({ src, dest, t }) => {
    if (dest === 'sink' && within(outages, t)) return 'drop';
    if (src === 'sink' && within(slow, t)) return 150 + sim.rand() * 150;
    return undefined;
  };

  const received = []; // every emit that reached the sink
  const stored = new Map(); // id -> first emit
  sim.service('sink', (msg, reply) => {
    const b = msg.body;
    received.push({ t: sim.now(), body: b });
    const key = typeof b.id === 'string' || typeof b.id === 'number' ? JSON.stringify(b.id) : null;
    if (b.type === 'emit' && key !== null && !stored.has(key)) {
      stored.set(key, { t: sim.now(), body: b });
      reply({ type: 'emit_ok' });
    } else if (b.type === 'emit' && key !== null) {
      reply({ type: 'emit_ok', duplicate: true });
    } else {
      reply({ type: 'error', code: 'bad_request', text: 'the sink understands { type: "emit", id, offset, account, balance }' });
    }
  });

  const downs = scheduleCrashes(sim, node, crashes, down);

  return () => {
    const malformed = received.find(({ body: b }) => b.type !== 'emit' || !(typeof b.id === 'string' || typeof b.id === 'number') || !isInt(b.offset) || typeof b.account !== 'string' || typeof b.balance !== 'number');
    const checks = [
      check(
        'Sink messages are { type: "emit", id, offset, account, balance }',
        !malformed,
        malformed && `The sink received ${j(malformed.body)} at ${ms(malformed.t)}. id is a string or number of your choosing; offset is the payment's offset.`,
      ),
    ];
    if (malformed) return checks;

    const crashNote = (t) => {
      const d = downs.find(([a]) => a >= t - 50);
      return d ? ` The node crashed at ${ms(d[0])}.` : '';
    };
    const perOffset = new Map();
    for (const s of stored.values()) {
      if (!perOffset.has(s.body.offset)) perOffset.set(s.body.offset, []);
      perOffset.get(s.body.offset).push(s);
    }
    const ghost = [...perOffset.keys()].find((o) => o < 0 || o >= list.length);
    const missing = list.filter((p) => !perOffset.has(p.offset));
    const doubled = list.filter((p) => (perOffset.get(p.offset) ?? []).length > 1);

    // Same id sent again with different content: the replay did not see the same state.
    let replayDiff = null;
    for (const r of received) {
      const s = stored.get(JSON.stringify(r.body.id));
      if (s && s !== r && (s.body.balance !== r.body.balance || s.body.offset !== r.body.offset || s.body.account !== r.body.account)) {
        replayDiff = `id ${j(r.body.id)} was first stored at ${ms(s.t)} as ${j(s.body)} and sent again at ${ms(r.t)} as ${j(r.body)}.${crashNote(s.t)} After a restart the balances must be exactly the ones that go with the saved offset: save both in one write.`;
        break;
      }
    }

    let wrong = null;
    let wrongCount = 0;
    for (const p of list) {
      const s = (perOffset.get(p.offset) ?? [])[0];
      if (!s || (s.body.account === p.account && s.body.balance === p.balance)) continue;
      wrongCount++;
      if (wrong) continue;
      const restart = downs.filter(([, b]) => b <= s.t).pop();
      wrong =
        `Payment ${p.offset} (${p.account}, ${p.amount > 0 ? '+' : ''}${p.amount}) was stored with ${j({ account: s.body.account, balance: s.body.balance })}; ` +
        `exactly-once processing gives ${p.account} a balance of ${p.balance} after it.` +
        (restart ? ` The node had restarted at ${ms(restart[1])}: balances and offset must be saved together, after the outputs they cover were acknowledged.` : '');
    }
    if (wrongCount > 1) wrong += ` (${wrongCount} payments in all.)`;

    let lostDetail = null;
    if (missing.length) {
      const p = missing[0];
      const tries = received.filter((r) => r.body.offset === p.offset);
      lostDetail = `${plural(missing.length, 'payment')} never reached the sink (offsets ${sample(missing.map((x) => x.offset))}). Payment ${p.offset} was appended at ${ms(p.at ?? 0)}.`;
      const dropped = outages.find(([a, b]) => b > (p.at ?? 0) && crashes.some((c) => c >= a && c < b));
      if (tries.length === 0 && dropped) lostDetail += ` The sink was unreachable from ${ms(dropped[0])} to ${ms(dropped[1])} and the node crashed in the middle: if the offset was saved before the sink acknowledged the output, the restarted node skips it.`;
    }

    checks.push(
      check('Every payment reaches the sink', missing.length === 0, lostDetail),
      check(
        'No payment is stored twice under different ids',
        doubled.length === 0,
        doubled.length && `Payment ${doubled[0].offset} is stored under ids ${perOffset.get(doubled[0].offset).map((s) => j(s.body.id)).join(' and ')} (at ${perOffset.get(doubled[0].offset).map((s) => ms(s.t)).join(' and ')}).${crashNote(perOffset.get(doubled[0].offset)[0].t)} A replayed payment must be sent with the same id as before, so the sink can recognise it: derive the id from the input, not from a counter or node.random().`,
      ),
      check('No output for a payment that does not exist', ghost === undefined, ghost !== undefined && `The sink stored an emit for offset ${ghost}; the log has offsets 0 to ${list.length - 1}.`),
      check('A replayed payment produces the same output', !replayDiff, replayDiff),
      check('Balances are exactly once: each payment counted once', !wrong, wrong),
      truncationCheck(log),
    );
    return checks;
  };
}
