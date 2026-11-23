// Workloads for the caching track.
//
// In every task the learner's nodes sit in front of a slow database, a fake
// service named "db" run by the harness. Clients send `get` (and later `put`)
// to the nodes; the nodes answer from memory or call the database. The
// database records every call it receives: the number of reads is how the
// checkers measure the load a cache takes off it.
//
// The database keeps the full history of every key, so a checker can tell
// which version a reply carried, when that version was replaced, and whether
// a newer write had already been acknowledged when the request was sent.

import { check, j, plural, unanswered } from '../workloads.js';

const ms = (t) => `${Math.round(t)}ms`;
const pct = (x) => `${Math.round(x * 100)}%`;
const isGetOk = (r) => !!r.res && r.res.type === 'get_ok';

// Registers the database.
//
// { type: 'read', key }          -> { type: 'read_ok', value }   after `delay` ms
// { type: 'write', key, value }  -> { type: 'write_ok' }         after `delay` ms
//
// A read returns the value the key holds when the database answers; a write
// takes effect when the database answers. decide(call) may return
// { silent: true } (never answer), { delay } or { hold } (extra ms on the way
// back to the node, applied with sim.intercept so that later messages
// overtake the reply).
function makeDb(sim, { delay = [30, 60], decide = null } = {}) {
  const secret = String(1000 + Math.floor(sim.rand() * 9000));
  const hist = new Map(); // key -> [{ value, t, by }]
  const byId = new Map();
  const held = new Map(); // msg_id of a read -> extra ms on its reply
  const db = {
    calls: [],
    // Optional ({ src, dest, t }) => ms of extra delay for node-to-node messages.
    nodeDelay: null,
    history(key) {
      if (!hist.has(key)) hist.set(key, [{ value: key + ':v0:' + secret, t: 0, by: 'initial' }]);
      return hist.get(key);
    },
    current: (key) => db.history(key)[db.history(key).length - 1].value,
    // The harness writes to the database directly, behind the cache's back.
    change(key) {
      const h = db.history(key);
      h.push({ value: key + ':v' + h.length + ':' + secret, t: sim.now(), by: 'harness' });
    },
    // Latest position of `value` in the key's history, or -1.
    lastIndexOf(key, value) {
      const h = db.history(key);
      for (let i = h.length - 1; i >= 0; i--) if (h[i].value === value) return i;
      return -1;
    },
    firstIndexOf: (key, value) => db.history(key).findIndex((e) => e.value === value),
    replacedAt: (key, i) => (i + 1 < db.history(key).length ? db.history(key)[i + 1].t : Infinity),
    reads: (pred = () => true) => db.calls.filter((c) => c.type === 'read' && pred(c)),
  };

  const record = (src, body, sent) => {
    const call = { type: body.type, key: body.key, src, sent, arrived: null, answered: null, heard: null, outcome: null, value: null };
    db.calls.push(call);
    if (body.msg_id != null) byId.set(body.msg_id, call);
    return call;
  };

  // Node-to-node messages, for explaining failures. The engine fills in
  // `dropped` and the delivery time `t1` on these same objects.
  db.nodeMsgs = [];
  sim.onSend = (msg) => {
    if (msg.dest === 'db') record(msg.src, msg.body, msg.t0);
    else if (msg.kind === 'node') db.nodeMsgs.push(msg);
  };
  sim.onDeliver = (msg) => {
    if (msg.src === 'db') {
      const call = byId.get(msg.body.in_reply_to);
      if (call) call.heard = sim.now();
    }
  };

  sim.intercept = ({ src, body, kind, dest, t }) => {
    if (src === 'db' && body.in_reply_to != null && held.has(body.in_reply_to)) return held.get(body.in_reply_to);
    if (kind === 'node' && db.nodeDelay) return db.nodeDelay({ src, dest, t });
    return undefined;
  };

  sim.service('db', (msg, reply) => {
    const b = msg.body;
    const call = byId.get(b.msg_id) ?? record(msg.src, b, sim.now());
    call.arrived = sim.now();
    if ((b.type !== 'read' && b.type !== 'write') || typeof b.key !== 'string') {
      call.outcome = 'bad_request';
      reply({ type: 'error', code: 'bad_request', text: 'the database understands { type: "read", key } and { type: "write", key, value }' });
      return;
    }
    const d = (decide && decide(call)) || {};
    if (d.silent) {
      call.outcome = 'silent';
      return;
    }
    if (d.hold && b.msg_id != null) held.set(b.msg_id, d.hold);
    const wait = d.delay ?? delay[0] + sim.rand() * (delay[1] - delay[0]);
    sim.at(sim.now() + wait, () => {
      call.answered = sim.now();
      call.outcome = 'ok';
      if (b.type === 'read') {
        call.value = db.current(b.key);
        reply({ type: 'read_ok', value: call.value });
      } else {
        call.value = b.value;
        db.history(b.key).push({ value: b.value, t: sim.now(), by: msg.src });
        reply({ type: 'write_ok' });
      }
    });
  });
  return db;
}

// One client request. The record is pushed at once and filled in when (if) the reply arrives.
function ask(sim, results, node, req, { timeout = 1000, client = 'c1', ...extra } = {}) {
  const r = { node, key: req.key, req, res: null, invoked: sim.now(), done: null, ...extra };
  results.push(r);
  r.promise = sim.clientRpc(node, req, { timeout, client }).then((res) => {
    r.res = res;
    r.done = sim.now();
    return res;
  });
  return r;
}

// Cumulative weights for a skewed (Zipf-like) choice among n keys: key 1 is
// the most popular, key n the least.
function zipf(n, s = 1) {
  const w = Array.from({ length: n }, (_, i) => 1 / Math.pow(i + 1, s));
  const total = w.reduce((a, b) => a + b, 0);
  let acc = 0;
  return w.map((x) => (acc += x / total));
}
const draw = (sim, cum) => {
  const x = sim.rand();
  const i = cum.findIndex((c) => x < c);
  return 1 + (i < 0 ? cum.length - 1 : i);
};

// Every successful get carries a value the database really held for that key.
function realValues(results, db, { allowErrors = () => false, errorAdvice = 'The database answers every read in this test.' } = {}) {
  const gets = results.filter((r) => r.req.type === 'get' && r.res);
  const wrongType = gets.find((r) => !isGetOk(r) && !(r.res.type === 'error' && allowErrors(r)));
  const invented = gets.find((r) => isGetOk(r) && db.lastIndexOf(r.key, r.res.value) < 0);
  return check(
    'Replies carry values from the database',
    !wrongType && !invented,
    wrongType
      ? `${wrongType.node} answered ${j(wrongType.req)} with ${j(wrongType.res)}. A get is answered with { type: "get_ok", value }, where value is what the database's read_ok carried. ${wrongType.res.type === 'error' ? errorAdvice : ''}`
      : invented &&
          `${invented.node} answered ${j(invented.req)} with ${j(invented.res.value)}, a value the database never held for "${invented.key}" (it holds ${j(db.current(invented.key))}). Pass on res.value from the read_ok reply.`,
  );
}

// No get returns a value that the database had replaced more than `limit` ms
// before the get was sent.
const TTL = 1000;
const TTL_SLACK = 100;
function fresh(results, db, limit = TTL + TTL_SLACK) {
  let worst = null;
  let count = 0;
  for (const r of results) {
    if (!isGetOk(r)) continue;
    const i = db.lastIndexOf(r.key, r.res.value);
    if (i < 0) continue;
    const replaced = db.replacedAt(r.key, i);
    const age = r.invoked - replaced;
    if (age > limit) {
      count++;
      if (!worst || age > worst.age) worst = { r, replaced, age, newer: db.history(r.key)[i + 1].value };
    }
  }
  return check(
    `No value is served more than ${TTL}ms after the database replaced it`,
    !worst,
    worst &&
      `${plural(count, 'get')} returned a value the database had replaced more than ${ms(limit)} earlier. Worst: at ${ms(worst.r.invoked)} ${worst.r.node} answered ${j(worst.r.req)} with ${j(worst.r.res.value)}, but the database replaced it with ${j(worst.newer)} at ${ms(worst.replaced)}, ${ms(worst.age)} before the request. Remember when each entry was read, treat entries older than ${TTL}ms as misses, and do not move that time forward on a hit: a key that is asked for often would then never expire.`,
  );
}

function readShare(name, gets, reads, maxShare, advice) {
  const share = gets ? reads / gets : 0;
  return check(
    name,
    share <= maxShare,
    `${gets} gets caused ${plural(reads, 'database read')} (${pct(share)}); at most ${pct(maxShare)} are allowed. ${advice}`,
  );
}

// ---------------------------------------------------------- cache-aside

export function cacheAside(sim, { ops = 300, keys = 30, gap = 12 }) {
  const db = makeDb(sim, { delay: [30, 60] });
  const cum = zipf(keys, 1.1);
  const results = [];
  for (let i = 0; i < ops; i++) {
    const key = 'k' + draw(sim, cum);
    const node = sim.pick(sim.nodeIds);
    sim.at(200 + i * gap, () => ask(sim, results, node, { type: 'get', key }, { client: 'c' + (1 + (i % 3)) }));
  }

  return () => {
    const pairs = new Set(results.map((r) => r.node + '|' + r.key));
    const distinctKeys = new Set(results.map((r) => r.key)).size;
    const reads = db.reads().length;
    const allowed = Math.ceil(pairs.size * 1.5) + 5;
    const perKey = new Map();
    for (const c of db.reads()) perKey.set(c.src + '|' + c.key, (perKey.get(c.src + '|' + c.key) ?? 0) + 1);
    const [busiest, busiestN] = [...perKey].sort((a, b) => b[1] - a[1])[0] ?? ['', 0];
    return [
      unanswered('Every request gets a reply', results, 'Every get is answered with get_ok, from memory or from the database.'),
      realValues(results, db),
      check(
        `The database sees about one read per key per node`,
        reads <= allowed,
        `${results.length} gets asked for ${distinctKeys} distinct keys (${pairs.size} distinct node/key pairs), and the database received ${plural(reads, 'read')}; at most ${allowed} are allowed. ${busiest ? `${busiest.split('|')[0]} alone read "${busiest.split('|')[1]}" ${busiestN} times.` : ''} After the first read of a key, keep its value in memory and answer later gets for it from there.`,
      ),
    ];
  };
}

// ------------------------------------------------------------------ LRU

const CAPACITY = 20;

export function cacheLru(sim, { perNode = 300, gap = 15, hot = 5, every = 3, replay = 40, minHitRate = 0.25 }) {
  const db = makeDb(sim, { delay: [30, 60] });
  const results = [];
  const replays = [];
  const probes = [];
  const n = sim.nodeIds.length;
  const mainEnd = 200 + perNode * gap;
  const replayStart = mainEnd + 300;
  let scanN = 0;
  sim.nodeIds.forEach((node, ni) => {
    const offset = Math.round((gap / n) * ni);
    const scanned = [];
    for (let i = 0; i < perNode; i++) {
      // A few hot keys, again and again, mixed into a scan of keys that are
      // never asked for twice. Halfway through, a different set of keys is hot.
      const shift = i >= perNode / 2 ? hot : 0;
      const key = i % every === 0 ? 'h' + (1 + shift + ((i / every) % hot)) : 's' + ++scanN;
      if (key[0] === 's') scanned.push(key);
      sim.at(200 + offset + i * gap, () => ask(sim, results, node, { type: 'get', key }, { client: 'c' + (1 + (i % 3)) }));
    }
    // Then ask once more for the most recent scan keys, newest first. A node
    // holding at most CAPACITY entries can answer at most CAPACITY of them from memory.
    scanned
      .slice(-replay)
      .reverse()
      .forEach((key, i) => sim.at(replayStart + offset + i * gap, () => ask(sim, replays, node, { type: 'get', key }, { client: 'c4' })));
  });
  let probesPlanned = 0;
  for (let t = 300; t < sim.duration - 450; t += 250) {
    for (const node of sim.nodeIds) {
      probesPlanned++;
      sim.at(t, async () => {
        const res = await sim.clientRpc(node, { type: 'stats' }, { probe: true, timeout: 400 });
        probes.push({ node, t, res });
      });
    }
  }

  return () => {
    const checks = [
      unanswered('Every request gets a reply', [...results, ...replays], 'Every get is answered with get_ok, from memory or from the database.'),
      realValues([...results, ...replays], db),
    ];

    const badProbe = probes.find((p) => !p.res || p.res.type !== 'stats_ok' || !Number.isInteger(p.res.entries));
    const biggest = probes.filter((p) => p.res && Number.isInteger(p.res.entries)).sort((a, b) => b.res.entries - a.res.entries)[0];
    checks.push(
      check(
        'Stats requests are answered',
        probes.length === probesPlanned && !badProbe,
        badProbe
          ? `At ${ms(badProbe.t)} ${badProbe.node} answered { "type": "stats" } with ${j(badProbe.res)}. Reply { type: "stats_ok", entries: <number of keys held> }.`
          : `Only ${probes.length} of ${probesPlanned} stats requests completed before the run ended.`,
      ),
      check(
        `At most ${CAPACITY} entries per node`,
        !biggest || biggest.res.entries <= CAPACITY,
        biggest && `At ${ms(biggest.t)} ${biggest.node} reported ${biggest.res.entries} entries; the limit is ${CAPACITY}. When storing a new key would go over the limit, evict one first.`,
      ),
    );

    const mainReads = db.reads((c) => c.sent < mainEnd + 150).length;
    const hitRate = results.length ? 1 - mainReads / results.length : 0;
    checks.push(
      check(
        `Hit rate of at least ${pct(minHitRate)}`,
        hitRate >= minHitRate,
        `${results.length} gets caused ${plural(mainReads, 'database read')}, a hit rate of ${pct(hitRate)}; at least ${pct(minHitRate)} is expected. One get in ${every} is for one of ${hot} hot keys (a different ${hot} in the second half); the rest scan keys that are never asked for again. Evict the entry that was used least recently, and count a hit as a use (move the key to the fresh end), so the hot keys stay while the scan passes through. Evicting the oldest insertion, or a random entry, throws the hot keys out every few rounds; a policy that never lets go of old favourites misses the new hot keys.`,
      ),
    );

    const replayBad = sim.nodeIds
      .map((node) => {
        const asked = replays.filter((r) => r.node === node);
        const reads = db.reads((c) => c.src === node && c.sent >= replayStart - 50).length;
        return { node, asked: asked.length, hits: asked.length - reads };
      })
      .find((x) => x.hits > CAPACITY);
    checks.push(
      check(
        `Memory stays bounded (old scan keys are gone)`,
        !replayBad,
        replayBad &&
          `After the scan, ${replayBad.node} was asked again for ${replayBad.asked} scan keys it had already read and answered ${replayBad.hits} of them from memory. With at most ${CAPACITY} entries it could hold at most ${CAPACITY} of them. Whatever stats says, the cache is keeping more than ${CAPACITY} keys.`,
      ),
    );
    return checks;
  };
}

// ------------------------------------------------------------------ TTL

export function cacheTtl(sim, { keys = 10, gap = 10, changeEvery = 700, maxShare = 0.4 }) {
  const db = makeDb(sim, { delay: [30, 60] });
  const cum = zipf(keys, 1);
  const end = sim.duration - 500;
  for (let k = 1; k <= keys; k++) {
    const key = 'k' + k;
    for (let t = 250 + sim.rand() * changeEvery; t < end; t += changeEvery * (0.5 + sim.rand())) sim.at(t, () => db.change(key));
  }
  const results = [];
  let i = 0;
  for (let t = 200; t < end; t += gap, i++) {
    const key = 'k' + draw(sim, cum);
    const node = sim.pick(sim.nodeIds);
    const client = 'c' + (1 + (i % 3));
    sim.at(t, () => ask(sim, results, node, { type: 'get', key }, { client }));
  }

  return () => [
    unanswered('Every request gets a reply', results, 'Every get is answered with get_ok, from memory or from the database.'),
    realValues(results, db),
    fresh(results, db),
    readShare(
      'The cache still absorbs most reads',
      results.length,
      db.reads().length,
      maxShare,
      `Keep entries for the full ${TTL}ms: an entry expires ${TTL}ms after it was read, not sooner, and the hot keys are asked for many times in that window.`,
    ),
  ];
}

// ------------------------------------------------------------ stampede

const DB_TIMEOUT = 300;
const PER_BURST = 2;

const BURSTS = {
  // Cold keys first, then the same keys again after their entries expired.
  bursts: {
    bursts: [
      { at: 300, node: 'n1', key: 'a' },
      { at: 600, node: 'n2', key: 'b' },
      { at: 900, node: 'n2', key: 'a' },
      { at: 1200, node: 'n1', key: 'c' },
      { at: 1900, node: 'n1', key: 'a' },
      { at: 2400, node: 'n2', key: 'b' },
      { at: 2800, node: 'n2', key: 'a' },
      { at: 3100, node: 'n1', key: 'c' },
    ],
    changes: [
      { at: 650, key: 'a' },
      { at: 1000, key: 'b' },
      { at: 1500, key: 'c' },
      { at: 2000, key: 'a' },
    ],
  },
  // The database loses the reads of the first and third bursts.
  lossy: {
    bursts: [
      { at: 300, node: 'n1', key: 'a', drop: true },
      { at: 1000, node: 'n1', key: 'a' },
      { at: 1700, node: 'n1', key: 'b', drop: true },
      { at: 2400, node: 'n1', key: 'b' },
      { at: 3100, node: 'n1', key: 'a' },
    ],
    changes: [{ at: 1500, key: 'a' }],
  },
};

export function cacheStampede(sim, { scenario = 'bursts', count = 25, delay = [80, 120], steady = null }) {
  const plan = BURSTS[scenario] ?? { bursts: [], changes: [] };
  const drops = plan.bursts.filter((b) => b.drop);
  const db = makeDb(sim, {
    delay,
    decide: (call) => {
      const t = call.arrived;
      if (call.type === 'read' && drops.some((b) => b.key === call.key && t >= b.at && t < b.at + 100)) return { silent: true };
      return null;
    },
  });
  for (const c of plan.changes) sim.at(c.at, () => db.change(c.key));
  const results = [];
  plan.bursts.forEach((b, bi) => {
    for (let i = 0; i < count; i++) {
      sim.at(b.at + i * 0.2, () => ask(sim, results, b.node, { type: 'get', key: b.key }, { client: 'c' + (1 + (i % 5)), burst: bi }));
    }
  });

  // A hot key asked for every few milliseconds, on every node, for the whole test.
  if (steady) {
    const end = sim.duration - 500;
    for (let t = 300; t < end; t += steady.change) sim.at(t, () => db.change('hot'));
    let i = 0;
    for (let t = 200; t < end; t += steady.gap, i++) {
      const node = sim.nodeIds[i % sim.nodeIds.length];
      const client = 'c' + (1 + (i % 4));
      sim.at(t, () => ask(sim, results, node, { type: 'get', key: 'hot' }, { client }));
    }
  }

  return () => {
    const isDropBurst = (r) => r.burst != null && plan.bursts[r.burst].drop;
    // Bursts on a key whose read was lost earlier have a check of their own below.
    const afterDrop = (r) => r.burst != null && plan.bursts.slice(0, r.burst).some((b) => b.drop && b.key === plan.bursts[r.burst].key);
    const checks = [
      unanswered(
        'Every request gets a reply',
        results,
        `Requests waiting for the same read must all be answered when it completes, and also when it fails: give up on a database read after ${DB_TIMEOUT}ms.`,
      ),
      realValues(results, db, {
        allowErrors: (r) => isDropBurst(r) || afterDrop(r),
        errorAdvice: 'A get that misses while a read of the same key is in flight waits for that read and passes on its value; only a read that fails may end in an error.',
      }),
      fresh(results, db),
    ];

    // Database reads per burst, from the node the burst hit.
    let worst = null;
    plan.bursts.forEach((b, bi) => {
      const next = plan.bursts.find((x, xi) => xi > bi && x.node === b.node && x.key === b.key);
      const until = Math.min(b.at + 600, next ? next.at : Infinity);
      const reads = db.reads((c) => c.src === b.node && c.key === b.key && c.sent >= b.at && c.sent < until).length;
      if (reads > PER_BURST && (!worst || reads > worst.reads)) worst = { b, reads };
    });
    if (plan.bursts.length) {
      checks.push(
        check(
          `At most ${PER_BURST} database reads per burst`,
          !worst,
          worst &&
            `At ${ms(worst.b.at)} ${count} gets for "${worst.b.key}" reached ${worst.b.node} within 5ms while it had no fresh entry, and ${worst.b.node} sent ${plural(worst.reads, 'database read')} for it; at most ${PER_BURST} are allowed. Keep a map of reads in flight: the first miss starts the read, later misses for the same key wait for that read instead of starting their own.`,
        ),
      );
    }

    if (steady) {
      const allowed = Math.ceil(((sim.duration - 700) / TTL) * 1.5) + 2;
      const greedy = sim.nodeIds
        .map((node) => ({ node, reads: db.reads((c) => c.src === node && c.key === 'hot').length }))
        .sort((a, b) => b.reads - a.reads)[0];
      checks.push(
        check(
          `About one read of the hot key per node per expiry`,
          greedy.reads <= allowed,
          `${greedy.node} read "hot" from the database ${greedy.reads} times; at most ${allowed} are allowed (one per ${TTL}ms expiry, plus slack). Each time the entry expires, every get that arrives during the ${delay[0]}–${delay[1]}ms read is a miss. Let them wait for the read that is already in flight.`,
        ),
      );
    }

    if (drops.length) {
      const stuck = results.find((r) => afterDrop(r) && !isDropBurst(r) && !isGetOk(r));
      checks.push(
        check(
          'A failed read does not block later requests',
          !stuck,
          stuck &&
            `The database lost the read for "${stuck.key}" during an earlier burst. At ${ms(stuck.invoked)} ${stuck.node} was asked for "${stuck.key}" again and answered ${stuck.res ? j(stuck.res) : 'nothing'}. When a read fails or times out, answer its waiters with an error and forget the read, so the next miss starts a new one.`,
        ),
      );
    }
    return checks;
  };
}

// --------------------------------------------------------- invalidation

const STAGE_HINTS = {
  'stale fill':
    'The read was sent before the write and its answer arrived after the invalidation, so storing it put the old value back. Keep a per-key generation: bump it on every invalidation, and store a read only if the generation is the same as when the read started.',
  'joined an old read':
    'The get arrived while a read sent before the write was still in flight, and waited for that read. An invalidation must also forget the reads in flight for that key, so that a later miss starts a fresh read.',
  'acknowledged too early':
    'put_ok reached the client before the other nodes had dropped their copies. Wait until every peer has acknowledged the invalidation before replying put_ok.',
  'lost invalidation':
    'The first message the writer sent to that node after the write was lost. node.rpc resolves to null when no answer comes back in time: send the invalidation again until the peer confirms it, and only then reply put_ok.',
};

// A best guess at why get `r` returned a value older than acknowledged put `pr`.
function diagnose(db, r, pr) {
  const written = db.history(pr.key)[db.firstIndexOf(pr.key, pr.req.value)].t;
  // The database read whose answer this get passed on.
  const source = db
    .reads((c) => c.src === r.node && c.key === r.key && c.value === r.res.value && c.heard != null && c.heard <= r.done)
    .pop();
  if (source && source.answered < written && source.heard >= r.invoked) return STAGE_HINTS['joined an old read'];
  if (source && source.answered < written && source.heard > written) {
    return `${r.node} passed on a read that the database answered at ${ms(source.answered)}, before the write took effect at ${ms(written)}, and stored it although its answer only arrived at ${ms(source.heard)}. Invalidate after the write completes (not only before it), and keep a per-key generation so that a read overtaken by an invalidation does not store its answer.`;
  }
  if (pr.node === r.node) return `${r.node} took the put itself and kept serving its old entry. Drop the key from the cache when the write completes, before replying put_ok.`;
  // Requests (not replies) from the writer to this node, sent between the put and the get,
  // that look like they are about this key.
  const about = db.nodeMsgs.filter(
    (m) =>
      m.src === pr.node &&
      m.dest === r.node &&
      m.body.in_reply_to == null &&
      (m.body.key === undefined || m.body.key === r.key) &&
      m.t0 >= pr.invoked &&
      m.t0 <= r.invoked,
  );
  if (about.some((m) => !m.dropped && m.t1 != null && m.t1 < r.invoked)) {
    return `${pr.node} sent ${r.node} a message about "${r.key}" that arrived before the get, yet ${r.node} kept serving its old copy. The invalidate handler must delete the entry.`;
  }
  const lost = about.find((m) => m.dropped);
  if (lost) return `The message ${pr.node} sent to ${r.node} at ${ms(lost.t0)} was lost, and put_ok went out anyway. node.rpc resolves to null when no answer comes back in time: send the invalidation again until the peer confirms it, and only then reply put_ok.`;
  const late = about.find((m) => !m.dropped);
  if (late) return `The message ${pr.node} sent to ${r.node} at ${ms(late.t0)} ${late.t1 != null ? `only arrived at ${ms(late.t1)}` : 'had not arrived'}, after the get. ${STAGE_HINTS['acknowledged too early']}`;
  return `${r.node} never heard from ${pr.node} about the write. The node that takes a put must tell every other node to drop the key, and wait for them, before replying put_ok.`;
}

// Staged interleavings, each a short sequence of client steps on its own key.
//   hold:  the reader's first read of the key is answered at once but its
//          reply is held back `hold` ms, so a put and its invalidation overtake it
//   slow:  messages the writer sends to other nodes just after the put are delayed
//   lose:  the first messages the writer sends to `lose.dest` after the put are lost
const STAGES = {
  races: [
    // A slow read, overtaken by a write and its invalidation, on another node.
    { at: 400, key: 'r1', reader: 'n2', writer: 'n1', kind: 'stale fill', hold: 150 },
    // A get after the write must not wait for a read that started before it.
    { at: 1300, key: 'r2', reader: 'n3', writer: 'n1', kind: 'joined an old read', hold: 220 },
    // The same race as the first, all on the node that took the write.
    { at: 2200, key: 'r3', reader: 'n1', writer: 'n1', kind: 'stale fill', hold: 150 },
    // Invalidations from the writer are slowed down; the client reads every node right after put_ok.
    { at: 3000, key: 'r4', writer: 'n1', kind: 'acknowledged too early', slow: 80 },
  ],
  lossy: [
    { at: 1500, key: 'r5', writer: 'n2', kind: 'lost invalidation', lose: { dest: 'n3', window: 100 } },
    { at: 3200, key: 'r6', writer: 'n3', kind: 'lost invalidation', lose: { dest: 'n1', window: 100 } },
  ],
};

export function cacheInvalidation(sim, { keys = 6, getGap = 8, putGap = 100, staged = null, maxShare = 0.6 }) {
  const stages = STAGES[staged] ?? [];
  const holds = stages.filter((s) => s.hold);
  const db = makeDb(sim, {
    delay: holds.length ? [20, 30] : [20, 50],
    decide: (call) => {
      const s = holds.find((x) => call.type === 'read' && call.key === x.key && call.src === x.reader && call.arrived >= x.at && call.arrived < x.at + 30);
      return s ? { hold: s.hold } : null;
    },
  });
  // Puts of the warm-then-write stages are sent at s.at + 300; the window covers their invalidations.
  db.nodeDelay = ({ src, dest, t }) => {
    for (const s of stages) {
      if (src !== s.writer || t < s.at + 300) continue;
      if (s.slow && t < s.at + 450) return s.slow;
      if (s.lose && dest === s.lose.dest && s.putSent != null && t >= s.putSent && t < s.putSent + s.lose.window) return 'drop';
    }
    return undefined;
  };

  const results = [];
  let w = 0;
  const putValue = (key) => key + ':w' + ++w;
  const end = sim.duration - 600;

  // Background traffic: skewed gets on every node, and puts through every node.
  const cum = zipf(keys, 1);
  let i = 0;
  for (let t = 200; t < end; t += getGap, i++) {
    const key = 'k' + draw(sim, cum);
    const node = sim.pick(sim.nodeIds);
    const client = 'c' + (1 + (i % 3));
    sim.at(t, () => ask(sim, results, node, { type: 'get', key }, { client }));
  }
  let p = 0;
  for (let t = 260; t < end; t += putGap, p++) {
    const key = 'k' + draw(sim, cum);
    const node = sim.pick(sim.nodeIds);
    const client = 'c' + (4 + (p % 2));
    sim.at(t, () => ask(sim, results, node, { type: 'put', key, value: putValue(key) }, { client }));
  }

  for (const s of stages) {
    if (s.hold) {
      // get (its read is slow) ... put ... get
      sim.at(s.at, async () => {
        ask(sim, results, s.reader, { type: 'get', key: s.key }, { client: 'c6' });
        await sim.sleep(60);
        await ask(sim, results, s.writer, { type: 'put', key: s.key, value: putValue(s.key) }, { client: 'c7' }).promise;
        if (s.kind === 'joined an old read') ask(sim, results, s.reader, { type: 'get', key: s.key }, { client: 'c7', stage: s });
        await sim.sleep(Math.max(0, s.at + 400 - sim.now()));
        ask(sim, results, s.reader, { type: 'get', key: s.key }, { client: 'c7', stage: s });
      });
    } else {
      // Warm every node, then put, then read every node straight after put_ok.
      sim.at(s.at, () => sim.nodeIds.forEach((node, k) => ask(sim, results, node, { type: 'get', key: s.key }, { client: 'c' + (6 + k) })));
      sim.at(s.at + 300, async () => {
        s.putSent = sim.now();
        await ask(sim, results, s.writer, { type: 'put', key: s.key, value: putValue(s.key) }, { client: 'c7' }).promise;
        for (const node of sim.nodeIds) ask(sim, results, node, { type: 'get', key: s.key }, { client: 'c7', stage: s });
      });
    }
  }

  return () => {
    const gets = results.filter((r) => r.req.type === 'get');
    const puts = results.filter((r) => r.req.type === 'put');
    const acked = puts.filter((r) => r.res && r.res.type === 'put_ok');

    const badPut = puts.find((r) => r.res && r.res.type !== 'put_ok');
    const lostPut = acked.find((r) => {
      const i = db.firstIndexOf(r.key, r.req.value);
      return i < 0 || db.history(r.key)[i].t > r.done;
    });

    // A get sent after put_ok must return that put's value or a newer one.
    let stale = null;
    let staleCount = 0;
    for (const r of gets) {
      if (!isGetOk(r)) continue;
      const got = db.lastIndexOf(r.key, r.res.value);
      if (got < 0) continue;
      let need = null;
      for (const pr of acked) {
        if (pr.key !== r.key || pr.done > r.invoked) continue;
        const i = db.firstIndexOf(pr.key, pr.req.value);
        if (i >= 0 && (!need || i > need.i)) need = { i, pr };
      }
      if (need && got < need.i) {
        staleCount++;
        // Report background traffic first (its causes are the basic ones), then staged races.
        if (!stale || (stale.r.stage && !r.stage)) stale = { r, need };
      }
    }

    // Did every staged sequence run to the end?
    const stagedGets = gets.filter((r) => r.stage);
    const expected = stages.reduce((n, s) => n + (s.hold ? (s.kind === 'joined an old read' ? 2 : 1) : sim.nodeIds.length), 0);

    const staleDetail = (() => {
      if (!stale) return '';
      const { r, need } = stale;
      const base = `${plural(staleCount, 'get')} returned a value older than a put that had already been acknowledged. ${r.stage ? `Staged "${r.stage.kind}" on "${r.key}": ` : 'First: '}at ${ms(r.invoked)} the client asked ${r.node} for "${r.key}" and got ${j(r.res.value)}, but put ${j(need.pr.req.value)} through ${need.pr.node} had already been acknowledged (at ${ms(need.pr.done)}).`;
      return `${base} ${r.stage ? STAGE_HINTS[r.stage.kind] : diagnose(db, r, need.pr)}`;
    })();

    const checks = [
      unanswered('Every request gets a reply', results, 'A put is answered with put_ok once the database has the value and the caches have let go of the old one.'),
      check(
        'Puts are written to the database before put_ok',
        !badPut && !lostPut,
        badPut
          ? `${badPut.node} answered ${j(badPut.req)} with ${j(badPut.res)}. Write the value with { type: "write", key, value } and reply { type: "put_ok" } once the database answers write_ok.`
          : lostPut &&
              `${lostPut.node} acknowledged ${j(lostPut.req)} at ${ms(lostPut.done)}, but the database ${db.firstIndexOf(lostPut.key, lostPut.req.value) < 0 ? 'never received that value' : 'only stored it afterwards'}. Wait for write_ok before replying put_ok.`,
      ),
      realValues(gets, db),
      check('No get returns a value older than an acknowledged put', !stale, staleDetail),
      readShare(
        'The cache still absorbs reads',
        gets.length,
        db.reads().length,
        maxShare,
        'Invalidate only the key that was written, and only when it is written; other keys and other gets should still be served from memory.',
      ),
    ];
    if (stages.length) {
      checks.push(
        check(
          'The staged sequences completed',
          stagedGets.length === expected && stagedGets.every((r) => r.res),
          `Only ${stagedGets.filter((r) => r.res).length} of the ${expected} staged gets were sent and answered. Each staged sequence waits for put_ok before its last gets, so a put that is never acknowledged stops it.`,
        ),
      );
    }
    return checks;
  };
}
