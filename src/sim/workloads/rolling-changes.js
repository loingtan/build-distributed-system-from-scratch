// Workloads for the "Rolling changes" track.
//
// Every task runs the same small service: a user directory spread over the
// cluster. A key's home is node.nodeIds[sum of its character codes % node
// count], and any node accepts a request and forwards it to the home:
//
//   -> { type: 'put', key, name }   <- { type: 'put_ok' }
//   -> { type: 'get', key }         <- { type: 'get_ok', name }
//
// Version 1 is in every starter and must not change. It forwards with
// fwd_put { key, name } / fwd_get { key } and keeps node.disk.users[key] = { name }.
//
// The harness rolls a version out the way an operator would: it stops sending
// traffic that involves a node (drain), crashes it, sets node.disk.version,
// restarts it and lets traffic back in, one node at a time. Each key has at
// most one client request in flight, so the checker always knows exactly what
// a get must return.

import { check, j, plural } from '../workloads.js';

const DRAIN = 250; // no new traffic involving a node from this long before it stops
const DOWN = 100; // how long it is stopped
const WARM = 100; // traffic returns this long after it restarts
const TIMEOUT = 500; // client timeout
const FLAG = 'cache_reads';
const WINDOW = 20; // roll-feature-flags: the guard looks at this many cache reads
const LIMIT = 5; // and turns the flag off at this many errors among them
const HEALTHY = 7; // a healthy cache fails every 7th read from a node
const DEGRADED = 2; // a degraded one every 2nd

const ms = (t) => `${Math.round(t)}ms`;
// A reply as the learner wrote it, without the in_reply_to the engine adds.
const show = (res) => {
  if (!res || typeof res !== 'object') return j(res);
  const { in_reply_to, ...rest } = res;
  return j(rest);
};

export function homeOf(key, ids) {
  let sum = 0;
  for (let i = 0; i < key.length; i++) sum += key.charCodeAt(i);
  return ids[sum % ids.length];
}

const FIRST = ['Ada', 'Grace', 'Alan', 'Edsger', 'Barbara', 'Leslie', 'Donald', 'Frances', 'John', 'Radia', 'Ken', 'Dennis', 'Margaret', 'Tony', 'Butler', 'Shafi', 'Niklaus'];
const LAST = ['Lovelace', 'Hopper', 'Turing', 'Dijkstra', 'Liskov', 'Lamport', 'Knuth', 'Allen', 'Backus', 'Perlman', 'Thompson', 'Ritchie', 'Hamilton', 'Hoare', 'Lampson', 'Goldwasser', 'Wirth', 'Kay', 'Cerf'];
// 17 x 19 combinations: two puts to one key never write the same name in a test.
const nameFor = (i) => `${FIRST[i % FIRST.length]} ${LAST[(i * 7 + 3) % LAST.length]}`;
const splitName = (name) => {
  const i = name.indexOf(' ');
  return [name.slice(0, i), name.slice(i + 1)];
};

// Everything version 1 ever sends: message types and their fields
// (type, msg_id and in_reply_to are always allowed).
const V1 = {
  node: { fwd_put: ['key', 'name'], fwd_get: ['key'], fwd_put_ok: [], fwd_get_ok: ['name'] },
  client: { put_ok: [], get_ok: ['name'], error: ['code'] },
};

// ------------------------------------------------------------ the driver

function drive(sim, p) {
  const ids = sim.nodeIds;
  const nodeSet = new Set(ids);
  const keys = Array.from({ length: p.keys ?? 12 }, (_, i) => `u${i + 1}`);
  const home = (key) => homeOf(key, ids);
  const h = {
    ids,
    keys,
    home,
    version: Object.fromEntries(ids.map((n) => [n, 1])),
    ops: [], // { kind, entry, key, home, client, name, invoke, complete, res, entryV, homeV, expect, tainted, final }
    model: new Map(), // key -> the latest acknowledged put
    tainted: new Set(), // keys whose last put failed: their value is unknown
    busy: new Set(), // keys with a request in flight
    blocked: [], // { node, start, end }: no new traffic involves the node
    steps: [], // steps taken so far: { at, node, from, to }
    allAt: null, // restart time of the step after which every node runs version 2 for good
    onCrash: [],
    onRestart: [],
    sendHooks: [],
    deliverHooks: [],
    v1Problem: null,
    finalAt: p.finalAt ?? sim.duration - 600,
  };
  sim.onSend = (msg) => {
    for (const f of h.sendHooks) f(msg);
  };
  sim.onDeliver = (msg) => {
    for (const f of h.deliverHooks) f(msg);
  };

  // ---- the rollout: [at, node, version] steps, each a drain, crash and restart
  let steps = (p.steps ?? []).map(([at, node, to]) => ({ at, node, to }));
  if (p.rollout) {
    const order = [...ids];
    if (p.rollout.shuffle) {
      for (let i = order.length - 1; i > 0; i--) {
        const k = Math.floor(sim.rand() * (i + 1));
        [order[i], order[k]] = [order[k], order[i]];
      }
    }
    steps = [...order.map((node, i) => ({ at: p.rollout.start + i * p.rollout.every, node, to: 2 })), ...steps];
  }
  steps.sort((a, b) => a.at - b.at);
  const v = { ...h.version };
  for (const s of steps) {
    v[s.node] = s.to;
    if (!ids.every((n) => v[n] === 2)) h.allAt = null;
    else if (h.allAt === null) h.allAt = s.at + DOWN;
  }
  h.plan = steps;
  for (const s of steps) {
    h.blocked.push({ node: s.node, start: s.at - DRAIN, end: s.at + DOWN + WARM });
    sim.at(s.at, () => {
      sim.crash(s.node);
      s.from = h.version[s.node];
      for (const f of h.onCrash) f(s);
      // After the crash, sim.disk returns the disk the restarted node will get.
      sim.disk(s.node).version = s.to;
      h.version[s.node] = s.to;
      h.steps.push(s);
    });
    sim.at(s.at + DOWN, () => {
      sim.restart(s.node);
      for (const f of h.onRestart) f(s);
    });
  }
  h.available = (node, t) => !h.blocked.some((b) => b.node === node && t >= b.start && t < b.end);

  // ---- clients
  let puts = 0;
  h.request = async (kind, entry, key, client, final = false) => {
    const op = {
      kind,
      entry,
      key,
      home: home(key),
      client,
      final,
      invoke: sim.now(),
      complete: null,
      res: null,
      entryV: h.version[entry],
      homeV: h.version[home(key)],
      expect: h.model.get(key) ?? null,
      tainted: h.tainted.has(key) || h.busy.has(key),
    };
    if (kind === 'put') op.name = nameFor(puts++);
    h.ops.push(op);
    if (!final) h.busy.add(key);
    const body = kind === 'put' ? { type: 'put', key, name: op.name } : { type: 'get', key };
    op.res = await sim.clientRpc(entry, body, { client, timeout: TIMEOUT });
    op.complete = sim.now();
    if (kind === 'put') {
      if (op.res?.type === 'put_ok') {
        h.model.set(key, op);
        h.tainted.delete(key);
      } else {
        h.tainted.add(key);
      }
    }
    if (!final) h.busy.delete(key);
    return op;
  };

  const gap = p.gap ?? 8;
  const until = p.until ?? sim.duration - 900;
  const getShare = p.getShare ?? 0.5;
  for (let t = 100; t < until; t += gap) {
    sim.at(t, () => {
      const now = sim.now();
      const up = ids.filter((n) => h.available(n, now));
      const free = keys.filter((k) => !h.busy.has(k) && h.available(home(k), now));
      if (!up.length || !free.length) return;
      const entry = sim.pick(up);
      const key = sim.pick(free);
      const client = `c${1 + Math.floor(sim.rand() * 3)}`;
      h.request(sim.rand() < getShare ? 'get' : 'put', entry, key, client);
    });
  }
  // Every key through every node, in parallel, once the traffic has stopped.
  sim.at(h.finalAt, () => {
    for (const key of keys) for (const node of ids) h.request('get', node, key, 'c8', true);
  });

  // ---- version-1 nodes must send exactly what version 1 sends
  const asked = new Map(ids.map((n) => [n, new Set()]));
  h.deliverHooks.push((msg) => {
    if (msg.kind === 'client' && nodeSet.has(msg.dest) && msg.body.msg_id != null) asked.get(msg.dest).add(msg.body.msg_id);
  });
  h.sendHooks.push((msg) => {
    if (h.v1Problem || !nodeSet.has(msg.src) || h.version[msg.src] !== 1) return;
    const b = msg.body;
    const fields = msg.kind === 'node' ? V1.node[b.type] : msg.kind === 'client' ? V1.client[b.type] : undefined;
    const said = `At ${ms(sim.now())}, ${msg.src}, still on version 1, sent ${j(b)} to ${msg.dest}`;
    if (!fields) {
      h.v1Problem = `${said}. Version 1 never sends ${msg.kind !== 'node' && msg.kind !== 'client' ? 'anything to a service' : `a "${b.type}" message`}.`;
      return;
    }
    const extra = Object.keys(b).find((k) => k !== 'type' && k !== 'msg_id' && k !== 'in_reply_to' && !fields.includes(k));
    if (extra) h.v1Problem = `${said}. Version 1 never sends the field "${extra}".`;
    else if (msg.kind === 'client' && !asked.get(msg.src).has(b.in_reply_to)) {
      h.v1Problem = `${said}, an answer to a request that client never sent to ${msg.src}. Version 1 only answers requests it received itself.`;
    }
  });
  return h;
}

// ------------------------------------------------------------ shared checks

const vname = (h, node, version) => `${node} (version ${version})`;

function describe(h, op) {
  const what = op.kind === 'put' ? `put ${op.key}=${j(op.name)}` : `get ${op.key}`;
  const where = op.home === op.entry ? `${vname(h, op.entry, op.entryV)}, its home` : `${vname(h, op.entry, op.entryV)}, home ${vname(h, op.home, op.homeV)}`;
  return `${what} sent to ${where}, at ${ms(op.invoke)}`;
}

function baseChecks(h, sim, { failAdvice = () => '', readAdvice = () => '' } = {}) {
  const failed = h.ops.filter((op) => op.res?.type !== `${op.kind}_ok`);
  const f = failed[0];
  const wrong = h.ops.find(
    (op) => op.kind === 'get' && op.res?.type === 'get_ok' && !op.tainted && (op.res.name ?? null) !== (op.expect ? op.expect.name : null),
  );
  const finals = h.ops.filter((op) => op.final);
  const finalsDone = finals.length === h.keys.length * h.ids.length && finals.every((op) => op.complete !== null);
  let wrongDetail = '';
  if (wrong) {
    const got = wrong.res.name === undefined ? `${show(wrong.res)}, with no name` : j(wrong.res.name);
    const want = wrong.expect
      ? `the latest put, ${describe(h, wrong.expect)}, wrote ${j(wrong.expect.name)}`
      : `nothing had been written to ${wrong.key}, so the answer is null`;
    wrongDetail = `${describe(h, wrong)} returned ${got}; ${want}.${readAdvice(wrong)}`;
  }
  return [
    check(
      'Every client request succeeds',
      !f,
      f && `${plural(failed.length, 'request')} of ${h.ops.length} failed. First: ${describe(h, f)}, ${f.res ? `was answered ${show(f.res)}` : `got no reply within ${TIMEOUT}ms`}.${failAdvice(f)}`,
    ),
    check('Every get returns the latest name', !wrong, wrongDetail),
    check(
      'Version-1 nodes behave exactly like version 1',
      !h.v1Problem,
      `${h.v1Problem} Leave runVersion1 as it is: in a real rollout it is already running on the machines that have not been upgraded, and nobody can change it there. Put new behaviour in runVersion2.`,
    ),
    check(
      'The final reads all ran',
      finalsDone,
      `The harness reads every key through every node at ${ms(h.finalAt)}; ${finals.filter((op) => op.complete !== null).length} of ${h.keys.length * h.ids.length} of those reads finished before the test ended.`,
    ),
  ];
}

// A request between two versions: say who needs what.
function readCross(op) {
  const mine = crossAdvice(op);
  if (mine || !op.expect) return mine;
  const theirs = crossAdvice(op.expect);
  return theirs && ` The latest put went from ${op.expect.entry} (version ${op.expect.entryV}) to ${op.expect.home} (version ${op.expect.homeV}).${theirs}`;
}

function crossAdvice(op) {
  if (op.entry === op.home || op.entryV === op.homeV) return '';
  return op.entryV === 1
    ? ` A version-1 node forwards with fwd_put { key, name } and fwd_get { key }, and expects fwd_put_ok and fwd_get_ok { name } back. Version 2 must keep answering exactly those.`
    : ` The home runs version 1: it answers only fwd_put and fwd_get, reads only key and name, and ignores every other field.`;
}

// ------------------------------------------------- 1. tolerant reader

export function rollTolerant(sim, p = {}) {
  const h = drive(sim, p);
  return () => {
    const gets = h.ops.filter((op) => op.kind === 'get' && op.res?.type === 'get_ok' && !op.tainted);
    const badBy = gets.find((op) => {
      const by = op.res.by ?? null;
      return by !== null && by !== (op.expect ? op.expect.client : null);
    });
    let badDetail = '';
    if (badBy) {
      const e = badBy.expect;
      badDetail = e
        ? `${describe(h, badBy)} returned by: ${j(badBy.res.by)}, but the latest put of ${badBy.key} came from ${e.client} (${describe(h, e)}).${
            e.entryV === 1 && e.entry !== e.home
              ? ' That put was forwarded by a version-1 node, which sends no "by". A missing field means the writer is unknown; keeping the previous writer reports the wrong one.'
              : ''
          }`
        : `${describe(h, badBy)} returned by: ${j(badBy.res.by)}, but nothing had been written to ${badBy.key}.`;
    }
    const later = gets.filter((op) => op.expect && h.allAt !== null && op.expect.invoke >= h.allAt);
    const missing = later.find((op) => (op.res.by ?? null) !== op.expect.client);
    return [
      ...baseChecks(h, sim, { failAdvice: crossAdvice, readAdvice: readCross }),
      check('A reported writer is always the right one', !badBy, badDetail),
      check(
        'Once every node runs version 2, gets say who wrote the name',
        later.length > 0 && !missing,
        missing
          ? `Every node has run version 2 since ${ms(h.allAt)}. ${describe(h, missing.expect)} came from ${missing.expect.client}, but ${describe(h, missing)} returned ${show(missing.res)}. Expected by: ${j(missing.expect.client)}. The node that receives the put knows the client (msg.src); the home does not, unless it is told.`
          : 'No get after the rollout read a name written after it, so the test could not judge this.',
      ),
    ];
  };
}

// ------------------------------------------------- 2. version negotiation

export function rollNegotiate(sim, p = {}) {
  const GRACE = 500;
  const h = drive(sim, p);
  let toV1 = null;
  let oldAfter = null;
  let fwd2After = 0;
  let fwd2Ever = 0;
  h.deliverHooks.push((msg) => {
    if (!toV1 && msg.kind === 'node' && msg.body.type === 'fwd2' && h.version[msg.dest] === 1) {
      toV1 = `${msg.src} sent ${j(msg.body)} to ${msg.dest} at ${ms(msg.t0)}, but ${msg.dest} was still running version 1. Version 1 has no handler for fwd2, so that request was silently lost. Use fwd2 only with a peer you know runs version 2.`;
    }
  });
  h.sendHooks.push((msg) => {
    if (msg.kind === 'node' && msg.body.type === 'fwd2') fwd2Ever++;
    if (msg.kind !== 'node' || h.allAt === null || sim.now() < h.allAt + GRACE) return;
    if (msg.body.type === 'fwd2') fwd2After++;
    if (!oldAfter && (msg.body.type === 'fwd_put' || msg.body.type === 'fwd_get')) {
      oldAfter = `${msg.src} sent ${msg.body.type} to ${msg.dest} at ${ms(sim.now())}, ${ms(sim.now() - h.allAt)} after the last node restarted on version 2. By then every node runs version 2 and should have found out about its peers. Remember that a node that restarts has forgotten what it learned, and that the first node to upgrade has to learn about the ones that follow.`;
    }
  });
  const advice = (op) => {
    if (op.entry !== op.home && op.entryV === 2 && op.homeV === 1) return ' The home runs version 1. If the request went to it as fwd2, version 1 had no handler for it.';
    if (op.entry !== op.home && op.entryV === 2 && op.homeV === 2) return ' Both nodes run version 2. With fwd2, the home must answer the client itself: node.send(client, { type: "get_ok", name, in_reply_to: reply_to }).';
    return crossAdvice(op);
  };
  return () => [
    ...baseChecks(h, sim, { failAdvice: advice, readAdvice: readCross }),
    check('A version-1 node never receives fwd2', !toV1, toV1),
    check(
      `From ${GRACE}ms after the rollout, nodes forward only with fwd2`,
      !oldAfter && fwd2After > 0,
      !fwd2Ever
        ? 'No node ever sent a fwd2. Once a node knows a peer runs version 2, it forwards to that peer with fwd2.'
        : oldAfter ?? `Every node has run version 2 since ${ms(h.allAt)}, but no node sent a single fwd2 after ${ms(h.allAt + GRACE)}.`,
    ),
  ];
}

// ------------------------------------------------- 3. expand and contract

export function rollExpand(sim, p = {}) {
  const h = drive(sim, p);
  const fin = p.finalize;
  const replies = [];
  let expand = null; // a record version 1 could not read correctly
  let dual = null; // a record version 2 wrote without first and last
  let contract = null; // a record not in the final shape after finalize
  let inspected = false;

  const usersOf = (node) => {
    const users = sim.disk(node).users;
    return users && typeof users === 'object' ? users : {};
  };
  // Records whose latest value the checker knows: written, not mid-write.
  const known = (node) => h.keys.filter((k) => h.home(k) === node && h.model.has(k) && !h.tainted.has(k) && !h.busy.has(k));

  function inspectExpand(node, when) {
    const users = usersOf(node);
    for (const key of known(node)) {
      const put = h.model.get(key);
      const rec = users[key];
      if (!rec || rec.name !== put.name) {
        expand ??= `${when}, ${node}'s record for ${key} was ${j(rec ?? null)}, but the latest put wrote ${j(put.name)}. Version 1 reads only "name", so after a rollback it would answer ${j(rec?.name ?? null)}. Until finalize, every record must keep an up-to-date name.`;
      } else if (put.homeV === 2) {
        const [first, last] = splitName(put.name);
        if (rec.first !== first || rec.last !== last) {
          dual ??= `${when}, ${node}'s record for ${key} was ${j(rec)}. ${node} ran version 2 when ${j(put.name)} was written (${describe(h, put)}), so the record should also hold first: ${j(first)} and last: ${j(last)}.`;
        }
      }
    }
  }

  h.onCrash.push((s) => {
    if (s.at < fin) inspectExpand(s.node, `When ${s.node} was stopped at ${ms(s.at)}${s.to === 1 ? ' to be rolled back to version 1' : ''}`);
  });
  sim.at(fin - 1, () => {
    for (const node of h.ids) inspectExpand(node, `Just before finalize, at ${ms(fin)}`);
  });
  sim.at(fin, () => {
    for (const node of h.ids) {
      sim.clientRpc(node, { type: 'finalize' }, { client: 'c9', timeout: TIMEOUT }).then((res) => replies.push({ node, res }));
    }
  });
  sim.at(sim.duration - 30, () => {
    inspected = true;
    for (const node of h.ids) {
      const users = usersOf(node);
      for (const key of known(node)) {
        const want = h.model.get(key).name;
        const [first, last] = splitName(want);
        const rec = users[key];
        const at = `At the end of the test, ${node}'s record for ${key} was ${j(rec ?? null)}`;
        if (!rec || typeof rec !== 'object') contract ??= `${at}; the latest put wrote ${j(want)}.`;
        else if ('name' in rec) {
          contract ??= `${at}. After finalize, every record is rewritten to { first, last } and "name" is gone, and later writes do not bring it back. A node that restarts after finalize must still know it has finalized: keep that on node.disk.`;
        } else if (rec.first !== first || rec.last !== last) {
          contract ??= `${at}, but the latest put wrote ${j(want)}. Was this record last changed by version 1 during a rollback? Version 1 sets "name" and leaves "first" and "last" as they were.`;
        }
      }
    }
  });

  const rollbacks = (node, before) => h.steps.filter((s) => s.node === node && s.to === 1 && s.from === 2 && s.at < before);
  const readAdvice = (op) => {
    const rb = rollbacks(op.home, op.invoke);
    const last = rb[rb.length - 1];
    if (op.invoke >= fin) {
      const r = h.steps.find((x) => x.node === op.home && x.at > fin && x.at < op.invoke);
      return r
        ? ` This read came after finalize, and ${op.home} restarted at ${ms(r.at + DOWN)} since then. A node that restarts must still know it has finalized: keep that on node.disk.`
        : ' This read came after finalize: records now hold only first and last.';
    }
    if (op.homeV === 1 && last) return ` ${op.home} was rolled back to version 1 at ${ms(last.at)}. Version 1 reads only "name", so every record must keep an up-to-date name until finalize.`;
    if (op.homeV === 2 && last) return ` ${op.home} ran version 1 for a while after ${ms(last.at)}. Version 1 sets "name" and leaves "first" and "last" as they were. Before finalize, which of them can version 2 trust?`;
    if (op.homeV === 2) return ' Version 2 also reads records that version 1 wrote, which have only "name".';
    return '';
  };

  return () => {
    const badFin = h.ids.find((n) => replies.find((r) => r.node === n)?.res?.type !== 'finalize_ok');
    const badRes = replies.find((r) => r.node === badFin)?.res;
    return [
      ...baseChecks(h, sim, { readAdvice }),
      check(
        'Every node answers finalize',
        !badFin,
        badFin && `${badFin} ${badRes ? `answered ${show(badRes)}` : 'did not answer'} { "type": "finalize" } sent at ${ms(fin)}. Expected { "type": "finalize_ok" }.`,
      ),
      check('Until finalize, every record keeps the right name', !expand, expand),
      check('Before finalize, version 2 writes first and last as well', !dual, dual),
      check('After finalize, records hold only first and last', inspected && !contract, contract ?? 'The final inspection of the disks did not run.'),
    ];
  };
}

// ------------------------------------------------- 4. feature flags

export function rollFlags(sim, p = {}) {
  const h = drive(sim, p);
  const calls = new Map(); // node -> cache reads received from it
  const flagReplies = [];
  const st = Object.fromEntries(
    h.ids.map((n) => [n, { state: 'off', window: [], since: null, getsOn: 0, callsOn: 0, missAt: null, getEvents: [], callEvents: new Set(), onRestarts: [], failTimes: [], onTimes: [], callTimes: [], changeTimes: [], trippedAt: null, killedAt: null }]),
  );
  let dark = null;
  let killed = null;
  let tripped = null;
  let unknownFlag = null;

  sim.service('cache', (msg, reply) => {
    const n = (calls.get(msg.src) ?? 0) + 1;
    calls.set(msg.src, n);
    if (msg.body.type !== 'read') {
      reply({ type: 'error', code: 'bad_request', text: 'the cache only understands { type: "read", key }' });
      return;
    }
    const period = p.degradeAt != null && sim.now() >= p.degradeAt ? DEGRADED : HEALTHY;
    if (n % period === 0) {
      reply({ type: 'error', code: 'unavailable' });
      return;
    }
    const put = h.model.get(msg.body.key);
    reply({ type: 'read_ok', name: put ? put.name : null });
  });

  for (const [at, target, on] of p.flags ?? []) {
    sim.at(at, () => {
      for (const node of target === 'all' ? h.ids : [target]) {
        const req = { type: 'set_flag', flag: FLAG, on };
        sim.clientRpc(node, req, { client: 'c9', timeout: TIMEOUT }).then((res) => flagReplies.push({ node, req, res, at }));
      }
    });
  }

  h.deliverHooks.push((msg) => {
    const node = msg.dest;
    if (!st[node] || !sim.isUp(node)) return;
    const s = st[node];
    const b = msg.body;
    if (msg.kind === 'client' && b.type === 'set_flag') {
      if (b.flag !== FLAG) unknownFlag ??= b.flag;
      if (b.on) {
        s.state = 'on';
        s.window = [];
        s.since = sim.now();
        s.onTimes.push(sim.now());
        s.changeTimes.push(sim.now());
      } else {
        s.state = 'killed';
        s.killedAt = sim.now();
        s.changeTimes.push(sim.now());
      }
    } else if (msg.kind === 'client' && b.type === 'get' && s.state === 'on') {
      s.getsOn++;
      s.getEvents.push({ eid: sim.eventId(), t: sim.now() });
    } else if (msg.src === 'cache' && s.state === 'on') {
      s.window.push({ ok: b.type === 'read_ok', t: sim.now() });
      if (b.type !== 'read_ok') s.failTimes.push(sim.now());
      if (s.window.length > WINDOW) s.window.shift();
      if (s.window.filter((o) => !o.ok).length >= LIMIT) {
        s.state = 'tripped';
        s.trippedAt = sim.now();
        s.changeTimes.push(sim.now());
      }
    }
  });
  const near = (s, t) => sim.real && s.changeTimes.some((c) => Math.abs(c - t) <= 5);
  h.sendHooks.push((msg) => {
    if (msg.dest !== 'cache' || !st[msg.src]) return;
    const node = msg.src;
    const s = st[node];
    const t = sim.now();
    const restarted = (since) => h.steps.find((x) => x.node === node && x.at > since && x.at < t);
    if (s.state === 'on') {
      s.callsOn++;
      s.callEvents.add(sim.eventId());
      s.callTimes.push(t);
    } else if (s.state === 'off') {
      dark ??= `${node} read the cache at ${ms(t)}, but nobody had turned ${FLAG} on for it. Deploying version 2 must not change behaviour: the new path stays dark until set_flag turns it on for that node.`;
    } else if (s.state === 'killed' && near(s, t)) {
      // Real processes: a get that arrived together with set_flag may be handled either side of it.
    } else if (s.state === 'killed') {
      const r = restarted(s.killedAt);
      killed ??= `set_flag turned ${FLAG} off on ${node} at ${ms(s.killedAt)}, but ${node} read the cache again at ${ms(t)}.${r ? ` It restarted at ${ms(r.at)} in between: keep flags on node.disk.` : ' set_flag with on: false must be stored like on: true, and the flag looked up on every get.'}`;
    } else if (s.state === 'tripped' && near(s, t)) {
      // Real processes: the get may have been handled just before the error reply that tripped it.
    } else if (s.state === 'tripped') {
      const r = restarted(s.trippedAt);
      tripped ??= r
        ? `${node} turned ${FLAG} off after its 5th error at ${ms(s.trippedAt)}, restarted at ${ms(r.at)} and read the cache again at ${ms(t)}. An automatic rollback is a flag change like any other: save it on node.disk, or a restart turns the broken path back on.`
        : `By ${ms(s.trippedAt)}, ${LIMIT} of ${node}'s last ${s.window.length} cache reads had failed, so it should have turned ${FLAG} off; it read the cache again at ${ms(t)}. Count the outcomes of the last ${WINDOW} reads and switch the flag off as soon as ${LIMIT} of them are errors.`;
    }
  });
  h.onRestart.push((x) => {
    const s = st[x.node];
    s.window = []; // memory is gone; so is the node's own count
    if (s.state === 'on') s.onRestarts.push(x.at + DOWN);
  });

  return () => {
    const bad = flagReplies.find((r) => r.res?.type !== 'set_flag_ok');
    // On the real runner, messages that arrive within a few ms of each other have no
    // defined handling order, so gets that close to a flag change are not judged.
    let used = null;
    for (const node of h.ids) {
      const s = st[node];
      if (s.callsOn >= s.getsOn) continue;
      const missing = s.getEvents.filter((g) => !s.callEvents.has(g.eid) && !near(s, g.t));
      if (sim.real && missing.length === 0) continue;
      const miss = missing[0] ?? s.getEvents.find((g) => !s.callEvents.has(g.eid));
      const t = miss ? miss.t : Infinity;
      const lastOn = Math.max(-1, ...s.onTimes.filter((x) => x <= t));
      const restart = s.onRestarts.filter((r) => r <= t && r >= lastOn).pop();
      const calledSince = (from) => s.callTimes.some((x) => x >= from && x <= t);
      const fails = s.failTimes.filter((x) => x < t).length;
      used = `While ${FLAG} was on, ${node} received ${s.getsOn} gets but read the cache only ${s.callsOn} times${miss ? ` (the first get it answered without the cache arrived at ${ms(miss.t)})` : ''}. With the flag on, every get asks the cache first.`;
      if (restart && calledSince(lastOn) && !calledSince(restart)) used += ` ${node} restarted at ${ms(restart)} with the flag on: flags must survive a restart, so keep them on node.disk.`;
      else if (lastOn >= 0 && !calledSince(lastOn)) used += ` set_flag turned it on at ${ms(lastOn)}, and no get since then used the cache. Look the flag up on every get, not once when the node starts.`;
      else if (fails) used += ` By then ${plural(fails, 'cache read')} had failed in all. The healthy cache fails one read in ${HEALTHY}; a node turns the flag off only once ${LIMIT} of its last ${WINDOW} reads failed, and turning the flag on starts a fresh count.`;
      break;
    }
    return [
      ...baseChecks(h, sim, {
        failAdvice: (op) => (op.kind === 'get' && op.entryV === 2 ? ' When the cache answers with an error, answer the get from the normal path instead: the client must not notice.' : crossAdvice(op)),
      }),
      check(
        'set_flag is answered',
        !bad && !unknownFlag,
        bad ? `${bad.node} ${bad.res ? `answered ${show(bad.res)}` : 'did not answer'} ${j(bad.req)} sent at ${ms(bad.at)}. Expected { "type": "set_flag_ok" }.` : `The harness only ever sets ${FLAG}.`,
      ),
      check('The cache stays dark until a node’s flag is on', !dark, dark),
      check('With the flag on, every get asks the cache first', !used, used),
      check('The kill switch takes effect at once', !killed, killed),
      check(`A node turns the flag off after ${LIMIT} errors in its last ${WINDOW} cache reads`, !tripped, tripped),
    ];
  };
}
