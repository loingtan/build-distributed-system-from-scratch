// Workloads for the "Primary and backups" track.
//
// All four share one client model. A few clients write to a key-value store:
//
//   -> { type: 'write', key, value }   <- { type: 'write_ok' }  (or { type: 'error', ... })
//   -> { type: 'read', key }           <- { type: 'read_ok', value }
//
// Each key has a single writer that waits for one write to finish before it
// sends the next, and the values it writes to a key are 1, 2, 3, ... That makes
// "older" and "newer" visible to the checker without looking inside the nodes:
// a node that answers 4 and later 3 for the same key went backwards.
//
// A write that fails or times out may or may not have been applied, so its
// writer abandons that key and carries on with a fresh one.

import { check, j, plural } from '../workloads.js';

const num = (v) => (typeof v === 'number' ? v : 0);
const ms = (t) => `${Math.round(t)}ms`;
const show = (key, v) => `${key}=${v === 0 ? 'null' : v}`;

function drive(sim, {
  clients = 3,
  keysPerClient = 2,
  gap = [10, 50],
  writeTo = sim.nodeIds,
  // Last write well before the end, so even a write that times out (1s) has
  // its outcome known before the final reads.
  until = sim.duration - 1250,
  readFrom = sim.nodeIds,
  readEvery = 40,
  readAfterWrite = null, // node ids to read from as soon as a write is acknowledged
  rotate = 0.1,
} = {}) {
  const h = {
    writes: [],
    reads: [],
    keys: new Map(), // key -> { sent, acked, uncertain }
    finals: [],
    downs: [],
    windows: [], // { start, end, nodes: Set } during which writes sent to `nodes` need not succeed
    replyFrom: new Map(), // msg_id of a client request -> who answered it
    finalAt: sim.duration - 150,
  };
  const nodeSet = new Set(sim.nodeIds);
  sim.onDeliver = (msg) => {
    if (msg.kind === 'client' && !nodeSet.has(msg.dest) && msg.body.in_reply_to != null && !h.replyFrom.has(msg.body.in_reply_to)) {
      h.replyFrom.set(msg.body.in_reply_to, msg.src);
    }
  };

  let nextKey = 0;
  const fresh = () => {
    const key = `k${++nextKey}`;
    h.keys.set(key, { sent: 0, acked: 0, uncertain: false });
    return key;
  };
  const active = [];

  h.read = async (node, key, { probe = true, client = 'c1' } = {}) => {
    const r = { node, key, invoke: sim.now(), complete: null, res: null, value: 0 };
    h.reads.push(r);
    r.res = await sim.clientRpc(node, { type: 'read', key }, { probe, client });
    r.complete = sim.now();
    r.value = num(r.res?.value);
    return r;
  };

  for (let c = 0; c < clients; c++) {
    const client = `c${c + 1}`;
    const mine = Array.from({ length: keysPerClient }, fresh);
    active.push(mine);
    sim.at(150 + c * 7, async () => {
      while (sim.now() < until) {
        const slot = Math.floor(sim.rand() * mine.length);
        const key = mine[slot];
        const info = h.keys.get(key);
        const value = ++info.sent;
        const node = sim.pick(writeTo);
        const w = { node, key, value, client, invoke: sim.now(), complete: null, res: null, ok: false, msgId: null };
        h.writes.push(w);
        const pending = sim.clientRpc(node, { type: 'write', key, value }, { client });
        w.msgId = sim.ops[sim.ops.length - 1].msg.body.msg_id;
        w.res = await pending;
        w.complete = sim.now();
        w.ok = w.res?.type === 'write_ok';
        if (w.ok) {
          info.acked = value;
          if (readAfterWrite) await Promise.all(readAfterWrite.map((n) => h.read(n, key, { probe: false, client })));
          if (sim.rand() < rotate) mine[slot] = fresh();
        } else {
          info.uncertain = true;
          mine[slot] = fresh();
        }
        await sim.sleep(gap[0] + sim.rand() * (gap[1] - gap[0]));
      }
    });
  }

  // Background reads: the same keys on every node, so nodes can be compared.
  for (let t = 100; t < h.finalAt - 100; t += readEvery) {
    sim.at(t, () => {
      const all = [...h.keys.keys()];
      const keys = new Set([sim.pick(sim.pick(active)), sim.pick(sim.pick(active)), sim.pick(all)]);
      for (const key of keys) for (const node of readFrom) if (sim.isUp(node)) h.read(node, key);
    });
  }

  sim.at(h.finalAt, () => {
    for (const key of h.keys.keys()) {
      for (const node of readFrom) h.read(node, key).then((r) => h.finals.push(r));
    }
  });

  h.isolate = (node, start, end = null) => {
    sim.at(start, () => sim.partition([sim.nodeIds.filter((n) => n !== node), [node]]));
    if (end !== null) sim.at(end, () => sim.heal());
  };
  h.crash = (node, start, end) => {
    h.downs.push({ node, start, end });
    sim.at(start, () => sim.crash(node));
    sim.at(end, () => sim.restart(node));
  };
  h.excuse = (nodes, start, end = Infinity) => h.windows.push({ start, end, nodes: new Set(nodes) });
  // Writes that had every reason to succeed.
  h.required = (grace = 300) =>
    h.writes.filter((w) => !h.windows.some((x) => x.nodes.has(w.node) && w.invoke <= x.end + grace && w.complete >= x.start));
  return h;
}

// For each item of `later`: the highest-valued item of `earlier` that had
// already completed when it was sent.
function floors(earlier, later) {
  const done = [...earlier].sort((a, b) => a.complete - b.complete);
  const out = new Map();
  let best = null;
  let i = 0;
  for (const r of [...later].sort((a, b) => a.invoke - b.invoke)) {
    while (i < done.length && done[i].complete <= r.invoke) {
      if (!best || done[i].value > best.value) best = done[i];
      i++;
    }
    out.set(r, best);
  }
  return out;
}

function groupBy(items, keyOf) {
  const groups = new Map();
  for (const item of items) {
    const k = keyOf(item);
    if (!groups.has(k)) groups.set(k, []);
    groups.get(k).push(item);
  }
  return [...groups.values()];
}

const goodReads = (h) => h.reads.filter((r) => r.res?.type === 'read_ok');

function writesSucceed(name, writes, { okFraction = null, advice = '' } = {}) {
  const silent = writes.filter((w) => !w.res && w.complete !== null);
  const failed = writes.filter((w) => w.res && !w.ok);
  const odd = failed.find((w) => w.res.type !== 'error');
  const tail = advice ? ` ${advice}` : '';
  if (!writes.length) return check(name, false, 'No write was sent where it could succeed; the test could not judge anything.');
  if (odd) return check(name, false, `${odd.node} answered ${j(odd.res)} to write ${show(odd.key, odd.value)} at ${ms(odd.invoke)}. A write is answered with { type: "write_ok" } or { type: "error", ... }.`);
  if (silent.length) {
    const w = silent[0];
    return check(name, false, `${plural(silent.length, 'write')} of ${writes.length} got no reply within 1s. First: ${w.node} never answered write ${show(w.key, w.value)} sent at ${ms(w.invoke)}.${tail}`);
  }
  if (okFraction === null) {
    const w = failed[0];
    return check(name, !w, w && `${plural(failed.length, 'write')} of ${writes.length} failed although nothing stood in the way. First: ${w.node} answered ${j(w.res)} to write ${show(w.key, w.value)} at ${ms(w.invoke)}.${tail}`);
  }
  const share = (writes.length - failed.length) / writes.length;
  return check(name, share >= okFraction, `Only ${Math.round(share * 100)}% of ${writes.length} writes succeeded on the lossy network; at least ${Math.round(okFraction * 100)}% should. A lost message is not a dead node: send it again.${tail}`);
}

function readsAnswered(h, advice = '') {
  const wasDown = (r) => h.downs.some((d) => d.node === r.node && r.invoke <= d.end && (r.complete ?? r.invoke) >= d.start);
  const bad = h.reads.find((r) => r.res?.type !== 'read_ok' && !wasDown(r));
  return check(
    'Reads are answered',
    !bad,
    bad && `${bad.node} ${bad.res ? `answered ${j(bad.res)}` : 'did not answer'} { type: "read", key: "${bad.key}" } sent at ${ms(bad.invoke)}. Expected { type: "read_ok", value }.${advice ? ` ${advice}` : ''}`,
  );
}

// A read may only return null or a value some client had already sent for that key.
function readsAreReal(h) {
  const sentAt = new Map(h.writes.map((w) => [`${w.key}:${w.value}`, w.invoke]));
  const bad = goodReads(h).find((r) => {
    const v = r.res.value;
    if (v === null || v === undefined) return false;
    const at = sentAt.get(`${r.key}:${v}`);
    return at === undefined || at > r.complete;
  });
  return check(
    'Reads return values that were written',
    !bad,
    bad && `${bad.node} answered ${j(bad.res.value)} for "${bad.key}" at ${ms(bad.complete)}, but no client had written that value to that key. Missing keys read as null.`,
  );
}

function neverBackwards(name, reads, advice) {
  for (const group of groupBy(reads, (r) => `${r.node} ${r.key}`)) {
    const floor = floors(group, group);
    for (const r of group) {
      const before = floor.get(r);
      if (before && r.value < before.value) {
        return check(name, false, `${r.node} answered ${show(r.key, before.value)} at ${ms(before.complete)} and then ${show(r.key, r.value)} to a read sent at ${ms(r.invoke)}: it went back to an older value. ${advice}`);
      }
    }
  }
  return check(name, true);
}

// Once a write is acknowledged, a read sent afterwards must not miss it.
function ackedWritesVisible(name, h, reads, advice) {
  const acked = h.writes.filter((w) => w.ok);
  const byKey = new Map(groupBy(acked, (w) => w.key).map((g) => [g[0].key, g]));
  let worst = null;
  let count = 0;
  for (const group of groupBy(reads, (r) => r.key)) {
    const floor = floors(byKey.get(group[0].key) ?? [], group);
    for (const r of group) {
      const w = floor.get(r);
      if (w && r.value < w.value) {
        count++;
        if (!worst || r.invoke < worst.r.invoke) worst = { r, w };
      }
    }
  }
  if (!worst) return check(name, true);
  const { r, w } = worst;
  return check(name, false, `${plural(count, 'read')} missed an acknowledged write. First: write ${show(w.key, w.value)} was acknowledged at ${ms(w.complete)} (sent to ${w.node}), yet ${r.node} answered ${show(r.key, r.value)} to a read sent at ${ms(r.invoke)}. ${advice}`);
}

function converged(name, h, nodes, advice) {
  const primary = sim0(h);
  for (const [key, info] of h.keys) {
    const got = new Map(h.finals.filter((r) => r.key === key).map((r) => [r.node, r]));
    for (const node of nodes) {
      const r = got.get(node);
      if (!r || r.res?.type !== 'read_ok') {
        return check(name, false, `${node} did not answer the final read of "${key}" at ${ms(h.finalAt)}.`);
      }
      if (r.value < info.acked) {
        return check(name, false, `At ${ms(h.finalAt)}, ${node} has ${show(key, r.value)}, but write ${show(key, info.acked)} was acknowledged. ${advice}`);
      }
      const ref = got.get(primary);
      if (ref && nodes.includes(primary) && ref.value !== r.value) {
        return check(name, false, `At ${ms(h.finalAt)}, ${node} has ${show(key, r.value)} while the primary ${primary} has ${show(key, ref.value)}. ${advice}`);
      }
    }
  }
  return check(name, true);
}
const sim0 = (h) => h.primary;

// ------------------------------------------------- 1. asynchronous replication

export function replAsync(sim, { cutOff = null, ...opts } = {}) {
  const h = drive(sim, opts);
  h.primary = sim.nodeIds[0];
  const last = sim.nodeIds[sim.nodeIds.length - 1];
  if (cutOff !== null) {
    h.isolate(last, cutOff);
    h.excuse([last], cutOff);
  }
  return () => {
    const reachable = cutOff === null ? sim.nodeIds : sim.nodeIds.filter((n) => n !== last);
    const reads = goodReads(h);
    return [
      writesSucceed('Writes are acknowledged', h.required(), {
        advice: cutOff === null
          ? 'A backup that receives a write passes it to the primary and relays the answer.'
          : `${last} was cut off from ${ms(cutOff)}. The primary must not wait for backups before it answers.`,
      }),
      readsAnswered(h, 'Every node answers reads from its own copy, even when it is cut off.'),
      readsAreReal(h),
      neverBackwards('A node never goes back to an older value', reads, 'Replication messages can overtake each other. Let the primary number its writes, and let a backup ignore a write that is older than what it holds for that key.'),
      converged(cutOff === null ? 'All nodes converge' : 'All reachable nodes converge', h, reachable, 'Every write the primary accepts must reach every backup it can talk to, and the newest one must win there.'),
    ];
  };
}

// -------------------------------------------------- 2. synchronous replication

export function replSync(sim, { partition = null, okFraction = null, ...opts } = {}) {
  const h = drive(sim, { readAfterWrite: sim.nodeIds, ...opts });
  h.primary = sim.nodeIds[0];
  const last = sim.nodeIds[sim.nodeIds.length - 1];
  if (partition) {
    h.isolate(last, partition[0], partition[1]);
    h.excuse(sim.nodeIds, partition[0], partition[1]);
  }
  return () => {
    const reads = goodReads(h);
    const silent = h.writes.filter((w) => !w.res && w.complete !== null);
    const during = partition ? h.writes.filter((w) => w.invoke > partition[0] && w.complete < partition[1]) : [];
    const checks = [
      check(
        'Every write gets an answer',
        silent.length === 0,
        silent.length && `${plural(silent.length, 'write')} got no reply within 1s. First: ${silent[0].node} never answered write ${show(silent[0].key, silent[0].value)} sent at ${ms(silent[0].invoke)}. When a backup cannot be reached, give up after a few hundred ms and answer { type: "error", code: "unavailable" }.`,
      ),
      writesSucceed(partition ? 'Writes succeed while every backup is reachable' : 'Writes are acknowledged', h.required(), { okFraction }),
      readsAnswered(h, 'Reads are answered from the node\'s own copy, so they work during a partition too.'),
      readsAreReal(h),
      ackedWritesVisible(
        'An acknowledged write is visible on every node',
        h,
        reads,
        partition
          ? `(${last} was cut off from ${ms(partition[0])} to ${ms(partition[1])}.) write_ok is a promise that every node has the write. If a backup has not confirmed it, the honest answer is an error.`
          : 'write_ok is a promise that every node has the write: send it only after every backup has confirmed.',
      ),
      neverBackwards('A node never goes back to an older value', reads, 'Retries and reordering can deliver an old write after a newer one; a backup must ignore it.'),
    ];
    if (partition) {
      checks.push(check('The partition test saw writes during the partition', during.length > 0, 'No write completed during the partition, so the test could not judge anything. Are writes being answered at all?'));
    }
    return checks;
  };
}

// ------------------------------------------------------------- 3. catching up

export function replCatchUp(sim, { partition = null, crash = null, okFraction = null, ...opts } = {}) {
  const h = drive(sim, opts);
  h.primary = sim.nodeIds[0];
  const last = sim.nodeIds[sim.nodeIds.length - 1];
  let story = 'Messages were lost on the way.';
  if (partition) {
    h.isolate(last, partition[0], partition[1]);
    h.excuse([last], partition[0], partition[1]);
    story = `${last} was cut off from ${ms(partition[0])} to ${ms(partition[1])}.`;
  }
  if (crash) {
    const node = crash.node === 'primary' ? h.primary : last;
    h.crash(node, crash.at, crash.restart);
    // Read every key from the restarted node at once, before anyone has had
    // a chance to send it what it lost: it must answer from its own disk.
    sim.at(crash.restart, () => {
      for (const key of h.keys.keys()) h.read(node, key);
    });
    h.excuse(crash.node === 'primary' ? sim.nodeIds : [node], crash.at, crash.restart);
    story = `${node} crashed at ${ms(crash.at)} and restarted at ${ms(crash.restart)}.`;
  }
  return () => {
    const reads = goodReads(h);
    return [
      writesSucceed('Writes are acknowledged', h.required(), { okFraction, advice: 'This task is asynchronous again: the primary answers without waiting for backups.' }),
      readsAnswered(h),
      readsAreReal(h),
      neverBackwards('A node never goes back to an older value', reads, `${story} A node that restarts must not serve less than it served before: keep what you have applied on node.disk.`),
      converged('Every node ends up with every acknowledged write', h, sim.nodeIds, `${story} A backup has to find out what it missed and fetch it, in order; a restarted primary must still have everything it acknowledged and must not reuse old sequence numbers.`),
    ];
  };
}

// -------------------------------------------------------- 4. chain replication

export function replChain(sim, opts = {}) {
  const ids = sim.nodeIds;
  const head = ids[0];
  const tail = ids[ids.length - 1];
  const h = drive(sim, { writeTo: [head], readFrom: [tail], readAfterWrite: [tail], ...opts });
  h.primary = tail;

  let stray = null;
  let nodeMsgs = 0;
  sim.onSend = (msg) => {
    if (msg.kind !== 'node') return;
    nodeMsgs++;
    if (!stray && Math.abs(ids.indexOf(msg.src) - ids.indexOf(msg.dest)) !== 1) stray = { src: msg.src, dest: msg.dest, type: msg.body.type, t: sim.now() };
  };

  // Requests sent to the wrong end of the chain.
  const misdirected = [];
  for (let i = 0; i < 6; i++) {
    sim.at(400 + i * 350, async () => {
      const notHead = sim.pick(ids.slice(1));
      const notTail = sim.pick(ids.slice(0, -1));
      const wreq = { type: 'write', key: 'elsewhere', value: i + 1 };
      const rreq = { type: 'read', key: 'k1' };
      const [w, r] = await Promise.all([
        sim.clientRpc(notHead, wreq, { client: 'c9' }),
        sim.clientRpc(notTail, rreq, { client: 'c9' }),
      ]);
      misdirected.push({ node: notHead, req: wreq, res: w, want: { type: 'error', code: 'not_head', head } });
      misdirected.push({ node: notTail, req: rreq, res: r, want: { type: 'error', code: 'not_tail', tail } });
    });
  }
  let stranger = null;
  sim.at(h.finalAt, async () => {
    stranger = await sim.clientRpc(tail, { type: 'read', key: 'elsewhere' }, { probe: true });
  });

  return () => {
    const reads = goodReads(h);
    const acked = h.writes.filter((w) => w.ok);
    const wrongSource = acked.find((w) => h.replyFrom.get(w.msgId) !== tail);
    const lost = misdirected.find((m) => !m.res || Object.keys(m.want).some((k) => m.res[k] !== m.want[k]));
    return [
      writesSucceed('Writes sent to the head are acknowledged', h.writes, { advice: 'A write is done when it reaches the tail. If a link loses a message, the node before the gap must send it again.' }),
      check(
        'write_ok is sent by the tail',
        acked.length > 0 && !wrongSource,
        wrongSource
          ? `Write ${show(wrongSource.key, wrongSource.value)} was acknowledged by ${h.replyFrom.get(wrongSource.msgId)}, not by the tail ${tail}. Only the tail knows the whole chain has the write. Carry the client id and msg_id down the chain so the tail can answer.`
          : 'No write was acknowledged.',
      ),
      check(
        'Requests at the wrong end are redirected',
        !lost,
        lost && `${lost.node} ${lost.res ? `answered ${j(lost.res)}` : 'did not answer'} ${j(lost.req)}. Expected ${j(lost.want)}.`,
      ),
      check(
        'A redirected write is not applied',
        stranger?.type === 'read_ok' && stranger.value === null,
        `The tail answered ${j(stranger)} for key "elsewhere", which was only ever written at nodes that are not the head.`,
      ),
      readsAnswered(h),
      readsAreReal(h),
      ackedWritesVisible('Tail reads see every acknowledged write', h, reads, 'The tail must have applied a write before it sends write_ok.'),
      neverBackwards('Tail reads never go backwards', reads, 'Writes must reach the tail in the order the head accepted them. Hops can reorder or repeat messages, so number the writes and apply them strictly in order.'),
      check(
        'Messages only travel between neighbours',
        !stray && nodeMsgs > 0,
        stray
          ? `${stray.src} sent "${stray.type}" to ${stray.dest} at ${ms(stray.t)}, but they are not next to each other in the chain ${ids.join(' → ')}.`
          : 'No node sent anything to another node.',
      ),
    ];
  };
}

// ============================================================================
// Tasks 5–7: reading from replicas, and more than one leader.
//
// These use their own clients. In tasks 5 and 6 every client carries a
// session token, with the protocol fixed by the briefs:
//
//   every request carries `token` (null at first); every write_ok and read_ok
//   carries a `token`; the client replaces its token with the one in each such
//   reply. What a token contains is up to the nodes.

// Extra delay on node-to-node messages, drawn per message, so that replicas
// lag by different amounts and replication messages overtake each other.
// `bandOf(src, dest)` returns [min, max] ms or nothing.
function addLag(sim, bandOf) {
  sim.intercept = ({ src, dest, kind }) => {
    if (kind !== 'node') return undefined;
    const band = bandOf(src, dest);
    if (!band) return undefined;
    return band[0] + sim.rand() * (band[1] - band[0]);
  };
}

function session(sim, id) {
  const s = { id, token: null, log: [] };
  s.rpc = async (node, body) => {
    const op = { client: id, node, req: body, sentToken: s.token, invoke: sim.now(), complete: null, res: null };
    s.log.push(op);
    op.res = await sim.clientRpc(node, { ...body, token: s.token }, { client: id });
    op.complete = sim.now();
    if (op.res && (op.res.type === 'read_ok' || op.res.type === 'write_ok') && op.res.token !== undefined) s.token = op.res.token;
    return op;
  };
  return s;
}

const showV = (key, v) => `${key}=${v === null || v === undefined ? 'null' : j(v)}`;
const tok = (t) => (t === null || t === undefined ? 'null' : j(t));

function everyAnswered(ops, advice) {
  const silent = ops.filter((o) => !o.res && o.complete !== null);
  const o = silent[0];
  return check(
    'Every request gets an answer',
    !o,
    o && `${plural(silent.length, 'request')} got no reply within 1s. First: ${o.node} never answered ${j({ ...o.req, token: o.sentToken })} from ${o.client}, sent at ${ms(o.invoke)}. ${advice}`,
  );
}

function tokensPresent(ops) {
  const o = ops.find((x) => x.res && (x.res.type === 'read_ok' || x.res.type === 'write_ok') && x.res.token === undefined);
  return check(
    'Replies carry a token',
    !o,
    o && `${o.node} answered ${j(o.res)} to ${j(o.req)} from ${o.client} at ${ms(o.complete)}, with no "token" field. Every write_ok and read_ok must carry a token for the client to send back.`,
  );
}

function opsSucceed(name, ops, okType, advice) {
  const bad = ops.filter((o) => o.res && o.res.type !== okType);
  const o = bad[0];
  return check(
    name,
    !o,
    o && `${plural(bad.length, 'request')} of ${ops.length} did not get ${okType}. First: ${o.node} answered ${j(o.res)} to ${j(o.req)} from ${o.client} (token ${tok(o.sentToken)}) at ${ms(o.complete)}. ${advice}`,
  );
}

function valuesReal(reads, writes) {
  const sentAt = new Map();
  for (const w of writes) {
    const k = `${w.req.key}:${j(w.req.value)}`;
    if (!sentAt.has(k)) sentAt.set(k, w.invoke);
  }
  const bad = reads.find((r) => {
    if (r.res?.type !== 'read_ok') return false;
    const v = r.res.value;
    if (v === null || v === undefined) return false;
    const at = sentAt.get(`${r.req.key}:${j(v)}`);
    return at === undefined || at > r.complete;
  });
  return check(
    'Reads return values that were written',
    !bad,
    bad && `${bad.node} answered ${j(bad.res.value)} for "${bad.req.key}" at ${ms(bad.complete)}, but no client had written that value to that key. Missing keys read as null.`,
  );
}

// ------------------------------------------------- 5. monotonic reads

export function replMonotonic(sim, { clients = 4, keysPerClient = 2, writeShare = 0.35, gap = [5, 30], lag = null, cutOff = null } = {}) {
  const ids = sim.nodeIds;
  const primary = ids[0];
  const last = ids[ids.length - 1];
  // Each backup lags by its own amount; the last one lags most.
  const bands = lag ?? Object.fromEntries(ids.slice(1).map((n, i) => [n, n === last ? [60, 220] : [0, 30 + 30 * i]]));
  addLag(sim, (src, dest) => bands[dest]);

  const until = sim.duration - 1250;
  const finalAt = sim.duration - 150;
  const sessions = [];
  const keys = [];
  const sent = new Map(); // key -> last value sent
  let nextKey = 0;
  const fresh = () => {
    const key = `k${++nextKey}`;
    keys.push(key);
    return key;
  };

  for (let c = 0; c < clients; c++) {
    const s = session(sim, `c${c + 1}`);
    sessions.push(s);
    const mine = Array.from({ length: keysPerClient }, fresh);
    sim.at(150 + c * 7, async () => {
      while (sim.now() < until) {
        if (sim.rand() < writeShare) {
          const slot = Math.floor(sim.rand() * mine.length);
          const key = mine[slot];
          const value = (sent.get(key) ?? 0) + 1;
          sent.set(key, value);
          const op = await s.rpc(sim.pick(ids), { type: 'write', key, value });
          // A write that failed may or may not have happened: start a fresh key.
          if (op.res?.type !== 'write_ok') mine[slot] = fresh();
        } else {
          const key = sim.rand() < 0.4 ? sim.pick(mine) : sim.pick(keys);
          await s.rpc(sim.pick(ids), { type: 'read', key });
        }
        await sim.sleep(gap[0] + sim.rand() * (gap[1] - gap[0]));
      }
    });
  }

  // New clients, with no token yet, reading from the cut-off backup.
  const newcomers = [];
  if (cutOff !== null) {
    sim.at(cutOff, () => sim.partition([ids.filter((n) => n !== last), [last]]));
    let i = 0;
    for (let t = cutOff + 200; t < until; t += 120) {
      sim.at(t, async () => {
        const s = session(sim, `c${100 + ++i}`); // client ids must look like c<number>
        newcomers.push(s);
        await s.rpc(last, { type: 'read', key: sim.pick(keys) });
      });
    }
  }

  const finals = new Map(); // node -> key -> res
  sim.at(finalAt, () => {
    for (const node of ids) {
      finals.set(node, new Map());
      for (const key of keys) {
        sim.clientRpc(node, { type: 'read', key, token: null }, { probe: true }).then((res) => finals.get(node).set(key, res));
      }
    }
  });

  return () => {
    const ops = sessions.flatMap((s) => s.log);
    const reads = ops.filter((o) => o.req.type === 'read');
    const writes = ops.filter((o) => o.req.type === 'write');
    const excused = (o) => cutOff !== null && o.node === last && (o.complete ?? o.invoke) >= cutOff;
    const reachable = cutOff === null ? ids : ids.filter((n) => n !== last);
    const checks = [
      opsSucceed('Writes are acknowledged', writes.filter((o) => !excused(o)), 'write_ok', 'A backup forwards a write to the primary and relays its answer, token included.'),
      everyAnswered([...ops, ...newcomers.flatMap((s) => s.log)], 'A replica that cannot catch up with a token in time should say so: { "type": "error", "code": "unavailable" }.'),
      opsSucceed(
        'Reads are answered',
        reads.filter((o) => !excused(o)),
        'read_ok',
        'Lag here is at most a few hundred ms. A replica that is behind the token should wait for replication to catch up (or ask the primary), not give up at once.',
      ),
    ];
    if (cutOff !== null) {
      const nr = newcomers.flatMap((s) => s.log);
      const bad = nr.find((o) => o.res?.type !== 'read_ok');
      checks.push(check(
        'A cut-off backup still serves new clients',
        nr.length > 0 && !bad,
        bad
          ? `${last} was cut off from ${ms(cutOff)}. New client ${bad.client}, token null, read "${bad.req.key}" there at ${ms(bad.invoke)} and got ${bad.res ? j(bad.res) : 'no answer'}. A client that has seen nothing yet can be served by any replica, however stale: answer it from the local copy.`
          : 'No new client read from the cut-off backup.',
      ));
    }
    checks.push(valuesReal(reads, writes), tokensPresent(ops));

    // Monotonic reads and read-your-writes, per client and key.
    let mono = null;
    let monoCount = 0;
    let ryw = null;
    let rywCount = 0;
    for (const s of sessions) {
      const seen = new Map(); // key -> read that returned the highest value
      const wrote = new Map(); // key -> acknowledged write with the highest value
      for (const o of s.log) {
        if (o.req.type === 'write') {
          if (o.res?.type === 'write_ok' && (!wrote.has(o.req.key) || wrote.get(o.req.key).req.value < o.req.value)) wrote.set(o.req.key, o);
          continue;
        }
        if (o.res?.type !== 'read_ok') continue;
        const key = o.req.key;
        const v = num(o.res.value);
        const prev = seen.get(key);
        if (prev && v < num(prev.res.value)) {
          monoCount++;
          if (!mono) mono = { o, prev };
        }
        const w = wrote.get(key);
        if (w && v < w.req.value) {
          rywCount++;
          if (!ryw) ryw = { o, w };
        }
        if (!prev || v > num(prev.res.value)) seen.set(key, o);
      }
    }
    checks.push(check(
      'Monotonic reads: a client never sees time go backwards',
      !mono,
      mono && `${plural(monoCount, 'read')} went backwards. First: client ${mono.o.client} read ${showV(mono.o.req.key, mono.prev.res.value)} from ${mono.prev.node} (answered at ${ms(mono.prev.complete)}), then read ${showV(mono.o.req.key, mono.o.res.value)} from ${mono.o.node} in a read sent at ${ms(mono.o.invoke)} with token ${tok(mono.o.sentToken)}. ${mono.o.node} was behind what the client had already seen. The token must tell a replica how far the client has seen, and a replica that is behind it must wait (or forward) instead of answering.`,
    ));
    checks.push(check(
      'Read your writes: a client always sees its own writes',
      !ryw,
      ryw && `${plural(rywCount, 'read')} missed the client's own write. First: client ${ryw.o.client} wrote ${showV(ryw.w.req.key, ryw.w.req.value)} through ${ryw.w.node} (acknowledged at ${ms(ryw.w.complete)}, token ${tok(ryw.w.res.token)}), then read ${showV(ryw.o.req.key, ryw.o.res.value)} from ${ryw.o.node} at ${ms(ryw.o.invoke)} sending token ${tok(ryw.o.sentToken)}. Either the token in write_ok does not cover the write, or ${ryw.o.node} answered before it had caught up with the token.`,
    ));

    const missing = reachable.filter((n) => !finals.has(n) || finals.get(n).size < keys.length);
    checks.push(check('The final inspection completed', missing.length === 0, `The final reads at ${ms(finalAt)} did not all come back (${missing.join(', ')}). Reads with token null must be answered at once from the local copy.`));
    let diff = null;
    for (const key of keys) {
      if (diff) break;
      const ref = finals.get(primary)?.get(key);
      for (const node of reachable) {
        const r = finals.get(node)?.get(key);
        if (!r || !ref || r.type !== 'read_ok' || ref.type !== 'read_ok') continue;
        if (j(r.value) !== j(ref.value)) {
          diff = `At ${ms(finalAt)}, ${node} has ${showV(key, r.value)} while the primary ${primary} has ${showV(key, ref.value)}. Every write must still reach every backup it can, in the primary's order.`;
          break;
        }
      }
    }
    checks.push(check(cutOff === null ? 'All nodes converge' : 'All reachable nodes converge', !diff, diff));
    return checks;
  };
}

// ------------------------------------------------- 6. consistent prefix

// Questions and answers. Askers write q<i>; an answerer reads q<i> somewhere
// and then writes a<i> (often through another node); observers read a<i> and
// then q<i>. Every key is written exactly once, so there are no conflicts.
export function replConsistentPrefix(sim, { askers = 2, answerers = 3, observers = 3, slow = [110, 230], fast = [0, 25], slowShare = 0.5, gap = [30, 90] } = {}) {
  const ids = sim.nodeIds;
  // Every ordered pair of nodes is a fast or a slow link, fixed for the run.
  const band = new Map();
  for (const a of ids) for (const b of ids) if (a !== b) band.set(`${a}>${b}`, sim.rand() < slowShare ? slow : fast);
  addLag(sim, (src, dest) => band.get(`${src}>${dest}`));

  const until = sim.duration - 1250;
  const finalAt = sim.duration - 150;
  const sessions = [];
  const questions = []; // { i, acked }
  const answered = []; // indices whose answer was sent
  const claimed = new Set();
  const nodeReads = []; // { node, key, value, invoke, complete }: every read, by any client or probe
  let qn = 0;
  const Q = (i) => 'Q' + i;
  const A = (i) => 'A' + i;
  const ok = (o) => o.res?.type === 'read_ok';
  const record = (o) => {
    if (ok(o)) nodeReads.push({ node: o.node, key: o.req.key, value: o.res.value, invoke: o.invoke, complete: o.complete, who: o.client });
    return o;
  };

  for (let c = 0; c < askers; c++) {
    const s = session(sim, `c${sessions.length + 1}`);
    sessions.push(s);
    sim.at(150 + c * 11, async () => {
      while (sim.now() < until - 300) {
        const i = ++qn;
        const op = await s.rpc(sim.pick(ids), { type: 'write', key: 'q' + i, value: Q(i) });
        if (op.res?.type === 'write_ok') questions.push(i);
        await sim.sleep(gap[0] + sim.rand() * (gap[1] - gap[0]));
      }
    });
  }
  for (let c = 0; c < answerers; c++) {
    const s = session(sim, `c${sessions.length + 1}`);
    sessions.push(s);
    sim.at(170 + c * 13, async () => {
      while (sim.now() < until) {
        // The newest open question: an old one has reached every node already.
        const i = questions.findLast((x) => !claimed.has(x));
        if (i === undefined) {
          await sim.sleep(10);
          continue;
        }
        claimed.add(i);
        // Read the question until it shows up, then answer it.
        for (let tries = 0; tries < 20 && sim.now() < until; tries++) {
          const r = record(await s.rpc(sim.pick(ids), { type: 'read', key: 'q' + i }));
          if (ok(r) && r.res.value !== null && r.res.value !== undefined) {
            answered.push(i);
            await s.rpc(sim.pick(ids), { type: 'write', key: 'a' + i, value: A(i) });
            break;
          }
          await sim.sleep(10);
        }
        await sim.sleep(sim.rand() * 15);
      }
    });
  }
  for (let c = 0; c < observers; c++) {
    const s = session(sim, `c${sessions.length + 1}`);
    sessions.push(s);
    sim.at(200 + c * 9, async () => {
      while (sim.now() < until) {
        const pool = answered.length ? answered.slice(-3) : null;
        if (!pool) {
          await sim.sleep(20);
          continue;
        }
        const i = sim.pick(pool);
        const x = sim.pick(ids);
        record(await s.rpc(x, { type: 'read', key: 'a' + i }));
        record(await s.rpc(sim.rand() < 0.5 ? x : sim.pick(ids), { type: 'read', key: 'q' + i }));
        await sim.sleep(5 + sim.rand() * 20);
      }
    });
  }
  // Token-less probes: every node is asked for a recent answer and then its question.
  for (let t = 300; t < until; t += 10) {
    sim.at(t, () => {
      if (!answered.length) return;
      const i = sim.pick(answered.slice(-2));
      for (const node of ids) {
        (async () => {
          for (const key of ['a' + i, 'q' + i]) {
            const invoke = sim.now();
            const res = await sim.clientRpc(node, { type: 'read', key, token: null }, { probe: true });
            if (res?.type === 'read_ok') nodeReads.push({ node, key, value: res.value, invoke, complete: sim.now(), who: 'a probe' });
          }
        })();
      }
    });
  }

  const finals = new Map();
  let finalKeys = [];
  sim.at(finalAt, () => {
    finalKeys = [];
    for (let i = 1; i <= qn; i++) finalKeys.push('q' + i, 'a' + i);
    for (const node of ids) {
      finals.set(node, new Map());
      for (const key of finalKeys) sim.clientRpc(node, { type: 'read', key, token: null }, { probe: true }).then((res) => finals.get(node).set(key, res));
    }
  });

  return () => {
    const ops = sessions.flatMap((s) => s.log);
    const reads = ops.filter((o) => o.req.type === 'read');
    const writes = ops.filter((o) => o.req.type === 'write');
    const present = (v) => v !== null && v !== undefined;
    const checks = [
      opsSucceed('Writes are acknowledged', writes, 'write_ok', 'Every node accepts writes itself; it does not need to ask anyone. If the client\'s token is ahead of this node, wait for replication first.'),
      everyAnswered(ops, 'Nothing is lost in this task, so waiting for replication to catch up with a token always ends within a few hundred ms.'),
      opsSucceed('Reads are answered', reads, 'read_ok', 'A node that is behind the client\'s token waits for replication to catch up, then answers.'),
      valuesReal(reads, writes),
      tokensPresent(ops),
      check('Questions get answered', answered.length >= 5, `Only ${answered.length} questions were answered, so the test could not judge much. Are writes replicated to every node?`),
    ];

    // Node level: once a node has shown a<i>, it must show q<i> from then on.
    let nodeBad = null;
    let nodeCount = 0;
    const shownA = new Map(); // `${node} ${i}` -> earliest completed read that returned a<i>
    for (const r of nodeReads) {
      if (r.key[0] === 'a' && present(r.value)) {
        const k = `${r.node} ${r.key.slice(1)}`;
        if (!shownA.has(k) || shownA.get(k).complete > r.complete) shownA.set(k, r);
      }
    }
    for (const r of nodeReads) {
      if (r.key[0] !== 'q' || present(r.value)) continue;
      const a = shownA.get(`${r.node} ${r.key.slice(1)}`);
      if (a && a.complete <= r.invoke) {
        nodeCount++;
        if (!nodeBad || r.invoke < nodeBad.r.invoke) nodeBad = { r, a };
      }
    }
    checks.push(check(
      'No node shows an answer before its question',
      !nodeBad,
      nodeBad && `${plural(nodeCount, 'read')} saw an answer without its question. First: ${nodeBad.a.node} showed ${showV(nodeBad.a.key, nodeBad.a.value)} to ${nodeBad.a.who} at ${ms(nodeBad.a.complete)}, then answered ${showV(nodeBad.r.key, null)} to a read from ${nodeBad.r.who} sent at ${ms(nodeBad.r.invoke)}. The answer was written by someone who had read the question, so it depends on it. A node must not apply a write before the writes it depends on, even if it arrives first over a faster link.`,
    ));

    // Session level: once a client has seen a<i>, wherever it reads q<i> next it must see it.
    let sesBad = null;
    let sesCount = 0;
    for (const s of sessions) {
      const sawA = new Map();
      for (const o of s.log) {
        if (!ok(o) || o.req.type !== 'read') continue;
        const i = o.req.key.slice(1);
        if (o.req.key[0] === 'a' && present(o.res.value) && !sawA.has(i)) sawA.set(i, o);
        if (o.req.key[0] === 'q' && !present(o.res.value) && sawA.has(i)) {
          sesCount++;
          if (!sesBad) sesBad = { o, a: sawA.get(i) };
        }
      }
    }
    checks.push(check(
      'No client sees an answer before its question',
      !sesBad,
      sesBad && `${plural(sesCount, 'read')} went wrong. First: client ${sesBad.o.client} read ${showV(sesBad.a.req.key, sesBad.a.res.value)} from ${sesBad.a.node} at ${ms(sesBad.a.complete)}, then read ${showV(sesBad.o.req.key, null)} from ${sesBad.o.node} in a read sent at ${ms(sesBad.o.invoke)} sending token ${tok(sesBad.o.sentToken)}. The token from a read must cover everything that value depends on, and a node behind the token must wait.`,
    ));

    const missing = ids.filter((n) => !finals.has(n) || finals.get(n).size < finalKeys.length);
    checks.push(check('The final inspection completed', missing.length === 0, `The final reads at ${ms(finalAt)} did not all come back (${missing.join(', ')}). Reads with token null must be answered at once.`));
    const acked = writes.filter((o) => o.res?.type === 'write_ok');
    let lost = null;
    for (const w of acked) {
      for (const node of ids) {
        const r = finals.get(node)?.get(w.req.key);
        if (r?.type === 'read_ok' && j(r.value) !== j(w.req.value)) {
          lost = `At ${ms(finalAt)}, ${node} has ${showV(w.req.key, r.value)}, but ${w.client} wrote ${showV(w.req.key, w.req.value)} through ${w.node} (acknowledged at ${ms(w.complete)}). Every node must apply every write in the end.`;
          break;
        }
      }
      if (lost) break;
    }
    checks.push(check('Every node ends up with every write', !lost, lost));
    return checks;
  };
}

// ------------------------------------------------- 7. multi-leader

// A timestamp is [counter, nodeId]; the higher counter wins, ties go to the
// higher node id (compared as strings).
const tsValid = (ts) => Array.isArray(ts) && ts.length === 2 && Number.isInteger(ts[0]) && typeof ts[1] === 'string';
const tsCmp = (a, b) => (a[0] !== b[0] ? a[0] - b[0] : a[1] < b[1] ? -1 : a[1] > b[1] ? 1 : 0);

export function replMultiLeader(sim, { clients = 4, keys = 3, writeShare = 0.6, gap = [5, 25], lag = [5, 60], partition = null } = {}) {
  const ids = sim.nodeIds;
  addLag(sim, () => lag);
  const until = sim.duration - 1250;
  const finalAt = sim.duration - 150;
  const keyNames = Array.from({ length: keys }, (_, i) => `k${i + 1}`);
  const ops = [];
  let sideA = null;
  let sideB = null;
  if (partition) {
    sideB = [ids[ids.length - 1]];
    sideA = ids.filter((n) => !sideB.includes(n));
    sim.at(partition[0], () => sim.partition([sideA, sideB]));
    sim.at(partition[1], () => sim.heal());
  }

  for (let c = 0; c < clients; c++) {
    const client = `c${c + 1}`;
    const home = ids[c % ids.length];
    let n = 0;
    sim.at(150 + c * 7, async () => {
      while (sim.now() < until) {
        const node = sim.rand() < 0.7 ? home : sim.pick(ids);
        const key = sim.pick(keyNames);
        const req = sim.rand() < writeShare ? { type: 'write', key, value: `${client}:${++n}` } : { type: 'read', key };
        const op = { client, node, req, invoke: sim.now(), complete: null, res: null };
        ops.push(op);
        op.res = await sim.clientRpc(node, req, { client });
        op.complete = sim.now();
        await sim.sleep(gap[0] + sim.rand() * (gap[1] - gap[0]));
      }
    });
  }

  const finals = new Map();
  sim.at(finalAt, () => {
    for (const node of ids) {
      finals.set(node, new Map());
      for (const key of keyNames) sim.clientRpc(node, { type: 'read', key }, { probe: true }).then((res) => finals.get(node).set(key, res));
    }
  });

  return () => {
    const reads = ops.filter((o) => o.req.type === 'read');
    const writes = ops.filter((o) => o.req.type === 'write');
    const story = partition ? ` (${sideB.join(', ')} was cut off from ${sideA.join(', ')} between ${ms(partition[0])} and ${ms(partition[1])}.)` : '';
    const checks = [
      everyAnswered(ops, 'Every node accepts writes and serves reads by itself; nothing should wait for another node.'),
      opsSucceed(partition ? 'Writes are accepted on both sides of the partition' : 'Writes are accepted', writes, 'write_ok', 'Each node is a leader: apply the write, answer, and replicate afterwards.'),
      opsSucceed('Reads are answered', reads, 'read_ok', 'Answer reads from the local copy.'),
      valuesReal(reads, writes),
    ];

    const acked = writes.filter((o) => o.res?.type === 'write_ok');
    const badTs = acked.find((o) => !tsValid(o.res.ts));
    const seenTs = new Map();
    let dupTs = null;
    for (const o of acked) {
      if (!tsValid(o.res.ts)) continue;
      const k = j(o.res.ts);
      if (seenTs.has(k) && !dupTs) dupTs = [seenTs.get(k), o];
      seenTs.set(k, o);
    }
    checks.push(check(
      'write_ok carries a unique timestamp [counter, node]',
      !badTs && !dupTs,
      badTs
        ? `${badTs.node} answered ${j(badTs.res)} to ${j(badTs.req)}. Expected { "type": "write_ok", "ts": [counter, nodeId] } with an integer counter and the accepting node's id.`
        : dupTs && `Writes ${showV(dupTs[0].req.key, dupTs[0].req.value)} (via ${dupTs[0].node}) and ${showV(dupTs[1].req.key, dupTs[1].req.value)} (via ${dupTs[1].node}) both got ts ${j(dupTs[1].res.ts)}. Two writes must never tie, or nodes cannot agree which one wins.`,
    ));

    // Lamport rule: a write accepted at a node beats every value of that key
    // the node had already shown (by acknowledging it or returning it).
    const tsOf = new Map(acked.filter((o) => tsValid(o.res.ts)).map((o) => [`${o.req.key}:${o.req.value}`, o]));
    let lam = null;
    let lamCount = 0;
    for (const node of ids) {
      for (const key of keyNames) {
        const shown = [];
        for (const o of ops) {
          if (o.node !== node || o.req.key !== key || !o.res) continue;
          const v = o.req.type === 'write' ? (o.res.type === 'write_ok' ? o.req.value : null) : o.res.type === 'read_ok' ? o.res.value : null;
          const w = v === null || v === undefined ? null : tsOf.get(`${key}:${v}`);
          if (w) shown.push({ at: o.complete, w, how: o.req.type === 'write' ? 'acknowledged' : `returned to ${o.client}` });
        }
        shown.sort((a, b) => a.at - b.at);
        const mine = acked.filter((o) => o.node === node && o.req.key === key && tsValid(o.res.ts)).sort((a, b) => a.invoke - b.invoke);
        let best = null;
        let i = 0;
        for (const w2 of mine) {
          while (i < shown.length && shown[i].at <= w2.invoke) {
            if (!best || tsCmp(shown[i].w.res.ts, best.w.res.ts) > 0) best = shown[i];
            i++;
          }
          if (best && best.w !== w2 && tsCmp(w2.res.ts, best.w.res.ts) <= 0) {
            lamCount++;
            if (!lam || w2.invoke < lam.w2.invoke) lam = { node, w2, best };
          }
        }
      }
    }
    checks.push(check(
      'A new write beats every value its node has already shown',
      !lam,
      lam && `${plural(lamCount, 'write')} lost to a value their own node already had. First: ${lam.node} ${lam.best.how} ${showV(lam.w2.req.key, lam.best.w.req.value)} (ts ${j(lam.best.w.res.ts)}) at ${ms(lam.best.at)}, then accepted ${showV(lam.w2.req.key, lam.w2.req.value)} from ${lam.w2.client} at ${ms(lam.w2.invoke)} and gave it ts ${j(lam.w2.res.ts)}, which loses. Use a Lamport clock: raise the counter past every timestamp the node receives, and add one for each new write. Wall clocks are not enough when they disagree.`,
    ));

    const missing = ids.filter((n) => !finals.has(n) || finals.get(n).size < keyNames.length);
    checks.push(check('The final inspection completed', missing.length === 0, `The final reads at ${ms(finalAt)} did not all come back (${missing.join(', ')}).`));
    let diff = null;
    let wrongWinner = null;
    for (const key of keyNames) {
      const got = ids.map((n) => [n, finals.get(n)?.get(key)]);
      if (got.some(([, r]) => r?.type !== 'read_ok')) continue;
      if (!diff && new Set(got.map(([, r]) => j(r.value))).size > 1) {
        diff = `At ${ms(finalAt)}, the nodes disagree on "${key}": ${got.map(([n, r]) => `${n} has ${j(r.value)}`).join(', ')}.${story} Writes stopped over a second earlier. Every node must end up with the same winner: decide by the timestamp, never by which write happened to arrive last, and make sure writes missed during a partition are sent again.`;
      }
      const kw = writes.filter((o) => o.req.key === key);
      if (!wrongWinner && kw.length && kw.every((o) => o.res?.type === 'write_ok' && tsValid(o.res.ts))) {
        const top = kw.reduce((a, b) => (tsCmp(b.res.ts, a.res.ts) > 0 ? b : a));
        const r = got.find(([, x]) => j(x.value) !== j(top.req.value));
        if (r) {
          const held = tsOf.get(`${key}:${r[1].value}`);
          wrongWinner = `At ${ms(finalAt)}, ${r[0]} has ${showV(key, r[1].value)}${held ? ` (ts ${j(held.res.ts)})` : ''}, but the write with the highest timestamp is ${showV(key, top.req.value)} (ts ${j(top.res.ts)}, via ${top.node}).${story} Compare counters first, then node ids as strings.`;
        }
      }
    }
    checks.push(check('All nodes converge', !diff, diff));
    checks.push(check('The write with the highest timestamp wins', !wrongWinner, wrongWinner));
    return checks;
  };
}
