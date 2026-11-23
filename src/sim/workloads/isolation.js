// Workloads for the "Isolation levels" track.
//
// One node is a database. Several clients (c1, c2, ...) run interactive
// transactions against it at the same time, one message per step:
//
//   begin {txn} -> begin_ok
//   read {txn, key} -> read_ok {value} | aborted
//   write {txn, key, value} -> write_ok | aborted
//   commit {txn} -> commit_ok | aborted
//   abort {txn} -> abort_ok
//
// The client picks a fresh txn id for every attempt. When the node answers
// `aborted`, the client gives up on that attempt (it sends nothing more for it)
// and retries the same logical transaction under a new id.
//
// Every value the workload writes is a JSON object carrying `by`, the id of the
// writing transaction, so each read names the transaction it observed. All the
// checks below work from that client-observed history alone.

import { check, j, plural, sample } from '../workloads.js';

// The briefs promise every request an answer within one second.
const TIMEOUT = 1000;
// A logical transaction may be retried this many times before the test gives up on it.
const MAX_ATTEMPTS = 25;

const SHAPES = {
  begin: ['begin_ok'],
  read: ['read_ok', 'aborted'],
  write: ['write_ok', 'aborted'],
  commit: ['commit_ok', 'aborted'],
  abort: ['abort_ok'],
  versions: ['versions_ok'],
};

const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
const ms = (t) => `${Math.round(t)}ms`;
const show = (v) => (v === null ? 'null (never written)' : j(v));
const writerOf = (v) => (v && typeof v === 'object' && typeof v.by === 'string' ? v.by : v === null ? 'init' : null);

// --------------------------------------------------------------- harness

function harness(sim) {
  const node = sim.nodeIds[0];
  const requests = []; // every client request, for the reply checks
  const attempts = []; // every transaction attempt, in the order they began
  const byTxn = new Map();
  const logicals = []; // every logical transaction (with its retries)
  let seq = 0;
  const ABORT = { abort: true };
  const FAIL = { fail: true };

  async function call(client, body) {
    const rec = { node, req: body, res: null, invoke: sim.now(), complete: null };
    requests.push(rec);
    rec.res = await sim.clientRpc(node, body, { timeout: TIMEOUT, client });
    rec.complete = sim.now();
    return rec;
  }

  // One attempt. `body(tx)` performs the reads and writes and returns 'commit'
  // or 'abort' (a voluntary abort by the client).
  async function attempt(client, kind, body, think) {
    const txn = 't' + ++seq;
    const a = { txn, client, kind, ops: [], begin: null, commit: null, status: 'open', end: null, info: {} };
    attempts.push(a);
    byTxn.set(txn, a);
    const b = await call(client, { type: 'begin', txn });
    a.begin = { invoke: b.invoke, complete: b.complete };
    if (!b.res || b.res.type !== 'begin_ok') {
      a.status = 'failed';
      a.end = sim.now();
      return a;
    }
    const step = async (f, key, value) => {
      if (think) await sim.sleep(think());
      const req = f === 'r' ? { type: 'read', txn, key } : { type: 'write', txn, key, value };
      const r = await call(client, req);
      if (!r.res) throw FAIL;
      if (r.res.type === 'aborted') {
        a.abortedOn = { f, key, t: r.complete };
        throw ABORT;
      }
      if (f === 'r') {
        if (r.res.type !== 'read_ok' || !('value' in r.res)) throw FAIL;
        const v = r.res.value === undefined ? null : r.res.value;
        a.ops.push({ f, key, value: v, invoke: r.invoke, complete: r.complete });
        return v;
      }
      if (r.res.type !== 'write_ok') throw FAIL;
      a.ops.push({ f, key, value, invoke: r.invoke, complete: r.complete });
      return value;
    };
    const tx = {
      txn,
      read: (key) => step('r', key),
      write: (key, value) => step('w', key, value),
      sleep: (t) => sim.sleep(t),
    };
    try {
      const decision = await body(tx, a);
      if (decision === 'abort') {
        await call(client, { type: 'abort', txn });
        a.status = 'client-aborted';
      } else {
        const c = await call(client, { type: 'commit', txn });
        a.commit = { invoke: c.invoke, complete: c.complete };
        if (!c.res) a.status = 'unknown';
        else if (c.res.type === 'commit_ok') a.status = 'committed';
        else if (c.res.type === 'aborted') a.status = 'aborted';
        else a.status = 'unknown';
      }
    } catch (e) {
      if (e === ABORT) a.status = 'aborted';
      else if (e === FAIL) {
        a.status = 'failed';
        // Give the node a chance to release whatever this attempt holds.
        await call(client, { type: 'abort', txn });
      } else throw e;
    }
    a.end = sim.now();
    return a;
  }

  // A logical transaction: retried under a new id until it commits.
  async function run(client, kind, body, { think = () => 2 + sim.rand() * 8 } = {}) {
    const l = { client, kind, attempts: [], done: false, start: sim.now() };
    logicals.push(l);
    for (let i = 0; i < MAX_ATTEMPTS; i++) {
      const a = await attempt(client, kind, body, think);
      l.attempts.push(a);
      if (a.status === 'committed' || a.status === 'client-aborted') {
        l.done = true;
        return a;
      }
      if (a.status !== 'aborted') {
        l.broken = true;
        return a;
      }
      if (sim.now() > sim.duration - 300) {
        l.cutoff = true;
        return a;
      }
      // Back off a little before retrying, more after each abort.
      await sim.sleep(2 + sim.rand() * Math.min(60, 8 * (i + 1)));
    }
    return l.attempts[l.attempts.length - 1];
  }

  return { node, requests, attempts, byTxn, logicals, run, call };
}

// -------------------------------------------------------------- scenarios
//
// Each scenario starts some client loops and records what it needs for its
// checks. Loops stop starting new transactions at `stop`.

const GROUP_KEYS = ['a', 'b', 'c'];
const groupKeys = (g) => GROUP_KEYS.map((k) => 'g' + g + '.' + k);
const DOCTORS = ['alice', 'bob', 'carol'];

function groupsScenario(sim, h, st, { groups = 2, writers = 2, readers = 2, pAbort = 0, writeGap = 0, readSpan = 0, ids }) {
  st.groups = groups;
  const loops = [];
  for (let w = 0; w < writers; w++) {
    const client = ids.next();
    loops.push(
      (async () => {
        await sim.sleep(sim.rand() * 30);
        while (sim.now() < st.stop) {
          const g = 1 + Math.floor(sim.rand() * groups);
          const abortHere = sim.rand() < pAbort;
          await h.run(client, 'group-write', async (tx, a) => {
            a.info.group = g;
            for (const k of groupKeys(g)) {
              await tx.write(k, { by: tx.txn });
              if (writeGap) await tx.sleep(writeGap * (0.5 + sim.rand()));
            }
            return abortHere ? 'abort' : 'commit';
          });
          await sim.sleep(5 + sim.rand() * 25);
        }
      })(),
    );
  }
  for (let r = 0; r < readers; r++) {
    const client = ids.next();
    loops.push(
      (async () => {
        await sim.sleep(sim.rand() * 30);
        while (sim.now() < st.stop) {
          const g = 1 + Math.floor(sim.rand() * groups);
          await h.run(client, 'group-read', async (tx, a) => {
            a.info.group = g;
            const keys = groupKeys(g);
            await tx.read(keys[0]);
            if (readSpan) await tx.sleep(readSpan * (0.5 + sim.rand()));
            for (const k of keys.slice(1)) await tx.read(k);
            // Read the first key again: within one snapshot it cannot change.
            await tx.read(keys[0]);
            return 'commit';
          });
          await sim.sleep(5 + sim.rand() * 25);
        }
      })(),
    );
  }
  st.keys.push(...Array.from({ length: groups }, (_, i) => groupKeys(i + 1)).flat());
  return loops;
}

// One long reader: reads one key of a group, waits while writers churn, reads the rest.
function longReaderScenario(sim, h, st, { span = 700, ids }) {
  const client = ids.next();
  return [
    (async () => {
      await sim.sleep(150);
      while (sim.now() < st.stop - span) {
        const g = 1 + Math.floor(sim.rand() * st.groups);
        await h.run(client, 'group-read', async (tx, a) => {
          a.info.group = g;
          a.info.long = true;
          const keys = groupKeys(g);
          await tx.read(keys[0]);
          await tx.sleep(span);
          for (const k of keys.slice(1)) await tx.read(k);
          await tx.read(keys[0]);
          return 'commit';
        });
        await sim.sleep(50);
      }
    })(),
  ];
}

// Read-modify-write increments on a few shared counters.
function countersScenario(sim, h, st, { counters = 2, clients = 4, think = 15, ids, own = false }) {
  const loops = [];
  for (let c = 0; c < clients; c++) {
    const client = ids.next();
    const keys = own ? ['own.' + client + '.x', 'own.' + client + '.y'] : Array.from({ length: counters }, (_, i) => 'count' + (i + 1));
    for (const k of keys) if (!st.counters.includes(k)) st.counters.push(k);
    loops.push(
      (async () => {
        await sim.sleep(sim.rand() * 30);
        while (sim.now() < st.stop) {
          const key = sim.pick(keys);
          await h.run(client, 'inc', async (tx, a) => {
            a.info.key = key;
            const v = await tx.read(key);
            const n = v && typeof v.n === 'number' ? v.n : 0;
            await tx.sleep(think * (0.5 + sim.rand()));
            await tx.write(key, { n: n + 1, by: tx.txn });
            return 'commit';
          });
          await sim.sleep(5 + sim.rand() * 20);
        }
      })(),
    );
  }
  return loops;
}

// Slow read-only transactions over every counter.
function scannersScenario(sim, h, st, { clients = 1, gap = 80, ids }) {
  const loops = [];
  for (let c = 0; c < clients; c++) {
    const client = ids.next();
    loops.push(
      (async () => {
        await sim.sleep(80 + sim.rand() * 60);
        while (sim.now() < st.stop - 300) {
          await h.run(
            client,
            'scan',
            async (tx) => {
              for (const k of st.counters) await tx.read(k);
              return 'commit';
            },
            { think: () => gap * (0.75 + sim.rand() * 0.5) },
          );
          await sim.sleep(20);
        }
      })(),
    );
  }
  return loops;
}

// The on-call rota. Each shift has `doctors` doctors, all on call at first
// (a key never written means on call). A doctor may go off call only if at
// least one other doctor of the shift stays on call.
function oncallScenario(sim, h, st, { shifts = 2, doctors = 2, clients = 4, pLeave = 0.65, think = 10, rota = 1, ids }) {
  st.shifts = shifts;
  st.doctors = doctors;
  const shiftKeys = (s) => DOCTORS.slice(0, doctors).map((d) => 's' + s + '.' + d);
  st.shiftKeys = shiftKeys;
  const isOn = (v) => v === null || (v && v.on === true);
  const loops = [];
  for (let c = 0; c < clients; c++) {
    const client = ids.next();
    loops.push(
      (async () => {
        await sim.sleep(sim.rand() * 20);
        while (sim.now() < st.stop) {
          const s = 1 + Math.floor(sim.rand() * shifts);
          const me = sim.pick(shiftKeys(s));
          if (sim.rand() < pLeave) {
            await h.run(client, 'leave', async (tx, a) => {
              a.info.shift = s;
              a.info.me = me;
              let on = 0;
              let meOn = false;
              for (const k of shiftKeys(s)) {
                const v = await tx.read(k);
                if (isOn(v)) on++;
                if (k === me) meOn = isOn(v);
              }
              a.info.sawOn = on;
              await tx.sleep(think * (0.5 + sim.rand()));
              if (meOn && on >= 2) {
                await tx.write(me, { on: false, by: tx.txn });
                a.info.left = true;
              }
              return 'commit';
            });
          } else {
            await h.run(client, 'rejoin', async (tx, a) => {
              a.info.shift = s;
              a.info.me = me;
              const v = await tx.read(me);
              await tx.sleep(think * (0.5 + sim.rand()));
              if (!isOn(v)) await tx.write(me, { on: true, by: tx.txn });
              return 'commit';
            });
          }
          await sim.sleep(5 + sim.rand() * 25);
        }
      })(),
    );
  }
  for (let r = 0; r < rota; r++) {
    const client = ids.next();
    loops.push(
      (async () => {
        await sim.sleep(40);
        while (sim.now() < st.stop) {
          const s = 1 + Math.floor(sim.rand() * shifts);
          await h.run(client, 'rota', async (tx, a) => {
            a.info.shift = s;
            let on = 0;
            for (const k of shiftKeys(s)) if (isOn(await tx.read(k))) on++;
            a.info.sawOn = on;
            return 'commit';
          });
          await sim.sleep(20 + sim.rand() * 30);
        }
      })(),
    );
  }
  for (let s = 1; s <= shifts; s++) st.keys.push(...shiftKeys(s));
  return loops;
}

// ----------------------------------------------------------------- checks

function replyChecks(h, sim) {
  // Requests sent in the last second could not have timed out before the run ended.
  const lost = h.requests.filter((r) => !r.res && r.invoke <= sim.duration - TIMEOUT - 20);
  const out = [
    check(
      'Every request gets a reply within 1s',
      lost.length === 0,
      lost.length &&
        `${plural(lost.length, 'request')} of ${h.requests.length} got no reply within one second. First: ${j(lost[0].req)}, sent at ${ms(lost[0].invoke)}. A transaction may wait for another one, but never for ever: if two transactions wait for each other, abort one of them.`,
    ),
  ];
  const bad = h.requests.find((r) => r.res && !(SHAPES[r.req.type] || []).includes(r.res.type));
  const noValue = h.requests.find((r) => r.res && r.res.type === 'read_ok' && !('value' in r.res));
  out.push(
    check(
      'Replies have the right types',
      !bad && !noValue,
      bad
        ? `The node answered ${j(bad.res)} to ${j(bad.req)}. A ${bad.req.type} is answered with ${SHAPES[bad.req.type].map((t) => '"' + t + '"').join(' or ')}.`
        : noValue && `The node answered ${j(noValue.res)} to ${j(noValue.req)}: read_ok must carry "value" (null for a key never written).`,
    ),
  );
  return out;
}

function progressChecks(h, { maxAbort }) {
  const out = [];
  const stuck = h.logicals.find((l) => !l.done && !l.broken && l.kind !== 'final');
  out.push(
    check(
      'Aborted transactions succeed when retried',
      !stuck,
      stuck &&
        `Client ${stuck.client}'s ${stuck.kind} transaction was aborted ${plural(stuck.attempts.length, 'time')} in a row (${sample(stuck.attempts.map((a) => a.txn))})${stuck.cutoff ? ' and was still being retried when the test ended' : ''}. Abort a transaction only when it really conflicts with another, so that a retry can succeed.`,
    ),
  );
  const tried = h.attempts.filter((a) => a.status === 'committed' || a.status === 'aborted');
  const aborted = tried.filter((a) => a.status === 'aborted');
  const committed = tried.length - aborted.length;
  const rate = tried.length ? aborted.length / tried.length : 0;
  out.push(
    check(
      'Most transactions commit',
      committed > 0 && rate <= maxAbort,
      `The node aborted ${aborted.length} of ${tried.length} attempts (${Math.round(rate * 100)}%); this test allows at most ${Math.round(maxAbort * 100)}%. First aborts: ${sample(aborted.map((a) => a.txn + (a.abortedOn ? ' (on ' + (a.abortedOn.f === 'r' ? 'read' : 'write') + ' of ' + a.abortedOn.key + ')' : ' (on commit)')))}. Abort only on a real conflict.`,
    ),
  );
  return out;
}

// The version a read observed, as a transaction id ('init' for null).
// Returns { error } when the value is not one any transaction wrote last.
function lastWrite(a, key, before) {
  let v;
  let found = false;
  for (const op of a.ops) {
    if (op === before) break;
    if (op.f === 'w' && op.key === key) {
      v = op.value;
      found = true;
    }
  }
  return found ? { value: v } : null;
}

function finalWrite(a, key) {
  let v;
  let found = false;
  for (const op of a.ops) if (op.f === 'w' && op.key === key) (v = op.value), (found = true);
  return found ? { value: v } : null;
}

function readsOf(h) {
  const out = [];
  for (const a of h.attempts) for (const op of a.ops) if (op.f === 'r') out.push({ a, op });
  return out;
}

// No dirty reads, reads see own writes, and every value read was written by someone.
function readChecks(h) {
  let dirty = null;
  let own = null;
  let unknown = null;
  for (const { a, op } of readsOf(h)) {
    const mine = lastWrite(a, op.key, op);
    if (mine) {
      if (!own && !same(mine.value, op.value)) {
        own = `${a.txn} wrote ${op.key} = ${j(mine.value)} and then read it back as ${show(op.value)} at ${ms(op.complete)}. A transaction must see its own writes.`;
      }
      continue;
    }
    const by = writerOf(op.value);
    if (by === 'init') continue;
    const w = by && h.byTxn.get(by);
    const wrote = w && finalWrite(w, op.key);
    if (!w || !wrote || (by === a.txn && !mine)) {
      unknown ??= `${a.txn} read ${op.key} = ${show(op.value)} at ${ms(op.complete)}, but no transaction wrote that value to ${op.key}.`;
      continue;
    }
    if (!same(wrote.value, op.value)) {
      dirty ??= `${a.txn} read ${op.key} = ${j(op.value)} at ${ms(op.complete)}, but ${by} later overwrote that key with ${j(wrote.value)} in the same transaction: only a transaction's final write may become visible, and only once it commits.`;
      continue;
    }
    if (w.status === 'aborted' || w.status === 'client-aborted' || w.status === 'failed') {
      dirty ??= `Dirty read: ${a.txn} read ${op.key} = ${j(op.value)} at ${ms(op.complete)}, but ${by} never committed (it ${w.status === 'client-aborted' ? 'was rolled back by its client' : 'was aborted'} at ${ms(w.end)}). Keep a transaction's writes private until it commits.`;
    } else if (w.status === 'committed' && w.commit.invoke > op.complete) {
      dirty ??= `Dirty read: ${a.txn} read ${op.key} = ${j(op.value)} at ${ms(op.complete)}, but ${by} did not ask to commit until ${ms(w.commit.invoke)}. Its write was visible while it was still uncommitted.`;
    } else if (w.status === 'open') {
      dirty ??= `Dirty read: ${a.txn} read ${op.key} = ${j(op.value)} at ${ms(op.complete)}, but ${by} had not committed by the end of the test.`;
    }
  }
  return [
    check('No dirty reads', !dirty, dirty),
    check('Transactions see their own writes', !own, own),
    check('Reads return values that were written', !unknown, unknown),
  ];
}

// Per-key versions written by committed transactions, with what each one read first.
function committedWriters(h, key) {
  return h.attempts.filter((a) => a.status === 'committed' && finalWrite(a, key));
}
const before = (w1, w2) => w1.commit.complete < w2.commit.invoke;

// A transaction that begins after a commit has completed must see it (or something newer).
function staleCheck(h) {
  let stale = null;
  const cache = new Map();
  const writersOf = (k) => cache.get(k) ?? cache.set(k, committedWriters(h, k)).get(k);
  for (const { a, op } of readsOf(h)) {
    if (stale) break;
    if (lastWrite(a, op.key, op)) continue;
    const by = writerOf(op.value);
    if (!by) continue;
    const v = by === 'init' ? null : h.byTxn.get(by);
    if (by !== 'init' && (!v || v.status !== 'committed')) continue;
    const newer = writersOf(op.key).find((w2) => w2 !== v && w2 !== a && w2.commit.complete < a.begin.invoke && (!v || before(v, w2)));
    if (newer) {
      stale = `${a.txn} began at ${ms(a.begin.invoke)} and read ${op.key} = ${show(op.value)}, but ${newer.txn} had overwritten ${op.key} and committed at ${ms(newer.commit.complete)}${v ? ', after ' + v.txn + ' committed' : ''}. A transaction must see every commit that finished before it began.`;
    }
  }
  return check('Transactions see earlier commits', !stale, stale);
}

// Snapshot isolation: the snapshot is taken at begin and never changes.
function snapshotChecks(h, st) {
  let repeat = null;
  let future = null;
  let skew = null;
  for (const a of h.attempts) {
    const seen = new Map();
    for (const op of a.ops) {
      if (op.f === 'w') {
        seen.set(op.key, { own: true });
        continue;
      }
      if (lastWrite(a, op.key, op)) continue;
      const by = writerOf(op.value);
      const prev = seen.get(op.key);
      if (prev && !prev.own && !repeat && !same(prev.value, op.value)) {
        repeat = `Non-repeatable read: ${a.txn} read ${op.key} = ${show(prev.value)} at ${ms(prev.t)} and then ${show(op.value)} at ${ms(op.complete)}. Within one transaction, every read comes from the same snapshot.`;
      }
      if (!prev) seen.set(op.key, { value: op.value, t: op.complete });
      const w = by && by !== 'init' ? h.byTxn.get(by) : null;
      if (!future && w && w !== a && w.commit && w.commit.invoke > a.begin.complete) {
        future = `${a.txn} began at ${ms(a.begin.complete)} but read ${op.key} = ${j(op.value)}, written by ${w.txn}, which asked to commit only at ${ms(w.commit.invoke)}. The snapshot is taken at begin: commits that happen later stay invisible to it.`;
      }
    }
    if (!skew && a.info.group !== undefined && a.kind === 'group-read') {
      const reads = a.ops.filter((o, i) => o.f === 'r' && a.ops.findIndex((x) => x.f === 'r' && x.key === o.key) === i);
      const writers = [...new Set(reads.map((o) => writerOf(o.value)))];
      if (writers.length > 1) {
        const r1 = reads.find((o) => writerOf(o.value) === writers[0]);
        const r2 = reads.find((o) => writerOf(o.value) === writers[1]);
        skew = `Read skew: ${a.txn} read ${r1.key} as written by ${writers[0]} but ${r2.key} as written by ${writers[1]}. Every writer rewrites the whole group g${a.info.group}, so any consistent snapshot shows one writer for all three keys.`;
      }
    }
  }
  return [
    check('Reads are repeatable', !repeat, repeat),
    check('No read skew: one snapshot per transaction', !skew, skew),
    check('The snapshot is taken at begin', !future, future),
  ];
}

// Increments: no two committed increments read the same version, and the final count adds up.
function lostUpdateChecks(h, st, finals) {
  let lost = null;
  const seen = new Map();
  for (const a of h.attempts) {
    if (a.kind !== 'inc' || a.status !== 'committed') continue;
    const r = a.ops.find((o) => o.f === 'r');
    const w = a.ops.find((o) => o.f === 'w');
    if (!r || !w) continue;
    const id = a.info.key + '@' + writerOf(r.value);
    const other = seen.get(id);
    if (other && !lost) {
      const n = r.value && r.value.n ? r.value.n : 0;
      lost = `Lost update: ${other.txn} and ${a.txn} both read ${a.info.key} = ${show(r.value)} and both committed ${a.info.key} = ${n + 1}, so one increment vanished. When two transactions update the same key concurrently, only the first to commit may succeed; abort the other so it retries on the new value.`;
    }
    seen.set(id, a);
  }
  let count = null;
  if (finals) {
    for (const k of st.counters) {
      const want = h.attempts.filter((a) => a.kind === 'inc' && a.status === 'committed' && a.info.key === k).length;
      const v = finals[k];
      const got = v && typeof v.n === 'number' ? v.n : 0;
      if (got !== want && !count) count = `${k} ends at ${got}, but ${plural(want, 'increment')} of it committed.`;
    }
  }
  return [check('No lost updates', !lost, lost), check('Counters equal the committed increments', !count, count)];
}

// Serializability, from the dependency graph of committed transactions. Every
// write in these workloads is preceded by a read of the same key in the same
// transaction, which orders each key's versions.
function serializableCheck(h) {
  const committed = h.attempts.filter((a) => a.status === 'committed');
  const next = new Map(); // key -> (version id -> writer attempt)
  let clash = null;
  const firstRead = (a, key) => a.ops.find((o) => o.f === 'r' && o.key === key && !lastWrite(a, key, o));
  for (const a of committed) {
    const keys = [...new Set(a.ops.filter((o) => o.f === 'w').map((o) => o.key))];
    for (const k of keys) {
      const r = firstRead(a, k);
      if (!r) continue;
      const prev = writerOf(r.value);
      const m = next.get(k) ?? next.set(k, new Map()).get(k);
      if (m.has(prev) && !clash) {
        clash = `Not serializable: ${m.get(prev).txn} and ${a.txn} both read ${k} = ${show(r.value)} and both overwrote it. In any serial order one of them would have read the other's write.`;
      }
      if (!m.has(prev)) m.set(prev, a);
    }
  }
  if (clash) return check('Committed transactions are serializable', false, clash);

  const edges = new Map(committed.map((a) => [a.txn, []]));
  const add = (from, to, why) => {
    if (from !== to && edges.has(from.txn) && edges.has(to.txn)) edges.get(from.txn).push({ to, why });
  };
  for (const a of committed) {
    for (const op of a.ops) {
      if (op.f !== 'r' || lastWrite(a, op.key, op)) continue;
      const by = writerOf(op.value);
      const w = by && by !== 'init' ? h.byTxn.get(by) : null;
      if (w) add(w, a, `${a.txn} read ${op.key} as written by ${w.txn}`);
      const succ = next.get(op.key) && next.get(op.key).get(by);
      if (succ && succ !== a) add(a, succ, `${a.txn} read ${op.key} = ${show(op.value)} before ${succ.txn} overwrote it`);
    }
  }
  // Find a cycle with an iterative depth-first search.
  const color = new Map();
  let cycle = null;
  for (const start of committed) {
    if (cycle || color.get(start.txn)) continue;
    const stack = [{ a: start, i: 0, why: null }];
    color.set(start.txn, 1);
    while (stack.length && !cycle) {
      const top = stack[stack.length - 1];
      const out = edges.get(top.a.txn);
      if (top.i >= out.length) {
        color.set(top.a.txn, 2);
        stack.pop();
        continue;
      }
      const e = out[top.i++];
      const c = color.get(e.to.txn);
      if (c === 1) {
        const from = stack.findIndex((s) => s.a === e.to);
        cycle = [...stack.slice(from + 1).map((s) => s.why), e.why];
        cycle.names = [...stack.slice(from).map((s) => s.a.txn), e.to.txn];
      } else if (!c) {
        color.set(e.to.txn, 1);
        stack.push({ a: e.to, i: 0, why: e.why });
      }
    }
  }
  return check(
    'Committed transactions are serializable',
    !cycle,
    cycle &&
      `No serial order explains what ${cycle.names.slice(0, -1).join(', ')} saw: ${cycle.join('; ')}. Each must come before the next, which is a cycle.` +
        (cycle.length === 2 ? ' Two transactions that each read what the other overwrote: write skew.' : ''),
  );
}

// The on-call invariant: every shift always has somebody on call.
function oncallCheck(h, st, finals) {
  let bad = null;
  let s = 0;
  for (const a of h.attempts) {
    if (a.status !== 'committed' || (a.kind !== 'leave' && a.kind !== 'rota') || a.info.sawOn === undefined) continue;
    if (a.info.sawOn === 0) {
      s = a.info.shift;
      const reads = a.ops.filter((o) => o.f === 'r').map((o) => o.key.split('.')[1] + ' off (by ' + writerOf(o.value) + ')');
      bad = `${a.txn} read shift s${a.info.shift} at ${ms(a.ops[a.ops.length - 1].complete)} and found nobody on call: ${reads.join(', ')}.`;
      break;
    }
  }
  if (!bad && finals) {
    for (let x = 1; x <= st.shifts && !bad; x++) {
      const keys = st.shiftKeys(x);
      const vals = keys.map((k) => finals[k]);
      if (vals.every((v) => v && v.on === false)) {
        s = x;
        bad = `At the end, nobody is on call for shift s${x}: ${keys.map((k, i) => k + ' off (by ' + vals[i].by + ')').join(', ')}.`;
      }
    }
  }
  if (bad) {
    // Name two of the leaves that did it, if they overlapped in time.
    const offBy = st.shiftKeys(s)
      .map((k) => h.attempts.filter((a) => a.kind === 'leave' && a.status === 'committed' && a.info.left && a.info.me === k))
      .flat();
    let pair = null;
    for (const x of offBy) for (const y of offBy) if (!pair && x !== y && x.info.me !== y.info.me && x.begin.invoke < y.end && y.begin.invoke < x.end) pair = [x, y];
    if (pair) {
      const [x, y] = pair;
      bad += ` For example ${x.txn} took ${x.info.me} off and ${y.txn} took ${y.info.me} off at the same time: each read ${x.info.sawOn} and ${y.info.sawOn} doctors on call, neither saw the other's write, and both committed. That is write skew: they wrote different keys, so first-committer-wins sees no conflict.`;
    }
  }
  return check('Somebody is always on call', !bad, bad);
}

function latencyCheck(h, { kinds, limit, why }) {
  let slow = null;
  for (const r of h.requests) {
    if (slow || !r.res) continue;
    const a = r.req.txn && h.byTxn.get(r.req.txn);
    if (!a || (kinds && !kinds.includes(a.kind)) || r.req.type === 'abort') continue;
    if (r.complete - r.invoke > limit) {
      slow = `${a.txn}'s ${r.req.type}${r.req.key ? ' of ' + r.req.key : ''} took ${ms(r.complete - r.invoke)} (sent at ${ms(r.invoke)}); at most ${limit}ms is allowed here. ${why}`;
    }
  }
  return check('Transactions do not wait needlessly', !slow, slow);
}

function kindProgressCheck(h, { kind, maxAbort, why }) {
  const tried = h.attempts.filter((a) => a.kind === kind && (a.status === 'committed' || a.status === 'aborted'));
  const aborted = tried.filter((a) => a.status === 'aborted');
  const rate = tried.length ? aborted.length / tried.length : 1;
  return check(
    'Read-only transactions commit',
    tried.length > 0 && rate <= maxAbort,
    `${aborted.length} of ${tried.length} ${kind} attempts were aborted (${sample(aborted.map((a) => a.txn))}); at most ${Math.round(maxAbort * 100)}% may be. ${why}`,
  );
}

// ----------------------------------------------------------------- runner

function makeIds() {
  let n = 0;
  return { next: () => 'c' + ++n };
}

// level: 'rc' | 'si' | 'lu' | '2pl' | 'ssi'
function isolationRun(sim, params, level) {
  const h = harness(sim);
  const ids = makeIds();
  const settle = params.settle ?? 900;
  const st = { stop: sim.duration - settle - 300, keys: [], counters: [] };
  const loops = [];
  if (params.groups) loops.push(...groupsScenario(sim, h, st, { ...params.groups, ids }));
  if (params.long) loops.push(...longReaderScenario(sim, h, st, { ...params.long, ids }));
  if (params.counters) loops.push(...countersScenario(sim, h, st, { ...params.counters, ids }));
  if (params.scanners) loops.push(...scannersScenario(sim, h, st, { ...params.scanners, ids }));
  if (params.oncall) loops.push(...oncallScenario(sim, h, st, { ...params.oncall, ids }));

  // After every client has finished (or the deadline passed), read everything once.
  const fin = { finals: null, versions: null, done: false, late: false };
  const finalClient = ids.next();
  sim.at(100, async () => {
    let quiet = false;
    Promise.all(loops).then(() => (quiet = true));
    while (!quiet && sim.now() < sim.duration - 450) await sim.sleep(20);
    fin.late = !quiet;
    await sim.sleep(30);
    const keys = [...st.keys, ...st.counters];
    const a = await h.run(finalClient, 'final', async (tx) => {
      for (const k of keys) await tx.read(k);
      return 'commit';
    }, { think: () => 0 });
    if (a && a.status === 'committed') {
      fin.finals = {};
      for (const op of a.ops) fin.finals[op.key] = op.value;
      fin.finalTxn = a;
    }
    if (params.gc) {
      await sim.sleep(10);
      const v = await h.call(finalClient, { type: 'versions' });
      fin.versions = v.res;
    }
    fin.done = true;
  });

  return () => {
    const checks = [...replyChecks(h, sim)];
    // Final values only mean something if nothing was still running when they were read.
    const finals = fin.late ? null : fin.finals;
    checks.push(
      check(
        'The final read completed',
        fin.done && fin.finals && !fin.late,
        fin.late
          ? `Clients were still retrying transactions ${settle}ms after the last one started; the final read had to start anyway. Is a transaction waiting for ever, or aborted again and again?`
          : 'The final read-only transaction over every key did not commit in time.',
      ),
    );
    checks.push(...progressChecks(h, { maxAbort: params.maxAbort ?? 0.1 }));
    checks.push(...readChecks(h));
    checks.push(staleCheck(h));
    if (level === 'rc' && params.groups && finals) {
      let torn = null;
      for (let g = 1; g <= st.groups && !torn; g++) {
        const ws = groupKeys(g).map((k) => writerOf(finals[k]));
        if (new Set(ws).size > 1) {
          torn = `After every transaction finished, group g${g} holds ${groupKeys(g)
            .map((k, i) => k + ' from ' + ws[i])
            .join(', ')}. Every writer rewrites all three keys, so their writes were interleaved (a dirty write) or a commit was applied only in part.`;
        }
      }
      checks.push(check('No dirty writes: commits apply whole', !torn, torn));
    }
    if (level === 'si' || level === 'lu') checks.push(...snapshotChecks(h, st));
    if (level === '2pl' || level === 'ssi') {
      if (params.groups) checks.push(snapshotChecks(h, st)[1]);
    }
    if (params.counters && level !== 'rc' && level !== 'si') checks.push(...lostUpdateChecks(h, st, finals));
    if (level === '2pl' || level === 'ssi') checks.push(serializableCheck(h));
    if (params.oncall) checks.push(oncallCheck(h, st, finals));
    if (params.fast) checks.push(latencyCheck(h, params.fast));
    if (params.readers) checks.push(kindProgressCheck(h, params.readers));
    if (params.gc && fin.done && !fin.late) {
      const v = fin.versions;
      const written = new Set(h.attempts.filter((a) => a.status === 'committed').flatMap((a) => a.ops.filter((o) => o.f === 'w').map((o) => o.key)));
      const okShape = v && v.type === 'versions_ok' && typeof v.count === 'number';
      checks.push(
        check(
          'Old versions are garbage-collected',
          okShape && v.count <= written.size,
          !okShape
            ? `The node answered ${j(v ?? null)} to { type: "versions" }. Expected { type: "versions_ok", count: N }.`
            : `After every transaction finished, the node still keeps ${v.count} versions of ${plural(written.size, 'written key')}. Once no open transaction's snapshot can see a version, drop it: with nothing open, only the newest version of each key is needed.`,
        ),
      );
    }
    return checks;
  };
}

export const isoReadCommitted = (sim, params) => isolationRun(sim, params, 'rc');
export const isoSnapshot = (sim, params) => isolationRun(sim, params, 'si');
export const isoLostUpdate = (sim, params) => isolationRun(sim, params, 'lu');
export const isoWriteSkew = (sim, params) => isolationRun(sim, params, '2pl');
export const isoSerializable = (sim, params) => isolationRun(sim, params, 'ssi');
