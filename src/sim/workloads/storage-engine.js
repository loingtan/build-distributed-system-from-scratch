// Workloads for the storage-engine track.
//
// One client talks to one node, one request at a time, so the checker always
// knows exactly what the store should hold. Between requests it crashes and
// restarts the node, and reads node.disk to check the on-disk structures the
// briefs fix: node.disk.log, and from the LSM tasks on node.disk.segments.

import { check, j, plural } from '../workloads.js';

const LOG_LIMIT = 50; // store-checkpoint: longest log allowed
const WRITES_PER_REWRITE = 10; // store-checkpoint: one rewrite allowed per this many writes
const MEMTABLE = 8; // LSM tasks: writes the memtable (and its log) may hold
const MAX_SEGMENTS = 4; // store-compaction onwards
const MAX_ENTRIES = 64; // store-compaction onwards: entries across all segments
const BLOOM_BUDGET = 0.2; // store-bloom: share of segment searches allowed for absent keys

const STOP = { stop: true };
const keyName = (i) => 'k' + String(i).padStart(3, '0');
const ms = (t) => `${Math.round(t)}ms`;

// A segment must be { entries: [[key, value], ...] } with keys strictly ascending.
function segmentProblem(seg, i) {
  if (!seg || typeof seg !== 'object' || !Array.isArray(seg.entries)) {
    return `node.disk.segments[${i}] is ${j(seg)}. A segment must be an object with an "entries" array.`;
  }
  let prev = null;
  for (const e of seg.entries) {
    if (!Array.isArray(e) || typeof e[0] !== 'string' || e.length < 2) {
      return `node.disk.segments[${i}] holds the entry ${j(e)}. Each entry must be [key, value], with value null for a tombstone.`;
    }
    if (prev !== null && !(prev < e[0])) {
      return prev === e[0]
        ? `node.disk.segments[${i}] holds the key "${e[0]}" twice. A segment keeps one entry per key: the latest.`
        : `node.disk.segments[${i}] has "${prev}" before "${e[0]}". Sort the entries by key when the segment is written.`;
    }
    prev = e[0];
  }
  return null;
}

function engine(level) {
  return (sim, { ops = 200, keys = 20, crashes = 0, churn = false, absent = false }) => {
    const id = sim.nodeIds[0];
    const model = new Map(); // key -> { value (null once deleted), t, seq }
    const touched = [];
    const fails = new Map(); // check name -> detail of the first violation
    const note = (name, detail) => {
      if (!fails.has(name)) fails.set(name, detail);
    };

    const N = {
      reply: 'Every request gets a reply',
      type: 'Replies have the expected type',
      read: 'Reads return the latest write',
      scan: 'Scans return the live keys in range, in key order',
      crash: 'Acknowledged writes survive a crash',
      durable: level === 1 ? 'Every write is in node.disk.log before it is acknowledged' : 'A write is on disk before it is acknowledged',
      logLen: `The log stays within ${level === 2 ? LOG_LIMIT : MEMTABLE} entries`,
      rewrites: 'The rest of the disk is rewritten rarely',
      format: 'Segments are sorted by key',
      immutable: 'Flushed segments are never modified',
      segCount: `At most ${MAX_SEGMENTS} segments`,
      segTotal: `At most ${MAX_ENTRIES} entries across all segments`,
      inSegments: 'Segments hold everything except the memtable',
      stats: 'stats reports segment searches',
      skip: 'Lookups for absent keys skip the segments',
      found: 'A key stored in a segment is searched for there',
      done: 'The test ran to the end',
    };

    let finished = false;
    let stopped = false;
    let writes = 0;
    let valueSeq = 0;
    let crashCount = 0;
    let lastCrash = -1;
    let rewrites = 0;
    let firstRewrite = null;
    let prevLog = null; // JSON of each log entry at the last observation
    let prevRest = null; // JSON of the disk without the log
    let prevSegs = null; // JSON of each segment
    let absentGets = 0;
    let absentReads = 0;
    let absentFull = 0;

    async function request(req, probe = false) {
      const res = await sim.clientRpc(id, req, { timeout: 500, probe });
      if (!res) {
        note(N.reply, `${id} never answered ${j(req)} sent at ${ms(sim.now() - 500)}, although it was up. The test stops at the first unanswered request.`);
        throw STOP;
      }
      if (res.type !== req.type + '_ok') {
        note(N.type, `${id} replied ${j(res)} to ${j(req)}; expected type "${req.type}_ok".`);
        throw STOP;
      }
      return res;
    }

    const logOf = (disk) => (Array.isArray(disk.log) ? disk.log : null);
    const stamp = () => {
      const disk = sim.disk(id);
      return { json: JSON.stringify(disk), logLen: logOf(disk) ? disk.log.length : -1 };
    };

    // Look at the disk after a request (or a restart) and check its shape.
    function inspect(what, booting = false) {
      const disk = sim.disk(id);
      const at = `after ${what} at ${ms(sim.now())}`;
      const log = logOf(disk);
      if (!log) {
        note(level === 1 ? N.durable : N.logLen, `node.disk.log is ${j(disk.log)} ${at}. Keep the log as an array at node.disk.log.`);
        return;
      }

      if (level === 2) {
        if (log.length > LOG_LIMIT) {
          note(N.logLen, `node.disk.log has ${log.length} entries ${at}; the limit is ${LOG_LIMIT}. Replaying an ever-longer log makes every restart slower. Save the whole map as a checkpoint, then start a fresh log.`);
        }
        const entries = log.map((e) => JSON.stringify(e));
        const rest = JSON.stringify({ ...disk, log: undefined });
        // What a node does to its disk while starting up (say, a checkpoint at
        // the end of recovery) is not charged to the writes.
        if (prevLog && !booting) {
          const appended = rest === prevRest && entries.length >= prevLog.length && prevLog.every((e, i) => e === entries[i]);
          if (!appended) {
            rewrites++;
            firstRewrite ??= sim.now();
          }
        }
        prevLog = entries;
        prevRest = rest;
      }

      if (level >= 3) {
        if (log.length > MEMTABLE) {
          note(N.logLen, `node.disk.log has ${log.length} entries ${at}; the limit is ${MEMTABLE}. Once the memtable has taken ${MEMTABLE} writes, flush it to a new segment and start a fresh log.`);
        }
        const segs = disk.segments;
        if (!Array.isArray(segs)) {
          note(N.format, `node.disk.segments is ${j(segs)} ${at}. Keep the segments in an array at node.disk.segments, oldest first.`);
          return;
        }
        for (let i = 0; i < segs.length; i++) {
          const problem = segmentProblem(segs[i], i);
          if (problem) {
            note(N.format, `${problem} (Seen ${at}.)`);
            return;
          }
        }
        if (level === 3) {
          const big = segs.findIndex((s) => s.entries.length > MEMTABLE);
          if (big >= 0) note(N.format, `node.disk.segments[${big}] has ${segs[big].entries.length} entries ${at}. A flushed memtable holds at most ${MEMTABLE}.`);
          const now = segs.map((s) => JSON.stringify(s));
          if (prevSegs) {
            const changed = prevSegs.findIndex((s, i) => s !== now[i]);
            if (changed >= 0) {
              note(N.immutable, `node.disk.segments[${changed}] was ${prevSegs[changed].slice(0, 70)}… and is now ${now[changed] === undefined ? 'gone' : now[changed].slice(0, 70) + '…'} ${at}. In this task a segment is written once and only ever read after that; new data goes into a new segment at the end.`);
            }
          }
          prevSegs = now;
        } else {
          const total = segs.reduce((n, s) => n + s.entries.length, 0);
          if (segs.length > MAX_SEGMENTS) {
            note(N.segCount, `node.disk.segments has ${segs.length} segments ${at}; the limit is ${MAX_SEGMENTS}. Every read may have to search each one, so merge them before replying.`);
          }
          if (total > MAX_ENTRIES) {
            const tombs = segs.reduce((n, s) => n + s.entries.filter((e) => e[1] === null).length, 0);
            const live = [...model.values()].filter((m) => m.value !== null).length;
            note(N.segTotal, `The segments hold ${total} entries ${at} (${tombs} of them tombstones) for ${plural(live, 'live key')}; the limit is ${MAX_ENTRIES}. A merge should keep only the newest entry per key, and a merge that includes the oldest segment can drop tombstones altogether.`);
          }
        }
      }
    }

    // Read the segments the way the brief says a lookup does (newest first).
    // Only what is still in the memtable may differ from the truth.
    function compareSegments(what) {
      if (level < 3 || fails.has(N.format)) return;
      const segs = sim.disk(id).segments;
      if (!Array.isArray(segs) || segs.some((s, i) => segmentProblem(s, i))) return;
      const stored = new Map();
      for (const s of segs) for (const [k, v] of s.entries) stored.set(k, v ?? null);
      const differ = [];
      for (const k of new Set([...model.keys(), ...stored.keys()])) {
        const want = model.get(k)?.value ?? null;
        if ((stored.get(k) ?? null) !== want) differ.push(k);
      }
      if (differ.length > MEMTABLE) {
        const k = differ[0];
        note(N.inSegments, `${what} at ${ms(sim.now())}: read newest-first, the segments disagree with the store on ${differ.length} keys (for example "${k}": segments say ${j(stored.get(k) ?? null)}, the store should hold ${j(model.get(k)?.value ?? null)}). Only the last ${MEMTABLE} writes may live outside the segments.`);
      }
    }

    function checkValue(key, got, how) {
      const m = model.get(key);
      const want = m ? m.value : null;
      if (got === want) return;
      // Blame the crash only while reads without one have been right: a
      // plain read bug would otherwise show up here too, with the wrong advice.
      const afterCrash = m && m.t < lastCrash && !fails.has(N.read);
      const crashNote = afterCrash ? ` ${id} was restarted at ${ms(lastCrash)}, after that write was acknowledged.` : '';
      let detail;
      if (!m) {
        detail = `${how} returned ${j(got)} for "${key}" at ${ms(sim.now())}, but that key was never written. A missing key reads as null.`;
      } else if (want === null) {
        detail = `${how} returned ${j(got)} for "${key}" at ${ms(sim.now())}, but the key was deleted at ${ms(m.t)}.${crashNote} A delete must be recorded as durably as a put, and must keep hiding older values wherever they are stored.`;
      } else if (got === null || got === undefined) {
        detail = `${how} found nothing for "${key}" at ${ms(sim.now())}, but put ${j(want)} was acknowledged at ${ms(m.t)}.${crashNote}${afterCrash ? ' Whatever was only in memory at the crash is gone; an acknowledged write has to be on node.disk.' : ''}`;
      } else {
        detail = `${how} returned ${j(got)} for "${key}" at ${ms(sim.now())}, an older value; the latest acknowledged put was ${j(want)} at ${ms(m.t)}.${crashNote}`;
      }
      note(afterCrash ? N.crash : N.read, detail);
    }

    async function write(key, value) {
      const before = stamp();
      const prev = model.get(key);
      const req = value === null ? { type: 'delete', key } : { type: 'put', key, value };
      await request(req);
      if (!model.has(key)) touched.push(key);
      model.set(key, { value, t: sim.now(), seq: ++writes });
      const after = stamp();
      // Deleting a key that is not there need not leave a trace.
      if (value !== null || (prev && prev.value !== null)) {
        if (level === 1 && before.logLen >= 0 && after.logLen <= before.logLen) {
          note(N.durable, `${id} acknowledged ${j(req)} at ${ms(sim.now())}, but node.disk.log still has ${after.logLen} entries, as before the request. Push a record of the change onto the log before replying.`);
        } else if (after.json === before.json) {
          note(N.durable, `${id} acknowledged ${j(req)} at ${ms(sim.now())}, but node.disk is exactly as it was before the request. If the node crashed now the write would be gone.`);
        }
      }
      inspect(j(req));
    }

    async function get(key) {
      const tracked = level === 5;
      const before = tracked ? await request({ type: 'stats' }, true) : null;
      const res = await request({ type: 'get', key });
      checkValue(key, res.value ?? null, 'get');
      if (!tracked) return;
      const after = await request({ type: 'stats' }, true);
      const segs = sim.disk(id).segments;
      const count = Array.isArray(segs) ? segs.length : 0;
      const delta = after.segment_reads - before.segment_reads;
      if (!Number.isInteger(delta) || delta < 0 || delta > count) {
        note(N.stats, `segment_reads went from ${j(before.segment_reads)} to ${j(after.segment_reads)} across get "${key}" at ${ms(sim.now())}, with ${plural(count, 'segment')} on disk. It must be a running count of segments whose entries a get searched: between 0 and ${count} more for this get.`);
        return;
      }
      const m = model.get(key);
      if (!m) {
        absentGets++;
        absentReads += delta;
        absentFull += count;
      } else if (m.value !== null && writes - m.seq >= MEMTABLE && delta === 0) {
        note(N.found, `get "${key}" at ${ms(sim.now())} returned its value, which was written ${writes - m.seq} writes ago and so lives in a segment, yet segment_reads did not move. Count every segment whose entries you search.`);
      }
    }

    async function scan(from, to) {
      const res = await request({ type: 'scan', from, to });
      const want = [...model].filter(([k, m]) => m.value !== null && k >= from && k < to).map(([k, m]) => [k, m.value]).sort((a, b) => (a[0] < b[0] ? -1 : 1));
      const got = res.entries;
      const where = `scan "${from}".."${to}" at ${ms(sim.now())}`;
      if (!Array.isArray(got) || got.some((e) => !Array.isArray(e) || e.length !== 2)) {
        return note(N.scan, `${where} replied ${j(res)}. Reply with entries: [[key, value], ...].`);
      }
      if (JSON.stringify(got) === JSON.stringify(want)) return;
      const unsorted = got.findIndex((e, i) => i > 0 && !(got[i - 1][0] < e[0]));
      if (unsorted > 0) return note(N.scan, `${where} returned "${got[unsorted - 1][0]}" before "${got[unsorted][0]}". Keys must come back in ascending order, each once.`);
      const outside = got.find((e) => !(e[0] >= from && e[0] < to));
      if (outside) return note(N.scan, `${where} returned "${outside[0]}", which is outside the range. from is inclusive, to is exclusive.`);
      const gotMap = new Map(got);
      const missing = want.find(([k]) => !gotMap.has(k));
      if (missing) return note(N.scan, `${where} left out "${missing[0]}" (value ${j(missing[1])}, written at ${ms(model.get(missing[0]).t)}). A scan has to look in the memtable and in every segment.`);
      for (const [k, v] of got) checkValue(k, v, 'scan');
    }

    async function crashAndRecover(twice) {
      for (let round = 0; round < (twice ? 2 : 1); round++) {
        sim.crash(id);
        crashCount++;
        await sim.sleep(round ? 4 : 10);
        // On the real runner this waits until the node has finished starting up,
        // so writes made during recovery are seen by the inspection below.
        await sim.restart(id);
        lastCrash = sim.now();
      }
      inspect('a restart', true);
      compareSegments('After a restart');
    }

    const crashAt = new Set();
    for (let c = 1; c <= crashes; c++) crashAt.add(Math.floor((c * ops) / (crashes + 1)));

    // Churn: every key is written once and deleted soon after, so the set of
    // keys ever seen keeps growing while few are alive.
    const alive = [];
    let nextKey = 0;
    const anyKey = () => (absent ? keyName(2 * Math.floor(sim.rand() * keys)) : keyName(Math.floor(sim.rand() * keys)));

    async function script() {
      let crashDue = false;
      for (let i = 0; i < ops; i++) {
        if (crashAt.has(i)) crashDue = true;
        const r = sim.rand();
        let wrote = null;
        if (churn) {
          if (r < 0.4 && alive.length < 35) {
            wrote = keyName(nextKey++);
            alive.push(wrote);
            await write(wrote, 'v' + ++valueSeq);
          } else if (r < 0.75 && alive.length > 0) {
            wrote = alive.shift();
            await write(wrote, null);
          } else if (r < 0.95 || level < 3) {
            await get(keyName(Math.max(0, nextKey - 1 - Math.floor(sim.rand() * 50))));
          } else {
            const a = Math.floor(sim.rand() * (nextKey + 1));
            await scan(keyName(Math.max(0, a - 40)), keyName(a));
          }
        } else if (r < 0.5) {
          wrote = anyKey();
          await write(wrote, 'v' + ++valueSeq);
        } else if (r < 0.64) {
          wrote = anyKey();
          await write(wrote, null);
        } else if (r < 0.94 || level < 3) {
          // In the Bloom filter task half the lookups are for keys that were
          // never written but sort between ones that were.
          if (absent && sim.rand() < 0.5) await get(keyName(2 * Math.floor(sim.rand() * keys) + 1));
          else await get(anyKey());
        } else {
          const a = Math.floor(sim.rand() * (absent ? 2 * keys : keys));
          const b = a + 1 + Math.floor(sim.rand() * 15);
          await scan(keyName(a), keyName(b));
        }
        if (crashDue && wrote !== null) {
          crashDue = false;
          // The acknowledgement has just arrived: the hardest moment to lose power.
          await crashAndRecover(crashCount % 3 === 2);
          await get(wrote);
          for (let n = 0; n < 3; n++) await get(sim.pick(touched));
        }
        await sim.sleep(2);
      }
      if (crashes > 0) await crashAndRecover(false);
      for (const key of touched.slice(-300)) await get(key);
      if (level >= 3) await scan('k', 'l');
      inspect('the final reads');
      compareSegments('At the end');
      finished = true;
    }

    sim.at(50, async () => {
      try {
        inspect('startup');
        await script();
      } catch (e) {
        if (e !== STOP) throw e;
        stopped = true;
      }
    });

    return () => {
      const result = (name) => check(name, !fails.has(name), fails.get(name));
      const names = [N.reply, N.type, N.read];
      if (level >= 3) names.push(N.scan);
      if (crashes > 0) names.push(N.crash);
      names.push(N.durable);
      if (level >= 2) names.push(N.logLen);
      if (level === 2) {
        const allowed = Math.floor(writes / WRITES_PER_REWRITE);
        if (rewrites > allowed) {
          note(N.rewrites, `Something on node.disk other than the tail of the log changed ${rewrites} times in ${writes} writes (first at ${ms(firstRewrite)}); at most ${allowed} are allowed, one per ${WRITES_PER_REWRITE} writes. Rewriting a checkpoint costs as much as the whole data set, so most writes should only push onto node.disk.log.`);
        }
        names.push(N.rewrites);
      }
      if (level >= 3) names.push(N.format);
      if (level === 3) names.push(N.immutable);
      if (level >= 4) names.push(N.segCount, N.segTotal);
      if (level >= 3) names.push(N.inSegments);
      if (level === 5) {
        if (finished && absentFull > 0 && absentReads > BLOOM_BUDGET * absentFull) {
          note(N.skip, `${absentGets} gets for keys that were never written searched ${absentReads} segments, out of ${absentFull} they would search with no filter (${Math.round((100 * absentReads) / absentFull)}%; the limit is ${Math.round(BLOOM_BUDGET * 100)}%). Give each segment a Bloom filter over its keys and skip the segment when the filter says the key is not there.`);
        }
        names.push(N.stats, N.found, N.skip);
      }
      const checks = names.map(result);
      if (!finished && !stopped) {
        checks.push(check(N.done, false, `The run ended at ${ms(sim.now())} with requests still to send. ${id} is taking far too long to answer.`));
      }
      return checks;
    };
  };
}

export const storeWal = engine(1);
export const storeCheckpoint = engine(2);
export const storeLsm = engine(3);
export const storeCompaction = engine(4);
export const storeBloom = engine(5);

// ------------------------------------------------------------------ B-tree

const PAGE_KEYS = 4; // store-btree: most keys a page may hold

// Walk the tree in node.disk from the root. Returns { problem } or
// { height, entries: Map(key -> value), pages: Map(id -> JSON) }.
export function btreeShape(disk, maxKeys = PAGE_KEYS) {
  const pages = disk && disk.pages;
  if (!pages || typeof pages !== 'object' || Array.isArray(pages)) return { problem: `node.disk.pages is ${j(pages)}. Keep the pages in an object at node.disk.pages, keyed by page id.` };
  if (disk.root === undefined || disk.root === null) return { problem: 'node.disk.root is not set. It must hold the id of the root page.' };
  const entries = new Map();
  const seen = new Set();
  let leafDepth = null;
  const walk = (id, depth, lo, hi, path) => {
    const key = String(id);
    const p = pages[key];
    const where = `page ${j(id)} (${path})`;
    if (!p || typeof p !== 'object') return `${where} does not exist in node.disk.pages.`;
    if (seen.has(key)) return `${where} is reached twice. Every page except the root has exactly one parent.`;
    seen.add(key);
    if (!Array.isArray(p.keys) || p.keys.some((k) => typeof k !== 'string')) return `${where} has keys ${j(p.keys)}; keys must be an array of strings.`;
    if (p.keys.length > maxKeys) return `${where} holds ${p.keys.length} keys; a page holds at most ${maxKeys}. Split a page as soon as it would hold more.`;
    for (let i = 1; i < p.keys.length; i++) if (!(p.keys[i - 1] < p.keys[i])) return `${where} has keys ${j(p.keys)}; they must be strictly ascending.`;
    const out = p.keys.find((k) => (lo !== null && k < lo) || (hi !== null && k >= hi));
    if (out !== undefined) return `${where} holds ${j(out)}, but its parent only sends keys ${lo === null ? '' : '>= ' + j(lo)}${lo !== null && hi !== null ? ' and ' : ''}${hi === null ? '' : '< ' + j(hi)} here.`;
    if (p.leaf === true) {
      if (!Array.isArray(p.values) || p.values.length !== p.keys.length) return `${where} is a leaf with ${p.keys.length} keys but values ${j(p.values)}; it needs one value per key.`;
      if (leafDepth === null) leafDepth = depth;
      else if (leafDepth !== depth) return `${where} is a leaf at depth ${depth}, but another leaf is at depth ${leafDepth}. All leaves must be at the same depth: grow the tree only at the root.`;
      p.keys.forEach((k, i) => entries.set(k, p.values[i]));
      return null;
    }
    if (p.leaf !== false) return `${where} has leaf ${j(p.leaf)}; it must be true or false.`;
    if (p.keys.length === 0) return `${where} is an inner page with no keys.`;
    if (!Array.isArray(p.children) || p.children.length !== p.keys.length + 1) return `${where} has ${p.keys.length} keys and children ${j(p.children)}; an inner page has one more child than keys.`;
    for (let i = 0; i < p.children.length; i++) {
      const problem = walk(p.children[i], depth + 1, i === 0 ? lo : p.keys[i - 1], i === p.keys.length ? hi : p.keys[i], `${path} > ${j(p.children[i])}`);
      if (problem) return problem;
    }
    return null;
  };
  const problem = walk(disk.root, 1, null, null, 'root');
  if (problem) return { problem };
  const orphan = Object.keys(pages).find((id) => !seen.has(id));
  if (orphan !== undefined) return { problem: `page ${j(orphan)} is in node.disk.pages but cannot be reached from the root. Remove pages that are no longer part of the tree in the same step that unlinks them.` };
  const json = new Map(Object.keys(pages).map((id) => [id, JSON.stringify(pages[id])]));
  return { height: leafDepth, entries, pages: json };
}

export function storeBtree(sim, { ops = 300, keys = 40, crashes = 0, ascending = false, deletes = 0.14 }) {
  const id = sim.nodeIds[0];
  const model = new Map(); // key -> { value, t }
  const touched = [];
  const fails = new Map();
  const note = (name, detail) => {
    if (!fails.has(name)) fails.set(name, detail);
  };
  const N = {
    reply: 'Every request gets a reply',
    type: 'Replies have the expected type',
    read: 'Reads return the latest write',
    scan: 'Scans return the live keys in range, in key order',
    crash: 'Acknowledged writes survive a crash',
    durable: 'A write is in the tree on disk before it is acknowledged',
    shape: `The pages form a valid B-tree (sorted, at most ${PAGE_KEYS} keys per page, all leaves at one depth)`,
    path: 'A write changes only a few pages, not the whole tree',
    done: 'The test ran to the end',
  };
  let finished = false;
  let stopped = false;
  let valueSeq = 0;
  let lastCrash = -1;
  let prev = null; // last good shape
  let maxHeight = 0;

  async function request(req) {
    const res = await sim.clientRpc(id, req, { timeout: 500 });
    if (!res) {
      note(N.reply, `${id} never answered ${j(req)} sent at ${ms(sim.now() - 500)}, although it was up. The test stops at the first unanswered request.`);
      throw STOP;
    }
    if (res.type !== req.type + '_ok') {
      note(N.type, `${id} replied ${j(res)} to ${j(req)}; expected type "${req.type}_ok".`);
      throw STOP;
    }
    return res;
  }

  // Check the tree after a request; for a write, also count the pages it touched.
  function inspect(what, write = false) {
    const at = `after ${what} at ${ms(sim.now())}`;
    const shape = btreeShape(sim.disk(id));
    if (shape.problem) {
      note(N.shape, `${shape.problem} (Seen ${at}.)`);
      prev = null;
      return;
    }
    maxHeight = Math.max(maxHeight, shape.height);
    const differ = [...new Set([...model.keys(), ...shape.entries.keys()])].filter((k) => (model.get(k)?.value ?? null) !== (shape.entries.has(k) ? shape.entries.get(k) : null));
    if (differ.length) {
      const k = differ[0];
      note(N.durable, `${at}, the tree on disk has ${j(shape.entries.get(k) ?? null)} for "${k}", but the store holds ${j(model.get(k)?.value ?? null)} (${plural(differ.length, 'key')} differ). Every acknowledged write must already be in the pages: a crash now would find only what they hold.`);
    }
    if (write && prev) {
      let changed = 0;
      for (const [pid, js] of shape.pages) if (prev.pages.get(pid) !== js) changed++;
      for (const pid of prev.pages.keys()) if (!shape.pages.has(pid)) changed++;
      const limit = 3 * shape.height + 2;
      if (changed > limit) {
        note(N.path, `${what} at ${ms(sim.now())} added, changed or removed ${changed} of ${shape.pages.size} pages; with a tree of height ${shape.height} the limit is ${limit}. A put or delete touches the pages on one root-to-leaf path, plus the new pages a split creates; it never rewrites the rest of the tree.`);
      }
    }
    prev = shape;
  }

  function checkValue(key, got, how) {
    const m = model.get(key);
    const want = m ? m.value : null;
    if (got === want) return;
    const afterCrash = m && m.t < lastCrash && !fails.has(N.read);
    const detail = !m
      ? `${how} returned ${j(got)} for "${key}" at ${ms(sim.now())}, but that key was never written. A missing key reads as null.`
      : `${how} returned ${j(got)} for "${key}" at ${ms(sim.now())}; the latest acknowledged write (at ${ms(m.t)}) left it ${j(want)}.${afterCrash ? ` ${id} was restarted at ${ms(lastCrash)}, after that write.` : ''}`;
    note(afterCrash ? N.crash : N.read, detail);
  }

  async function write(key, value) {
    const before = JSON.stringify(sim.disk(id));
    const old = model.get(key);
    const req = value === null ? { type: 'delete', key } : { type: 'put', key, value };
    await request(req);
    if (!model.has(key)) touched.push(key);
    model.set(key, { value, t: sim.now() });
    if ((value !== null || (old && old.value !== null)) && JSON.stringify(sim.disk(id)) === before) {
      note(N.durable, `${id} acknowledged ${j(req)} at ${ms(sim.now())}, but node.disk is exactly as it was before the request. If the node crashed now the write would be gone.`);
    }
    inspect(j(req), true);
  }

  async function get(key) {
    const res = await request({ type: 'get', key });
    checkValue(key, res.value ?? null, 'get');
  }

  async function scan(from, to) {
    const res = await request({ type: 'scan', from, to });
    const want = [...model].filter(([k, m]) => m.value !== null && k >= from && k < to).map(([k, m]) => [k, m.value]).sort((a, b) => (a[0] < b[0] ? -1 : 1));
    const got = res.entries;
    const where = `scan "${from}".."${to}" at ${ms(sim.now())}`;
    if (!Array.isArray(got) || got.some((e) => !Array.isArray(e) || e.length !== 2)) return note(N.scan, `${where} replied ${j(res)}. Reply with entries: [[key, value], ...].`);
    if (JSON.stringify(got) === JSON.stringify(want)) return;
    const unsorted = got.findIndex((e, i) => i > 0 && !(got[i - 1][0] < e[0]));
    if (unsorted > 0) return note(N.scan, `${where} returned "${got[unsorted - 1][0]}" before "${got[unsorted][0]}". Keys must come back in ascending order, each once.`);
    const outside = got.find((e) => !(e[0] >= from && e[0] < to));
    if (outside) return note(N.scan, `${where} returned "${outside[0]}", which is outside the range. from is inclusive, to is exclusive.`);
    const gotMap = new Map(got);
    const missing = want.find(([k]) => !gotMap.has(k));
    if (missing) return note(N.scan, `${where} left out "${missing[0]}". A scan has to visit every leaf whose key range overlaps [from, to): descend into each child that can hold keys in it.`);
    for (const [k, v] of got) checkValue(k, v, 'scan');
  }

  async function crashAndRecover(twice) {
    for (let round = 0; round < (twice ? 2 : 1); round++) {
      sim.crash(id);
      await sim.sleep(round ? 4 : 10);
      await sim.restart(id);
      lastCrash = sim.now();
    }
    inspect('a restart');
  }

  const crashAt = new Set();
  for (let c = 1; c <= crashes; c++) crashAt.add(Math.floor((c * ops) / (crashes + 1)));
  let nextKey = 0;
  const anyKey = () => keyName(Math.floor(sim.rand() * keys));

  async function script() {
    let crashDue = false;
    let crashCount = 0;
    for (let i = 0; i < ops; i++) {
      if (crashAt.has(i)) crashDue = true;
      const r = sim.rand();
      let wrote = null;
      if (r < 0.5) {
        wrote = ascending && nextKey < keys ? keyName(nextKey++) : anyKey();
        await write(wrote, 'v' + ++valueSeq);
      } else if (r < 0.5 + deletes) {
        wrote = anyKey();
        await write(wrote, null);
      } else if (r < 0.92) {
        await get(anyKey());
      } else {
        const a = Math.floor(sim.rand() * keys);
        await scan(keyName(a), keyName(a + 1 + Math.floor(sim.rand() * 15)));
      }
      if (crashDue && wrote !== null) {
        crashDue = false;
        await crashAndRecover(crashCount++ % 3 === 2);
        await get(wrote);
        for (let n = 0; n < 3; n++) await get(sim.pick(touched));
      }
      await sim.sleep(2);
    }
    if (crashes > 0) await crashAndRecover(false);
    for (const key of touched) await get(key);
    await scan('k', 'l');
    inspect('the final reads');
    finished = true;
  }

  sim.at(50, async () => {
    try {
      await script();
    } catch (e) {
      if (e !== STOP) throw e;
      stopped = true;
    }
  });

  return () => {
    const result = (name) => check(name, !fails.has(name), fails.get(name));
    if (finished && maxHeight < 3) note(N.shape, `The tree never grew past height ${maxHeight}. With ${keys} keys and at most ${PAGE_KEYS} per page it needs at least three levels: split full pages, and a full root becomes two pages under a new root.`);
    const names = [N.reply, N.type, N.read, N.scan];
    if (crashes > 0) names.push(N.crash);
    names.push(N.durable, N.shape, N.path);
    const checks = names.map(result);
    if (!finished && !stopped) checks.push(check(N.done, false, `The run ended at ${ms(sim.now())} with requests still to send. ${id} is taking far too long to answer.`));
    return checks;
  };
}
