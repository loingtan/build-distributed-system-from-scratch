// Workloads for the Sharding track.
//
//   shardRouting    put/get through any node; every key lives on exactly one node
//   shardRing       set_members + lookup; how many keys move, and how even the shares are
//   shardRebalance  data must follow its key when the membership changes

import { check, j, plural, sample, unanswered } from '../workloads.js';

// The same hash the starters hand to the learner. Used here only to work out
// what "hash % N" would have done, for comparison in the failure messages.
export function hash(str) {
  let h = 2166136261;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  h ^= h >>> 16;
  h = Math.imul(h, 0x85ebca6b);
  h ^= h >>> 13;
  h = Math.imul(h, 0xc2b2ae35);
  h ^= h >>> 16;
  return h >>> 0;
}

const pct = (n, of) => `${of ? Math.round((100 * n) / of) : 0}%`;
const list = (ids) => `[${ids.join(', ')}]`;

function badReplyType(results) {
  const bad = results.find((r) => r.res && r.res.type !== `${r.req.type}_ok`);
  return check('Replies are put_ok / get_ok', !bad, bad && `${bad.node} replied ${j(bad.res)} to ${j(bad.req)}.`);
}

// Ask every node which keys it stores. Returns Map(node -> Set(keys)), or a
// description of the first reply that was missing or malformed.
async function localKeys(sim) {
  const replies = await Promise.all(sim.nodeIds.map((n) => sim.clientRpc(n, { type: 'local_keys' }, { client: 'c9' })));
  const held = new Map();
  let problem = null;
  replies.forEach((res, i) => {
    const node = sim.nodeIds[i];
    if (!res) problem ??= `${node} did not answer local_keys.`;
    else if (res.type !== 'local_keys_ok' || !Array.isArray(res.keys)) problem ??= `${node} replied ${j(res)}; expected { type: "local_keys_ok", keys: [...] }.`;
    else held.set(node, new Set(res.keys));
  });
  return { held, problem };
}

// Ask every node who owns each key. Returns the owner list (one per key) the
// nodes agree on, or the first problem found.
async function lookupAll(sim, keys, members, when) {
  const replies = await Promise.all(sim.nodeIds.map((n) => sim.clientRpc(n, { type: 'lookup', keys }, { probe: true })));
  let first = null;
  for (let i = 0; i < replies.length; i++) {
    const node = sim.nodeIds[i];
    const res = replies[i];
    if (!res) return { silent: `${node} did not answer lookup ${when}.` };
    if (res.type !== 'lookup_ok' || !Array.isArray(res.owners) || res.owners.length !== keys.length) {
      return { shape: `${node} replied ${j(res)} to a lookup of ${plural(keys.length, 'key')}; expected { type: "lookup_ok", owners: [...] } with one owner per key, in the same order.` };
    }
    const k = res.owners.findIndex((o) => !members.includes(o));
    if (k >= 0) return { outsider: `${when}, ${node} says ${keys[k]} belongs to ${j(res.owners[k])}, which is not one of the members ${list(members)}.` };
    if (!first) first = { node, owners: res.owners };
    else {
      const d = res.owners.findIndex((o, x) => o !== first.owners[x]);
      if (d >= 0) return { disagree: `${when}, ${first.node} says ${keys[d]} belongs to ${first.owners[d]} but ${node} says ${res.owners[d]}. The owner may depend only on the key and the member list, not on which node is asked or the order the list arrived in.` };
    }
  }
  return { owners: first.owners };
}

async function setMembers(sim, members) {
  const req = { type: 'set_members', members: [...members] };
  const replies = await Promise.all(sim.nodeIds.map((n) => sim.clientRpc(n, req, { client: 'c8' })));
  const i = replies.findIndex((r) => !r || r.type !== 'set_members_ok');
  return i < 0 ? null : `${sim.nodeIds[i]} replied ${j(replies[i])} to ${j(req)}; expected { type: "set_members_ok" }.`;
}

// ------------------------------------------------------ hash partitioning

export function shardRouting(sim, { keys = 240, clients = 8, spread = [0.5, 1.5], msgsPerOp = 2 }) {
  const ids = sim.nodeIds;
  const names = Array.from({ length: keys }, (_, i) => `user:${1000 + i}`);
  const results = [];
  const latest = new Map(); // key -> last acknowledged value
  const via = new Map(); // key -> node that took that put
  const unsure = new Set(); // keys with an unanswered put
  const stopAt = sim.duration - 800;
  let nextValue = 1;
  let wrongRead = null;
  let held = null;
  let heldProblem = 'The test did not get as far as asking for local_keys.';

  const put = async (key) => {
    const node = sim.pick(ids);
    const req = { type: 'put', key, value: nextValue++ };
    const res = await sim.clientRpc(node, req);
    results.push({ node, req, res });
    if (res && res.type === 'put_ok') {
      latest.set(key, req.value);
      via.set(key, node);
    } else unsure.add(key);
  };
  const get = async (key, client = 'c1') => {
    const node = sim.pick(ids);
    const req = { type: 'get', key };
    const res = await sim.clientRpc(node, req, { client });
    results.push({ node, req, res });
    if (!res || res.type !== 'get_ok' || unsure.has(key)) return;
    const want = latest.has(key) ? latest.get(key) : null;
    if ((res.value ?? null) !== want) {
      wrongRead ??= latest.has(key)
        ? `${node} answered get ${key} with ${j(res.value)}, but ${via.get(key)} acknowledged put ${key} = ${want} before that. Whichever node a request arrives at, it has to end up at the one node that owns the key.`
        : `${node} answered get ${key} with ${j(res.value)}, but that key was never written; the value should be null.`;
    }
  };

  // Each client works through its own keys, one request at a time.
  for (let c = 0; c < clients; c++) {
    sim.at(100 + c * 5, async () => {
      const mine = names.filter((_, i) => i % clients === c);
      for (const key of mine) {
        if (sim.now() > stopAt) return;
        await put(key);
        if (sim.rand() < 0.5) await get(key);
      }
      for (const key of mine) {
        if (sim.now() > stopAt) return;
        if (sim.rand() < 0.3) {
          await put(key);
          await get(key);
        }
      }
    });
  }

  sim.at(sim.duration - 600, async () => {
    const written = [...latest.keys()];
    await Promise.all([...written, 'user:nobody', 'user:missing'].map((key) => get(key, 'c2')));
    const r = await localKeys(sim);
    held = r.held;
    heldProblem = r.problem;
  });

  return () => {
    const checks = [
      unanswered('Every request gets a reply', results, 'A node that does not own the key still has to answer: forward the request to the owner, wait for its reply and pass it on.'),
      badReplyType(results),
      check('A get on any node returns the last value put, whichever node took the put', !wrongRead, wrongRead),
      check('Every node answers local_keys with { type: "local_keys_ok", keys: [...] }', !heldProblem, heldProblem),
    ];
    if (heldProblem) return checks;

    const written = [...latest.keys()].filter((k) => !unsure.has(k));
    let copies = null;
    let nowhere = null;
    for (const key of written) {
      const on = ids.filter((n) => held.get(n).has(key));
      if (on.length > 1) copies ??= `${key} is stored on ${on.join(' and ')}. Its puts arrived at different nodes and each kept a copy; all of them must send it to the same owner.`;
      if (on.length === 0) nowhere ??= `put ${key} was acknowledged by ${via.get(key)}, but no node lists it in local_keys.`;
    }
    const fair = written.length / ids.length;
    const counts = ids.map((n) => ({ node: n, count: written.filter((k) => held.get(n).has(k)).length }));
    const off = counts.find((c) => c.count > fair * spread[1] || c.count < fair * spread[0]);
    const perOp = sim.stats.nodeMsgs / Math.max(1, sim.ops.length);
    checks.push(
      check('Every key is stored on exactly one node', !copies && !nowhere && written.length > 0, copies ?? nowhere ?? 'No put was acknowledged.'),
      check(
        `Each node stores between ${spread[0]}× and ${spread[1]}× its fair share of the keys`,
        !off && !copies,
        off
          ? `${off.node} stores ${off.count} of ${written.length} keys; a fair share is ${Math.round(fair)}. All nodes: ${counts.map((c) => `${c.node}=${c.count}`).join(', ')}. Pick the owner from a hash of the key so the keys spread out.`
          : 'Fix the duplicate copies first; the shares are counted once each key has one home.',
      ),
      check(
        `At most ${msgsPerOp} node-to-node messages per client request`,
        perOp <= msgsPerOp,
        `Your nodes exchanged ${perOp.toFixed(1)} messages per request (${sim.stats.nodeMsgs} for ${plural(sim.ops.length, 'request')}). Every node can work out the owner by itself, so one forward and one reply is the most a request needs.`,
      ),
    );
    return checks;
  };
}

// -------------------------------------------- consistent hashing, vnodes

// plan: the member lists to switch to, in order. The cluster starts with
// every node as a member.
export function shardRing(sim, { keys = 2000, plan = [], spread = null, writes = 40 }) {
  const ids = sim.nodeIds;
  const names = Array.from({ length: keys }, (_, i) => `user:${1000 + i}`);
  const fail = {};
  const results = [];
  let finished = false;
  let wrongRead = null;
  let misplaced = null;
  let heldProblem = null;
  let worst = null; // the membership change that moved the most keys needlessly

  const modulo = (members) => {
    const sorted = [...members].sort();
    return names.map((k) => sorted[hash(k) % sorted.length]);
  };

  const survey = async (members, when) => {
    const r = await lookupAll(sim, names, members, when);
    for (const kind of ['silent', 'shape', 'outsider', 'disagree']) if (r[kind]) fail[kind] ??= r[kind];
    if (!r.owners) return null;
    const counts = new Map(members.map((m) => [m, 0]));
    for (const o of r.owners) counts.set(o, counts.get(o) + 1);
    const shares = members.map((m) => `${m}=${counts.get(m)}`).join(', ');
    const fair = keys / members.length;
    const none = members.find((m) => counts.get(m) === 0);
    if (none) fail.empty ??= `${when}, ${none} is a member but owns none of the ${keys} keys (${shares}).`;
    if (spread) {
      const off = members.find((m) => counts.get(m) > fair * spread[1] || counts.get(m) < fair * spread[0]);
      if (off) fail.spread ??= `${when}, ${off} owns ${counts.get(off)} of ${keys} keys; a fair share is ${Math.round(fair)}, so the allowed range is ${Math.ceil(fair * spread[0])} to ${Math.floor(fair * spread[1])}. All members: ${shares}. With few points per node the arcs are as uneven as the hash makes them; many points per node average out.`;
    }
    return r.owners;
  };

  sim.at(100, async () => {
    let members = [...ids];
    let owners = await survey(members, `With the starting members ${list(members)}`);
    for (const next of plan) {
      const problem = await setMembers(sim, next);
      if (problem) fail.ack ??= problem;
      const after = await survey(next, `After set_members ${list(next)}`);
      if (owners && after) {
        let moved = 0;
        let needless = 0;
        let example = null;
        for (let i = 0; i < keys; i++) {
          if (owners[i] === after[i]) continue;
          moved++;
          // Fine if the old owner is gone or the new owner has just arrived.
          if (next.includes(owners[i]) && members.includes(after[i])) {
            needless++;
            example ??= `${names[i]} went from ${owners[i]} to ${after[i]}`;
          }
        }
        if (needless && (!worst || needless > worst.needless)) {
          const before = modulo(members);
          const mod = modulo(next).filter((o, i) => o !== before[i]).length;
          worst = {
            needless,
            text: `Going from ${list(members)} to ${list(next)} gave ${moved} of ${keys} keys (${pct(moved, keys)}) a new owner. ${needless} of those moved between two nodes that were members before and after, e.g. ${example}. Only ${moved - needless} (${pct(moved - needless, keys)}) had to move. For comparison, hash(key) % N over these lists moves ${pct(mod, keys)}.`,
          };
        }
      }
      members = next;
      owners = after;
    }

    // The data has to follow the ring: fresh keys must land on the node lookup names.
    const fresh = Array.from({ length: writes }, (_, i) => `order:${500 + i}`);
    const value = new Map();
    await Promise.all(
      fresh.map(async (key, i) => {
        const node = sim.pick(ids);
        const req = { type: 'put', key, value: 9000 + i };
        const res = await sim.clientRpc(node, req);
        results.push({ node, req, res });
        if (res && res.type === 'put_ok') value.set(key, req.value);
      }),
    );
    await Promise.all(
      fresh.map(async (key) => {
        const node = sim.pick(ids);
        const req = { type: 'get', key };
        const res = await sim.clientRpc(node, req, { client: 'c2' });
        results.push({ node, req, res });
        if (res && res.type === 'get_ok' && value.has(key) && res.value !== value.get(key)) {
          wrongRead ??= `${node} answered get ${key} with ${j(res.value)} after put ${key} = ${value.get(key)} was acknowledged. put and get must route with the same ownerOf that lookup uses.`;
        }
      }),
    );
    const where = await lookupAll(sim, fresh, members, `With members ${list(members)}`);
    const { held, problem } = await localKeys(sim);
    heldProblem = problem;
    if (where.owners && !problem) {
      fresh.forEach((key, i) => {
        if (!value.has(key)) return;
        const on = ids.filter((n) => held.get(n).has(key));
        if (on.length !== 1 || on[0] !== where.owners[i]) {
          misplaced ??= `lookup says ${key} belongs to ${where.owners[i]}, but it is stored on ${on.length ? on.join(' and ') : 'no node'}. The members are ${list(members)}.`;
        }
      });
    }
    finished = true;
  });

  return () => {
    const checks = [
      check('Every node acknowledges set_members', !fail.ack, fail.ack),
      check('Every node answers lookup with one owner per key', !fail.silent && !fail.shape, fail.silent ?? fail.shape),
      check('Owners are always current members', !fail.outsider, fail.outsider),
      check('All nodes agree on the owner of every key', !fail.disagree, fail.disagree),
    ];
    if (fail.silent || fail.shape) return checks;
    checks.push(
      check('A key changes owner only if its old owner left or its new owner just joined', !worst, worst?.text),
      check('Every member owns some keys', !fail.empty, fail.empty),
    );
    if (spread) checks.push(check(`Every member owns between ${spread[0]}× and ${spread[1]}× its fair share`, !fail.spread, fail.spread));
    checks.push(
      unanswered('put and get still get replies', results),
      check(
        'put and get follow the ring: a key is stored on the node lookup names',
        finished && !wrongRead && !misplaced && !heldProblem,
        wrongRead ?? misplaced ?? heldProblem ?? 'The test ran out of time before the put/get phase finished; some requests were not answered.',
      ),
    );
    return checks;
  };
}

// ------------------------------------------------------------ rebalancing

// initial: member list to set before any write (default: every node).
// plan: member lists to switch to. After each switch, `rewrite` keys are
// overwritten at once, then the test waits `settle` ms and checks everything.
export function shardRebalance(sim, { keys = 200, initial = null, plan = [], settle = 1500, rewrite = 0 }) {
  const ids = sim.nodeIds;
  const names = Array.from({ length: keys }, (_, i) => `user:${1000 + i}`);
  const results = [];
  const latest = new Map();
  const older = new Map(); // key -> Set of values it held before the latest
  const unsure = new Map(); // key -> Set of values whose put was never acknowledged
  const fail = {};
  let nextValue = 1;
  let finished = false;

  const put = async (key) => {
    const node = sim.pick(ids);
    const req = { type: 'put', key, value: nextValue++ };
    const res = await sim.clientRpc(node, req);
    results.push({ node, req, res });
    if (res && res.type === 'put_ok') {
      if (latest.has(key)) (older.get(key) ?? older.set(key, new Set()).get(key)).add(latest.get(key));
      latest.set(key, req.value);
    } else (unsure.get(key) ?? unsure.set(key, new Set()).get(key)).add(req.value);
  };

  const verify = async (members, when) => {
    const reads = await Promise.all(
      names.map(async (key) => {
        const node = sim.pick(ids);
        const req = { type: 'get', key };
        const res = await sim.clientRpc(node, req, { client: 'c2' });
        results.push({ node, req, res });
        return { node, key, res };
      }),
    );
    const where = await lookupAll(sim, names, members, when);
    for (const kind of ['silent', 'shape', 'outsider', 'disagree']) if (where[kind]) fail[kind] ??= where[kind];
    const { held, problem } = await localKeys(sim);
    if (problem) fail.held ??= problem;

    const gone = [];
    const stale = [];
    for (const r of reads) {
      if (!r.res || r.res.type !== 'get_ok' || !latest.has(r.key)) continue;
      const got = r.res.value ?? null;
      if (got === latest.get(r.key) || unsure.get(r.key)?.has(got)) continue;
      if (got === null) gone.push(r);
      else if (older.get(r.key)?.has(got)) stale.push(r);
      else fail.garbage ??= `${when}, ${r.node} answered get ${r.key} with ${j(got)}, a value that was never written to it.`;
    }
    if (gone.length) {
      const r = gone[0];
      const on = problem ? null : ids.filter((n) => held.get(n).has(r.key));
      const owner = where.owners ? where.owners[names.indexOf(r.key)] : null;
      fail.lost ??=
        `${when}, ${gone.length} of ${keys} keys read as null (e.g. ${sample(gone.map((g) => g.key), 3)}). ${r.key} = ${latest.get(r.key)} was acknowledged` +
        (owner ? `; it now belongs to ${owner}` : '') +
        (on ? (on.length ? `, but the value is still sitting on ${on.join(' and ')}. The old owner has to hand it over.` : ', and no node stores it any more. Delete a key only after the new owner has confirmed it has it.') : '.');
    }
    if (stale.length) {
      const r = stale[0];
      fail.stale ??= `${when}, ${r.node} answered get ${r.key} with ${j(r.res.value)}, an old value; the last acknowledged put was ${latest.get(r.key)}. ${plural(stale.length, 'key')} went back in time. A handed-over value must not replace one that a later put already stored on the new owner.`;
    }
    if (where.owners && !problem) {
      names.forEach((key, i) => {
        if (!latest.has(key)) return;
        const owner = where.owners[i];
        const strays = ids.filter((n) => n !== owner && held.get(n).has(key));
        if (strays.length) fail.stray ??= `${when}, ${key} belongs to ${owner} but ${strays.join(' and ')} still ${strays.length === 1 ? 'stores' : 'store'} a copy. Once the new owner has confirmed the handoff, the old owner should delete its copy.`;
        else if (!held.get(owner).has(key)) fail.absent ??= `${when}, ${key} belongs to ${owner} but ${owner} does not list it in local_keys.`;
      });
    }
  };

  sim.at(100, async () => {
    let members = [...ids];
    if (initial) {
      const problem = await setMembers(sim, initial);
      if (problem) fail.ack ??= problem;
      members = initial;
    }
    await Promise.all(names.map(put));
    await verify(members, `Before any change, with members ${list(members)}`);
    for (const next of plan) {
      const problem = await setMembers(sim, next);
      if (problem) fail.ack ??= problem;
      const changed = sim.now();
      if (rewrite) {
        const picked = new Set();
        while (picked.size < Math.min(rewrite, keys)) picked.add(sim.pick(names));
        await Promise.all([...picked].map(put));
      }
      await sim.sleep(settle);
      await verify(next, `${Math.round(sim.now() - changed)}ms after set_members ${list(next)}`);
      members = next;
    }
    finished = true;
  });

  return () => {
    const checks = [
      unanswered('Every put and get gets a reply', results, 'If messages between nodes can be lost, a forwarded request has to be retried until the owner answers.'),
      badReplyType(results),
      check('Every node acknowledges set_members', !fail.ack, fail.ack),
      check('Every node answers lookup and local_keys', !fail.silent && !fail.shape && !fail.held, fail.silent ?? fail.shape ?? fail.held),
      check('All nodes agree on owners, and owners are members', !fail.outsider && !fail.disagree, fail.outsider ?? fail.disagree),
      check('No acknowledged write is lost when the members change', !fail.lost && !fail.absent, fail.lost ?? fail.absent),
      check('Reads return the newest value, not one from before a handoff', !fail.stale && !fail.garbage, fail.stale ?? fail.garbage),
      check('Once settled, each key is stored only on its owner', !fail.stray, fail.stray),
      check('The test ran to the end', finished, 'Some requests timed out, so the last membership changes were never checked. Fix the unanswered requests first.'),
    ];
    return checks;
  };
}

// ------------------------------------------------- changes under load

// plan: member lists for epochs 1, 2, ... The change to epoch i+1 starts
// gap ms after the previous one (first at `start`) and reaches each node at a random moment within `spread` ms.
// For every change but the last, one node gets it only after the next one, so
// it must ignore it. Clients read and write throughout. `delay` holds back
// half of the node-to-node messages by up to that many ms while changes run.
export function shardEpochs(sim, { keys = 80, clients = 8, plan = [], start = 600, gap = 400, spread = 250, delay = 0, slow = null, readShare = 0.4 }) {
  const ids = sim.nodeIds;
  const names = Array.from({ length: keys }, (_, i) => `user:${1000 + i}`);
  const writes = new Map(names.map((k) => [k, []])); // per key, in order: { value, node, invoke, complete, acked }
  const byValue = new Map(); // value -> { key, index }
  const reads = []; // { node, key, res, invoke, complete }
  const finals = [];
  const results = [];
  const acks = [];
  const staleSent = []; // { node, epoch, newer, t }
  const sentMax = new Map(ids.map((n) => [n, 0]));
  const K = plan.length;
  // gap may be a list, used in turn: [150, 500] means 150ms to the next change, then 500ms, ...
  const gaps = Array.isArray(gap) ? gap : [gap];
  const changeAt = plan.map(() => 0);
  for (let i = 1; i < K; i++) changeAt[i] = changeAt[i - 1] + gaps[(i - 1) % gaps.length];
  for (let i = 0; i < K; i++) changeAt[i] += start;
  const changesEnd = (K ? changeAt[K - 1] : start) + spread + 300;
  const clientsEnd = changesEnd + 300;
  const finalAt = sim.duration - 1300; // leaves time for a 1000ms read timeout, then the lookups
  let nextValue = 1;
  let views = null; // node -> owners, from the final lookup
  let viewProblem = null;
  let held = null;
  let heldProblem = null;
  let finished = false;

  // While the changes run: hold back half of all node-to-node messages by up
  // to `delay` ms, and every request (not reply) sent by `slow.node` by about
  // `slow.ms`, so that node's handoffs are still in flight when it gets keys back.
  if (delay > 0 || slow) {
    sim.intercept = ({ src, body, kind, t }) => {
      if (kind !== 'node' || t < start || t >= changesEnd) return undefined;
      let extra = 0;
      if (slow && src === slow.node && body.in_reply_to == null) extra += slow.ms * (0.8 + 0.4 * sim.rand());
      if (delay > 0 && sim.rand() < 0.5) extra += sim.rand() * delay;
      return extra || undefined;
    };
  }

  // When each node receives each epoch.
  const when = plan.map((_, i) => ids.map(() => changeAt[i] + sim.rand() * spread));
  let prev = null;
  for (let i = K - 2; i >= 0; i--) {
    const late = sim.pick(ids.filter((n) => n !== prev));
    prev = late;
    const x = ids.indexOf(late);
    when[i][x] = when[i + 1][x] + 20 + sim.rand() * 120;
  }
  plan.forEach((members, i) => {
    ids.forEach((node, x) => {
      sim.at(when[i][x], async () => {
        const epoch = i + 1;
        if (sentMax.get(node) > epoch) staleSent.push({ node, epoch, newer: sentMax.get(node), t: sim.now() });
        sentMax.set(node, Math.max(sentMax.get(node), epoch));
        const req = { type: 'set_members', epoch, members: [...members] };
        const res = await sim.clientRpc(node, req, { client: 'c9' });
        acks.push({ node, req, res });
      });
    });
  });

  const put = async (key) => {
    const node = sim.pick(ids);
    const req = { type: 'put', key, value: nextValue++ };
    const list = writes.get(key);
    const w = { value: req.value, node, invoke: sim.now(), complete: null, acked: false };
    byValue.set(req.value, { key, index: list.length });
    list.push(w);
    const res = await sim.clientRpc(node, req);
    w.complete = sim.now();
    w.acked = !!res && res.type === 'put_ok';
    results.push({ node, req, res });
  };
  const get = async (key, into, client = 'c1') => {
    const node = sim.pick(ids);
    const req = { type: 'get', key };
    const invoke = sim.now();
    const res = await sim.clientRpc(node, req, { client });
    results.push({ node, req, res });
    if (res && res.type === 'get_ok') into.push({ node, key, res, invoke, complete: sim.now() });
  };

  sim.at(100, async () => {
    await Promise.all(names.map(put));
    // Each client writes only its own keys, one request at a time, so the
    // writes to any one key have a clear order. Reads go to any key.
    await Promise.all(
      Array.from({ length: clients }, async (_, c) => {
        const mine = names.filter((_, i) => i % clients === c);
        while (sim.now() < clientsEnd) {
          if (sim.rand() < readShare) await get(sim.pick(names), reads, `c${c + 1}`);
          else await put(sim.pick(mine));
        }
      }),
    );
  });

  sim.at(finalAt, async () => {
    await Promise.all(names.map((key) => get(key, finals, 'c10')));
    const replies = await Promise.all(ids.map((n) => sim.clientRpc(n, { type: 'lookup', keys: names }, { probe: true })));
    views = new Map();
    replies.forEach((res, x) => {
      if (!res) viewProblem ??= `${ids[x]} did not answer lookup.`;
      else if (res.type !== 'lookup_ok' || !Array.isArray(res.owners) || res.owners.length !== keys) viewProblem ??= `${ids[x]} replied ${j(res)} to a lookup of ${keys} keys.`;
      else views.set(ids[x], res.owners);
    });
    const r = await localKeys(sim);
    held = r.held;
    heldProblem = r.problem;
    finished = true;
  });

  // Is this read's value at least as new as the last put acknowledged before it began?
  const judge = (r) => {
    const list = writes.get(r.key);
    let last = -1;
    list.forEach((w, i) => {
      if (w.acked && w.complete < r.invoke) last = i;
    });
    const v = r.res.value ?? null;
    if (v === null) return last < 0 ? null : { kind: 'missing', r, want: list[last] };
    const at = byValue.get(v);
    if (!at || at.key !== r.key) return { kind: 'garbage', r };
    if (at.index < last) return { kind: 'stale', r, want: list[last], got: list[at.index] };
    return null;
  };
  const t = (ms) => `${Math.round(ms)}ms`;

  return () => {
    const bad = reads.map(judge).filter(Boolean);
    const missing = bad.filter((b) => b.kind === 'missing');
    const stale = bad.filter((b) => b.kind === 'stale');
    const garbage = bad.find((b) => b.kind === 'garbage') ?? finals.map(judge).find((b) => b && b.kind === 'garbage');
    const badAck = acks.find((a) => !a.res || a.res.type !== 'set_members_ok');
    const fbad = finals.map(judge).filter(Boolean);
    const lost = fbad.find((b) => b.kind === 'missing');
    const old = fbad.find((b) => b.kind === 'stale');

    const checks = [
      unanswered('Every put and get gets a reply', results, 'While two nodes disagree about the members, a forwarded request can bounce between them until one of them catches up with the newer epoch. A request held back while a node gathers its keys must also be answered within the client\'s 1000ms.'),
      badReplyType(results),
      check(
        'Every set_members is acknowledged, stale ones included',
        !badAck && acks.length === K * ids.length,
        badAck ? `${badAck.node} replied ${j(badAck.res)} to ${j(badAck.req)}.` : `Only ${acks.length} of ${K * ids.length} set_members requests were answered.`,
      ),
      check(
        'A get never misses a write that was already acknowledged',
        !missing.length,
        missing.length &&
          `${missing[0].r.node} answered get ${missing[0].r.key} with null at ${t(missing[0].r.invoke)}, but put ${missing[0].r.key} = ${missing[0].want.value} was acknowledged at ${t(missing[0].want.complete)}. ${plural(missing.length, 'read')} came back empty like this. A node that has just become the owner of a key must not answer for it until it has the newest copy from whoever held it before.`,
      ),
      check(
        'A get never returns a value older than one already acknowledged',
        !stale.length,
        stale.length &&
          `${stale[0].r.node} answered get ${stale[0].r.key} at ${t(stale[0].r.invoke)} with ${stale[0].got.value}, but put ${stale[0].r.key} = ${stale[0].want.value} had replaced it at ${t(stale[0].want.complete)}. ${plural(stale.length, 'read')} went back in time like this. Either an older copy replaced a newer one (compare versions, not arrival order), or a node answered from a copy that someone else had already taken over.`,
      ),
      check('Reads only return values that were written to that key', !garbage, garbage && `${garbage.r.node} answered get ${garbage.r.key} with ${j(garbage.r.res.value)}.`),
      check(
        'After the changes settle, every key holds its last acknowledged value',
        !lost && !old,
        lost
          ? `At ${t(lost.r.invoke)} get ${lost.r.key} returned null; put ${lost.r.key} = ${lost.want.value} was acknowledged at ${t(lost.want.complete)}. The write is gone.`
          : old && `At ${t(old.r.invoke)} get ${old.r.key} returned ${old.got.value}; put ${old.r.key} = ${old.want.value} was acknowledged later, at ${t(old.want.complete)}. An older copy overwrote it during a handoff.`,
      ),
    ];

    let disagree = viewProblem;
    if (!disagree && views && views.size === ids.length) {
      const sig = (n) => views.get(n).join(',');
      const counts = new Map();
      for (const n of ids) counts.set(sig(n), (counts.get(sig(n)) ?? 0) + 1);
      const common = [...counts.entries()].sort((a, b) => b[1] - a[1])[0][0];
      const odd = ids.find((n) => sig(n) !== common);
      if (odd) {
        const ref = ids.find((n) => sig(n) === common);
        const k = views.get(odd).findIndex((o, i) => o !== views.get(ref)[i]);
        const s = staleSent.filter((x) => x.node === odd).pop();
        disagree =
          `After the last change (epoch ${K}), ${odd} says ${names[k]} belongs to ${views.get(odd)[k]}, but ${ids.filter((n) => sig(n) === common).join(', ')} say ${views.get(ref)[k]}.` +
          (s ? ` ${odd} was sent set_members epoch ${s.epoch} at ${t(s.t)}, after it had been sent epoch ${s.newer}. A set_members whose epoch is not newer than one the node already has must be ignored (but still acknowledged).` : '');
      }
    }
    checks.push(check('After the last change, all nodes agree on the owners; stale set_members are ignored', !disagree, disagree));

    let stray = heldProblem;
    if (!stray && !disagree && held && views) {
      const owners = views.get(ids[0]);
      names.forEach((key, i) => {
        if (!writes.get(key).some((w) => w.acked)) return;
        const extra = ids.filter((n) => n !== owners[i] && held.get(n).has(key));
        if (extra.length) stray ??= `${key} belongs to ${owners[i]}, but ${extra.join(' and ')} still ${extra.length === 1 ? 'stores' : 'store'} a copy ${t(finalAt - changesEnd)} after the last change.`;
        else if (!held.get(owners[i]).has(key)) stray ??= `${key} belongs to ${owners[i]}, but ${owners[i]} does not list it in local_keys.`;
      });
    }
    checks.push(
      check('Once settled, each key is stored only on its owner', !stray && !disagree, stray ?? 'Fix the disagreement about owners first.'),
      check('The final inspection completed', finished, 'The final reads, lookups and local_keys did not all finish before the end of the test. Some requests are probably hanging.'),
    );
    return checks;
  };
}

// ------------------------------------------------------- key ranges

// Ranges and indexes subtrack. Shared: the range table a node reports,
// [[from, owner], ...] sorted by from, the first from being "".
function tableProblem(ranges, ids) {
  if (!Array.isArray(ranges) || ranges.length === 0) return 'ranges must be a non-empty array of [from, owner] pairs.';
  for (let i = 0; i < ranges.length; i++) {
    const r = ranges[i];
    if (!Array.isArray(r) || r.length !== 2 || typeof r[0] !== 'string' || !ids.includes(r[1])) return `entry ${i} is ${j(r)}; each entry must be [from, owner] with owner a node id.`;
    if (i === 0 && r[0] !== '') return `the first range starts at ${j(r[0])}; it must start at "" so that every key has an owner.`;
    if (i > 0 && !(ranges[i - 1][0] < r[0])) return `${j(ranges[i - 1][0])} comes before ${j(r[0])}; the ranges must be sorted by their start, each start once.`;
  }
  return null;
}

// Index of the range holding key: the last entry whose from <= key.
function rangeOf(table, key) {
  let i = 0;
  while (i + 1 < table.length && table[i + 1][0] <= key) i++;
  return i;
}

// max: most keys a range may hold. share: most of the keys one node may hold.
export function shardKeyRange(sim, { keys = 400, clients = 8, max = 40, share = 0.5, scans = 30, settle = 400 }) {
  const ids = sim.nodeIds;
  const names = Array.from({ length: keys }, (_, i) => 'k' + String(i).padStart(4, '0'));
  const results = [];
  const latest = new Map(); // key -> last acknowledged value
  const unsure = new Set();
  const fail = {};
  const stopAt = sim.duration - 2200;
  let nextValue = 1;
  let table = null;
  let finished = false;
  let watching = null; // Set of nodes seen in node-to-node messages during the current scan
  let scanned = 0;

  sim.onSend = (msg) => {
    if (watching && msg.kind === 'node') {
      watching.add(msg.src);
      watching.add(msg.dest);
    }
  };

  const put = async (key) => {
    const node = sim.pick(ids);
    const req = { type: 'put', key, value: nextValue++ };
    const res = await sim.clientRpc(node, req);
    results.push({ node, req, res });
    if (res && res.type === 'put_ok') latest.set(key, req.value);
    else unsure.add(key);
  };
  const get = async (key, client = 'c1') => {
    const node = sim.pick(ids);
    const req = { type: 'get', key };
    const res = await sim.clientRpc(node, req, { client });
    results.push({ node, req, res });
    if (!res || res.type !== 'get_ok' || unsure.has(key)) return;
    const want = latest.has(key) ? latest.get(key) : null;
    if ((res.value ?? null) !== want) fail.read ??= `${node} answered get ${key} with ${j(res.value)} at ${Math.round(sim.now())}ms; the last acknowledged put was ${j(want)}.`;
  };

  // Writes in random order, so ranges fill up and split all over the key space.
  sim.at(100, async () => {
    const order = [...names];
    for (let i = order.length - 1; i > 0; i--) {
      const k = Math.floor(sim.rand() * (i + 1));
      [order[i], order[k]] = [order[k], order[i]];
    }
    await Promise.all(
      Array.from({ length: clients }, async (_, c) => {
        const mine = order.filter((_, i) => i % clients === c);
        for (const key of mine) {
          if (sim.now() > stopAt) return;
          await put(key);
          if (sim.rand() < 0.25) await get(key, 'c' + (c + 1));
          if (sim.rand() < 0.15) await put(sim.pick(mine.filter((k) => latest.has(k))));
        }
      }),
    );
    await sim.sleep(settle);
    await inspect();
  });

  async function inspect() {
    // 1. Every node's range table: all must be well formed and identical.
    const tables = await Promise.all(ids.map((n) => sim.clientRpc(n, { type: 'ranges' }, { client: 'c9' })));
    for (let i = 0; i < ids.length; i++) {
      const res = tables[i];
      if (!res || res.type !== 'ranges_ok') {
        fail.table ??= `${ids[i]} replied ${j(res)} to { type: "ranges" }; expected { type: "ranges_ok", ranges: [[from, owner], ...] }.`;
        continue;
      }
      const p = tableProblem(res.ranges, ids);
      if (p) fail.table ??= `${ids[i]}'s range table is wrong: ${p}`;
    }
    if (!fail.table) {
      const sig = (r) => JSON.stringify(r.ranges);
      const odd = tables.findIndex((r) => sig(r) !== sig(tables[0]));
      if (odd >= 0) {
        const a = tables[0].ranges;
        const b = tables[odd].ranges;
        const diff = a.find((r) => !b.some((s) => s[0] === r[0] && s[1] === r[1])) ?? b.find((r) => !a.some((s) => s[0] === r[0] && s[1] === r[1]));
        fail.agree = `${settle}ms after the last write, ${ids[0]} reports ${a.length} ranges and ${ids[odd]} reports ${b.length}; they differ at ${j(diff)}. Every split has to be announced to every node.`;
      } else table = tables[0].ranges;
    }

    // 2. Where every key is stored.
    const { held, problem } = await localKeys(sim);
    if (problem) fail.held = problem;
    const written = names.filter((k) => latest.has(k) && !unsure.has(k));
    if (table && !problem) {
      const count = table.map(() => 0);
      for (const key of written) {
        const r = rangeOf(table, key);
        count[r]++;
        const owner = table[r][1];
        const on = ids.filter((n) => held.get(n).has(key));
        if (on.length !== 1 || on[0] !== owner) {
          fail.place ??= `${key} falls in the range starting at ${j(table[r][0])}, owned by ${owner}, but it is stored on ${on.length ? on.join(' and ') : 'no node'}. When a range is split, the keys of the new range must move to its new owner.`;
        }
      }
      const big = count.findIndex((c) => c > max);
      if (big >= 0) {
        const end = big + 1 < table.length ? j(table[big + 1][0]) : 'the end';
        fail.size = `The range from ${j(table[big][0])} to ${end}, owned by ${table[big][1]}, holds ${count[big]} keys; the limit is ${max}. ${table.length === 1 ? 'There is only one range: nothing ever split it.' : 'Split a range as soon as a put takes it past the limit.'}`;
      }
    }
    if (!problem && written.length) {
      const per = ids.map((n) => ({ n, c: written.filter((k) => held.get(n).has(k)).length }));
      const top = per.reduce((a, b) => (b.c > a.c ? b : a));
      if (top.c > share * written.length) {
        fail.share = `${top.n} stores ${top.c} of ${written.length} keys (${pct(top.c, written.length)}); the limit is ${pct(share, 1)}. All nodes: ${per.map((p) => p.n + '=' + p.c).join(', ')}. Give the upper half of a split range to another node.`;
      }
    }

    // 3. Scans, one at a time, in a quiet cluster.
    for (let s = 0; s < scans; s++) {
      const a = Math.floor(sim.rand() * keys);
      const width = s % 5 === 4 ? 40 + Math.floor(sim.rand() * 120) : 1 + Math.floor(sim.rand() * 12);
      const from = names[a];
      const to = a + width < keys ? names[a + width] : 'k9999';
      const node = sim.pick(ids);
      const req = { type: 'scan', from, to };
      watching = new Set();
      const res = await sim.clientRpc(node, req, { client: 'c3' });
      const seen = watching;
      watching = null;
      results.push({ node, req, res });
      if (!res) continue;
      scanned++;
      const where = `scan ${j(from)}..${j(to)} through ${node}`;
      const want = written.filter((k) => k >= from && k < to).map((k) => [k, latest.get(k)]);
      if (!Array.isArray(res.entries) || res.entries.some((e) => !Array.isArray(e) || e.length !== 2)) {
        fail.scan ??= `${where} replied ${j(res)}; expected { type: "scan_ok", entries: [[key, value], ...] }.`;
      } else if (JSON.stringify(res.entries) !== JSON.stringify(want)) {
        const got = res.entries;
        const bad = got.findIndex((e, i) => i > 0 && !(got[i - 1][0] < e[0]));
        const missing = want.find(([k]) => !got.some((e) => e[0] === k));
        const extra = got.find(([k]) => !(k >= from && k < to));
        fail.scan ??=
          bad > 0
            ? `${where} returned ${j(got[bad - 1][0])} before ${j(got[bad][0])}. Entries must be in ascending key order, each key once.`
            : extra
              ? `${where} returned ${j(extra[0])}, outside the range. from is inclusive, to is exclusive.`
              : missing
                ? `${where} left out ${missing[0]} (value ${missing[1]}). It returned ${got.length} of them; ${want.length} keys are in that range.`
                : `${where} returned ${j(got).slice(0, 120)}, expected ${j(want).slice(0, 120)}.`;
      }
      if (table && !fail.contact) {
        const allowed = new Set([node]);
        table.forEach(([start, owner], i) => {
          const end = i + 1 < table.length ? table[i + 1][0] : null;
          if (start < to && (end === null || end > from)) allowed.add(owner);
        });
        const extra = [...seen].filter((n) => !allowed.has(n));
        if (extra.length) {
          fail.contact = `${where} involved ${[...seen].sort().join(', ')} in messages between nodes, but only ${[...allowed].sort().join(', ')} own a range that overlaps it (or took the request). ${extra.join(' and ')} had nothing to contribute. Use the range table to send the scan only to the owners of overlapping ranges.`;
        }
      }
    }

    // 4. Final reads.
    await Promise.all(written.map((key) => get(key, 'c2')));
    finished = true;
  }

  return () => {
    const checks = [
      unanswered('Every request gets a reply', results, 'A node that does not own a key forwards the request to the owner in its range table and relays the answer.'),
      check('A get returns the last value put', !fail.read, fail.read),
      check('Every node answers ranges with a well-formed table', !fail.table, fail.table),
      check('All nodes agree on the range table once writes stop', !fail.table && !fail.agree, fail.agree ?? 'Fix the range tables first.'),
      check('Each key is stored only on the owner of its range', table && !fail.held && !fail.place, fail.held ?? fail.place ?? 'Fix the range tables first.'),
      check(`No range holds more than ${max} keys`, table && !fail.held && !fail.size, fail.size ?? 'Fix the range tables first.'),
      check(`No node stores more than ${pct(share, 1)} of the keys`, !fail.held && !fail.share, fail.held ?? fail.share),
      check('Scans return every key in the range, in order', !fail.scan && scanned > 0, fail.scan ?? 'No scan was answered.'),
      check('A scan involves only the owners of overlapping ranges', table && !fail.contact, fail.contact ?? 'Fix the range tables first.'),
      check('The final inspection completed', finished, 'The scans and final reads did not finish before the end of the test. Some requests are probably hanging.'),
    ];
    return checks;
  };
}

// ------------------------------------------------------- hot spots

// Counters: incr adds delta, get returns the sum. hotShare of all requests go
// to one key (the celebrity), picked at random; with shift, a second
// celebrity takes over halfway. A node's load is the number of requests (not
// replies) delivered to it, counted from `grace` ms after each celebrity
// appears. No node may handle more than `share` times the average.
export function shardHotSpot(sim, { cold = 100, clients = 10, hotShare = 0.7, share = 1.5, msgsPerOp = 2.6, grace = 600, shift = false }) {
  const ids = sim.nodeIds;
  const names = Array.from({ length: cold }, (_, i) => `post:${1000 + i}`);
  const start = 300;
  const stopAt = sim.duration - 1200;
  const shiftAt = shift ? Math.round((start + stopAt) / 2) : null;
  const hot1 = sim.pick(names);
  const hot2 = shift ? sim.pick(names.filter((k) => k !== hot1)) : null;
  const hotAt = (t) => (shift && t >= shiftAt ? hot2 : hot1);
  const counting = (t) => t >= start + grace && t < stopAt && !(shift && t >= shiftAt && t < shiftAt + grace);
  const incrs = new Map(names.map((k) => [k, []])); // { delta, invoke, complete, acked }
  const gets = []; // { node, key, value, invoke, complete }
  const results = [];
  const load = new Map(ids.map((n) => [n, 0]));
  let finalDone = false;

  sim.onDeliver = (msg) => {
    if (msg.src === 'probe' || msg.dest === 'probe' || !load.has(msg.dest)) return;
    if (msg.body.in_reply_to != null || !counting(sim.now())) return;
    load.set(msg.dest, load.get(msg.dest) + 1);
  };

  const incr = async (key) => {
    const node = sim.pick(ids);
    const req = { type: 'incr', key, delta: 1 + Math.floor(sim.rand() * 3) };
    const w = { delta: req.delta, invoke: sim.now(), complete: Infinity, acked: false };
    incrs.get(key).push(w);
    const res = await sim.clientRpc(node, req);
    results.push({ node, req, res });
    if (res && res.type === 'incr_ok') {
      w.complete = sim.now();
      w.acked = true;
    }
  };
  const get = async (key, client) => {
    const node = sim.pick(ids);
    const req = { type: 'get', key };
    const invoke = sim.now();
    const res = await sim.clientRpc(node, req, { client });
    results.push({ node, req, res });
    if (res && res.type === 'get_ok') gets.push({ node, key, value: res.value, invoke, complete: sim.now() });
  };

  for (let c = 0; c < clients; c++) {
    sim.at(start + c * 3, async () => {
      while (sim.now() < stopAt) {
        if (sim.rand() < hotShare) {
          const key = hotAt(sim.now());
          if (sim.rand() < 0.9) await incr(key);
          else await get(key, 'c' + (c + 1));
        } else {
          const key = sim.pick(names);
          if (sim.rand() < 0.8) await get(key, 'c' + (c + 1));
          else await incr(key);
        }
      }
    });
  }

  sim.at(sim.duration - 700, async () => {
    const keys = [hot1, ...(hot2 ? [hot2] : []), ...names.filter((k) => k !== hot1 && k !== hot2).slice(0, 20)];
    await Promise.all(keys.map((k) => get(k, 'c20')));
    finalDone = true;
  });

  return () => {
    const checks = [unanswered('Every request gets a reply', results, 'A request for a hot key still has to be answered within 1000ms, fan-out read included.')];
    const badType = results.find((r) => r.res && r.res.type !== r.req.type + '_ok');
    checks.push(check('Replies are incr_ok / get_ok', !badType, badType && `${badType.node} replied ${j(badType.res)} to ${j(badType.req)}.`));

    let wrong = null;
    let wrongCount = 0;
    for (const g of gets) {
      const list = incrs.get(g.key);
      const lo = list.filter((w) => w.acked && w.complete < g.invoke).reduce((s, w) => s + w.delta, 0);
      const hi = list.filter((w) => w.invoke < g.complete).reduce((s, w) => s + w.delta, 0);
      if (typeof g.value !== 'number' || g.value < lo || g.value > hi) {
        wrongCount++;
        wrong ??=
          typeof g.value !== 'number'
            ? `${g.node} answered get ${g.key} with ${j(g.value)} at ${Math.round(g.invoke)}ms; expected a number (0 for a key never incremented).`
            : g.value < lo
              ? `${g.node} answered get ${g.key} with ${g.value} at ${Math.round(g.invoke)}ms, but increments adding up to ${lo} had already been acknowledged.${g.key === hot1 || g.key === hot2 ? ' If the increments of a hot key are spread over several nodes, a read has to collect every part, whichever node it arrives at.' : ''}`
              : `${g.node} answered get ${g.key} with ${g.value} at ${Math.round(g.invoke)}ms, but all increments sent before the reply add up to only ${hi}. Is an increment counted twice?`;
      }
    }
    checks.push(check('A get counts every acknowledged increment, and nothing that was never sent', !wrong, wrong && (wrongCount > 1 ? `${wrong} (${plural(wrongCount, 'read')} like this.)` : wrong)));

    const total = [...load.values()].reduce((a, b) => a + b, 0);
    const fair = total / ids.length;
    const top = ids.reduce((a, b) => (load.get(b) > load.get(a) ? b : a));
    const hotOwners = [hot1, hot2].filter(Boolean).map((k) => `${k} hashes to ${ids[hash(k) % ids.length]}`).join('; ');
    checks.push(
      check(
        `No node handles more than ${share}× its fair share of requests`,
        total > 0 && load.get(top) <= share * fair,
        total === 0
          ? 'No requests were counted.'
          : `${top} handled ${load.get(top)} of ${total} requests (${(load.get(top) / fair).toFixed(2)}× its fair share; the limit is ${share}×), counted from ${grace}ms after each celebrity appeared. All nodes: ${ids.map((n) => n + '=' + load.get(n)).join(', ')}. ${pct(hotShare, 1)} of the traffic goes to one key (${hotOwners}). Notice the hot key and spread its increments over several nodes.`,
      ),
    );
    const perOp = sim.stats.nodeMsgs / Math.max(1, sim.ops.length);
    checks.push(
      check(
        `At most ${msgsPerOp} messages between nodes per client request`,
        perOp <= msgsPerOp,
        `Your nodes exchanged ${perOp.toFixed(2)} messages per request (${sim.stats.nodeMsgs} for ${plural(sim.ops.length, 'request')}). A fan-out read costs a message to every part, so spread only the keys that are actually hot; a cold key stays on its one owner.`,
      ),
      check('The final reads completed', finalDone, 'The final reads did not finish before the end of the test.'),
    );
    return checks;
  };
}

// ------------------------------------------------------- secondary index

// Records { color, n } sharded by key; find { color } returns the keys whose
// record has that color. The index may lag: a find must reflect every write
// acknowledged at least `lag` ms before it began, for keys not written since.
// delay: while clients run, half of all node-to-node messages are held back by
// up to that many ms. bursts: share of changes that are followed at once by
// one or two more changes to the same key.
export function shardSecondaryIndex(sim, { records = 60, clients = 6, lag = 250, delay = 0, bursts = 0.3, settle = 500 }) {
  const ids = sim.nodeIds;
  const colors = ['red', 'blue', 'green', 'black', 'white', 'silver'];
  const names = Array.from({ length: records }, (_, i) => `car:${100 + i}`);
  const writes = new Map(names.map((k) => [k, []])); // { color, value, invoke, complete, acked }
  const finds = []; // { node, color, keys, invoke, complete, quiet }
  const results = [];
  const fail = {};
  const stopAt = sim.duration - settle - 1500;
  let n = 1;
  let watching = null;
  let quietDone = false;

  if (delay > 0) {
    sim.intercept = ({ kind, t }) => (kind === 'node' && t < stopAt && sim.rand() < 0.5 ? sim.rand() * delay : undefined);
  }
  sim.onSend = (msg) => {
    if (watching && msg.kind === 'node') {
      watching.count++;
      watching.nodes.add(msg.src);
      watching.nodes.add(msg.dest);
    }
  };

  const put = async (key, color) => {
    const node = sim.pick(ids);
    const value = { color, n: n++ };
    const w = { color, value, invoke: sim.now(), complete: Infinity, acked: false };
    writes.get(key).push(w);
    const req = { type: 'put', key, value };
    const res = await sim.clientRpc(node, req);
    results.push({ node, req, res });
    if (res && res.type === 'put_ok') {
      w.complete = sim.now();
      w.acked = true;
    }
  };
  const current = (key) => {
    const list = writes.get(key);
    return list.length ? list[list.length - 1].color : null;
  };
  const find = async (color, client, quiet = false) => {
    const node = sim.pick(ids);
    const req = { type: 'find', color };
    const invoke = sim.now();
    const res = await sim.clientRpc(node, req, { client });
    results.push({ node, req, res });
    if (!res || res.type !== 'find_ok') return;
    if (!Array.isArray(res.keys) || res.keys.some((k) => typeof k !== 'string')) {
      fail.shape ??= `${node} replied ${j(res)} to ${j(req)}; expected { type: "find_ok", keys: ["car:101", ...] }.`;
      return;
    }
    finds.push({ node, color, keys: res.keys, invoke, complete: sim.now(), quiet });
  };
  const get = async (key, client) => {
    const node = sim.pick(ids);
    const req = { type: 'get', key };
    const invoke = sim.now();
    const res = await sim.clientRpc(node, req, { client });
    results.push({ node, req, res });
    if (!res || res.type !== 'get_ok') return;
    const list = writes.get(key);
    // Any value from the last put acknowledged before the get, or a later one.
    const from = list.findLastIndex((w) => w.acked && w.complete < invoke);
    const allowed = list.slice(Math.max(0, from)).filter((w) => w.invoke < sim.now()).map((w) => JSON.stringify(w.value));
    if (from < 0) allowed.push('null');
    const before = from >= 0 ? list[from] : null;
    const ok = allowed.includes(JSON.stringify(res.value ?? null));
    if (!ok) fail.get ??= `${node} answered get ${key} with ${j(res.value)} at ${Math.round(invoke)}ms; the last acknowledged put was ${j(before?.value ?? null)}.`;
  };

  for (let c = 0; c < clients; c++) {
    sim.at(100 + c * 4, async () => {
      const client = 'c' + (c + 1);
      const mine = names.filter((_, i) => i % clients === c);
      for (const key of mine) await put(key, sim.pick(colors));
      while (sim.now() < stopAt) {
        const r = sim.rand();
        if (r < 0.45) {
          const key = sim.pick(mine);
          const times = sim.rand() < bursts ? 2 + Math.floor(sim.rand() * 2) : 1;
          for (let i = 0; i < times; i++) await put(key, sim.pick(colors.filter((x) => x !== current(key))));
        } else if (r < 0.85) await find(sim.pick(colors), client);
        else await get(sim.pick(names), client);
        await sim.sleep(5 + sim.rand() * 20);
      }
    });
  }

  // Once things settle, ask for each color in turn, alone, and watch which
  // nodes take part.
  sim.at(stopAt + settle, async () => {
    for (const color of colors) {
      watching = { count: 0, nodes: new Set() };
      const before = finds.length;
      await find(color, 'c9', true);
      const seen = watching;
      watching = null;
      if (finds.length > before && seen.nodes.size > 2 && !fail.contact) {
        const f = finds[finds.length - 1];
        fail.contact = `find ${color} through ${f.node} involved ${[...seen.nodes].sort().join(', ')} (${plural(seen.count, 'message')} between nodes). With a global index, all the keys of one color are listed on one node: the coordinator asks that node and nobody else.`;
      }
    }
    await Promise.all(names.map((k) => get(k, 'c9')));
    quietDone = true;
  });

  const t = (x) => `${Math.round(x)}ms`;
  return () => {
    let missing = null;
    let extra = null;
    let bad = 0;
    for (const f of finds) {
      const got = new Set(f.keys);
      const stranger = f.keys.find((k) => !writes.has(k));
      if (stranger) {
        extra ??= `find ${f.color} through ${f.node} at ${t(f.invoke)} returned ${j(stranger)}, which is not a record that was ever written.`;
        bad++;
        continue;
      }
      for (const key of names) {
        const relevant = writes.get(key).filter((w) => w.invoke < f.complete);
        if (!relevant.length) continue;
        const last = relevant[relevant.length - 1];
        const stable = relevant.every((w) => w.acked) && last.complete <= f.invoke - lag;
        if (stable) {
          const want = last.color === f.color;
          if (want && !got.has(key)) {
            bad++;
            missing ??= `find ${f.color} through ${f.node} at ${t(f.invoke)} left out ${key}, which has been ${f.color} since its put was acknowledged at ${t(last.complete)}, ${t(f.invoke - last.complete)} earlier. The index may lag by at most ${lag}ms.`;
          } else if (!want && got.has(key)) {
            bad++;
            const prev = relevant.filter((w) => w.color === f.color).pop();
            extra ??= `find ${f.color} through ${f.node} at ${t(f.invoke)} returned ${key}, but ${key} became ${last.color} at ${t(last.complete)}, ${t(f.invoke - last.complete)} earlier${prev ? ` (it was ${f.color} until the put acknowledged at ${t(prev.complete)})` : ''}. When a record changes color, the index node of the old color must be told to drop it. And index updates travel separately, so they can arrive out of order: if the removal from the ${f.color} list arrives before the add it was meant to undo, the stale entry stays forever. Give each version of a record a number and let an index entry change only for a newer version.`;
          }
        } else if (got.has(key) && !relevant.some((w) => w.color === f.color)) {
          bad++;
          extra ??= `find ${f.color} through ${f.node} at ${t(f.invoke)} returned ${key}, which was never ${f.color}.`;
        }
      }
    }
    const checks = [
      unanswered('Every request gets a reply', results),
      check('Replies have the expected shape', !fail.shape && !results.some((r) => r.res && r.res.type !== r.req.type + '_ok'), fail.shape ?? (() => { const r = results.find((x) => x.res && x.res.type !== x.req.type + '_ok'); return r && `${r.node} replied ${j(r.res)} to ${j(r.req)}.`; })()),
      check('A get returns the latest record', !fail.get, fail.get),
      check(`A find includes every record that has had that color for ${lag}ms`, !missing, missing && (bad > 1 ? `${missing} (${plural(bad, 'wrong answer')} in all.)` : missing)),
      check(`A find leaves out records that changed color more than ${lag}ms ago`, !extra, extra),
      check('A find asks only the one node that holds the index for that color', quietDone && !fail.contact, fail.contact ?? 'The quiet finds at the end did not finish.'),
      check('The final inspection completed', quietDone, 'The finds and reads at the end did not finish before the end of the test.'),
    ];
    return checks;
  };
}
