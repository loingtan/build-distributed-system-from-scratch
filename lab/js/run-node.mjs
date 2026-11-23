#!/usr/bin/env node
// Runs a browser-lab JavaScript node file as a real process.
//
// The file is the same function body the browser lab runs, with `node` in
// scope; this shim implements that API over JSON lines on stdin/stdout.
// node.disk is saved to <data_dir>/disk.json (fsynced) before every message
// the node sends and at the end of every event, so it survives a SIGKILL.

import fs from 'node:fs';
import path from 'node:path';
import readline from 'node:readline';

const file = process.argv[2];
if (!file) {
  console.error('usage: run-node.mjs <node.js>');
  process.exit(2);
}
const code = fs.readFileSync(file, 'utf8');

let current = 0; // event being processed (echoed back as `cause`)
let timerEvents = 0;
let nextMsgId = Date.now() * 1000; // never reused across restarts
let epoch = Date.now();
let offset = 0;
let diskPath = null;
let lastDisk = '';
let api = null;
const handlers = new Map();
const pending = new Map();
const warned = new Set();

function assertJson(value, where = 'body') {
  if (value === null || value === undefined) return;
  const t = typeof value;
  if (t === 'string' || t === 'boolean') return;
  if (t === 'number') {
    if (!Number.isFinite(value)) throw new Error(`Message ${where} is ${value}; messages must be plain JSON.`);
    return;
  }
  if (t === 'function' || t === 'symbol' || t === 'bigint') throw new Error(`Message ${where} is a ${t}; messages must be plain JSON.`);
  if (Array.isArray(value)) return value.forEach((v, i) => assertJson(v, `${where}[${i}]`));
  const proto = Object.getPrototypeOf(value);
  if (proto !== Object.prototype && proto !== null) {
    throw new Error(`Message ${where} is a ${value.constructor?.name ?? 'object'}. Messages travel as JSON, so convert it first (for example [...mySet]).`);
  }
  for (const k of Object.keys(value)) assertJson(value[k], `${where}.${k}`);
}

function persist() {
  if (!diskPath || !api) return;
  let text;
  try {
    text = JSON.stringify(api.disk);
  } catch (e) {
    report('disk', `node.disk is not JSON: ${e.message}`);
    return;
  }
  if (text === lastDisk) return;
  const tmp = diskPath + '.tmp';
  const fd = fs.openSync(tmp, 'w');
  fs.writeSync(fd, text);
  fs.fsyncSync(fd);
  fs.closeSync(fd);
  fs.renameSync(tmp, diskPath);
  lastDisk = text;
}

function report(where, message) {
  process.stderr.write(`!error ${JSON.stringify({ where, message: String(message) })}\n`);
}

function out(dest, body) {
  persist();
  process.stdout.write(JSON.stringify({ src: api.id, dest, body, cause: current }) + '\n');
}

// Run fn as one event: `current` is its id until every microtask it caused
// has run, then node.disk is saved.
function event(id, where, fn) {
  current = id;
  try {
    const r = fn();
    if (r && typeof r.then === 'function') r.then(null, (e) => report(where, e?.message ?? e));
  } catch (e) {
    report(where, e?.message ?? e);
  }
  setImmediate(() => {
    if (current === id) current = 0;
    persist();
  });
}

const clone = (v) => JSON.parse(JSON.stringify(v));

function makeApi(init) {
  const ids = [...init.node_ids];
  const a = {
    id: init.node_id,
    nodeIds: ids,
    peers: ids.filter((n) => n !== init.node_id),
    disk: {},
    on(type, fn) {
      if (typeof fn !== 'function') throw new Error(`node.on("${type}", …) needs a function.`);
      handlers.set(type, fn);
    },
    send(dest, body) {
      assertJson(body);
      out(dest, body);
    },
    reply(req, body) {
      if (!req || !req.src || !req.body) throw new Error('node.reply(msg, body) needs the message you are replying to.');
      assertJson(body);
      out(req.src, { ...body, in_reply_to: req.body.msg_id });
    },
    rpc(dest, body, { timeout = 1000 } = {}) {
      const msg_id = ++nextMsgId;
      return new Promise((resolve) => {
        assertJson(body);
        pending.set(msg_id, resolve);
        out(dest, { ...body, msg_id });
        setTimeout(() => {
          if (pending.delete(msg_id)) event(-(++timerEvents), 'rpc timeout', () => resolve(null));
        }, timeout);
      });
    },
    after(ms, fn) {
      let live = true;
      setTimeout(() => live && event(-(++timerEvents), 'timer', fn), Math.max(0, Number(ms) || 0));
      return { cancel: () => (live = false) };
    },
    every(ms, fn) {
      const h = setInterval(() => event(-(++timerEvents), 'timer', fn), Math.max(1, Number(ms) || 0));
      return { cancel: () => clearInterval(h) };
    },
    now: () => Date.now() - epoch + offset,
    random: () => Math.random(),
    log: (...args) => process.stderr.write(args.map((x) => (typeof x === 'string' ? x : JSON.stringify(x))).join(' ') + '\n'),
  };
  if (init.kv) {
    a.kv = {
      read: async (key) => (await a.rpc('kv', { type: 'read', key }, { timeout: 5000 }))?.value ?? null,
      write: async (key, value) => {
        await a.rpc('kv', { type: 'write', key, value }, { timeout: 5000 });
      },
      cas: async (key, from, to) => (await a.rpc('kv', { type: 'cas', key, from: from ?? null, to }, { timeout: 5000 }))?.type === 'cas_ok',
    };
  }
  return a;
}

function start(env) {
  const init = env.body;
  epoch = init.epoch_ms ?? Date.now();
  offset = init.clock_offset_ms ?? 0;
  api = makeApi(init);
  if (init.data_dir) {
    fs.mkdirSync(init.data_dir, { recursive: true });
    diskPath = path.join(init.data_dir, 'disk.json');
    try {
      lastDisk = fs.readFileSync(diskPath, 'utf8');
      api.disk = JSON.parse(lastDisk);
    } catch {
      api.disk = {};
      lastDisk = '';
    }
  }
  const shim = { log: api.log, info: api.log, warn: api.log, error: api.log, debug: api.log };
  const clear = (h) => h && typeof h.cancel === 'function' && h.cancel();
  let factory;
  try {
    factory = new Function('node', 'console', 'setTimeout', 'setInterval', 'clearTimeout', 'clearInterval', `"use strict";\n${code}`);
  } catch (e) {
    report('compile', e.message);
    process.exit(1);
  }
  event(env.id ?? 0, 'startup', () => factory(api, shim, (fn, ms) => api.after(ms, fn), (fn, ms) => api.every(ms, fn), clear, clear));
  persist();
  process.stdout.write(JSON.stringify({ src: api.id, dest: env.src, body: { type: 'init_ok', in_reply_to: init.msg_id }, cause: env.id ?? 0 }) + '\n');
}

const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
rl.on('line', (line) => {
  let env;
  try {
    env = JSON.parse(line);
  } catch {
    return;
  }
  if (!api) {
    if (env.body?.type === 'init') start(env);
    return;
  }
  const body = env.body;
  const irt = body.in_reply_to;
  if (irt != null && pending.has(irt)) {
    const resolve = pending.get(irt);
    pending.delete(irt);
    event(env.id, 'rpc reply', () => resolve(clone(body)));
    return;
  }
  const handler = handlers.get(body.type);
  if (!handler) {
    if (irt == null && !warned.has(body.type)) {
      warned.add(body.type);
      api.log(`No handler for "${body.type}" messages (from ${env.src}); ignoring them.`);
    }
    return;
  }
  event(env.id, `handler "${body.type}"`, () => handler({ src: env.src, dest: env.dest, body: clone(body) }));
});
rl.on('close', () => process.exit(0));
