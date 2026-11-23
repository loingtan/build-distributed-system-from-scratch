// HTTP API that lets the web page edit and run Go nodes as real processes.
// Mounted into the Vite dev server (see vite.config.js), so `npm run dev`
// serves both the page and this API.
//
//   GET  /api/lab                      { ok, go, docker }   what this machine can run
//   GET  /api/go/<slug>                { code, starter, solution, notes, saved }
//   PUT  /api/go/<slug>                body { code }        save practice/<slug>/main.go
//   POST /api/go/<slug>/reset          restore the starter
//   POST /api/go/<slug>/run            body { code, docker, only? } -> NDJSON stream:
//        (closing the request stops the run; `only` runs a single test index)
//        {"type":"build"} {"type":"build-error","output":...}
//        {"type":"start","index":i} {"type":"result","index":i,"result":{...}}
//        {"type":"done"} | {"type":"error","message":...}

import { spawnSync } from 'node:child_process';
import { bySlug } from '../src/curriculum/catalog.js';
import { prepareProgram, runTest, seedFor, cleanupProgram, saveResults, goFile, goNotes, goSnippets, ensurePractice, practiceCode, goify } from './core.mjs';

let running = null; // one run at a time: tests are real-time and CPU sensitive

function available(cmd, args) {
  const r = spawnSync(cmd, args, { encoding: 'utf8' });
  return !r.error && r.status === 0;
}
let caps = null;
function capabilities() {
  caps ??= { ok: true, go: available('go', ['version']), docker: available('docker', ['version', '--format', '{{.Server.Version}}']) };
  return caps;
}

async function readJson(req) {
  let raw = '';
  for await (const chunk of req) raw += chunk;
  return raw ? JSON.parse(raw) : {};
}

function send(res, status, body) {
  res.statusCode = status;
  res.setHeader('Content-Type', 'application/json');
  res.end(JSON.stringify(body));
}

// Checker messages name the JavaScript API; show Go names to a Go learner.
function goifyResult(r) {
  return { ...r, checks: r.checks.map((c) => (c.detail ? { ...c, detail: goify(c.detail) } : c)) };
}

export function labMiddleware() {
  return async (req, res, next) => {
    const url = new URL(req.url, 'http://localhost');
    if (!url.pathname.startsWith('/api/')) return next();
    try {
      if (url.pathname === '/api/lab') return send(res, 200, capabilities());

      const m = url.pathname.match(/^\/api\/go\/([\w-]+)(\/(run|reset))?$/);
      if (!m) return send(res, 404, { error: 'not found' });
      const task = bySlug[m[1]];
      if (!task) return send(res, 404, { error: `no task ${m[1]}` });
      const starter = goFile(task.slug, 'starter');
      if (!starter) return send(res, 404, { error: `no Go starter for ${task.slug}` });

      if (!m[2] && req.method === 'GET') {
        const saved = practiceCode(task.slug);
        return send(res, 200, { code: saved ?? starter, starter, solution: goFile(task.slug, 'solution'), notes: goNotes(task.slug), snippets: goSnippets(task.slug), saved: saved != null });
      }
      if (!m[2] && req.method === 'PUT') {
        const { code } = await readJson(req);
        if (typeof code !== 'string') return send(res, 400, { error: 'code must be a string' });
        ensurePractice(task.slug, code);
        return send(res, 200, { ok: true });
      }
      if (m[3] === 'reset' && req.method === 'POST') {
        ensurePractice(task.slug, starter);
        return send(res, 200, { code: starter });
      }
      if (m[3] === 'run' && req.method === 'POST') {
        const { code, docker, only, seed } = await readJson(req);
        if (typeof code !== 'string') return send(res, 400, { error: 'code must be a string' });
        const indexes = Number.isInteger(only) && only >= 0 && only < task.tests.length ? [only] : task.tests.map((_, i) => i);
        if (running) return send(res, 409, { error: `Another run (${running}) is in progress. Wait for it to finish.` });
        running = task.slug;
        res.statusCode = 200;
        res.setHeader('Content-Type', 'application/x-ndjson');
        res.setHeader('Cache-Control', 'no-cache');
        const emit = (obj) => res.write(JSON.stringify(obj) + '\n');
        const stop = new AbortController();
        // The page closed the request (Stop button, navigation): kill the test in progress.
        res.on('close', () => stop.abort());
        let program = null;
        try {
          const dir = ensurePractice(task.slug, code);
          emit({ type: 'build' });
          program = prepareProgram(dir, { docker: !!docker, name: task.slug });
          if (program.buildError) {
            emit({ type: 'build-error', output: program.buildError });
          } else {
            const results = [];
            for (const i of indexes) {
              if (stop.signal.aborted) break;
              emit({ type: 'start', index: i });
              const base = Number.isInteger(seed) ? seed : undefined;
              const r = { ...goifyResult(await runTest(task, i, program, { docker: !!docker, signal: stop.signal, seed: base })), seed: seedFor(task, i, base) };
              if (stop.signal.aborted) break;
              results[i] = r;
              emit({ type: 'result', index: i, result: r });
            }
            if (!stop.signal.aborted && indexes.length === task.tests.length && !Number.isInteger(seed)) saveResults(task, 'go', !!docker, results);
          }
          if (!stop.signal.aborted) emit({ type: 'done' });
        } catch (e) {
          emit({ type: 'error', message: e?.message ?? String(e) });
        } finally {
          cleanupProgram(program);
          running = null;
          res.end();
        }
        return;
      }
      return send(res, 405, { error: 'method not allowed' });
    } catch (e) {
      return send(res, 500, { error: e?.message ?? String(e) });
    }
  };
}

// Vite plugin: adds the API to `npm run dev` and `npm run preview`.
export function labPlugin() {
  return {
    name: 'quorum-lab-api',
    configureServer(server) {
      server.middlewares.use(labMiddleware());
    },
    configurePreviewServer(server) {
      server.middlewares.use(labMiddleware());
    },
  };
}
