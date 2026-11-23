import { useCallback, useEffect, useRef, useState } from 'react';

const STALL_MS = 8000;

const stalled = () => ({
  ok: false,
  checks: [
    {
      name: 'Finishes in time',
      ok: false,
      detail:
        'The run stopped responding and was cancelled. Look for a loop that never ends, such as a while (true) with no await inside it.',
    },
  ],
  stats: null,
  trace: [],
  logs: [],
  nodeIds: [],
  partitions: [],
  duration: 0,
});

// Runs a challenge's tests in a worker and reports results as they arrive.
// `lang` is part of the key: results from a JS run must not count for Go.
export function useRunner(slug, testCount, lang = 'js') {
  const [run, setRun] = useState({ status: 'idle', results: [], current: -1 });
  const worker = useRef(null);
  const watchdog = useRef(null);
  const request = useRef(null);

  const stop = useCallback(() => {
    clearTimeout(watchdog.current);
    worker.current?.terminate();
    worker.current = null;
    request.current?.abort();
    request.current = null;
  }, []);

  // Go: the lab server builds the code and runs every test against real
  // processes, streaming one JSON line per event.
  const startGo = useCallback(
    async (code, { docker = false, only = null, seed = null } = {}) => {
      stop();
      const ctrl = new AbortController();
      request.current = ctrl;
      // A single-test run keeps the other tests' last results on screen.
      setRun((r) => ({
        status: 'running',
        only,
        lang: 'go',
        current: -1,
        phase: 'build',
        results: only == null || r.results.length !== testCount ? Array(testCount).fill(null) : r.results.map((x, i) => (i === only ? null : x)),
      }));
      const fail = (name, detail) =>
        setRun((r) => ({
          status: 'done',
          current: -1,
          results: r.results.map((x, i) => x ?? (i === (only ?? 0) ? { ...stalled(), checks: [{ name, ok: false, detail }] } : { skipped: true })),
        }));
      try {
        const res = await fetch(`/api/go/${slug}/run`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ code, docker, only, seed }),
          signal: ctrl.signal,
        });
        if (!res.ok) {
          const body = await res.json().catch(() => ({}));
          return fail('The lab server accepted the run', body.error ?? `HTTP ${res.status}`);
        }
        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buf = '';
        for (;;) {
          const { value, done } = await reader.read();
          if (done) break;
          buf += decoder.decode(value, { stream: true });
          let nl;
          while ((nl = buf.indexOf('\n')) >= 0) {
            const line = buf.slice(0, nl);
            buf = buf.slice(nl + 1);
            if (!line.trim()) continue;
            const ev = JSON.parse(line);
            if (ev.type === 'start') setRun((r) => ({ ...r, current: ev.index, phase: 'test' }));
            else if (ev.type === 'result') setRun((r) => ({ ...r, results: r.results.map((x, i) => (i === ev.index ? ev.result : x)) }));
            else if (ev.type === 'build-error') return fail('Code compiles', ev.output);
            else if (ev.type === 'error') return fail('The run completed', ev.message);
            else if (ev.type === 'done') setRun((r) => ({ ...r, status: 'done', current: -1, results: r.results.map((x) => x ?? { skipped: true }) }));
          }
        }
      } catch (e) {
        if (e.name !== 'AbortError') fail('The lab server is reachable', `${e.message}. Start the site with "npm run dev" so the Go runner is available.`);
      } finally {
        if (request.current === ctrl) request.current = null;
      }
    },
    [slug, testCount, stop],
  );

  useEffect(() => {
    setRun({ status: 'idle', results: [], current: -1 });
    return stop;
  }, [slug, lang, stop]);

  const start = useCallback(
    (code, { only = null, seed = null } = {}) => {
      stop();
      const w = new Worker(new URL('../sim/worker.js', import.meta.url), { type: 'module' });
      worker.current = w;
      setRun((r) => ({
        status: 'running',
        only,
        lang: 'js',
        current: only ?? 0,
        results: only == null || r.results.length !== testCount ? Array(testCount).fill(null) : r.results.map((x, i) => (i === only ? null : x)),
      }));

      const giveUp = () => {
        stop();
        setRun((r) => ({
          status: 'done',
          current: -1,
          results: r.results.map((x, i) => x ?? (i === Math.max(0, r.current) ? stalled() : { skipped: true })),
        }));
      };
      const arm = () => {
        clearTimeout(watchdog.current);
        watchdog.current = setTimeout(giveUp, STALL_MS);
      };
      arm();

      w.onmessage = ({ data }) => {
        arm();
        if (data.type === 'start') setRun((r) => ({ ...r, current: data.index }));
        if (data.type === 'result') {
          setRun((r) => ({ ...r, results: r.results.map((x, i) => (i === data.index ? data.result : x)) }));
        }
        if (data.type === 'done') {
          stop();
          setRun((r) => ({ ...r, status: 'done', current: -1, results: r.results.map((x) => x ?? { skipped: true }) }));
        }
      };
      w.onerror = giveUp;
      w.postMessage({ slug, code, only, seed });
    },
    [slug, testCount, stop],
  );

  // Stop button: abort the run; tests that had not finished show as not run.
  const cancel = useCallback(() => {
    stop();
    setRun((r) => (r.status === 'running' ? { status: 'done', current: -1, stopped: true, results: r.results.map((x) => x ?? { skipped: true }) } : r));
  }, [stop]);

  return [run, start, startGo, cancel];
}
