# Authoring a track

A track is four kinds of file. Slugs are globally unique, kebab-case.

```
src/curriculum/tracks/<track-id>.js          metadata, subtracks, task definitions
src/sim/workloads/<track-id>.js              client workloads + checkers for its tasks
src/curriculum/briefs/<task-slug>.md         what the learner reads
src/curriculum/code/<task-slug>.starter.js   what the editor opens with
src/curriculum/code/<task-slug>.solution.js  reference solution
```

Check your work with `npm run verify -- --track <track-id>` (or `-- <task-slug>`).
Every solution must pass every test; every starter must fail at least one.

Read these first, they are the model to follow:

- `src/curriculum/catalog.js` (existing tracks: mistakes / questions / reading shape; `extra` shows task definitions)
- `src/sim/workloads.js` (existing workloads and the exported helpers)
- `src/curriculum/api.md` (the node API the learner sees)
- `src/curriculum/briefs/broadcast-faults.md`, `g-counter.md`, `leader-election.md` (brief style)
- `src/curriculum/code/leader-election.{starter,solution}.js`, `txn-replicated.solution.js` (code style)
- `src/sim/engine.js` (the simulator, if you need exact semantics)

## The track file

```js
import { myWorkload } from '../../sim/workloads/<track-id>.js';

export default {
  id: '<track-id>',            // already set in the stub; do not change id, code or category
  code: 'XX',
  category: '...',
  title: '...',
  level: 'Beginner' | 'Intermediate' | 'Advanced',
  blurb: 'One or two sentences.',
  subtracks: [
    {
      title: 'Subtrack name',
      tasks: [
        {
          slug: 'my-task',
          title: 'Short title',
          level: 'Easy' | 'Medium' | 'Hard',
          minutes: 20,
          summary: 'One sentence shown on the task card.',
          concepts: ['two to four', 'short tags'],
          workload: myWorkload,           // the function itself
          tests: [
            // nodeCount and duration (ms of simulated time) are required.
            // Optional: latency: [min, max] ms (default [2, 12]), dropRate: 0..1 between nodes,
            // kv: true (adds node.kv), maxEvents (default 60000), clockSkew: ms (each node's
            // node.now() is off by a fixed amount up to that much either way), params: passed to the workload.
            { name: 'Healthy network', blurb: 'One sentence saying what this test does.', nodeCount: 3, duration: 4000, params: { ops: 60 } },
          ],
          hints: ['Three to five, each giving away a little more. Markdown allowed.'],
        },
      ],
    },
  ],
  mistakes: [{ title, symptom, fix }],            // 3-4
  questions: [{ q, a }],                          // 3
  reading: [{ kind: 'Paper'|'Blog'|'Book'|'Docs', title, url, note }],  // 2-3, real, stable URLs
};
```

Aim for 4–5 tasks in 1–2 subtracks, ordered so each builds on the one before. A later
task's starter is usually the previous task's solution with a comment saying what is
now missing; run against the new tests it must fail.

## Learner code

The file runs once per node, top to bottom, as a function body with `node` in scope.
See `api.md`. In short: `node.id`, `node.peers`, `node.nodeIds`, `node.on(type, handler)`,
`node.reply(msg, body)`, `node.send(dest, body)`, `await node.rpc(dest, body, { timeout })`
(resolves to the reply body or `null`), `node.after(ms, fn)`, `node.every(ms, fn)` (both
return `{ cancel() }`), `node.now()`, `node.random()`, `node.log()`, `node.kv` (if the test
sets `kv: true`), and `node.disk` (a plain JSON object that survives `sim.crash`).

Message bodies are plain JSON objects with a `type`. A request `foo` is answered with
`foo_ok` by convention. Do not use template-literal backticks in starter/solution code
(they are loaded as raw text; string concatenation keeps them simple).

## Workloads

```js
import { check, j, plural, sample, unanswered, wrongType, schedulePartition, split } from '../workloads.js';

export function myWorkload(sim, params) {
  // schedule client activity now; return a function that produces the checks at the end
  sim.at(100, async () => { const res = await sim.clientRpc('n1', { type: 'foo' }); ... });
  return () => [check('Name of the guarantee', ok, 'What went wrong, with concrete values.')];
}
```

`sim` offers:

| | |
|---|---|
| `sim.nodeIds`, `sim.duration`, `sim.now()` | cluster ids, test length, simulated time |
| `sim.rand()`, `sim.pick(array)` | seeded randomness; never use `Math.random` or `Date` |
| `sim.at(time, fn)` | run `fn` at an absolute simulated time (fn may be async) |
| `await sim.sleep(ms)` | inside an async workload function |
| `await sim.clientRpc(nodeId, body, { timeout = 1000, client = 'c1', probe = false })` | one client request; resolves to the reply body or `null`. `probe: true` keeps it out of the history, stats and timeline (use for periodic measurement) |
| `sim.partition([[...], [...]])`, `sim.heal()` | block node-to-node messages between groups. Clients and services always get through |
| `sim.crash(id)`, `await sim.restart(id)`, `sim.isUp(id)` | kill a node (memory, timers, in-flight work lost; `node.disk` kept) and start it again from the top of the file. Messages to a down node are lost. On the real runner `restart` returns a promise that resolves once the node has finished starting up; await it before inspecting the disk |
| `sim.pause(id, ms)`, `sim.isPaused(id)` | freeze a node for `ms` (a GC pause, a VM migration, `SIGSTOP`). It keeps its memory; messages sent to it are not lost but queue, its timers (including `rpc` timeouts) do not fire, and when it resumes it handles everything that piled up, late, in the order it was due, with `node.now()` jumped forward. `onDeliver` still runs when a message reaches the paused node. A crash during a pause ends it and loses the queued messages. Browser: deliveries and timers are held until resume. Real runner: `SIGSTOP`/`SIGCONT` of the node's process group (the Docker supervisor's `pause`/`resume` ops); there an overdue timer and a queued message race on resume, so keep pauses shorter than the timeouts a test relies on. The result's `pauses` lists `{ node, start, end }` |
| `sim.disk(id)` | read a node's disk from the checker |
| `sim.service(name, (msg, reply) => { ... })` | a fake external service the nodes can `node.rpc(name, ...)`; call `reply(body)` to answer, or not, to simulate failure. Register it at the top of the workload |
| `sim.onSend = (msg) => ...`, `sim.onDeliver = (msg) => ...` | observe every message (`msg.src`, `msg.dest`, `msg.body`, `msg.kind` is `'node'`, `'client'` or `'kv'`). `onDeliver` runs before the receiver's handler, so a partition or crash started there takes effect before the receiver reacts |
| `sim.intercept = ({ src, dest, body, kind, t }) => 'drop' \| ms \| undefined` | decide the fate of each message as it is sent: `'drop'` loses it, a number adds that many ms of delay (so later messages overtake it). Use it to stage exact interleavings. Probe traffic is never intercepted. Prefer keying on `src`/`dest` and timing over learner-chosen message types The `body` passed to `intercept` is the object that will be delivered, so a workload may rewrite it in place (the byzantine track plays lying nodes this way); both runners must keep that true |
| `sim.eventId()` | index of the event being processed |
| `sim.stats` | `{ nodeMsgs, clientMsgs, kvMsgs, dropped }` so far |
| `sim.ops` | history of non-probe client requests `{ node, client, req, res, invoke, complete, msg }` |

Helpers from `workloads.js`: `check(name, ok, detail)`, `j(value)` (short JSON),
`plural(n, word)`, `sample(array)`, `unanswered(name, results, extraAdvice)` and
`wrongType(results, expectedType)` (both take `[{ node, req, res }]`),
`schedulePartition(sim, [start, end])`, `split(sim)` (random minority/majority split).

Rules for checkers:

- Deterministic. Same code, same result, in Node and in the browser.
- Check guarantees, not implementations. Any correct design must pass; the node-to-node
  protocol is the learner's to design unless the brief fixes it.
- Each failing check's `detail` names nodes, values and times, and where possible says
  what to think about. It is the main teaching surface.
- Leave slack in timing requirements, and state every timing requirement in the brief.
- Schedule final reads late enough for the system to settle, and before `sim.duration`.
- Keep a test under ~40,000 events; the whole track should verify in a few seconds.
- The same checkers also run against real processes (`npm run lab`), where `sim.real` is
  true. Handling a message there takes real time (often an fsync), so margins such as
  "requests sent just before a crash are excused" must allow tens of milliseconds, not
  the browser's two. A test whose client sends a fixed script one request at a time can
  set `sequential: true` to get 1.6x the wall-clock time on the real runner.
- Make sure end-of-run inspections really ran: if a final read is still in flight when the
  run stops, its check silently passes. Inspect in parallel, early enough, and add an
  explicit "the final inspection completed" check.
- Probe requests draw from the shared random source, so adding a probe while debugging
  changes the run. To trace without changing it, observe with `sim.onDeliver`.
- Before writing a test that must catch a specific bug, write the buggy variant and make
  sure it fails that test, and only that test, for the intended reason.

## Briefs

Markdown, 150–350 words: why this matters, "What you build" with example messages in
`json` blocks (`→` request, `←` reply), the one idea the task teaches, and "The tests
check" as a short list. Plain language, no hype. Do not copy text from other sites.
