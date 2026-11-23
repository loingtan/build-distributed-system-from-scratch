// Shared by the CLI (lab/cli.mjs) and the web server (lab/server.mjs):
// building a node program and running one test of a task against real
// processes.

import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { workloads } from '../src/sim/workloads.js';
import { realSimulate } from './realsim.mjs';
import { LocalMachine, DockerMachine } from './machines.mjs';

export const LAB = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.dirname(LAB);
export const STATE = path.join(ROOT, '.lab');
export const PRACTICE = path.join(ROOT, 'practice');
export const GO_TASKS = path.join(LAB, 'go', 'tasks');

// -------------------------------------------------------------- building
function run(cmd, args, opts = {}) {
  const r = spawnSync(cmd, args, { encoding: 'utf8', ...opts });
  if (r.error) throw new Error(`Could not run ${cmd}: ${r.error.message}`);
  return r;
}

let dockerArchCache;
function dockerArch() {
  if (dockerArchCache) return dockerArchCache;
  const r = run('docker', ['version', '--format', '{{.Server.Arch}}']);
  if (r.status !== 0) throw new Error(`Docker is not available: ${r.stderr.trim() || r.stdout.trim()}`);
  dockerArchCache = r.stdout.trim();
  return dockerArchCache;
}

export function goBuild(dir, outFile, { linux = false } = {}) {
  fs.mkdirSync(path.dirname(outFile), { recursive: true });
  const env = { ...process.env, CGO_ENABLED: '0' };
  if (linux) Object.assign(env, { GOOS: 'linux', GOARCH: dockerArch() });
  const r = run('go', ['build', '-o', outFile, '.'], { cwd: dir, env });
  if (r.status !== 0) return { ok: false, output: (r.stderr || r.stdout).trim() };
  return { ok: true };
}

function supervisorBinary() {
  const out = path.join(STATE, 'bin', 'linux', 'supervisor');
  const r = goBuild(path.join(LAB, 'go', 'supervisor'), out, { linux: true });
  if (!r.ok) throw new Error(`Could not build the container supervisor:\n${r.output}`);
  return out;
}

let buildCounter = 0;

// A "program" is something we can run as a node: a Go directory or a JS file.
// Returns { buildError } if a Go program does not compile.
export function prepareProgram(target, { docker, name, image }) {
  const abs = path.resolve(target);
  if (!fs.existsSync(abs)) throw new Error(`${target} does not exist.`);
  if (abs.endsWith('.js')) {
    const shim = path.join(LAB, 'js', 'run-node.mjs');
    if (docker) {
      return {
        lang: 'js',
        image: image || 'node:22-alpine',
        mounts: [[path.join(LAB, 'js'), '/labjs', 'ro'], [path.dirname(abs), '/code', 'ro'], [path.dirname(supervisorBinary()), '/lab', 'ro']],
        argv: ['node', '/labjs/run-node.mjs', `/code/${path.basename(abs)}`],
      };
    }
    return { lang: 'js', argv: [process.execPath, shim, abs] };
  }
  const goDir = fs.statSync(abs).isDirectory() ? abs : path.dirname(abs);
  if (!fs.existsSync(path.join(goDir, 'go.mod')) && !fs.readdirSync(goDir).some((f) => f.endsWith('.go'))) {
    throw new Error(`${target} is neither a .js file nor a Go package.`);
  }
  const binName = `${name}-${process.pid}-${++buildCounter}`;
  if (docker) {
    const sup = supervisorBinary();
    const out = path.join(path.dirname(sup), binName);
    const r = goBuild(goDir, out, { linux: true });
    if (!r.ok) return { buildError: r.output };
    return { lang: 'go', image: image || 'alpine:3.20', mounts: [[path.dirname(sup), '/lab', 'ro']], argv: [`/lab/${binName}`], cleanup: [out] };
  }
  const out = path.join(STATE, 'bin', 'local', binName);
  const r = goBuild(goDir, out);
  if (!r.ok) return { buildError: r.output };
  return { lang: 'go', argv: [out], cleanup: [out] };
}

export function cleanupProgram(program) {
  for (const f of program?.cleanup ?? []) fs.rmSync(f, { force: true });
}

// --------------------------------------------------------------- running
// Same seeds as the browser engine (src/sim/runner.js): fixed per test by default,
// or base + test index when a seed is given.
export const seedFor = (task, testIndex, seed) => (Number.isInteger(seed) ? seed + testIndex : 7919 * (testIndex + 1) + task.slug.length);
export async function runTest(task, testIndex, program, { docker, seed, skew, signal } = {}) {
  const test = task.tests[testIndex];
  const workDir = fs.mkdtempSync(path.join(os.tmpdir(), `qlab-${task.slug}-`));
  const runId = `${process.pid}-${Date.now().toString(36)}-${testIndex}`;
  const makeMachine = docker
    ? (id, dataDir) => new DockerMachine(id, dataDir, { image: program.image, mounts: program.mounts, argv: program.argv, name: `qlab-${runId}-${id}` })
    : (id, dataDir) => {
        const m = new LocalMachine(id, null, { argv: program.argv, cwd: ROOT });
        m.dataDirInside = dataDir;
        return m;
      };
  try {
    return await realSimulate({
      makeMachine,
      workDir,
      nodeCount: test.nodeCount,
      duration: test.sequential ? Math.round(test.duration * 1.6) : test.duration,
      latency: test.latency,
      dropRate: test.dropRate,
      kv: test.kv,
      maxEvents: test.maxEvents,
      seed: seedFor(task, testIndex, seed),
      clockSkew: skew ? Number(skew) : test.clockSkew ?? 0,
      signal,
      workload: typeof task.workload === 'function' ? task.workload : workloads[task.workload],
      params: test.params,
    });
  } finally {
    fs.rmSync(workDir, { recursive: true, force: true });
  }
}

export function saveResults(task, lang, docker, results) {
  const dir = path.join(STATE, 'results');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(
    path.join(dir, `${task.slug}.json`),
    JSON.stringify({ slug: task.slug, at: new Date().toISOString(), lang, mode: docker ? 'docker' : 'local', results }),
  );
}

// ------------------------------------------------------------ Go content
export { goify, goBrief } from '../src/lib/goify.js';

export const goTemplate = (slug) => path.join(GO_TASKS, slug, 'starter');

// Optional lab/go/tasks/<slug>/GO.md: notes for Go readers where the brief or
// hints assume JavaScript.
export function goNotes(slug) {
  const f = path.join(GO_TASKS, slug, 'GO.md');
  return fs.existsSync(f) ? fs.readFileSync(f, 'utf8').trim() : null;
}

export function goSnippets(slug) {
  const f = path.join(GO_TASKS, slug, 'snippets.md');
  return fs.existsSync(f) ? fs.readFileSync(f, 'utf8') : null;
}

export function goFile(slug, kind) {
  const f = path.join(GO_TASKS, slug, kind, 'main.go');
  return fs.existsSync(f) ? fs.readFileSync(f, 'utf8') : null;
}

// practice/<slug>/ with main.go and a go.mod pointing at the library.
export function ensurePractice(slug, main) {
  const dir = path.join(PRACTICE, slug);
  fs.mkdirSync(dir, { recursive: true });
  const rel = path.relative(dir, path.join(LAB, 'go', 'quorumlab'));
  const mod = path.join(dir, 'go.mod');
  if (!fs.existsSync(mod)) fs.writeFileSync(mod, `module node\n\ngo 1.22\n\nrequire quorumlab v0.0.0\n\nreplace quorumlab => ${rel}\n`);
  if (main != null) fs.writeFileSync(path.join(dir, 'main.go'), main);
  return dir;
}

export function practiceCode(slug) {
  const f = path.join(PRACTICE, slug, 'main.go');
  return fs.existsSync(f) ? fs.readFileSync(f, 'utf8') : null;
}
