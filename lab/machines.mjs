// Where node processes run.
//
// A machine runs one node process at a time. Each start gets an incarnation
// number so output from a killed process is never mistaken for its successor.
//
// Events:  'out' (inc, line)   a line on the process's stdout
//          'err' (inc, line)   a line on stderr
//          'exit' (inc, { code, signal })
// Methods: start(inc), write(inc, line), kill(inc) -> Promise, dispose() -> Promise,
//          pause(inc) / resume(inc)   SIGSTOP / SIGCONT the process group
//
// LocalMachine runs the process directly on this computer.
// DockerMachine runs it inside its own container (own filesystem, process
// space and memory limit); a small supervisor in the container starts and
// SIGKILLs the process on command, so crashes and restarts take milliseconds.

import { spawn, spawnSync } from 'node:child_process';
import { EventEmitter } from 'node:events';
import readline from 'node:readline';

function lines(stream, fn) {
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
  rl.on('line', fn);
  return rl;
}

export class LocalMachine extends EventEmitter {
  constructor(id, dataDir, { argv, cwd, env = {} }) {
    super();
    this.id = id;
    this.dataDirInside = dataDir;
    this.argv = argv;
    this.cwd = cwd;
    this.env = env;
    this.proc = null;
    this.inc = 0;
  }

  start(inc) {
    this.inc = inc;
    const proc = spawn(this.argv[0], this.argv.slice(1), {
      cwd: this.cwd,
      env: { ...process.env, ...this.env, LAB_NODE_ID: this.id, LAB_DATA_DIR: this.dataDirInside },
      stdio: ['pipe', 'pipe', 'pipe'],
      detached: true, // own process group, so a kill takes any children with it
    });
    this.proc = proc;
    proc.exited = new Promise((resolve) => proc.on('close', (code, signal) => resolve({ code, signal })));
    proc.stdin.on('error', () => {});
    proc.on('error', (e) => this.emit('err', inc, `could not start ${this.argv[0]}: ${e.message}`));
    lines(proc.stdout, (l) => this.emit('out', inc, l));
    lines(proc.stderr, (l) => this.emit('err', inc, l));
    proc.exited.then((info) => {
      if (this.proc === proc) this.proc = null;
      this.emit('exit', inc, info);
    });
  }

  write(inc, line) {
    if (this.proc && inc === this.inc && this.proc.stdin.writable) this.proc.stdin.write(line + '\n');
  }

  signal(inc, sig) {
    const proc = this.proc;
    if (!proc || inc !== this.inc) return;
    try {
      process.kill(-proc.pid, sig);
    } catch {
      try {
        proc.kill(sig);
      } catch {
        // already gone
      }
    }
  }

  pause(inc) {
    this.signal(inc, 'SIGSTOP');
  }

  resume(inc) {
    this.signal(inc, 'SIGCONT');
  }

  kill(inc) {
    const proc = this.proc;
    if (!proc || inc !== this.inc) return Promise.resolve();
    try {
      process.kill(-proc.pid, 'SIGKILL');
    } catch {
      try {
        proc.kill('SIGKILL');
      } catch {
        // already gone
      }
    }
    return proc.exited;
  }

  async dispose() {
    if (this.proc) await this.kill(this.inc);
  }
}

export class DockerMachine extends EventEmitter {
  constructor(id, dataDir, { image, mounts, argv, env = {}, name, memory = '512m', cpus }) {
    super();
    this.id = id;
    this.dataDirInside = '/data';
    this.argv = argv;
    this.env = env;
    this.name = name;
    this.kills = new Map();
    this.inc = 0;
    const args = ['run', '-i', '--rm', '--name', name, '--network', 'none', '--memory', memory, '--hostname', id];
    if (cpus) args.push('--cpus', String(cpus));
    for (const [host, inside, mode] of mounts) args.push('-v', `${host}:${inside}${mode ? `:${mode}` : ''}`);
    args.push('-v', `${dataDir}:/data`, image, '/lab/supervisor');
    this.container = spawn('docker', args, { stdio: ['pipe', 'pipe', 'pipe'] });
    this.container.stdin.on('error', () => {});
    this.closed = new Promise((resolve) => this.container.on('close', resolve));
    lines(this.container.stdout, (l) => this.onEvent(l));
    lines(this.container.stderr, (l) => this.emit('err', this.inc, `[docker] ${l}`));
  }

  send(cmd) {
    if (this.container.stdin.writable) this.container.stdin.write(JSON.stringify(cmd) + '\n');
  }

  onEvent(line) {
    let ev;
    try {
      ev = JSON.parse(line);
    } catch {
      this.emit('err', this.inc, `[supervisor] ${line}`);
      return;
    }
    if (ev.ev === 'out') this.emit('out', ev.inc, ev.line);
    else if (ev.ev === 'err') this.emit('err', ev.inc, ev.line);
    else if (ev.ev === 'exit') this.emit('exit', ev.inc, { code: ev.code, signal: ev.signal || null });
    else if (ev.ev === 'killed') {
      this.kills.get(ev.inc)?.();
      this.kills.delete(ev.inc);
    }
  }

  start(inc) {
    this.inc = inc;
    this.send({ op: 'start', inc, argv: this.argv, env: { ...this.env, LAB_NODE_ID: this.id, LAB_DATA_DIR: '/data' } });
  }

  write(inc, line) {
    this.send({ op: 'in', inc, line });
  }

  pause(inc) {
    this.send({ op: 'pause', inc });
  }

  resume(inc) {
    this.send({ op: 'resume', inc });
  }

  kill(inc) {
    return new Promise((resolve) => {
      this.kills.set(inc, resolve);
      this.send({ op: 'kill', inc });
      setTimeout(() => {
        if (this.kills.delete(inc)) resolve();
      }, 5000);
    });
  }

  async dispose() {
    this.container.stdin.end();
    const t = setTimeout(() => spawnSync('docker', ['rm', '-f', this.name], { stdio: 'ignore' }), 3000);
    await this.closed;
    clearTimeout(t);
  }
}
