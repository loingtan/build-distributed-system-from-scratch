#!/usr/bin/env node
// Quorum Lab, local runner: every node is a real OS process.
//
//   npm run lab -- list                         tracks and tasks, with your progress
//   npm run lab -- brief <task>                 print a task's brief
//   npm run lab -- new <task>                   start a Go solution in practice/<task>
//   npm run lab -- test <task> [dir|file.js]    build and run the task's tests
//        --docker      one container per node (default: plain local processes)
//        --test N      only test N (1-based)
//        --seed N      vary the fault schedule
//        --verbose     print every check, not just failures
//   npm run lab -- verify [task...] [--track ID] [--kind solution|starter|both] [--lang go|js] [--docker]
//                                               run the reference code through the real runner

import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { tracks, challenges, bySlug } from '../src/curriculum/catalog.js';
import { LAB, ROOT, STATE, PRACTICE, GO_TASKS, prepareProgram as prepare, runTest as runOne, saveResults, cleanupProgram, goify, goBrief, goSnippets, goTemplate, goNotes, ensurePractice } from './core.mjs';

const tty = process.stdout.isTTY;
const c = (code) => (s) => (tty ? `\x1b[${code}m${s}\x1b[0m` : String(s));
const green = c(32);
const red = c(31);
const dim = c(2);
const bold = c(1);
const yellow = c(33);

// ------------------------------------------------------------------ args
const argv = process.argv.slice(2);
const flags = {};
const positional = [];
for (let i = 0; i < argv.length; i++) {
  const a = argv[i];
  if (a.startsWith('--')) {
    const key = a.slice(2);
    const next = argv[i + 1];
    if (next !== undefined && !next.startsWith('--') && ['test', 'seed', 'track', 'kind', 'lang', 'image', 'skew'].includes(key)) {
      flags[key] = next;
      i++;
    } else flags[key] = true;
  } else positional.push(a);
}
const [command, ...rest] = positional;

function die(msg) {
  console.error(red(msg));
  process.exit(2);
}

function findTask(slug) {
  const t = bySlug[slug];
  if (!t) die(`No task "${slug}". Run "npm run lab -- list" to see them.`);
  return t;
}

function printResult(test, i, r, ms, { verbose, lang }) {
  const say = lang === 'go' ? goify : (t) => t;
  const s = r.stats;
  const mark = r.ok ? green('PASS') : red('FAIL');
  const extra = [`${s.ops} requests`, `${s.nodeMsgs} node msgs`];
  if (s.kvMsgs) extra.push(`${s.kvMsgs} service msgs`);
  if (s.dropped) extra.push(`${s.dropped} lost`);
  if (s.ops) extra.push(`p50 ${Math.round(s.p50)}ms`);
  console.log(`  ${mark}  ${bold(`${i + 1}. ${test.name}`)}  ${dim(`${extra.join(' · ')} · ${(ms / 1000).toFixed(1)}s`)}`);
  for (const ch of r.checks) {
    if (ch.ok && !verbose) continue;
    console.log(`        ${ch.ok ? green('✓') : red('✗')} ${ch.name}`);
    if (!ch.ok && ch.detail) console.log(`          ${say(ch.detail).split('\n').join('\n          ')}`);
  }
  if (!r.ok) {
    const problems = r.logs.filter((l) => l.level === 'error' || l.level === 'warn').slice(0, 5);
    for (const l of problems) console.log(dim(`          ${Math.round(l.t)}ms ${l.source}: ${l.text.split('\n')[0]}`));
  }
}

const prepareProgram = (target, { docker, name }) => prepare(target, { docker, name, image: flags.image });
const runTest = (task, i, program, opts) => runOne(task, i, program, { ...opts, skew: flags.skew });

async function runTask(task, target, { docker, only, seed, verbose, quiet, save = true }) {
  const program = prepareProgram(target, { docker, name: task.slug });
  if (program.buildError) {
    console.log(`  ${red('BUILD FAILED')}\n${program.buildError.replace(/^/gm, '    ')}`);
    return { ok: false, results: [] };
  }
  const indexes = only != null ? [only] : task.tests.map((_, i) => i);
  const results = [];
  for (const i of indexes) {
    const t0 = Date.now();
    const r = await runTest(task, i, program, { docker, seed });
    results[i] = r;
    if (!quiet) printResult(task.tests[i], i, r, Date.now() - t0, { verbose, lang: program.lang });
  }
  cleanupProgram(program);
  if (save && only == null) saveResults(task, program.lang, docker, results);
  return { ok: indexes.every((i) => results[i].ok), results };
}

// -------------------------------------------------------------- commands
async function cmdTest() {
  const [slug, target] = rest;
  if (!slug) die('Usage: npm run lab -- test <task> [dir|file.js]');
  const task = findTask(slug);
  const where = target ?? path.join(PRACTICE, slug);
  if (!fs.existsSync(where)) die(`Nothing at ${path.relative(ROOT, where)}. Start one with: npm run lab -- new ${slug}`);
  const docker = !!flags.docker;
  console.log(`${bold(task.title)} ${dim(`(${task.id}, ${task.tests.length} tests, ${docker ? 'Docker containers' : 'local processes'})`)}`);
  const only = flags.test ? Number(flags.test) - 1 : null;
  const { ok } = await runTask(task, where, { docker, only, seed: flags.seed ? Number(flags.seed) : undefined, verbose: !!flags.verbose });
  console.log(ok ? green('\nAll tests pass.') : red('\nSome tests fail.'));
  process.exitCode = ok ? 0 : 1;
}

function cmdNew() {
  const [slug] = rest;
  if (!slug) die('Usage: npm run lab -- new <task>');
  const task = findTask(slug);
  const dir = path.join(PRACTICE, slug);
  if (fs.existsSync(dir) && !flags.force) die(`${path.relative(ROOT, dir)} already exists (use --force to overwrite).`);
  fs.mkdirSync(dir, { recursive: true });
  const starter = path.join(goTemplate(slug), 'main.go');
  const main = fs.existsSync(starter) ? fs.readFileSync(starter, 'utf8') : genericStarter(task);
  fs.rmSync(path.join(dir, 'go.mod'), { force: true });
  ensurePractice(slug, main);
  console.log(`Created ${path.relative(ROOT, dir)}/main.go${fs.existsSync(starter) ? '' : dim(' (generic skeleton: no Go starter for this task yet)')}`);
  console.log(`Read the brief:  npm run lab -- brief ${slug}`);
  console.log(`Run the tests:   npm run lab -- test ${slug}            ${dim('(add --docker for one container per node)')}`);
}

function genericStarter(task) {
  return `// ${task.title}: see "npm run lab -- brief ${task.slug}" for what to build.
package main

import (
	"context"
	"log"

	"quorumlab"
)

func main() {
	n := quorumlab.NewNode()

	// Handle each request type the brief describes, for example:
	n.Handle("example", func(ctx context.Context, msg quorumlab.Message) error {
		var body map[string]any
		if err := msg.Decode(&body); err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "example_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
`;
}

function cmdBrief() {
  const [slug] = rest;
  if (!slug) die('Usage: npm run lab -- brief <task>');
  const task = findTask(slug);
  const md = fs.readFileSync(path.join(ROOT, 'src', 'curriculum', 'briefs', `${slug}.md`), 'utf8');
  console.log(bold(`${task.id}  ${task.title}`) + dim(`  ·  ${task.trackTitle} · ${task.level} · ~${task.minutes} min\n`));
  const text = flags.js ? md : goBrief(md, goSnippets(slug));
  console.log(text.replace(/^## (.*)$/gm, (_, h) => bold(h)));
  if (!flags.js) {
    const notes = goNotes(slug);
    if (notes) console.log(bold('Notes for Go') + '\n' + notes + '\n');
    console.log(dim('(JavaScript API names translated to Go; the Go library is described in lab/README.md. Use --js for the original.)\n'));
  }
  console.log(bold('Tests'));
  task.tests.forEach((t, i) => console.log(`  ${i + 1}. ${t.name}: ${dim(t.blurb)}`));
  console.log(dim(`\nHints: npm run lab -- hints ${slug}`));
}

function cmdHints() {
  const [slug] = rest;
  const task = findTask(slug);
  task.hints.forEach((h, i) => console.log(`${bold(`${i + 1}.`)} ${flags.js ? h : goify(h)}\n`));
  const notes = flags.js ? null : goNotes(slug);
  if (notes) console.log(bold('Notes for Go') + '\n' + notes);
}

function cmdList() {
  const resultsDir = path.join(STATE, 'results');
  const status = (slug) => {
    try {
      const r = JSON.parse(fs.readFileSync(path.join(resultsDir, `${slug}.json`), 'utf8'));
      return r.results.every((x) => x && x.ok) ? green('✓') : red('✗');
    } catch {
      return fs.existsSync(path.join(PRACTICE, slug)) ? yellow('…') : dim('·');
    }
  };
  for (const t of tracks) {
    console.log(bold(`${t.number} ${t.title}`) + dim(`  ${t.level}`));
    for (const task of t.tasks) {
      const go = fs.existsSync(path.join(goTemplate(task.slug), 'main.go')) ? '' : dim('  (no Go starter yet)');
      console.log(`   ${status(task.slug)} ${task.id.padEnd(6)} ${task.slug.padEnd(26)} ${dim(task.title)}${go}`);
    }
  }
}

async function cmdVerify() {
  const kind = flags.kind || 'solution';
  const lang = flags.lang || 'go';
  const docker = !!flags.docker;
  let list = rest.length ? rest.map(findTask) : challenges;
  if (flags.track) list = list.filter((t) => t.trackId === flags.track);
  let problems = 0;
  let skipped = 0;
  for (const task of list) {
    for (const k of kind === 'both' ? ['solution', 'starter'] : [kind]) {
      const target =
        lang === 'js' ? path.join(ROOT, 'src', 'curriculum', 'code', `${task.slug}.${k}.js`) : path.join(GO_TASKS, task.slug, k);
      if (!fs.existsSync(target)) {
        skipped++;
        continue;
      }
      console.log(`${bold(task.id)} ${task.slug} ${dim(`[${lang} ${k}]`)}`);
      const { ok, results } = await runTask(task, target, { docker, verbose: false, save: false, quiet: k === 'starter' });
      if (k === 'starter') {
        console.log(`  starter: ${results.map((r, i) => `${r?.ok ? 'pass' : 'fail'} (${task.tests[i].name})`).join(', ')}${ok ? red('   <-- STARTER PASSES EVERYTHING') : ''}`);
        if (ok) problems++;
      } else if (!ok) problems++;
    }
  }
  console.log(problems ? red(`\n${problems} problem(s)`) : green('\nAll checked.') + (skipped ? dim(` (${skipped} without ${lang} code skipped)`) : ''));
  process.exitCode = problems ? 1 : 0;
}

const commands = { test: cmdTest, new: cmdNew, brief: cmdBrief, hints: cmdHints, list: cmdList, verify: cmdVerify };
if (!commands[command]) {
  console.log(fs.readFileSync(fileURLToPath(import.meta.url), 'utf8').split('\n').slice(1, 14).map((l) => l.replace(/^\/\/ ?/, '')).join('\n'));
  process.exit(command ? 2 : 0);
}
try {
  await commands[command]();
} catch (e) {
  die(e?.message ?? String(e));
}
