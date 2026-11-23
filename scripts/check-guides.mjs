// Checks track guides and concept entries against docs/guides.md.
//   node scripts/check-guides.mjs [track-id ...]
// Runs under plain Node: loads the catalog and the concept files directly.

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const { tracks } = await import(path.join(ROOT, 'src/curriculum/catalog.js'));
const only = process.argv.slice(2);
const slugify = (s) => s.toLowerCase().replace(/[+]/g, ' plus ').replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');

const HEADINGS = ['The problem', 'The core idea', 'A worked example', 'Why it is correct', 'Trade-offs and failure modes', 'In real systems', 'Common misconceptions', 'How the tasks build it', 'Further reading'];
let problems = 0;
const say = (track, msg) => {
  problems++;
  console.log(`  ${track}: ${msg}`);
};

// All concept terms and aliases defined by any track.
const defined = new Map();
for (const t of tracks) {
  const f = path.join(ROOT, 'src/curriculum/concepts', `${t.id}.js`);
  if (!fs.existsSync(f)) continue;
  try {
    const mod = await import(f + '?t=' + Date.now());
    for (const e of mod.default ?? []) {
      for (const k of [e.term, ...(e.aliases ?? [])]) defined.set(slugify(k), t.id);
      if (!e.summary || !e.detail) say(t.id, `concept "${e.term}" needs both summary and detail`);
    }
  } catch (e) {
    say(t.id, `concepts file does not load: ${e.message}`);
  }
}

for (const t of tracks) {
  if (only.length && !only.includes(t.id)) continue;
  const before = problems;
  const g = path.join(ROOT, 'src/curriculum/guides', `${t.id}.md`);
  if (!fs.existsSync(g)) {
    console.log(`  ${t.id}: (no guide yet)`);
  } else {
    const md = fs.readFileSync(g, 'utf8');
    const h2 = [...md.matchAll(/^## (.+)$/gm)].map((m) => m[1].trim());
    const missing = HEADINGS.filter((h) => !h2.includes(h));
    if (missing.length) say(t.id, `guide is missing headings: ${missing.join(', ')}`);
    const order = HEADINGS.filter((h) => h2.includes(h));
    if (order.join('|') !== h2.filter((h) => HEADINGS.includes(h)).join('|')) say(t.id, 'guide headings are out of order');
    const prose = md.replace(/<figure>[\s\S]*?<\/figure>/g, '').replace(/```[\s\S]*?```/g, '');
    const words = prose.split(/\s+/).filter(Boolean).length;
    if (words < 2200 || words > 4800) say(t.id, `guide has ${words} words (aim for 2,500–4,000)`);
    const figures = (md.match(/<figure>/g) ?? []).length;
    if (figures < 2) say(t.id, `guide has ${figures} diagrams (at least 2)`);
    const svgs = md.match(/<svg[\s\S]*?<\/svg>/g) ?? [];
    for (const s of svgs) {
      if (/(fill|stroke)="#|(fill|stroke)="(black|white|red|blue|green|gray|grey)"|(fill|stroke):\s*#/i.test(s)) say(t.id, 'an SVG uses a hard-coded colour; use var(--…)');
      if (!/viewBox=/.test(s)) say(t.id, 'an SVG has no viewBox');
    }
    if (/^ {4,}<figure>/m.test(md)) say(t.id, 'an indented <figure> will render as code');
    for (const task of t.tasks) if (!md.includes(`(#/c/${task.slug})`)) say(t.id, `guide does not link task ${task.slug}`);
  }
  const tags = [...new Set(t.tasks.flatMap((c) => c.concepts))];
  const undefinedTags = tags.filter((tag) => !defined.has(slugify(tag)));
  if (undefinedTags.length) say(t.id, `concept tags without an entry: ${undefinedTags.join(', ')}`);
  if (problems === before) console.log(`  ${t.id}: ok`);
}
console.log(problems ? `\n${problems} problem(s)` : '\nAll guides and concepts check out.');
process.exitCode = problems ? 1 : 0;
