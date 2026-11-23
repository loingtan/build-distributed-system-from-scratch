// The concepts glossary. Each track contributes entries in
// ./concepts/<track-id>.js:
//
//   export default [{ term, aliases?: [], summary, detail, related?: [] }]
//
// summary is one sentence; detail is markdown (a few paragraphs). Entries are
// merged across tracks by term (case-insensitive); the first track in curriculum
// order that defines a term provides its text. An alias that another entry
// already claims is ignored, not the whole entry.

import { tracks, challenges } from './catalog.js';

const files = import.meta.glob('./concepts/*.js', { eager: true, import: 'default' });

export const slugify = (s) =>
  s
    .toLowerCase()
    .replace(/[+]/g, ' plus ')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');

const byKey = new Map(); // normalized term or alias -> entry
const entries = [];

for (const track of tracks) {
  for (const raw of files[`./concepts/${track.id}.js`] ?? []) {
    // An earlier track already defines this term: keep that text.
    if (byKey.has(slugify(raw.term))) continue;
    const entry = { ...raw, slug: slugify(raw.term), trackId: track.id, related: raw.related ?? [] };
    entries.push(entry);
    byKey.set(entry.slug, entry);
    // Aliases only claim spellings nobody has claimed yet.
    for (const a of raw.aliases ?? []) if (!byKey.has(slugify(a))) byKey.set(slugify(a), entry);
  }
}

// Which tasks practise each concept (from the tasks' concept tags).
for (const task of challenges) {
  for (const tag of task.concepts) {
    const entry = byKey.get(slugify(tag));
    if (!entry) continue;
    (entry.tasks ??= []).push(task);
  }
}

entries.sort((a, b) => a.term.localeCompare(b.term, 'en', { sensitivity: 'base' }));

export const concepts = entries;
export const conceptFor = (tag) => byKey.get(slugify(tag)) ?? null;

const guides = import.meta.glob('./guides/*.md', { eager: true, query: '?raw', import: 'default' });
export const guideFor = (trackId) => guides[`./guides/${trackId}.md`] ?? null;
