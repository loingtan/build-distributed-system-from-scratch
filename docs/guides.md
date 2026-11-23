# Writing a track guide and its concept entries

Each track has a long-form guide, shown at `#/tracks/<track-id>/guide`, and glossary
entries for its concepts, merged into `#/concepts`. The guide is where a learner goes to
*understand* the idea before (or while) building it; the briefs only say what to build.

```
src/curriculum/guides/<track-id>.md      the guide (markdown, inline SVG allowed)
src/curriculum/concepts/<track-id>.js    glossary entries for the track's concepts
```

Read first: the track file (`src/curriculum/catalog.js` for tracks 01–08 by id, or
`src/curriculum/tracks/<id>.js`), every brief of the track (`src/curriculum/briefs/<slug>.md`),
and the reference solutions (`src/curriculum/code/<slug>.solution.js`, Go versions in
`lab/go/tasks/<slug>/solution/main.go`). The guide must agree with what the tasks test.

## The guide

**Length:** 2,500–4,000 words. **Audience:** a working programmer who has not studied
distributed systems; assume they know what a process, a socket and a hash map are, nothing
more. **Voice:** plain, precise, concrete. Explain mechanisms with examples, not adjectives.
No hype, no filler, no "In this guide we will…". Do not copy text from other sites or
books; write your own explanations and cite sources in Further reading.

Structure (use exactly these `##` headings, in this order; `###` subsections as needed):

1. `## The problem` — what goes wrong without this idea. Start from a small, concrete
   failure (two nodes, one message lost, a specific wrong outcome), not a definition.
2. `## The core idea` — the mechanism, built up step by step. Define every term the first
   time it appears, in **bold**. Include at least two diagrams (see below).
3. `## A worked example` — one complete run traced message by message or step by step,
   with concrete node names, values and times, including at least one fault (a lost
   message, a partition, a crash) and how the mechanism handles it. A table or a numbered
   list of steps works well.
4. `## Why it is correct` — the invariant or argument that makes it work (e.g. "any two
   majorities overlap", "merge is commutative, associative and idempotent"). Informal but
   rigorous: say exactly which property is guaranteed and under which assumptions.
5. `## Trade-offs and failure modes` — what it costs (latency, messages, availability,
   storage), what it does not protect against, and what breaks if an assumption fails.
6. `## In real systems` — how two to four named, real systems apply or vary the idea
   (e.g. Kafka, etcd, Cassandra, DynamoDB, PostgreSQL, Redis, Kubernetes, Spanner). Only
   state facts you are confident about; prefer well-known, stable behaviour.
7. `## Common misconceptions` — three to five bullets: a wrong belief in bold, then why
   it is wrong.
8. `## How the tasks build it` — one short paragraph per task of the track (link each as
   `[TASK-ID Title](#/c/<slug>)`), saying which part of the idea it exercises and what its
   tests stress. Do not give away solutions.
9. `## Further reading` — three to six bullets with real, stable URLs (papers, official
   docs, well-known blog posts), each with one line on why it is worth reading.

Formatting:
- Code samples are rare and short; prefer the protocol's JSON messages in ```json blocks.
  If you show code, show the idea in pseudocode (```text), not one language.
- Use tables for comparisons and step-by-step traces.
- A `> ` blockquote is rendered as a highlighted key point; use at most three per guide.

### Diagrams

Inline SVG inside a `<figure>` with a `<figcaption>`. The page has light and dark themes,
so **never hard-code colours**: use CSS variables in `fill`/`stroke` via `style`, e.g.
`style="fill: var(--surface); stroke: var(--accent)"`.

Available: `--ink` (text), `--muted`, `--faint`, `--line` (borders, grid), `--surface`
(boxes), `--accent` (main highlight), `--accent-soft`, `--good`, `--bad` (failures, lost
messages), `--warn`, `--m1` … `--m6` (distinct series colours). Text:
`style="fill: var(--ink); font: 13px var(--sans)"` (or `var(--mono)` for node ids/values).

Rules: set a `viewBox` and no fixed width/height; keep it under ~640×320 units; label
nodes `n1`, `n2`…; show time flowing left→right or top→bottom consistently; mark lost
messages with a dashed `--bad` line and an ×. Keep each SVG under ~80 lines. Put a blank
line before and after the `<figure>` block, and do not indent it (markdown would treat it
as code).

Example:

```html
<figure>
<svg viewBox="0 0 520 150" role="img" aria-label="n1 sends to n2 and n3; the message to n3 is lost">
  <g style="font: 13px var(--mono); fill: var(--ink)">
    <text x="10" y="34">n1</text><text x="10" y="84">n2</text><text x="10" y="134">n3</text>
  </g>
  <g style="stroke: var(--line)"><line x1="40" y1="30" x2="510" y2="30"/><line x1="40" y1="80" x2="510" y2="80"/><line x1="40" y1="130" x2="510" y2="130"/></g>
  <line x1="80" y1="30" x2="180" y2="80" style="stroke: var(--accent); stroke-width: 2"/>
  <line x1="80" y1="30" x2="140" y2="90" style="stroke: var(--bad); stroke-width: 2; stroke-dasharray: 5 4"/>
  <text x="140" y="98" style="fill: var(--bad); font: 15px var(--sans)">×</text>
</svg>
<figcaption>n1 tells both peers; the copy to n3 is lost and nobody notices.</figcaption>
</figure>
```

## The concept entries

`src/curriculum/concepts/<track-id>.js`:

```js
export default [
  {
    term: 'Quorum',                       // as it should be displayed
    aliases: ['majority quorum'],         // other spellings used as task tags (optional)
    summary: 'One sentence, under 25 words, that a newcomer can understand.',
    detail: `Two to four short paragraphs of markdown: a precise definition, a concrete
example with numbers or node names, the key property or trade-off, and where it shows up
in practice. No headings.`,
    related: ['Read quorum', 'Write quorum'], // other concept terms (optional)
  },
];
```

Cover **every concept tag used by the tasks of your track** (the `concepts` arrays of its
tasks); each tag must match an entry's `term` or one of its `aliases` once both are
lowercased and non-alphanumerics collapsed (e.g. tag `W + R > N` matches term `W + R > N`).
You may add up to five more entries for important ideas the guide introduces. Write in a
template literal; escape backticks inside it. If a concept is already defined by an
earlier track (see other files in `src/curriculum/concepts/`), still include it if it is a
tag of your track, but you may keep it shorter — the earliest track's text is shown. An
alias already claimed by another entry is simply ignored.

## Check

```sh
node scripts/check-guides.mjs <track-id> < /dev/null
npx vite build < /dev/null
```

`scripts/check-guides.mjs` reports missing headings, concept tags without an entry, word
counts and SVGs with hard-coded colours. Fix everything it reports for your tracks.
