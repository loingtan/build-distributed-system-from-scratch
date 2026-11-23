import { useEffect, useMemo, useState } from 'react';
import { Marked } from 'marked';
import { trackById, tracks } from '../curriculum/catalog.js';
import { concepts, conceptFor, guideFor, slugify } from '../curriculum/concepts.js';
import { useStore } from '../lib/store.js';
import { TaskCard } from './Tracks.jsx';

// Markdown with ids on headings, so a guide can have a table of contents.
function render(md) {
  const toc = [];
  const marked = new Marked({
    renderer: {
      heading({ tokens, depth }) {
        const text = this.parser.parseInline(tokens);
        const plain = text.replace(/<[^>]+>/g, '');
        const id = slugify(plain);
        if (depth === 2 || depth === 3) toc.push({ id, text: plain, depth });
        return `<h${depth} id="${id}">${text}</h${depth}>`;
      },
    },
  });
  return { html: marked.parse(md), toc };
}

function Prose({ md, className = 'prose' }) {
  const html = useMemo(() => new Marked().parse(md ?? ''), [md]);
  return <div className={className} dangerouslySetInnerHTML={{ __html: html }} />;
}

// A concept tag that links to its glossary entry when one exists.
export function ConceptChip({ tag }) {
  const c = conceptFor(tag);
  if (!c) return <span className="concept">{tag}</span>;
  return (
    <a className="concept" href={`#/concepts/${c.slug}`} title={c.summary}>
      {tag}
    </a>
  );
}

export function GuidePage({ id }) {
  const track = trackById[id];
  const state = useStore();
  const md = guideFor(id);
  const { html, toc } = useMemo(() => render(md ?? ''), [md]);
  const index = tracks.indexOf(track);
  const prev = tracks[index - 1];
  const next = tracks[index + 1];
  const first = track.tasks.find((t) => !state.passed[t.slug]) ?? track.tasks[0];
  const trackConcepts = useMemo(
    () => [...new Set(track.tasks.flatMap((t) => t.concepts))].map(conceptFor).filter(Boolean).filter((c, i, a) => a.indexOf(c) === i),
    [track],
  );

  useEffect(() => {
    window.scrollTo(0, 0);
  }, [id]);

  return (
    <main className="page guide-page">
      <nav className="breadcrumb" aria-label="Breadcrumb">
        <a href="#/">Home</a> / <a href="#/tracks">Tracks</a> / <a href={`#/tracks/${id}`}>{track.title}</a> / <span>Guide</span>
      </nav>
      <div className="guide-layout">
        <aside className="guide-toc" aria-label="On this page">
          <b>On this page</b>
          {toc.map((h) => (
            <a key={h.id} href={`#${h.id}`} className={h.depth === 3 ? 'sub' : ''} onClick={(e) => {
              e.preventDefault();
              document.getElementById(h.id)?.scrollIntoView({ behavior: 'smooth', block: 'start' });
            }}>
              {h.text}
            </a>
          ))}
          <a className="btn btn-primary guide-cta" href={`#/c/${first.slug}`}>Practise: {first.title} →</a>
        </aside>
        <article className="guide">
          <p className="eyebrow">{track.number} · Guide</p>
          <h1>{track.title}</h1>
          {md ? (
            <div className="prose guide-prose" dangerouslySetInnerHTML={{ __html: html }} />
          ) : (
            <p className="page-lede">The guide for this track has not been written yet. The track page has its key concepts and reading list.</p>
          )}
          {trackConcepts.length > 0 && (
            <section className="guide-concepts">
              <h2>Concepts in this track</h2>
              <dl>
                {trackConcepts.map((c) => (
                  <div key={c.slug}>
                    <dt><a href={`#/concepts/${c.slug}`}>{c.term}</a></dt>
                    <dd>{c.summary}</dd>
                  </div>
                ))}
              </dl>
            </section>
          )}
          <section>
            <h2>Practise it</h2>
            <div className="task-grid">
              {track.tasks.map((t) => <TaskCard key={t.slug} task={t} state={state} />)}
            </div>
          </section>
          <nav className="track-nav" aria-label="Other guides">
            {prev ? <a href={`#/tracks/${prev.id}/guide`}>← {prev.title}</a> : <span />}
            {next ? <a href={`#/tracks/${next.id}/guide`}>{next.title} →</a> : <span />}
          </nav>
        </article>
      </div>
    </main>
  );
}

export function ConceptsPage({ slug }) {
  const [query, setQuery] = useState('');
  const q = query.trim().toLowerCase();
  const shown = q
    ? concepts.filter((c) => [c.term, ...(c.aliases ?? []), c.summary].some((s) => s.toLowerCase().includes(q)))
    : concepts;
  const groups = useMemo(() => {
    const m = new Map();
    for (const c of shown) {
      const letter = /[a-z]/i.test(c.term[0]) ? c.term[0].toUpperCase() : '#';
      (m.get(letter) ?? m.set(letter, []).get(letter)).push(c);
    }
    return [...m];
  }, [shown]);

  useEffect(() => {
    if (!slug) return;
    setQuery(''); // a link to one concept should never land on a filtered-out entry
    const t = setTimeout(() => document.getElementById(`c-${slug}`)?.scrollIntoView({ block: 'start' }), 50);
    return () => clearTimeout(t);
  }, [slug]);

  return (
    <main className="page concepts-page">
      <nav className="breadcrumb" aria-label="Breadcrumb">
        <a href="#/">Home</a> / <span>Concepts</span>
      </nav>
      <h1>Concepts</h1>
      <p className="page-lede">
        {concepts.length} ideas from distributed systems, each with a short definition, a longer explanation, and the tasks where you build
        it.
      </p>
      <input
        className="concept-search"
        type="search"
        placeholder="Search concepts, e.g. quorum, idempotence, fencing"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        aria-label="Search concepts"
      />
      {groups.map(([letter, list]) => (
        <section key={letter} className="concept-group">
          <h2>{letter}</h2>
          {list.map((c) => (
            <details key={c.slug} id={`c-${c.slug}`} className="concept-entry" open={c.slug === slug || undefined}>
              <summary>
                <b>{c.term}</b>
                <span>{c.summary}</span>
              </summary>
              <div className="concept-body">
                <Prose md={c.detail} />
                {c.related.length > 0 && (
                  <p className="concept-related">
                    Related:{' '}
                    {c.related.map((r, i) => {
                      const rc = conceptFor(r);
                      return (
                        <span key={r}>
                          {i > 0 && ', '}
                          {rc ? <a href={`#/concepts/${rc.slug}`}>{rc.term}</a> : r}
                        </span>
                      );
                    })}
                  </p>
                )}
                {c.tasks?.length > 0 && (
                  <p className="concept-tasks">
                    Practise it in:{' '}
                    {c.tasks.map((t, i) => (
                      <span key={t.slug}>
                        {i > 0 && ' · '}
                        <a href={`#/c/${t.slug}`}>{t.id} {t.title}</a>
                      </span>
                    ))}
                  </p>
                )}
                <p className="concept-tasks">
                  Explained in the <a href={`#/tracks/${c.trackId}/guide`}>{trackById[c.trackId].title} guide</a>.
                </p>
              </div>
            </details>
          ))}
        </section>
      ))}
      {shown.length === 0 && <p className="page-lede">No concept matches “{query}”.</p>}
    </main>
  );
}
