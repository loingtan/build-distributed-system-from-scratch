import { categories, tracks, challenges, planned, trackById } from '../curriculum/catalog.js';
import { useStore } from '../lib/store.js';

export const levelClass = (level) => `level level-${level.toLowerCase().replace(/[^a-z]/g, '')}`;
const statusOf = (c, state) => (state.passed[c.slug] ? 'pass' : state.code[c.slug] ? 'started' : 'idle');

// All tracks, grouped by category.
export function TracksIndex() {
  const state = useStore();
  return (
    <main className="page">
      <nav className="breadcrumb" aria-label="Breadcrumb"><a href="#/">Home</a> / <span>Tracks</span></nav>
      <h1>All tracks</h1>
      <p className="page-lede">
        {tracks.length} tracks and {challenges.length} tasks, from a node that echoes to Raft leader election.
      </p>

      {categories.map((cat) => (
        <section key={cat.id} className="cat">
          <h2>{cat.title}</h2>
          <p className="section-lede">{cat.blurb}</p>
          <div className="track-grid">
            {tracks.filter((t) => t.category === cat.id).map((t) => {
              const n = t.tasks.filter((c) => state.passed[c.slug]).length;
              return (
                <a key={t.id} className="track-card" href={`#/tracks/${t.id}`}>
                  <div className="card-top">
                    <span className="card-num">{t.number}</span>
                    <span className={levelClass(t.level)}>{t.level}</span>
                  </div>
                  <h3>{t.title}</h3>
                  <p>{t.blurb}</p>
                  <div className="card-foot">
                    <span>{t.tasks.length} tasks · about {Math.round(t.minutes / 5) * 5} min</span>
                    <span className="card-progress">
                      <span className="bar"><i style={{ width: `${(n / t.tasks.length) * 100}%` }} /></span>
                      {n}/{t.tasks.length}
                    </span>
                  </div>
                </a>
              );
            })}
            {planned.filter((p) => p.category === cat.id).map((p) => (
              <div key={p.title} className="track-card soon">
                <div className="card-top"><span className="level">Coming soon</span></div>
                <h3>{p.title}</h3>
                <p>{p.blurb}</p>
              </div>
            ))}
          </div>
        </section>
      ))}
    </main>
  );
}

// One track: subtracks and task cards, then mistakes, review questions, concepts and reading.
export function TrackPage({ id }) {
  const state = useStore();
  const track = trackById[id];
  const cat = categories.find((c) => c.id === track.category);
  const done = track.tasks.filter((c) => state.passed[c.slug]).length;
  const nextTask = track.tasks.find((c) => !state.passed[c.slug]) ?? track.tasks[0];
  const index = tracks.indexOf(track);
  const prev = tracks[index - 1];
  const next = tracks[index + 1];
  const concepts = [...new Set(track.tasks.flatMap((c) => c.concepts))];

  return (
    <main className="page">
      <nav className="breadcrumb" aria-label="Breadcrumb">
        <a href="#/">Home</a> / <a href="#/tracks">Tracks</a> / <span>{track.title}</span>
      </nav>

      <header className="track-hero">
        <span className="track-big">{track.number}</span>
        <div>
          <h1>{track.title}</h1>
          <p className="track-meta">
            <span className={levelClass(track.level)}>{track.level}</span>
            <span>{cat.title}</span>
            <span>{track.tasks.length} tasks</span>
            <span>about {Math.round(track.minutes / 5) * 5} min</span>
          </p>
          <p className="page-lede">{track.blurb}</p>
          {index > 0 && done === 0 && (
            <p className="prereq">Tracks build on each other. If something here feels unfamiliar, try <a href={`#/tracks/${prev.id}`}>{prev.title}</a> first.</p>
          )}
          <div className="hero-actions">
            <a className="btn btn-primary btn-large" href={`#/c/${nextTask.slug}`}>
              {done === 0 ? 'Start track' : done === track.tasks.length ? 'Review track' : `Continue: ${nextTask.title}`} →
            </a>
            <a className="btn btn-quiet btn-large" href={`#/tracks/${track.id}/guide`}>Read the guide</a>
            <span className="card-progress">
              <span className="bar"><i style={{ width: `${(done / track.tasks.length) * 100}%` }} /></span>
              {done}/{track.tasks.length} passed
            </span>
          </div>
        </div>
      </header>

      <section>
        <h2>Subtracks and tasks</h2>
        {track.subtracks.map((sub) => {
          const n = sub.tasks.filter((c) => state.passed[c.slug]).length;
          return (
            <div key={sub.title} className="subtrack">
              <div className="subtrack-head">
                <h3>{sub.title}</h3>
                <span className="dots" aria-hidden="true">
                  {sub.tasks.map((c) => <i key={c.slug} className={state.passed[c.slug] ? 'on' : ''} />)}
                </span>
                <span className="count">{n}/{sub.tasks.length}</span>
              </div>
              <div className="task-grid">
                {sub.tasks.map((c) => <TaskCard key={c.slug} task={c} state={state} />)}
              </div>
            </div>
          );
        })}
      </section>

      <section>
        <h2>Common mistakes</h2>
        <p className="section-lede">What usually goes wrong in this track, how it shows up, and the way out.</p>
        <ol className="mistakes">
          {track.mistakes.map((m) => (
            <li key={m.title}>
              <details>
                <summary>{m.title}</summary>
                <dl>
                  <dt>Symptom</dt><dd>{m.symptom}</dd>
                  <dt>Fix</dt><dd>{m.fix}</dd>
                </dl>
              </details>
            </li>
          ))}
        </ol>
      </section>

      <section>
        <h2>Check your understanding</h2>
        <p className="section-lede">Questions an interviewer might ask. Answer in your head, then open the card.</p>
        <div className="questions">
          {track.questions.map((q) => (
            <details key={q.q}>
              <summary>{q.q}</summary>
              <p>{q.a}</p>
            </details>
          ))}
        </div>
      </section>

      <section className="two-col">
        <div>
          <h3>Concepts covered</h3>
          <div className="tags big">{concepts.map((k) => <span key={k}>{k}</span>)}</div>
        </div>
        <div>
          <h3>Going deeper</h3>
          <ul className="reading">
            {track.reading.map((r) => (
              <li key={r.url}>
                <a href={r.url} target="_blank" rel="noreferrer">
                  <em>{r.kind}</em>
                  <b>{r.title}</b>
                  <span>{r.note}</span>
                </a>
              </li>
            ))}
          </ul>
        </div>
      </section>

      <nav className="track-nav" aria-label="Other tracks">
        {prev ? <a href={`#/tracks/${prev.id}`}>← {prev.number} {prev.title}</a> : <span />}
        {next ? <a href={`#/tracks/${next.id}`}>{next.number} {next.title} →</a> : <span />}
      </nav>
    </main>
  );
}

export function TaskCard({ task: c, state, showTrack = false }) {
  const status = statusOf(c, state);
  return (
    <a className="task-card" href={`#/c/${c.slug}`}>
      <div className="card-top">
        <span className="task-id">{c.id}</span>
        <span className={levelClass(c.level)}>{c.level}</span>
        <span className="task-status">
          <span className={`mark mark-${status}`} />
          {status === 'pass' ? 'Passed' : status === 'started' ? 'In progress' : `${c.minutes} min`}
        </span>
      </div>
      <h4>{c.title}</h4>
      <p>{c.summary}</p>
      <div className="tags">
        {showTrack ? <span>{c.trackTitle}</span> : c.concepts.map((k) => <span key={k}>{k}</span>)}
      </div>
    </a>
  );
}
