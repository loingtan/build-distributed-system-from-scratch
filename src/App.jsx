import { useEffect, useState } from 'react';
import { bySlug, challenges, tracks, trackById } from './curriculum/catalog.js';
import { update, useStore } from './lib/store.js';
import Home from './components/Home.jsx';
import Workspace from './components/Workspace.jsx';
import { TracksIndex, TrackPage } from './components/Tracks.jsx';
import { ProjectsIndex, ProjectPage } from './components/Projects.jsx';
import { GuidePage, ConceptsPage } from './components/Learn.jsx';
import { projectById } from './curriculum/projects.js';

// #/ home · #/tracks · #/tracks/<id> · #/c/<task slug>
const parseRoute = () => {
  const hash = window.location.hash;
  const task = hash.match(/^#\/c\/([\w-]+)/);
  if (task && bySlug[task[1]]) return { page: 'task', slug: task[1] };
  const guide = hash.match(/^#\/tracks\/([\w-]+)\/guide/);
  if (guide && trackById[guide[1]]) return { page: 'guide', id: guide[1] };
  const track = hash.match(/^#\/tracks\/([\w-]+)/);
  if (track && trackById[track[1]]) return { page: 'track', id: track[1] };
  const concept = hash.match(/^#\/concepts\/?([\w-]*)/);
  if (concept) return { page: 'concepts', slug: concept[1] || null };
  if (/^#\/tracks/.test(hash)) return { page: 'tracks' };
  const project = hash.match(/^#\/projects\/([\w-]+)/);
  if (project && projectById[project[1]]) return { page: 'project', id: project[1] };
  if (/^#\/projects/.test(hash)) return { page: 'projects' };
  return { page: 'home' };
};

const systemTheme = () => (window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');

export default function App() {
  const state = useStore();
  const [route, setRoute] = useState(parseRoute);
  const slug = route.page === 'task' ? route.slug : null;
  const [drawer, setDrawer] = useState(false);
  const [system, setSystem] = useState(systemTheme);
  const theme = state.theme ?? system;

  useEffect(() => {
    const onHash = () => {
      setRoute(parseRoute());
      setDrawer(false);
      window.scrollTo(0, 0);
    };
    window.addEventListener('hashchange', onHash);
    return () => window.removeEventListener('hashchange', onHash);
  }, []);

  useEffect(() => {
    const mq = window.matchMedia('(prefers-color-scheme: dark)');
    const onChange = () => setSystem(systemTheme());
    mq.addEventListener('change', onChange);
    return () => mq.removeEventListener('change', onChange);
  }, []);

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
  }, [theme]);

  useEffect(() => {
    if (!drawer) return;
    const onKey = (e) => e.key === 'Escape' && setDrawer(false);
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [drawer]);

  const challenge = slug ? bySlug[slug] : null;
  const index = challenge ? challenges.findIndex((c) => c.slug === slug) : -1;
  const prev = index > 0 ? challenges[index - 1] : null;
  const next = index >= 0 ? challenges[index + 1] : null;
  const done = challenges.filter((c) => state.passed[c.slug]).length;
  const progressLang = state.lang === 'go' ? 'Go' : 'JavaScript';

  useEffect(() => {
    document.title = challenge ? `${challenge.title} · Quorum Lab` : 'Quorum Lab · distributed systems by practice';
  }, [challenge]);

  return (
    <div className={challenge ? 'app app-work' : 'app'}>
      <header className="topbar">
        <a className="brand" href="#/" aria-label="Quorum Lab home">
          <svg viewBox="0 0 24 24" aria-hidden="true">
            <path d="M12 5 L5 18 L19 18 Z" fill="none" stroke="currentColor" strokeWidth="1.4" opacity=".45" />
            <circle cx="12" cy="5" r="3" />
            <circle cx="5" cy="18" r="3" />
            <circle cx="19" cy="18" r="3" />
          </svg>
          <span>Quorum Lab</span>
        </a>

        {challenge && (
          <nav className="crumbs" aria-label="Challenge navigation">
            <button className="btn btn-ghost" onClick={() => setDrawer(true)} aria-haspopup="dialog">
              <svg viewBox="0 0 16 16" aria-hidden="true"><path d="M2 4h12M2 8h12M2 12h12" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" /></svg>
              <span className="crumb-label">
                <em>{challenge.number}</em> {challenge.title}
              </span>
            </button>
            <a className={`btn btn-ghost step-btn${prev ? '' : ' off'}`} href={prev ? `#/c/${prev.slug}` : undefined} aria-label="Previous challenge" aria-disabled={!prev}>←</a>
            <a className={`btn btn-ghost step-btn${next ? '' : ' off'}`} href={next ? `#/c/${next.slug}` : undefined} aria-label="Next challenge" aria-disabled={!next}>→</a>
          </nav>
        )}

        {!challenge && (
          <nav className="mainnav" aria-label="Main">
            <a href="#/" className={route.page === 'home' ? 'on' : ''}>Home</a>
            <a href="#/tracks" className={route.page.startsWith('track') ? 'on' : ''}>Tracks</a>
            <a href="#/projects" className={route.page.startsWith('project') ? 'on' : ''}>Projects</a>
            <a href="#/concepts" className={route.page === 'concepts' ? 'on' : ''}>Concepts</a>
          </nav>
        )}

        <div className="topbar-right">
          <a className="mini-progress" href={challenge ? `#/tracks/${challenge.trackId}` : '#/tracks'} title={`Tasks passed in ${progressLang}`}>
            <span className="bar"><i style={{ width: `${(done / challenges.length) * 100}%` }} /></span>
            {state.lang === 'go' && <em className="lang-tag">Go</em>}
            {done}/{challenges.length}
          </a>
          <button
            className="btn btn-ghost icon"
            onClick={() => update({ theme: theme === 'dark' ? 'light' : 'dark' })}
            aria-label={`Switch to ${theme === 'dark' ? 'light' : 'dark'} theme`}
          >
            {theme === 'dark' ? (
              <svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="3.2" fill="currentColor" /><path d="M8 1v2M8 13v2M1 8h2M13 8h2M3 3l1.4 1.4M11.6 11.6 13 13M3 13l1.4-1.4M11.6 4.4 13 3" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" /></svg>
            ) : (
              <svg viewBox="0 0 16 16" aria-hidden="true"><path d="M13.5 9.6A6 6 0 0 1 6.4 2.5a6 6 0 1 0 7.1 7.1Z" fill="currentColor" /></svg>
            )}
          </button>
        </div>
      </header>

      {challenge ? (
        <Workspace key={challenge.slug} challenge={challenge} theme={theme} />
      ) : route.page === 'tracks' ? (
        <TracksIndex />
      ) : route.page === 'guide' ? (
        <GuidePage key={route.id} id={route.id} />
      ) : route.page === 'concepts' ? (
        <ConceptsPage slug={route.slug} />
      ) : route.page === 'track' ? (
        <TrackPage key={route.id} id={route.id} />
      ) : route.page === 'projects' ? (
        <ProjectsIndex />
      ) : route.page === 'project' ? (
        <ProjectPage key={route.id} id={route.id} />
      ) : (
        <Home />
      )}

      {drawer && (
        <div className="drawer-back" onClick={() => setDrawer(false)}>
          <aside className="drawer" role="dialog" aria-label="Curriculum" onClick={(e) => e.stopPropagation()}>
            <div className="drawer-head">
              <b>Curriculum</b>
              <button className="btn btn-ghost icon" onClick={() => setDrawer(false)} aria-label="Close">✕</button>
            </div>
            {tracks.map((track) => (
              <div key={track.id} className="drawer-track">
                <h4>
                  <a href={`#/tracks/${track.id}`}>{track.number} {track.title}</a>
                </h4>
                {challenges
                  .filter((c) => c.trackId === track.id)
                  .map((c) => (
                    <a key={c.slug} href={`#/c/${c.slug}`} className={c.slug === slug ? 'on' : ''} aria-current={c.slug === slug ? 'page' : undefined}>
                      <span className={`mark mark-${state.passed[c.slug] ? 'pass' : state.code[c.slug] ? 'started' : 'idle'}`} />
                      <em>{c.number}</em>
                      {c.title}
                    </a>
                  ))}
              </div>
            ))}
            <a className="drawer-home" href="#/tracks">← All tracks</a>
          </aside>
        </div>
      )}
    </div>
  );
}
