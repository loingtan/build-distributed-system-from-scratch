import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { marked } from 'marked';
import { challenges } from '../curriculum/catalog.js';
import { brief, starter, solution, apiReference, apiReferenceGo } from '../lib/content.js';
import { goify, goBrief } from '../lib/goify.js';
import { setIn, update, useStore, getState } from '../lib/store.js';
import { useRunner } from '../lib/useRunner.js';
import CodeEditor from './CodeEditor.jsx';
import { ConceptChip } from './Learn.jsx';
import Results from './Results.jsx';

const isGoNow = (s) => s.lang === 'go' && !!(labInfo?.ok && labInfo.go);

// What the local lab server can run (null until known; { ok: false } on a static build).
let labInfo = null;
function useLab() {
  const [info, setInfo] = useState(labInfo);
  useEffect(() => {
    if (labInfo) return;
    fetch('/api/lab')
      .then((r) => (r.ok ? r.json() : { ok: false }))
      .catch(() => ({ ok: false }))
      .then((v) => {
        labInfo = v;
        setInfo(v);
      });
  }, []);
  return info;
}

const Markdown = ({ text }) => {
  const html = useMemo(() => marked.parse(text), [text]);
  return <div className="prose" dangerouslySetInnerHTML={{ __html: html }} />;
};

// Drag a divider; `apply` receives the pointer position as a percentage of the container.
function useDivider(container, axis, apply) {
  return useCallback(
    (e) => {
      e.preventDefault();
      const rect = container.current.getBoundingClientRect();
      const move = (ev) => {
        const pct = axis === 'x' ? ((ev.clientX - rect.left) / rect.width) * 100 : ((ev.clientY - rect.top) / rect.height) * 100;
        apply(pct);
      };
      const up = () => {
        window.removeEventListener('pointermove', move);
        window.removeEventListener('pointerup', up);
        document.body.classList.remove('dragging');
      };
      document.body.classList.add('dragging');
      window.addEventListener('pointermove', move);
      window.addEventListener('pointerup', up);
    },
    [container, axis, apply],
  );
}

export default function Workspace({ challenge, theme }) {
  const state = useStore();
  const { slug } = challenge;
  const index = challenges.findIndex((c) => c.slug === slug);
  const next = challenges[index + 1];
  // state.passed is the list for the selected language (see store.js); check it
  // against the language actually in use, which can fall back to JS.
  const passed = !!(isGoNow(state) ? state.passedGo : state.passedJs)?.[slug];

  const [tab, setTab] = useState('brief');
  const [resetCount, setResetCount] = useState(0);
  const [mobilePane, setMobilePane] = useState('brief');
  const code = useRef('');
  const lab = useLab();
  const goReady = !!(lab?.ok && lab.go);
  const lang = state.lang === 'go' && goReady ? 'go' : 'js';
  const isGo = lang === 'go';
  const [run, start, startGo, cancel] = useRunner(slug, challenge.tests.length, lang);
  const [goData, setGoData] = useState(null); // { code, starter, solution, notes }
  const [goError, setGoError] = useState(null);

  useEffect(() => {
    if (!isGo) return;
    setGoData(null);
    setGoError(null);
    fetch(`/api/go/${slug}`)
      .then(async (r) => (r.ok ? r.json() : Promise.reject(new Error((await r.json().catch(() => ({}))).error ?? `HTTP ${r.status}`))))
      .then(setGoData)
      .catch((e) => setGoError(e.message));
  }, [slug, isGo, resetCount]);
  const split = useRef(null);
  const work = useRef(null);
  const briefScroll = useRef(null);

  const initial = useMemo(
    () => (isGo ? goData?.code ?? '' : getState().code[slug] ?? starter(slug)),
    [slug, resetCount, isGo, goData], // eslint-disable-line react-hooks/exhaustive-deps
  );
  useEffect(() => {
    code.current = initial;
  }, [initial]);

  useEffect(() => {
    setTab('brief');
    setMobilePane('brief');
    briefScroll.current?.scrollTo(0, 0);
  }, [slug]);

  const saveTimer = useRef(null);
  const onChange = (text) => {
    code.current = text;
    clearTimeout(saveTimer.current);
    saveTimer.current = setTimeout(() => {
      if (isGo) saveGo(text);
      else setIn('code', slug, text);
    }, 300);
  };

  const saveGo = (text) =>
    fetch(`/api/go/${slug}`, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ code: text }) }).catch(() => {});

  // Go compiler output ("./main.go:12:5: message") becomes editor diagnostics.
  const diagnostics = useMemo(() => {
    if (!isGo) return null;
    const build = run.results.flatMap((r) => r?.checks ?? []).find((c) => c.name === 'Code compiles' && !c.ok);
    if (!build?.detail) return [];
    return [...build.detail.matchAll(/main\.go:(\d+):(\d+):\s*(.+)/g)].map((m) => ({ line: +m[1], col: +m[2], message: m[3] }));
  }, [isGo, run.results]);

  const runOne = useCallback(
    (i, { seed = null } = {}) => {
      if (isGo) {
        if (goData) startGo(code.current, { docker: !!getState().docker, only: i, seed });
      } else {
        setIn('code', slug, code.current);
        start(code.current, { only: i, seed });
      }
    },
    [isGo, goData, startGo, start, slug],
  );

  const runTests = useCallback(() => {
    if (run.status === 'running') return;
    setMobilePane('code');
    if (isGo) {
      if (!goData) return;
      startGo(code.current, { docker: !!getState().docker });
      return;
    }
    setIn('code', slug, code.current);
    start(code.current);
  }, [slug, start, startGo, isGo, goData, run.status]);

  // Cmd/Ctrl+Enter anywhere on the page.
  useEffect(() => {
    const onKey = (e) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
        e.preventDefault();
        runTests();
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [runTests]);

  // Only a full run in the current language counts as passing the task.
  const allPass = run.status === 'done' && run.lang === lang && run.only == null && run.results.length > 0 && run.results.every((r) => r && r.ok);
  useEffect(() => {
    if (allPass) setIn(isGo ? 'passedGo' : 'passed', slug, true);
  }, [allPass, slug, isGo]);

  const reset = () => {
    if (isGo) {
      if (code.current !== goData?.starter && !window.confirm('Replace your Go code with the starter code?')) return;
      fetch(`/api/go/${slug}/reset`, { method: 'POST' }).then(() => setResetCount((n) => n + 1));
      return;
    }
    if (code.current !== starter(slug) && !window.confirm('Replace your code with the starter code?')) return;
    update((s) => {
      const rest = { ...s.code };
      delete rest[slug];
      return { code: rest };
    });
    setResetCount((n) => n + 1);
  };

  const useSolution = () => {
    if (isGo) {
      saveGo(goData.solution).then(() => setResetCount((n) => n + 1));
      return;
    }
    setIn('code', slug, solution(slug));
    setResetCount((n) => n + 1);
  };

  const dragX = useDivider(split, 'x', (pct) => update({ briefWidth: Math.max(24, Math.min(62, pct)) }));
  const dragY = useDivider(work, 'y', (pct) => update({ editorHeight: Math.max(18, Math.min(82, pct)) }));

  const hintsShown = state.hints[slug] ?? 0;
  const revealed = passed || !!state.revealed[slug];
  const running = run.status === 'running';
  const doneCount = run.results.filter((r) => r && !r.skipped).length;

  return (
    <div className={`workspace show-${mobilePane}`} ref={split} style={{ '--brief': `${state.briefWidth}%`, '--editor': `${state.editorHeight}%` }}>
      <div className="mobile-switch" role="tablist">
        <button role="tab" aria-selected={mobilePane === 'brief'} className={mobilePane === 'brief' ? 'on' : ''} onClick={() => setMobilePane('brief')}>Read</button>
        <button role="tab" aria-selected={mobilePane === 'code'} className={mobilePane === 'code' ? 'on' : ''} onClick={() => setMobilePane('code')}>Code</button>
      </div>

      <section className="pane brief-pane" aria-label="Challenge">
        <div className="pane-tabs" role="tablist">
          {[
            ['brief', 'Brief'],
            ['hints', `Hints${hintsShown ? ` ${hintsShown}/${challenge.hints.length}` : ''}`],
            ['api', 'Node API'],
            ['solution', 'Solution'],
          ].map(([id, label]) => (
            <button key={id} role="tab" aria-selected={tab === id} className={tab === id ? 'on' : ''} onClick={() => setTab(id)}>
              {label}
            </button>
          ))}
        </div>
        <div className="pane-scroll" ref={briefScroll}>
          {tab === 'brief' && (
            <article>
              <p className="eyebrow">
                <a href={`#/tracks/${challenge.trackId}`}>{challenge.trackTitle}</a> · {challenge.id} · <a href={`#/tracks/${challenge.trackId}/guide`}>Read the guide</a>
              </p>
              <h1>
                {challenge.title}
                {passed && <span className="done-tag">{isGo ? 'Passed in Go' : 'Passed'}</span>}
              </h1>
              <p className="meta">
                <span>{challenge.level}</span>
                <span>about {challenge.minutes} min</span>
                {challenge.concepts.map((c) => (
                  <ConceptChip key={c} tag={c} />
                ))}
              </p>
              <Markdown text={isGo ? goBrief(brief(slug), goData?.snippets) : brief(slug)} />
              {isGo && goData?.notes && (
                <div className="go-notes">
                  <h2>Notes for Go</h2>
                  <Markdown text={goData.notes} />
                </div>
              )}
              <p className="stuck">
                Stuck? Open <button className="link" onClick={() => setTab('hints')}>Hints</button> for a nudge, or the{' '}
                <button className="link" onClick={() => setTab('api')}>Node API</button> for everything <code>node</code> can do.
              </p>
            </article>
          )}

          {tab === 'hints' && (
            <article>
              <h2>Hints</h2>
              <p className="lede">Each one gives away a little more. Take them one at a time.</p>
              <ol className="hints">
                {challenge.hints.slice(0, hintsShown).map((h, i) => (
                  <li key={i}>
                    <Markdown text={isGo ? goify(h) : h} />
                  </li>
                ))}
              </ol>
              {hintsShown < challenge.hints.length ? (
                <button className="btn btn-quiet" onClick={() => setIn('hints', slug, hintsShown + 1)}>
                  {hintsShown === 0 ? 'Show the first hint' : `Show hint ${hintsShown + 1} of ${challenge.hints.length}`}
                </button>
              ) : (
                <p className="lede">
                  That is every hint. If it still does not click, the <button className="link" onClick={() => setTab('solution')}>reference solution</button> is commented.
                </p>
              )}
            </article>
          )}

          {tab === 'api' && (
            <article>
              <h2>Node API</h2>
              <Markdown text={isGo ? apiReferenceGo : apiReference} />
            </article>
          )}

          {tab === 'solution' && (
            <article>
              <h2>Reference solution</h2>
              {revealed ? (
                <>
                  <p className="lede">One way to do it, not the only one. If yours passes and looks different, compare the trade-offs.</p>
                  <pre className="code-block"><code>{isGo ? goData?.solution ?? 'Loading…' : solution(slug)}</code></pre>
                  <button className="btn btn-quiet" onClick={useSolution}>Load into the editor</button>
                </>
              ) : (
                <>
                  <p className="lede">
                    You learn most from the attempt that fails. Run your code, read the failing check, look at the timeline, and try the hints first.
                  </p>
                  <button className="btn btn-quiet" onClick={() => setIn('revealed', slug, true)}>Show it anyway</button>
                </>
              )}
            </article>
          )}
        </div>
      </section>

      <div className="divider divider-x" onPointerDown={dragX} role="separator" aria-orientation="vertical" />

      <section className="pane work-pane" ref={work} aria-label="Editor and results">
        <div className="editor-wrap">
          <div className="editor-bar">
            <div className="lang-switch" role="group" aria-label="Language">
              <button className={!isGo ? 'on' : ''} onClick={() => update({ lang: 'js' })} title="JavaScript in the browser simulator">JS</button>
              <button
                className={isGo ? 'on' : ''}
                onClick={() => update({ lang: 'go' })}
                disabled={!goReady}
                title={goReady ? 'Go, run as real processes by the local lab server' : lab === null ? 'Checking for the lab server…' : 'Go needs the lab server: start the site with "npm run dev" on a machine with Go installed'}
              >
                Go
              </button>
            </div>
            <span className="file">
              {isGo ? (
                <>
                  <b>main.go</b> one real process per node
                </>
              ) : (
                <>
                  <b>node.js</b> runs on every node
                </>
              )}
            </span>
            {isGo && lab?.docker && (
              <label className="docker-toggle" title="Run each node in its own Docker container">
                <input type="checkbox" checked={!!state.docker} onChange={(e) => update({ docker: e.target.checked })} /> Docker
              </label>
            )}
            <button className="btn btn-ghost" onClick={reset}>Reset</button>
            {running && (
              <button className="btn btn-ghost" onClick={cancel} title="Stop the tests">
                Stop
              </button>
            )}
            <button className="btn btn-primary" onClick={runTests} disabled={running || (isGo && !goData)}>
              {running
                ? run.phase === 'build'
                  ? 'Building…'
                  : `Running ${Math.min(doneCount + 1, challenge.tests.length)}/${challenge.tests.length}…`
                : 'Run tests'}
              {!running && <span className="keys"><kbd>⌘</kbd><kbd>↵</kbd></span>}
            </button>
          </div>
          {isGo && goError ? (
            <div className="editor-host editor-msg">Could not load the Go code: {goError}</div>
          ) : isGo && !goData ? (
            <div className="editor-host editor-msg">Loading the Go code…</div>
          ) : (
            <CodeEditor docKey={`${slug}:${resetCount}:${lang}`} initial={initial} onChange={onChange} onRun={runTests} language={lang} diagnostics={diagnostics} />
          )}
        </div>

        <div className="divider divider-y" onPointerDown={dragY} role="separator" aria-orientation="horizontal" />

        <div className="results-wrap">
          {allPass && (
            <div className="success" role="status">
              <div>
                <b>All tests pass.</b>{' '}
                {next ? 'Your cluster held up. Ready for the next one?' : 'That was the last challenge. You have built the core of a distributed system.'}
              </div>
              {next ? (
                <a className="btn btn-primary" href={`#/c/${next.slug}`}>
                  Next: {next.title} →
                </a>
              ) : (
                <a className="btn btn-primary" href="#/tracks">Back to all tracks</a>
              )}
            </div>
          )}
          <Results challenge={challenge} run={run} theme={theme} onRun={runTests} onRunOne={runOne} />
        </div>
      </section>
    </div>
  );
}
