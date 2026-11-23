import { useEffect, useState } from 'react';
import Timeline from './Timeline.jsx';

const Mark = ({ state }) => <span className={`mark mark-${state}`} aria-hidden="true" />;

const stateOf = (r, running) => (running ? 'running' : !r ? 'idle' : r.skipped ? 'idle' : r.ok ? 'pass' : 'fail');

export default function Results({ challenge, run, theme, onRun, onRunOne }) {
  const [selected, setSelected] = useState(0);
  const [tab, setTab] = useState('checks');

  // When a run finishes, jump to the first failing test.
  useEffect(() => {
    if (run.status !== 'done') return;
    // A single-test rerun stays on that test; a full run jumps to the first failure.
    if (run.only != null) {
      setSelected(run.only);
      setTab('checks');
      return;
    }
    const firstFail = run.results.findIndex((r) => r && !r.skipped && !r.ok);
    setSelected(firstFail === -1 ? 0 : firstFail);
    setTab('checks');
  }, [run.status]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    setSelected(0);
    setTab('checks');
  }, [challenge.slug]);

  if (run.status === 'idle') {
    return (
      <div className="results results-empty">
        <div>
          <h3>{challenge.tests.length === 1 ? '1 test' : `${challenge.tests.length} tests`} waiting</h3>
          <ul className="test-preview">
            {challenge.tests.map((t) => (
              <li key={t.name}>
                <b>{t.name}</b>
                <span>{t.blurb}</span>
              </li>
            ))}
          </ul>
          <button className="btn btn-quiet" onClick={onRun}>
            Run them <kbd>⌘</kbd><kbd>↵</kbd>
          </button>
        </div>
      </div>
    );
  }

  const result = run.results[selected];
  const test = challenge.tests[selected];
  const ready = result && !result.skipped;
  const failed = ready ? result.checks.filter((c) => !c.ok) : [];
  const logCount = ready ? result.logs.length : 0;
  const hasProblemLogs = ready && result.logs.some((l) => l.level === 'error' || l.level === 'warn');

  return (
    <div className="results">
      <div className="test-tabs" role="tablist" aria-label="Tests">
        {challenge.tests.map((t, i) => (
          <button
            key={t.name}
            role="tab"
            aria-selected={i === selected}
            className={i === selected ? 'on' : ''}
            onClick={() => setSelected(i)}
          >
            <Mark state={stateOf(run.results[i], run.status === 'running' && run.current === i)} />
            {t.name}
          </button>
        ))}
      </div>

      <div className="result-body">
        <div className="result-head">
          <div className="result-blurb">
            <p>{test.blurb}</p>
            {onRunOne && run.status !== 'running' && (
              <div className="rerun">
                <button className="btn btn-quiet btn-small" onClick={() => onRunOne(selected)} title="Rerun only this test, with the same fault schedule">
                  Run only this test
                </button>
                <button
                  className="btn btn-quiet btn-small"
                  onClick={() => onRunOne(selected, { seed: 1 + Math.floor(Math.random() * 1e6) })}
                  title="Rerun this test with a different, equally valid fault schedule: tells bad luck from a real bug"
                >
                  Try another seed
                </button>
              </div>
            )}
          </div>
          {ready && result.stats && <Stats s={result.stats} r={result} />}
          {ready && result.seed != null && <p className="seed-note">seed {result.seed}</p>}
        </div>

        {!ready && (
          <p className="pending">
            {run.status === 'running' ? (run.current === selected ? 'Running…' : 'Queued…') : 'Not run: an earlier test stopped the run.'}
          </p>
        )}

        {ready && (
          <>
            <div className="sub-tabs" role="tablist">
              <button role="tab" aria-selected={tab === 'checks'} className={tab === 'checks' ? 'on' : ''} onClick={() => setTab('checks')}>
                Checks
                <em className={failed.length ? 'bad' : 'good'}>
                  {result.checks.length - failed.length}/{result.checks.length}
                </em>
              </button>
              <button role="tab" aria-selected={tab === 'timeline'} className={tab === 'timeline' ? 'on' : ''} onClick={() => setTab('timeline')}>
                Timeline
              </button>
              <button role="tab" aria-selected={tab === 'logs'} className={tab === 'logs' ? 'on' : ''} onClick={() => setTab('logs')}>
                Logs
                {logCount > 0 && <em className={hasProblemLogs ? 'bad' : ''}>{logCount}</em>}
              </button>
            </div>

            {tab === 'checks' && (
              <ul className="checks">
                {result.checks.map((c) => (
                  <li key={c.name} className={c.ok ? 'ok' : 'no'}>
                    <Mark state={c.ok ? 'pass' : 'fail'} />
                    <div>
                      <span>{c.name}</span>
                      {!c.ok && c.detail && <p>{c.detail}</p>}
                    </div>
                  </li>
                ))}
                {failed.length > 0 && result.trace.length > 0 && (
                  <li className="nudge">
                    <button className="link" onClick={() => setTab('timeline')}>
                      See what the nodes actually sent →
                    </button>
                  </li>
                )}
              </ul>
            )}

            {tab === 'timeline' && <Timeline result={result} theme={theme} />}

            {tab === 'logs' && (
              <div className="logs">
                {result.logs.length === 0 && (
                  <p className="pending">
                    Nothing logged. Call <code>node.log(…)</code> or <code>console.log(…)</code> in your code to trace what a node is doing.
                  </p>
                )}
                {result.logs.map((l, i) => (
                  <div key={i} className={`log log-${l.level}`}>
                    <time>{Math.round(l.t)}ms</time>
                    <b>{l.source}</b>
                    <span>{l.text}</span>
                  </div>
                ))}
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}

function Stats({ s, r }) {
  const items = [];
  if (s.ops) items.push([s.ops.toLocaleString(), 'client requests']);
  if (s.nodeMsgs || !s.kvMsgs) items.push([s.nodeMsgs.toLocaleString(), 'node messages']);
  if (s.ops && s.nodeMsgs) items.push([s.msgsPerOp.toFixed(1), 'per request']);
  if (s.kvMsgs) items.push([s.kvMsgs.toLocaleString(), r.serviceIds?.length === 1 ? `${r.serviceIds[0]} messages` : 'service messages']);
  if (s.dropped) items.push([s.dropped.toLocaleString(), 'lost']);
  if (s.ops) items.push([`${Math.round(s.p50)}ms`, 'median reply']);
  items.push([`${(r.duration / 1000).toFixed(1)}s`, 'simulated']);
  return (
    <dl className="stats">
      {items.map(([value, label]) => (
        <div key={label}>
          <dt>{label}</dt>
          <dd>{value}</dd>
        </div>
      ))}
    </dl>
  );
}
