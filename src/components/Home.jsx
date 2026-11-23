import { categories, tracks, challenges, planned } from '../curriculum/catalog.js';
import { useStore } from '../lib/store.js';
import { projects } from '../curriculum/projects.js';
import { ProjectCard } from './Projects.jsx';

const features = [
  ['Build real mechanisms', 'Gossip, CRDTs, logical clocks, a replicated log, Raft election. Each task is working code, not a quiz.'],
  ['One step at a time', 'Every track is split into subtracks of small tasks. Each task starts from what you built in the one before.'],
  ['A cluster in your browser', 'A simulator runs your node on a whole cluster, with latency, lost messages and partitions. Nothing to install.'],
  ['Failures you can see', 'Checks name the guarantee that broke, and a timeline shows every message so you can find out why.'],
];

export default function Home() {
  const { passed, code } = useStore();
  const done = challenges.filter((c) => passed[c.slug]).length;
  const nextUp = challenges.find((c) => !passed[c.slug]) ?? challenges[0];
  const started = done > 0 || Object.keys(code).length > 0;

  return (
    <main className="home">
      <header className="hero">
        <div className="hero-text">
          <p className="stat-line">
            <b>{tracks.length}</b> tracks <i>·</i> <b>{challenges.length}</b> challenges <i>·</i> <b>{projects.length}</b> projects
          </p>
          <h1>Build distributed systems from scratch.</h1>
          <p className="hero-lede">
            Learn the ideas behind Kafka, Dynamo and etcd by implementing them one small task at a time. You write the node; a simulator
            runs the cluster and the network does its worst.
          </p>
          <div className="hero-actions">
            <a className="btn btn-primary btn-large" href={`#/c/${nextUp.slug}`}>
              {started ? `Continue: ${nextUp.title}` : 'Start learning'} →
            </a>
            <a className="btn btn-quiet btn-large" href="#/projects">Capstone projects</a>
          </div>
          <p className="hero-note">Free, no account, nothing to install. Progress is saved in this browser.</p>
        </div>
        <ClusterArt />
      </header>

      <section className="how">
        {features.map(([title, text]) => (
          <div key={title}>
            <h3>{title}</h3>
            <p>{text}</p>
          </div>
        ))}
      </section>

      <section className="path">
        <div className="map-head">
          <div>
            <h2>Learning path</h2>
            <p className="section-lede">Work through the stages in order, or jump to the track you came for.</p>
          </div>
          <div className="progress" aria-label={`${done} of ${challenges.length} challenges passed`}>
            <div className="bar"><i style={{ width: `${(done / challenges.length) * 100}%` }} /></div>
            <span>{done} of {challenges.length} passed</span>
          </div>
        </div>

        <ol className="stages">
          {categories.map((cat, i) => (
            <li key={cat.id} className="stage">
              <span className="stage-num">{i + 1}</span>
              <div className="stage-body">
                <h3>{cat.title}</h3>
                <p>{cat.blurb}</p>
                <div className="pills">
                  {tracks.filter((t) => t.category === cat.id).map((t) => {
                    const n = t.tasks.filter((c) => passed[c.slug]).length;
                    return (
                      <a key={t.id} href={`#/tracks/${t.id}`} className={n === t.tasks.length ? 'pill done' : 'pill'}>
                        <em>{t.number}</em>
                        {t.title}
                        <span>{n}/{t.tasks.length}</span>
                      </a>
                    );
                  })}
                  {planned.filter((p) => p.category === cat.id).map((p) => (
                    <span key={p.title} className="pill soon" title={p.blurb}>{p.title}<span>coming soon</span></span>
                  ))}
                </div>
              </div>
            </li>
          ))}
        </ol>
        <a className="btn btn-quiet" href="#/tracks">View all tracks →</a>
      </section>

      <section className="path">
        <div className="map-head">
          <div>
            <h2>Capstone projects</h2>
            <p className="section-lede">Paths through the tracks that add up to one real system.</p>
          </div>
        </div>
        <div className="track-grid project-grid">
          {projects.map((p) => <ProjectCard key={p.id} project={p} passed={passed} />)}
        </div>
      </section>

      <section className="about">
        <h2>Learning by building</h2>
        <p>
          Reading about consensus is one thing. Watching your own leader election elect two leaders, finding the missing vote check in
          the message timeline, and fixing it, is what makes the idea stay. Every task here is a small program with a checker that does
          not take your word for it.
        </p>
        <p className="inspired">
          In the tradition of{' '}
          <a href="https://github.com/jepsen-io/maelstrom" target="_blank" rel="noreferrer">Maelstrom</a>,{' '}
          <a href="https://fly.io/dist-sys/" target="_blank" rel="noreferrer">Gossip Glomers</a> and{' '}
          <a href="https://pdos.csail.mit.edu/6.824/" target="_blank" rel="noreferrer">MIT 6.5840</a>.
        </p>
      </section>
    </main>
  );
}

// Five nodes exchanging messages; a partition cuts two of them off.
function ClusterArt() {
  const pts = [[150, 34], [262, 112], [220, 236], [80, 236], [38, 112]];
  const links = [[0, 1], [0, 2], [0, 3], [0, 4], [1, 2], [2, 3], [3, 4], [1, 4]];
  const cut = new Set(['0-3', '1-4', '2-3']);
  return (
    <svg className="art" viewBox="0 0 300 270" role="img" aria-label="Five nodes exchanging messages; a partition cuts two of them off.">
      <path className="art-cut" d="M 20 190 C 90 140, 150 200, 285 170" />
      {links.map(([a, b]) => (
        <line key={`${a}-${b}`} className={cut.has(`${a}-${b}`) ? 'art-link lost' : 'art-link'} x1={pts[a][0]} y1={pts[a][1]} x2={pts[b][0]} y2={pts[b][1]} />
      ))}
      {links.filter(([a, b]) => !cut.has(`${a}-${b}`)).map(([a, b], i) => (
        <circle key={i} className="art-msg" r="4">
          <animateMotion dur={`${1.6 + i * 0.35}s`} repeatCount="indefinite" path={`M ${pts[a][0]} ${pts[a][1]} L ${pts[b][0]} ${pts[b][1]}`} />
        </circle>
      ))}
      {pts.map(([x, y], i) => (
        <g key={i}>
          <circle className="art-node" cx={x} cy={y} r="21" />
          <text className="art-label" x={x} y={y + 4} textAnchor="middle">n{i + 1}</text>
        </g>
      ))}
    </svg>
  );
}
