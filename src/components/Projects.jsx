import { projects, projectById } from '../curriculum/projects.js';
import { useStore } from '../lib/store.js';
import { TaskCard, levelClass } from './Tracks.jsx';

export function ProjectCard({ project, passed }) {
  const done = project.tasks.filter((t) => passed[t.slug]).length;
  return (
    <a className="track-card project-card" href={`#/projects/${project.id}`}>
      <div className="card-top">
        <span className={levelClass(project.level)}>{project.level}</span>
      </div>
      <h3>{project.title}</h3>
      <p><b>{project.tagline}</b> {project.blurb}</p>
      <div className="tags">{project.tags.map((t) => <span key={t}>{t}</span>)}</div>
      <div className="card-foot">
        <span>{project.stages.length} stages · {project.tasks.length} tasks · about {project.hours}h</span>
        <span className="card-progress">
          <span className="bar"><i style={{ width: `${(done / project.tasks.length) * 100}%` }} /></span>
          {done}/{project.tasks.length}
        </span>
      </div>
    </a>
  );
}

export function ProjectsIndex() {
  const { passed } = useStore();
  return (
    <main className="page">
      <nav className="breadcrumb" aria-label="Breadcrumb"><a href="#/">Home</a> / <span>Projects</span></nav>
      <h1>Capstone projects</h1>
      <p className="page-lede">
        Each project is a path through the tracks that adds up to one recognisable system. Tasks you have already passed in a track
        count here too.
      </p>
      <div className="track-grid project-grid">
        {projects.map((p) => <ProjectCard key={p.id} project={p} passed={passed} />)}
      </div>
    </main>
  );
}

export function ProjectPage({ id }) {
  const state = useStore();
  const project = projectById[id];
  const done = project.tasks.filter((t) => state.passed[t.slug]).length;
  const nextTask = project.tasks.find((t) => !state.passed[t.slug]) ?? project.tasks[0];

  return (
    <main className="page">
      <nav className="breadcrumb" aria-label="Breadcrumb">
        <a href="#/">Home</a> / <a href="#/projects">Projects</a> / <span>{project.title}</span>
      </nav>

      <header>
        <p className="track-meta">
          <span className={levelClass(project.level)}>{project.level}</span>
          <span>{project.stages.length} stages</span>
          <span>{project.tasks.length} tasks</span>
          <span>about {project.hours}h</span>
        </p>
        <h1>Build {project.title}</h1>
        <p className="page-lede">{project.blurb}</p>
        <div className="tags big" style={{ marginTop: 16 }}>{project.tags.map((t) => <span key={t}>{t}</span>)}</div>
        <div className="hero-actions">
          <a className="btn btn-primary btn-large" href={`#/c/${nextTask.slug}`}>
            {done === 0 ? 'Start project' : done === project.tasks.length ? 'Review project' : `Continue: ${nextTask.title}`} →
          </a>
          <span className="card-progress">
            <span className="bar"><i style={{ width: `${(done / project.tasks.length) * 100}%` }} /></span>
            {done}/{project.tasks.length} passed
          </span>
        </div>
      </header>

      <section>
        <h2>Project map</h2>
        <p className="section-lede">Each row is a stage, each dot a task. Click a dot to open it.</p>
        <div className="project-map">
          {project.stages.map((stage, i) => (
            <div key={stage.title} className="map-row">
              <span className="map-label"><em>{i + 1}</em>{stage.title}</span>
              <span className="map-dots">
                {stage.tasks.map((t) => (
                  <a
                    key={t.slug}
                    href={`#/c/${t.slug}`}
                    className={state.passed[t.slug] ? 'on' : state.code[t.slug] ? 'half' : ''}
                    title={`${t.id} ${t.title}${state.passed[t.slug] ? ' (passed)' : ''}`}
                    aria-label={`${t.title}${state.passed[t.slug] ? ', passed' : ''}`}
                  />
                ))}
              </span>
            </div>
          ))}
        </div>
      </section>

      <section className="two-col">
        <div>
          <h3>What you will build</h3>
          <ul className="builds">{project.builds.map((b) => <li key={b}>{b}</li>)}</ul>
        </div>
        <div>
          <h3>The core trade-off</h3>
          <p className="tradeoff">{project.tradeoff}</p>
        </div>
      </section>

      <section>
        <h2>Stages</h2>
        {project.stages.map((stage, i) => {
          const n = stage.tasks.filter((t) => state.passed[t.slug]).length;
          return (
            <div key={stage.title} className="subtrack">
              <div className="subtrack-head">
                <h3><span className="stage-index">{i + 1}</span>{stage.title}</h3>
                <span className="count" style={{ marginLeft: 'auto' }}>{n}/{stage.tasks.length}</span>
              </div>
              <p className="section-lede" style={{ margin: '-6px 0 12px' }}>{stage.blurb}</p>
              <div className="task-grid">
                {stage.tasks.map((t) => <TaskCard key={t.slug} task={t} state={state} showTrack />)}
              </div>
            </div>
          );
        })}
      </section>
    </main>
  );
}
