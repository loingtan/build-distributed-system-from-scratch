import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';

// Every message of a run, drawn as a line from sender to receiver over time.
// The canvas is only as wide as the viewport; scrolling moves a time window
// across it, so zooming in never creates an enormous canvas.

const LANE = 46;
const TOP = 30;
const BOTTOM = 28;
const GUTTER = 62;
const PAD = 14;
const TYPE_COLOURS = 6;

const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();

function tickStep(msPerPx) {
  const want = msPerPx * 90;
  return [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000].find((s) => s >= want) ?? 10000;
}

export default function Timeline({ result, theme }) {
  const scroller = useRef(null);
  const canvas = useRef(null);
  const [width, setWidth] = useState(600);
  const [zoom, setZoom] = useState(1);
  const [scroll, setScroll] = useState(0);
  const [hover, setHover] = useState(null);
  const [showClients, setShowClients] = useState(true);
  const [big, setBig] = useState(false);
  const [viewH, setViewH] = useState(() => window.innerHeight);

  useEffect(() => {
    const onResize = () => setViewH(window.innerHeight);
    window.addEventListener('resize', onResize);
    return () => window.removeEventListener('resize', onResize);
  }, []);

  useEffect(() => {
    if (!big) return;
    const onKey = (e) => e.key === 'Escape' && setBig(false);
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [big]);

  // Rows grow to fill the space: at least 260px of plot inline, the whole window when expanded.
  const plotTarget = big ? Math.max(300, viewH - 200) : 260;

  const { lanes, laneY, types, height } = useMemo(() => {
    const lanes = ['clients', ...result.nodeIds, ...(result.serviceIds ?? [])];
    const lane = Math.max(LANE, Math.min(big ? 200 : 110, Math.floor(plotTarget / lanes.length)));
    const laneY = new Map(lanes.map((id, i) => [id, TOP + lane * i + lane / 2]));
    const counts = new Map();
    for (const m of result.trace) if (m.kind === 'node') counts.set(m.body.type, (counts.get(m.body.type) ?? 0) + 1);
    const types = [...counts].sort((a, b) => b[1] - a[1]).map(([type, count], i) => ({ type, count, colour: i % TYPE_COLOURS }));
    return { lanes, laneY, types, height: TOP + lane * lanes.length + BOTTOM };
  }, [result, big, plotTarget]);

  const typeColour = useMemo(() => new Map(types.map((t) => [t.type, t.colour])), [types]);
  const dropped = useMemo(() => result.trace.filter((m) => m.dropped).length, [result]);

  useLayoutEffect(() => {
    const el = scroller.current;
    const measure = () => setWidth(el.clientWidth);
    measure();
    const ro = new ResizeObserver(measure);
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  useEffect(() => {
    setZoom(1);
    setScroll(0);
    setHover(null);
    if (scroller.current) scroller.current.scrollLeft = 0;
  }, [result]);

  const duration = Math.max(1, result.duration);
  const plot = Math.max(50, width - GUTTER - PAD) * zoom; // pixels for the whole run
  const content = GUTTER + plot + PAD;
  const x = (t) => GUTTER + (t / duration) * plot - scroll;
  const y = (id) => laneY.get(laneY.has(id) ? id : 'clients');

  const visible = useMemo(() => {
    const t0 = ((scroll - 40) / plot) * duration;
    const t1 = ((scroll + width) / plot) * duration;
    return result.trace.filter((m) => m.t1 >= t0 && m.t0 <= t1 && (showClients || m.kind !== 'client'));
  }, [result, scroll, plot, width, duration, showClients]);

  useEffect(() => {
    const c = canvas.current;
    const dpr = window.devicePixelRatio || 1;
    c.width = Math.round(width * dpr);
    c.height = Math.round(height * dpr);
    const g = c.getContext('2d');
    g.setTransform(dpr, 0, 0, dpr, 0, 0);

    const col = {
      bg: css('--surface'),
      line: css('--line'),
      ink: css('--ink'),
      muted: css('--muted'),
      faint: css('--faint'),
      bad: css('--bad'),
      badSoft: css('--bad-soft'),
      kv: css('--m5'),
      types: Array.from({ length: TYPE_COLOURS }, (_, i) => css(`--m${i + 1}`)),
    };
    g.fillStyle = col.bg;
    g.fillRect(0, 0, width, height);
    g.font = `11px ${css('--sans')}`;
    g.textBaseline = 'middle';

    // Partitions: shaded spans behind everything.
    for (const p of result.partitions) {
      const a = Math.max(GUTTER, x(p.start));
      const b = Math.min(width, x(p.end));
      if (b <= a) continue;
      g.fillStyle = col.badSoft;
      g.fillRect(a, TOP - 6, b - a, height - TOP - BOTTOM + 12);
      g.fillStyle = col.bad;
      g.textAlign = 'left';
      g.fillText(`partition  ${p.groups.map((grp) => grp.join(' ')).join('  |  ')}`, a + 6, 14);
    }

    // Crashed nodes: a grey band on the node's own lane while it is down.
    for (const d of result.downs ?? []) {
      const a = Math.max(GUTTER, x(d.start));
      const b = Math.min(width, x(d.end));
      if (b <= a) continue;
      g.fillStyle = col.faint;
      g.globalAlpha = 0.28;
      g.fillRect(a, y(d.node) - 9, b - a, 18);
      g.globalAlpha = 1;
      g.fillStyle = col.muted;
      g.textAlign = 'left';
      g.fillText('down', a + 5, y(d.node) - 16);
    }

    // Time axis.
    const step = tickStep(duration / plot);
    g.textAlign = 'center';
    for (let t = Math.ceil((((scroll - 40) / plot) * duration) / step) * step; t <= duration; t += step) {
      const px = x(t);
      if (px > width) break;
      if (px < GUTTER) continue;
      g.strokeStyle = col.line;
      g.globalAlpha = 0.6;
      g.beginPath();
      g.moveTo(px, TOP - 6);
      g.lineTo(px, height - BOTTOM + 6);
      g.stroke();
      g.globalAlpha = 1;
      g.fillStyle = col.faint;
      g.fillText(t >= 1000 && step >= 100 ? `${t / 1000}s` : `${t}ms`, px, height - 11);
    }

    // Lanes.
    for (const id of lanes) {
      g.strokeStyle = id === 'clients' ? col.line : col.muted;
      g.globalAlpha = id === 'clients' ? 1 : 0.55;
      g.lineWidth = 1;
      g.beginPath();
      g.moveTo(GUTTER, y(id));
      g.lineTo(width, y(id));
      g.stroke();
      g.globalAlpha = 1;
    }

    // Messages. Client traffic first so node traffic sits on top.
    const dense = visible.length > 1500;
    const draw = (m, strong) => {
      const x0 = x(m.t0);
      const x1 = x(m.t1);
      const y0 = y(m.src);
      const y1 = y(m.dest);
      let colour = col.muted;
      let alpha = dense ? 0.22 : 0.4;
      if (m.kind === 'node') {
        colour = col.types[typeColour.get(m.body.type) ?? 0];
        alpha = dense ? 0.55 : 0.85;
      } else if (m.kind === 'kv') {
        colour = col.kv;
        alpha = dense ? 0.35 : 0.6;
      }
      if (m.dropped) colour = col.bad;
      g.strokeStyle = colour;
      g.fillStyle = colour;
      g.globalAlpha = strong ? 1 : alpha;
      g.lineWidth = strong ? 2.5 : 1.2;
      if (m.dropped) {
        // Lost in transit: stop halfway, with a cross.
        const mx = (x0 + x1) / 2;
        const my = (y0 + y1) / 2;
        g.setLineDash([3, 3]);
        g.beginPath();
        g.moveTo(x0, y0);
        g.lineTo(mx, my);
        g.stroke();
        g.setLineDash([]);
        g.beginPath();
        g.moveTo(mx - 3, my - 3);
        g.lineTo(mx + 3, my + 3);
        g.moveTo(mx + 3, my - 3);
        g.lineTo(mx - 3, my + 3);
        g.stroke();
      } else {
        g.beginPath();
        g.moveTo(x0, y0);
        g.lineTo(x1, y1);
        g.stroke();
        g.beginPath();
        g.arc(x1, y1, strong ? 3.5 : 2.2, 0, Math.PI * 2);
        g.fill();
      }
      g.globalAlpha = 1;
    };
    for (const m of visible) if (m.kind !== 'node') draw(m, false);
    for (const m of visible) if (m.kind === 'node') draw(m, false);
    if (hover) draw(hover.msg, true);

    // Lane labels, on an opaque gutter so messages scroll underneath.
    g.fillStyle = col.bg;
    g.fillRect(0, 0, GUTTER - 6, height);
    g.textAlign = 'right';
    for (const id of lanes) {
      g.fillStyle = id === 'clients' ? col.faint : col.ink;
      g.font = `${id === 'clients' ? 400 : 600} 12px ${css(id === 'clients' ? '--sans' : '--mono')}`;
      g.fillText(id, GUTTER - 14, y(id));
    }
  }, [result, visible, width, height, scroll, zoom, hover, theme, lanes, typeColour, plot, duration]); // eslint-disable-line react-hooks/exhaustive-deps

  const onMove = (e) => {
    const rect = canvas.current.getBoundingClientRect();
    const px = e.clientX - rect.left;
    const py = e.clientY - rect.top;
    let best = null;
    let bestD = 7;
    for (const m of visible) {
      const ax = x(m.t0);
      const ay = y(m.src);
      const bx = m.dropped ? (ax + x(m.t1)) / 2 : x(m.t1);
      const by = m.dropped ? (ay + y(m.dest)) / 2 : y(m.dest);
      const dx = bx - ax;
      const dy = by - ay;
      const len = dx * dx + dy * dy || 1;
      const u = Math.max(0, Math.min(1, ((px - ax) * dx + (py - ay) * dy) / len));
      const d = Math.hypot(px - (ax + u * dx), py - (ay + u * dy));
      // Prefer node traffic when lines overlap.
      const score = d - (m.kind === 'node' ? 1.5 : 0);
      if (score < bestD) {
        bestD = score;
        best = m;
      }
    }
    setHover(best ? { msg: best, px, py } : null);
  };

  const changeZoom = (next) => {
    const z = Math.max(1, Math.min(64, next));
    const el = scroller.current;
    const centre = (scroll + width / 2 - GUTTER) / plot;
    setZoom(z);
    requestAnimationFrame(() => {
      const newPlot = Math.max(50, width - GUTTER - PAD) * z;
      el.scrollLeft = Math.max(0, centre * newPlot + GUTTER - width / 2);
    });
  };

  const empty = result.trace.length === 0;

  return (
    <>
    {big && <div className="tl-backdrop" onClick={() => setBig(false)} />}
    <div className={big ? 'timeline big' : 'timeline'}>
      <div className="tl-bar">
        <div className="tl-legend">
          {types.map((t) => (
            <span key={t.type} className="tl-key">
              <i style={{ background: `var(--m${t.colour + 1})` }} />
              {t.type} <b>{t.count.toLocaleString()}</b>
            </span>
          ))}
          {dropped > 0 && (
            <span className="tl-key">
              <i style={{ background: 'var(--bad)' }} />
              lost <b>{dropped.toLocaleString()}</b>
            </span>
          )}
          {types.length === 0 && !empty && <span className="tl-key muted">No messages between nodes in this run.</span>}
        </div>
        <label className="tl-toggle">
          <input type="checkbox" checked={showClients} onChange={(e) => setShowClients(e.target.checked)} /> client traffic
        </label>
        <div className="tl-zoom" role="group" aria-label="Zoom">
          <button onClick={() => changeZoom(zoom / 2)} disabled={zoom <= 1} aria-label="Zoom out">−</button>
          <span>{zoom}×</span>
          <button onClick={() => changeZoom(zoom * 2)} disabled={zoom >= 64} aria-label="Zoom in">+</button>
        </div>
        <button className="btn btn-quiet tl-expand" onClick={() => setBig((b) => !b)} aria-label={big ? 'Close the large view' : 'Expand the timeline'}>
          {big ? 'Close ✕' : 'Expand ⤢'}
        </button>
      </div>
      <div className="tl-frame">
      <div className="tl-scroll" ref={scroller} onScroll={(e) => setScroll(e.currentTarget.scrollLeft)}>
        <div style={{ width: content, height }}>
          <canvas
            ref={canvas}
            style={{ width, height }}
            onMouseMove={onMove}
            onMouseLeave={() => setHover(null)}
          />
        </div>
      </div>
        {hover && (
          <div
            className="tl-tip"
            style={{
              left: Math.min(Math.max(8, hover.px + 14), Math.max(8, width - 300)),
              top: hover.py > height / 2 ? undefined : hover.py + 16,
              bottom: hover.py > height / 2 ? height - hover.py + 12 : undefined,
            }}
          >
            <div className="tl-tip-head">
              <b>{hover.msg.src}</b> → <b>{hover.msg.dest}</b>
              <span>
                {Math.round(hover.msg.t0)}ms{hover.msg.dropped ? ' · lost' : ` → ${Math.round(hover.msg.t1)}ms`}
              </span>
            </div>
            <pre>{clip(JSON.stringify(hover.msg.body, null, 1))}</pre>
          </div>
        )}
      </div>
      <p className="tl-note">
        {empty
          ? 'Nothing was sent in this run.'
          : `Time runs left to right; each line is one message from sender to receiver. Hover a line to read it${zoom === 1 ? ', zoom in to separate busy moments' : ''}.`}
        {result.traceTruncated && ' Only the first 6,000 messages are drawn.'}
        {big && ' Press Esc to close.'}
      </p>
    </div>
    </>
  );
}

function clip(text) {
  const lines = text.split('\n');
  const short = lines.length > 14 ? [...lines.slice(0, 13), ' …'] : lines;
  return short.map((l) => (l.length > 64 ? `${l.slice(0, 63)}…` : l)).join('\n');
}
