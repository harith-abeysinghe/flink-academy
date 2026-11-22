/* Animated SVG lesson diagrams: a tiny scene + step-timeline engine (no libraries, works offline).
   Lessons embed one with ${FA.D('name')}. A diagram is registered with
   FA.registerDiagram(name, { title, w, h, build(S, cfg) -> [{ cap, run(A, S) }] }).
   build() draws the static scene; each step's run() animates it with the Anim helpers.
   Going to step k rebuilds the scene, replays steps 0..k-1 instantly, then animates step k. */
window.FA = window.FA || {};
(function (FA) {
  'use strict';
  const NS = 'http://www.w3.org/2000/svg';
  const CANCEL = { cancelled: true };
  let UID = 0;
  FA.diagrams = {};
  FA.registerDiagram = (name, def) => { FA.diagrams[name] = def; };
  FA.D = (name, cfg) => FA.W('diagram', Object.assign({ name }, cfg || {}));
  /* a step cancelled mid-flight rejects its parallel tweens with CANCEL; that is expected, not an error */
  window.addEventListener('unhandledrejection', e => { if (e.reason === CANCEL) e.preventDefault(); });

  /* [fill, stroke, text] for boxes; opaque so lines behind boxes stay hidden */
  const TONES = {
    box: ['#1a2234', '#3a4766', '#e6ebf5'],
    muted: ['#121926', '#2a3550', '#6f7c98'],
    info: ['#15284a', '#3d8bfd', '#d6e5ff'],
    accent: ['#3a1a26', '#e6526f', '#ffd6de'],
    purple: ['#241d48', '#7c5cff', '#e0d8ff'],
    ok: ['#11332a', '#33c481', '#d2f5e4'],
    warn: ['#382b12', '#f5a623', '#ffe9c4'],
    bad: ['#3d1a1d', '#ef4b4b', '#ffd6d6'],
    idle: ['#2b63c4', '#6a9cf0', '#ffffff'],
    busy: ['#c93636', '#f07070', '#ffffff'],
    bp: ['#050608', '#6b7280', '#ffffff'],
    ghost: ['none', '#2a3550', '#6f7c98']
  };
  const SOLID = { info: '#3d8bfd', accent: '#e6526f', purple: '#7c5cff', ok: '#33c481', warn: '#f5a623', bad: '#ef4b4b', muted: '#56627e', white: '#e6ebf5', bp: '#0a0a0a', idle: '#2b63c4', busy: '#c93636' };
  const DARK_TEXT = { ok: 1, warn: 1, white: 1 };
  FA.DG = { TONES, SOLID };

  function el(tag, attrs, parent) {
    const e = document.createElementNS(NS, tag);
    if (attrs) for (const k in attrs) if (attrs[k] !== undefined && attrs[k] !== null && attrs[k] !== false) e.setAttribute(k, attrs[k]);
    if (parent) parent.appendChild(e);
    return e;
  }
  function setPos(g, x, y) { g._tx = x; g._ty = y; g.setAttribute('transform', `translate(${x},${y})`); }
  function getv(e, k) {
    if (k === 'tx') return e._tx || 0;
    if (k === 'ty') return e._ty || 0;
    const v = e.getAttribute(k);
    if (v === null) return k === 'opacity' ? 1 : 0;
    return parseFloat(v);
  }
  function setv(e, k, v) {
    if (k === 'tx') setPos(e, v, e._ty || 0);
    else if (k === 'ty') setPos(e, e._tx || 0, v);
    else e.setAttribute(k, k === 'opacity' ? Math.max(0, Math.min(1, v)) : v);
  }
  const easeIO = p => p < 0.5 ? 4 * p * p * p : 1 - Math.pow(-2 * p + 2, 3) / 2;
  const linear = p => p;

  /* ---------- Scene: drawing helpers ---------- */
  class Scene {
    constructor(svg, uid) { this.svg = svg; this.uid = uid; this.mk = {}; this.defs = el('defs', null, svg); this.root = el('g', null, svg); }
    marker(color) {
      if (this.mk[color]) return this.mk[color];
      const id = this.uid + 'm' + Object.keys(this.mk).length;
      const m = el('marker', { id, viewBox: '0 0 10 10', refX: 8.5, refY: 5, markerWidth: 6.5, markerHeight: 6.5, orient: 'auto-start-reverse' }, this.defs);
      el('path', { d: 'M0,0 L10,5 L0,10 z', fill: color }, m);
      return (this.mk[color] = `url(#${id})`);
    }
    g(o = {}) {
      const g = el('g', { opacity: o.hidden ? 0 : null }, o.parent || this.root);
      if (o.x !== undefined || o.y !== undefined) setPos(g, o.x || 0, o.y || 0);
      return g;
    }
    rect(x, y, w, h, o = {}) {
      const t = TONES[o.tone || 'box'];
      return el('rect', { x, y, width: w, height: h, rx: o.rx ?? 8, fill: o.fill || t[0], stroke: o.stroke || t[1], 'stroke-width': o.sw || 1.4, 'stroke-dasharray': o.dash, opacity: o.hidden ? 0 : o.opacity }, o.parent || this.root);
    }
    text(x, y, str, o = {}) {
      const t = el('text', { x, 'font-size': o.size || 13, 'text-anchor': o.anchor || 'middle', fill: o.fill || (o.tone ? TONES[o.tone][2] : '#e6ebf5'), 'font-weight': o.weight, class: o.mono ? 'mono' : null, opacity: o.hidden ? 0 : o.opacity, 'font-style': o.italic ? 'italic' : null }, o.parent || this.root);
      t._x = x; t._y = y; t._size = o.size || 13;
      this.setText(t, str);
      return t;
    }
    setText(t, str) {
      while (t.firstChild) t.removeChild(t.firstChild);
      const lines = String(str).split('\n'), lh = t._size * 1.25, y0 = t._y - (lines.length - 1) * lh / 2;
      lines.forEach((ln, i) => { const s = el('tspan', { x: t._x, y: y0 + i * lh, dy: '0.35em' }, t); s.textContent = ln; });
    }
    /* Box with centred label (+ optional sub line). o.corner puts the label top-left (for containers). */
    box(x, y, w, h, label, o = {}) {
      const g = this.g({ parent: o.parent, x, y, hidden: o.hidden });
      const tone = o.tone || 'box', T = TONES[tone];
      g._kind = 'box'; g._tone = tone;
      Object.assign(g, { bx: x, by: y, w, h, cx: x + w / 2, cy: y + h / 2, L: x, R: x + w, T: y, B: y + h });
      g.r = this.rect(0, 0, w, h, { parent: g, tone, rx: o.rx, dash: o.dash, sw: o.sw });
      const size = o.size || 13, lines = String(label).split('\n').length;
      if (o.corner) {
        g.t = this.text(12, 15, label, { parent: g, size: o.size || 12, anchor: 'start', fill: T[2], weight: 600 });
      } else {
        const sub = o.sub ? String(o.sub).split('\n').length : 0;
        const lh1 = size * 1.25, lh2 = 11 * 1.25, total = lines * lh1 + sub * lh2 + (sub ? 3 : 0), top = h / 2 - total / 2;
        g.t = this.text(w / 2, top + lines * lh1 / 2, label, { parent: g, size, weight: o.weight ?? 600, fill: T[2], mono: o.mono });
        if (o.sub) g.s = this.text(w / 2, top + lines * lh1 + 3 + sub * lh2 / 2, o.sub, { parent: g, size: 11, fill: '#93a0bb', mono: o.subMono });
      }
      return g;
    }
    /* Arrow from (x1,y1) to (x2,y2). o.bend curves it, o.via adds corner points, o.label sits at the midpoint. Returns a group (g.p = path). */
    arrow(x1, y1, x2, y2, o = {}) {
      const color = o.color || '#5d6a86';
      const g = this.g({ parent: o.parent, hidden: o.hidden });
      let d = `M${x1},${y1} L${x2},${y2}`, mx = (x1 + x2) / 2, my = (y1 + y2) / 2;
      if (o.bend) {
        const dx = x2 - x1, dy = y2 - y1, len = Math.hypot(dx, dy) || 1, cx = mx - dy / len * o.bend, cy = my + dx / len * o.bend;
        d = `M${x1},${y1} Q${cx},${cy} ${x2},${y2}`; mx = 0.25 * x1 + 0.5 * cx + 0.25 * x2; my = 0.25 * y1 + 0.5 * cy + 0.25 * y2;
      }
      if (o.via) { const pts = [[x1, y1], ...o.via, [x2, y2]]; d = 'M' + pts.map(p => p.join(',')).join(' L'); const m = pts[Math.floor(pts.length / 2)]; mx = m[0]; my = m[1]; }
      g.p = el('path', { d, fill: 'none', stroke: color, 'stroke-width': o.sw || 1.6, 'stroke-dasharray': o.dash, 'marker-end': o.head === false ? null : this.marker(color), 'marker-start': o.both ? this.marker(color) : null }, g);
      if (o.label) {
        const lx = mx + (o.lx || 0), ly = my + (o.ly ?? -10);
        if (o.labelBg) this.rect(lx - o.label.length * 3.6 - 6, ly - 9, o.label.length * 7.2 + 12, 18, { parent: g, rx: 9, fill: '#0e1422', stroke: color, sw: 1 });
        g.lbl = this.text(lx, ly, o.label, { parent: g, size: o.lsize || 11, fill: o.lcolor || '#93a0bb', weight: o.lweight || 600, mono: o.lmono });
      }
      return g;
    }
    line(x1, y1, x2, y2, o = {}) { return this.arrow(x1, y1, x2, y2, Object.assign({ head: false }, o)); }
    /* A record / message chip. Shapes: circle (default for 1-2 chars), pill, sq (serialized bytes), bar (checkpoint barrier). */
    token(x, y, label, o = {}) {
      const g = this.g({ parent: o.parent, x, y, hidden: o.hidden });
      g._kind = 'token';
      const tone = o.tone || 'info', fill = SOLID[tone] || tone, size = o.size || 11;
      label = String(label);
      if (o.shape === 'bar') {
        const hh = o.h || 30;
        g.c = el('rect', { x: -3.5, y: -hh / 2, width: 7, height: hh, rx: 2, fill }, g);
        g.t = this.text(0, -hh / 2 - 9, label, { parent: g, size, weight: 700, fill });
        g._bar = true;
        return g;
      }
      if (o.shape === 'sq') g.c = el('rect', { x: -8, y: -8, width: 16, height: 16, rx: 3, fill }, g);
      else if (o.shape === 'pill' || label.length > 2) { const w = o.w || Math.max(26, label.length * size * 0.6 + 14); g.c = el('rect', { x: -w / 2, y: -11, width: w, height: 22, rx: 11, fill }, g); }
      else g.c = el('circle', { r: o.r || 12, fill }, g);
      if (o.stroke) { g.c.setAttribute('stroke', o.stroke); g.c.setAttribute('stroke-width', 2.5); }
      g.t = this.text(0, 0, label, { parent: g, size, weight: 700, fill: DARK_TEXT[tone] ? '#10141c' : '#fff' });
      return g;
    }
    /* Recolour a box, token, rect or text. Fill/stroke changes are smoothed by CSS transitions. */
    tone(g, tone) {
      if (!g) return;
      const T = TONES[tone] || TONES.box;
      if (g._kind === 'box') {
        g.r.setAttribute('fill', T[0]); g.r.setAttribute('stroke', T[1]); g.t.setAttribute('fill', T[2]);
        if (g.s) g.s.setAttribute('fill', ['idle', 'busy', 'bp'].includes(tone) ? '#e8eefc' : '#93a0bb');
        g._tone = tone;
      } else if (g._kind === 'token') {
        const c = SOLID[tone] || tone; g.c.setAttribute('fill', c);
        g.t.setAttribute('fill', g._bar ? c : (DARK_TEXT[tone] ? '#10141c' : '#fff'));
      } else if (g.tagName === 'text') g.setAttribute('fill', T[2]);
      else { g.setAttribute('fill', T[0]); g.setAttribute('stroke', T[1]); }
    }
    stroke(g, color, w) { const r = g.r || g.c || g; r.setAttribute('stroke', color); if (w) r.setAttribute('stroke-width', w); }
    label(g, str) { this.setText(g.t || g, str); }
    sub(g, str) { if (g.s) this.setText(g.s, str); }
    remove(e) { if (e && e.parentNode) e.parentNode.removeChild(e); }
    /* any raw SVG element, e.g. S.shape('polygon', { points: '…', fill: '…' }, { hidden: true }) */
    shape(tag, attrs, o = {}) { const e = el(tag, attrs, o.parent || this.root); if (o.hidden) e.setAttribute('opacity', 0); return e; }
  }

  /* ---------- Anim: promise-based tweens that respect cancel + instant mode ---------- */
  class Anim {
    constructor(svg, tok, instant, S) { this.svg = svg; this.tok = tok; this.instant = instant; this.S = S; }
    live() { return !this.tok.dead && this.svg.isConnected; }
    wait(ms) {
      if (this.instant) return Promise.resolve();
      return new Promise((res, rej) => setTimeout(() => this.live() ? res() : rej(CANCEL), ms));
    }
    tween(ms, fn, ease = easeIO) {
      if (this.instant || ms <= 0) { fn(1); return Promise.resolve(); }
      return new Promise((res, rej) => {
        const t0 = performance.now();
        const step = now => {
          if (!this.live()) return rej(CANCEL);
          const p = Math.min(1, (now - t0) / ms);
          fn(ease(p));
          if (p < 1) requestAnimationFrame(step); else res();
        };
        requestAnimationFrame(step);
      });
    }
    to(e, props, ms = 600, ease) {
      if (!e) return Promise.resolve();
      const keys = Object.keys(props), from = keys.map(k => getv(e, k));
      return this.tween(ms, q => keys.forEach((k, i) => setv(e, k, from[i] + (props[k] - from[i]) * q)), ease);
    }
    move(e, x, y, ms = 700, ease) { return this.to(e, { tx: x, ty: y }, ms, ease); }
    /* move through a list of [x,y] points at constant speed */
    async path(e, pts, ms = 900) {
      let len = 0, x = e._tx || 0, y = e._ty || 0;
      const segs = pts.map(p => { const l = Math.hypot(p[0] - x, p[1] - y); x = p[0]; y = p[1]; len += l; return l; });
      for (let i = 0; i < pts.length; i++) await this.move(e, pts[i][0], pts[i][1], len ? ms * segs[i] / len : 0, linear);
    }
    show(...els) { return Promise.all(els.flat().map(e => this.to(e, { opacity: 1 }, 380))); }
    hide(...els) { return Promise.all(els.flat().map(e => this.to(e, { opacity: 0 }, 320))); }
    fade(e, op, ms = 380) { return this.to(e, { opacity: op }, ms); }
    async blink(e, n = 2, ms = 170) { for (let i = 0; i < n; i++) { await this.fade(e, 0.25, ms); await this.fade(e, 1, ms); } }
    count(t, from, to, ms, fmt = v => Math.round(v)) { return this.tween(ms, q => this.S.setText(t, fmt(from + (to - from) * q))); }
    text(t, str) { this.S.setText(t.t || t, str); }
    tone(g, tone) { this.S.tone(g, tone); }
    all(...ps) { return Promise.all(ps.flat()); }
    /* create a token at `from`, move it to `to`, then remove it unless o.keep */
    async fly(label, from, to, o = {}) {
      const tk = this.S.token(from[0], from[1], label, Object.assign({}, o, { hidden: false }));
      if (o.via) await this.path(tk, [...o.via, to], o.ms || 900);
      else await this.move(tk, to[0], to[1], o.ms || 800, o.ease);
      if (!o.keep) { await this.fade(tk, 0, 220); this.S.remove(tk); }
      return tk;
    }
    async stagger(items, fn, gap = 160) {
      const ps = [];
      for (let i = 0; i < items.length; i++) { ps.push(fn(items[i], i)); if (i < items.length - 1) await this.wait(gap); }
      return Promise.all(ps);
    }
  }

  Object.assign(FA.DG, { Scene, Anim, CANCEL });

  /* ---------- Widget: player UI ---------- */
  const strip = s => String(s).replace(/<[^>]+>/g, '');
  FA.registerWidget('diagram', function (host, cfg) {
    const def = FA.diagrams[cfg.name];
    if (!def) { host.textContent = 'Missing diagram: ' + cfg.name; return; }
    const h = FA.h;
    const reduce = !!(window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches);
    host.classList.add('dg');
    const svg = el('svg', { viewBox: `0 0 ${def.w || 800} ${def.h}`, class: 'dg-svg', role: 'img', 'aria-label': cfg.title || def.title });
    const cap = h('div', { class: 'dg-cap', 'aria-live': 'polite' });
    const btn = (txt, title, fn) => h('button', { class: 'dg-b', title, 'aria-label': title, onclick: () => { autoPaused = false; started = true; fn(); } }, txt);
    const bRestart = btn('↺', 'Replay from the start', () => { playing = true; show(0, true); });
    const bPrev = btn('‹', 'Previous step', () => { playing = false; show(cur - 1, true); });
    const bPlay = btn('▶', 'Play', () => togglePlay());
    const bNext = btn('›', 'Next step', () => { playing = false; primed ? show(0, true) : show(cur + 1, true); });
    const dots = h('div', { class: 'dg-dots' });
    host.append(
      h('div', { class: 'wtitle' }, cfg.title || def.title),
      h('div', { class: 'dg-stage' }, svg),
      h('div', { class: 'dg-foot' }, h('div', { class: 'dg-side' }, h('div', { class: 'dg-ctrl' }, bRestart, bPrev, bPlay, bNext), dots), cap));

    let steps = [], cur = 0, playing = false, tok = null, started = false, autoPaused = false, primed = false;
    function build() { svg.innerHTML = ''; const S = new Scene(svg, 'dg' + (++UID)); steps = def.build(S, cfg) || []; return S; }
    function ui() {
      bPlay.textContent = playing ? '❚❚' : '▶';
      bPlay.title = playing ? 'Pause' : 'Play'; bPlay.setAttribute('aria-label', bPlay.title);
      bPrev.disabled = cur <= 0 || primed; bNext.disabled = cur >= steps.length - 1 && !primed;
      if (dots.children.length !== steps.length) {
        dots.innerHTML = '';
        steps.forEach((_, i) => dots.appendChild(h('button', { title: 'Step ' + (i + 1), 'aria-label': 'Step ' + (i + 1), onclick: () => { autoPaused = false; started = true; playing = false; show(i, true); } })));
      }
      [...dots.children].forEach((d, i) => { d.className = i === cur ? 'on' : (i < cur ? 'done' : ''); });
      const s = steps[cur];
      cap.innerHTML = s ? `<span class="n">${cur + 1}/${steps.length}</span>${s.cap}` : '';
    }
    const hold = s => s.hold || Math.min(7500, Math.max(2600, 1300 + strip(s.cap).length * 34));
    async function show(k, animate) {
      if (tok) tok.dead = true;
      const my = tok = { dead: false };
      primed = false;
      const S = build();
      k = Math.max(0, Math.min(k, steps.length - 1));
      svg.classList.add('instant');
      const fast = new Anim(svg, my, true, S);
      try {
        for (let i = 0; i < k; i++) if (steps[i].run) await steps[i].run(fast, S);
        if (my.dead) return;
        cur = k; ui();
        void svg.getBoundingClientRect();
        svg.classList.remove('instant');
        if (steps[k].run) await steps[k].run(animate && !reduce ? new Anim(svg, my, false, S) : fast, S);
        if (my.dead || !playing) return;
        if (k >= steps.length - 1) { playing = false; ui(); return; }
        await new Anim(svg, my, false, S).wait(hold(steps[k]));
        if (!my.dead && playing) show(k + 1, true);
      } catch (e) { if (e !== CANCEL) console.error('diagram ' + cfg.name, e); }
    }
    function prime() { if (tok) tok.dead = true; tok = { dead: false }; build(); cur = 0; primed = true; ui(); }
    function togglePlay() {
      if (playing) { playing = false; ui(); return; }
      playing = true; ui();
      if (primed || cur >= steps.length - 1) show(0, true); else show(cur + 1, true);
    }
    prime();
    if (reduce) { show(0, false); return; }
    if ('IntersectionObserver' in window) {
      const io = new IntersectionObserver(es => {
        if (!host.isConnected) { io.disconnect(); return; }
        const vis = es[es.length - 1].isIntersecting;
        if (vis && !started) { started = true; playing = true; show(0, true); }
        else if (!vis && playing) { playing = false; autoPaused = true; ui(); }
        else if (vis && autoPaused) { autoPaused = false; togglePlay(); }
      }, { threshold: 0.5 });
      io.observe(host);
    }
  });
})(window.FA);
