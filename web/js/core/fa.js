/* Flink Academy core: namespace, DOM helpers, progress store, router */
window.FA = window.FA || {};
(function (FA) {
  'use strict';
  FA.levels = [];
  FA.widgets = {};
  FA.views = {};
  FA.registerLevel = function (lvl) { FA.levels.push(lvl); FA.levels.sort((a, b) => a.id - b.id); };
  FA.registerWidget = function (name, fn) { FA.widgets[name] = fn; };
  FA.W = (name, cfg) => `<div data-widget="${name}" data-config='${JSON.stringify(cfg || {}).replace(/'/g, '&#39;')}'></div>`;
  FA.C = (type, title, html) => `<div class="callout ${type}"><b>${title}</b>${html}</div>`;

  /* ---------- DOM helpers ---------- */
  FA.h = function (tag, attrs, ...kids) {
    const el = document.createElement(tag);
    if (attrs) for (const [k, v] of Object.entries(attrs)) {
      if (v === null || v === undefined || v === false) continue;
      if (k === 'class') el.className = v;
      else if (k === 'html') el.innerHTML = v;
      else if (k.startsWith('on') && typeof v === 'function') el.addEventListener(k.slice(2), v);
      else if (k === 'style' && typeof v === 'object') Object.assign(el.style, v);
      else el.setAttribute(k, v === true ? '' : v);
    }
    for (const kid of kids.flat()) {
      if (kid === null || kid === undefined || kid === false) continue;
      el.appendChild(kid instanceof Node ? kid : document.createTextNode(String(kid)));
    }
    return el;
  };
  FA.esc = s => String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  FA.toast = function (msg, ms = 2600) {
    const t = FA.h('div', { class: 'toast', html: msg });
    document.body.appendChild(t); setTimeout(() => t.remove(), ms);
  };
  FA.shuffle = function (arr) { const a = arr.slice(); for (let i = a.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1)); [a[i], a[j]] = [a[j], a[i]]; } return a; };
  FA.fmt = function (n) {
    if (!isFinite(n)) return '∞';
    if (Math.abs(n) >= 1e6) return (n / 1e6).toFixed(2) + 'M';
    if (Math.abs(n) >= 1e3) return (n / 1e3).toFixed(1) + 'K';
    return Math.round(n).toString();
  };
  FA.mountWidgets = function (root) {
    root.querySelectorAll('[data-widget]').forEach(el => {
      const name = el.getAttribute('data-widget');
      let cfg = {};
      try { cfg = JSON.parse(el.getAttribute('data-config') || '{}'); } catch (e) { console.warn('bad widget cfg', e); }
      const fn = FA.widgets[name];
      el.classList.add('widget');
      if (!fn) { el.textContent = 'Missing widget: ' + name; return; }
      try { fn(el, cfg); } catch (e) { console.error(e); el.textContent = 'Widget error: ' + e.message; }
    });
  };

  /* ---------- Progress store ---------- */
  const LS_KEY = 'flink-academy-progress-v1';
  const RANKS = [[0, 'Intern Streamer'], [300, 'Junior Operator'], [800, 'Stream Engineer'], [1500, 'Watermark Wrangler'], [2400, 'Checkpoint Champion'], [3400, 'Backpressure Buster'], [4500, 'Flink Architect']];
  FA.store = {
    state: null, serverAvailable: false, _t: null,
    blank() { return { levels: {}, xp: 0, challenges: {}, labSaved: null, updatedAt: new Date(0).toISOString() }; },
    async init() {
      let local = null;
      try { local = JSON.parse(localStorage.getItem(LS_KEY) || 'null'); } catch (e) { local = null; }
      this.state = Object.assign(this.blank(), local || {});
      if (location.protocol.startsWith('http')) {
        try {
          const r = await fetch('/api/progress', { cache: 'no-store' });
          if (r.ok) {
            this.serverAvailable = true;
            const body = await r.json();
            if (body && body.data && (!local || (body.data.updatedAt || '') > (local.updatedAt || ''))) {
              this.state = Object.assign(this.blank(), body.data);
              this._writeLocal();
            }
          }
        } catch (e) { this.serverAvailable = false; }
      }
    },
    _writeLocal() { try { localStorage.setItem(LS_KEY, JSON.stringify(this.state)); } catch (e) { /* storage may be unavailable */ } },
    save() {
      this.state.updatedAt = new Date().toISOString();
      this._writeLocal();
      if (!this.serverAvailable) return;
      clearTimeout(this._t);
      this._t = setTimeout(() => {
        fetch('/api/progress', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ data: this.state }) }).catch(() => {});
      }, 400);
      FA.renderSideStats && FA.renderSideStats();
    },
    lvl(id) { return this.state.levels[id] || (this.state.levels[id] = { read: false, best: 0, passed: false, attempts: 0 }); },
    isUnlocked(id) {
      const idx = FA.levels.findIndex(l => l.id === id);
      if (idx <= 0) return true;
      return !!this.lvl(FA.levels[idx - 1].id).passed;
    },
    addXp(n, why) { this.state.xp += n; this.save(); FA.toast(`+${n} XP${why ? ' · ' + FA.esc(why) : ''}`); },
    rank() {
      let r = RANKS[0], next = null;
      for (let i = 0; i < RANKS.length; i++) { if (this.state.xp >= RANKS[i][0]) { r = RANKS[i]; next = RANKS[i + 1] || null; } }
      return { name: r[1], floor: r[0], next: next ? next[0] : null, nextName: next ? next[1] : null };
    },
    async reset() {
      this.state = this.blank(); this._writeLocal();
      if (this.serverAvailable) { await fetch('/api/reset', { method: 'POST' }).catch(() => {}); }
      this.save();
    },
    exportJson() { return JSON.stringify(this.state, null, 2); },
    importJson(txt) { const d = JSON.parse(txt); this.state = Object.assign(this.blank(), d); this.save(); }
  };

  /* ---------- Router ---------- */
  FA.routes = [];
  FA.route = function (pattern, handler, navKey) { FA.routes.push({ re: new RegExp('^' + pattern + '$'), handler, navKey }); };
  FA.go = function (hash) { location.hash = hash; };
  FA.render = function () {
    const hash = (location.hash || '#/').slice(1) || '/';
    const main = document.getElementById('main');
    if (FA._cleanup) { try { FA._cleanup(); } catch (e) { } FA._cleanup = null; }
    for (const r of FA.routes) {
      const m = hash.match(r.re);
      if (m) {
        main.innerHTML = '';
        main.scrollTop = 0;
        document.querySelectorAll('.nav a').forEach(a => a.classList.toggle('active', a.dataset.nav === r.navKey));
        r.handler(main, ...m.slice(1));
        return;
      }
    }
    FA.go('#/');
  };
  FA.onCleanup = fn => { FA._cleanup = fn; };

  FA.renderSideStats = function () {
    const el = document.getElementById('side-stats'); if (!el || !FA.store.state) return;
    const r = FA.store.rank();
    const passed = FA.levels.filter(l => FA.store.lvl(l.id).passed).length;
    const pct = r.next ? Math.round(100 * (FA.store.state.xp - r.floor) / (r.next - r.floor)) : 100;
    el.innerHTML = `<div style="color:var(--text);font-weight:600">${FA.esc(r.name)}</div>
      <div>${FA.store.state.xp} XP${r.next ? ` · next: ${FA.esc(r.nextName)} @ ${r.next}` : ' · max rank'}</div>
      <div class="xpbar"><div style="width:${pct}%"></div></div>
      <div>Levels passed: ${passed} / ${FA.levels.length}</div>
      <div style="margin-top:4px">${FA.store.serverAvailable ? '💾 Saved to disk' : '🌐 Saved in browser only'}</div>`;
  };

  FA.start = async function () {
    await FA.store.init();
    FA.renderSideStats();
    window.addEventListener('hashchange', FA.render);
    FA.render();
  };
})(window.FA);
