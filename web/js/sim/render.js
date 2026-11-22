/* Renders a compiled job like the Flink Web UI job graph (SVG) + slot view */
(function (FA) {
  'use strict';
  const NS = 'http://www.w3.org/2000/svg';
  const S = (tag, attrs, text) => { const e = document.createElementNS(NS, tag); for (const k in attrs) e.setAttribute(k, attrs[k]); if (text !== undefined) e.textContent = text; return e; };

  // Flink colours: idle = blue, busy = red, backpressured = black; values in between are blended.
  function mix(idle, busy, bp) {
    const tot = idle + busy + bp || 1;
    const a = [61, 139, 253], b = [235, 64, 52], c = [20, 20, 20];
    const rgb = [0, 1, 2].map(i => Math.round((a[i] * idle + b[i] * busy + c[i] * bp) / tot));
    // light tint so text stays readable, like the Web UI
    return `rgb(${rgb.join(',')})`;
  }
  FA.flinkColor = mix;

  function wrap(text, max) {
    const parts = text.split(' -> ');
    const lines = [];
    parts.forEach((p, i) => {
      let s = (i > 0 ? '→ ' : '') + p;
      while (s.length > max) { lines.push(s.slice(0, max)); s = '  ' + s.slice(max); }
      lines.push(s);
    });
    return lines;
  }

  function renderGraph(container, res, opts) {
    opts = Object.assign({ selected: -1, onSelect: null, animate: true }, opts || {});
    container.innerHTML = '';
    const W = 230, GAP = 120, PADX = 30, PADY = 34;
    const nodes = res.vertices.map(v => {
      const lines = wrap(v.name, 30);
      const hgt = 30 + lines.length * 16 + 52;
      return { v, lines, h: hgt };
    });
    const maxH = Math.max(120, ...nodes.map(n => n.h));
    const width = PADX * 2 + nodes.length * W + Math.max(0, nodes.length - 1) * GAP;
    const height = maxH + PADY * 2 + 10;
    const svg = S('svg', { width, height, viewBox: `0 0 ${width} ${height}` });
    const defs = S('defs', {});
    const marker = S('marker', { id: 'arr', markerWidth: 10, markerHeight: 10, refX: 9, refY: 3, orient: 'auto' });
    marker.appendChild(S('path', { d: 'M0,0 L9,3 L0,6 z', fill: '#8c96a8' }));
    defs.appendChild(marker); svg.appendChild(defs);
    const style = S('style', {}, '@keyframes dash{to{stroke-dashoffset:-20}} .flow{animation:dash linear infinite}');
    svg.appendChild(style);

    nodes.forEach((n, i) => {
      n.x = PADX + i * (W + GAP); n.y = PADY + (maxH - n.h) / 2;
    });
    // edges
    nodes.forEach((n, i) => {
      if (i === 0) return;
      const a = nodes[i - 1], strat = n.v.inEdge ? n.v.inEdge.strat : '';
      const x1 = a.x + W, y1 = a.y + a.h / 2, x2 = n.x - 4, y2 = n.y + n.h / 2;
      const path = `M${x1},${y1} C${x1 + GAP / 2},${y1} ${x2 - GAP / 2},${y2} ${x2},${y2}`;
      svg.appendChild(S('path', { d: path, stroke: '#8c96a8', 'stroke-width': 1.6, fill: 'none', 'marker-end': 'url(#arr)' }));
      if (opts.animate && res.throughput > 0) {
        const speed = Math.max(0.25, 2.5 - Math.log10(Math.max(10, res.throughput)) / 3);
        const fl = S('path', { d: path, stroke: '#1890ff', 'stroke-width': 2.4, fill: 'none', 'stroke-dasharray': '3 17', class: 'flow' });
        fl.style.animationDuration = speed + 's';
        svg.appendChild(fl);
      }
      const mx = (x1 + x2) / 2, my = (y1 + y2) / 2 - 8;
      const lw = strat.length * 7.6 + 14;
      svg.appendChild(S('rect', { x: mx - lw / 2, y: my - 12, width: lw, height: 20, rx: 4, fill: '#fff', stroke: '#d9d9d9' }));
      svg.appendChild(S('text', { x: mx, y: my + 3, 'text-anchor': 'middle', 'font-size': 11.5, 'font-weight': 600, fill: strat === 'FORWARD' ? '#389e0d' : strat === 'HASH' ? '#d46b08' : strat === 'BROADCAST' ? '#c41d7f' : '#0958d9', 'font-family': 'Consolas,monospace' }, strat));
    });
    // nodes
    nodes.forEach((n, i) => {
      const v = n.v;
      const g = S('g', { class: 'fl-node' + (opts.selected === v.id ? ' sel' : ''), transform: `translate(${n.x},${n.y})` });
      const fill = res.status === 'RUNNING' ? mix(v.idleMax, v.busyMax, v.bpMax) : '#bfbfbf';
      g.appendChild(S('rect', { class: 'box', width: W, height: n.h, rx: 6, fill: '#ffffff' }));
      g.appendChild(S('rect', { width: W, height: 6, rx: 3, fill }));
      g.appendChild(S('rect', { x: 0, y: n.h - 46, width: W, height: 46, fill, opacity: 0.92 }));
      let y = 24;
      n.lines.forEach(l => { g.appendChild(S('text', { x: 10, y, 'font-size': 12.5, 'font-weight': 600 }, l)); y += 16; });
      g.appendChild(S('text', { x: 10, y: y + 4, 'font-size': 12, style: 'fill:#595959' }, `Parallelism: ${v.p}`));
      const tc = '#fff';
      const fmtP = x => res.status === 'RUNNING' ? Math.round(x * 100) + '%' : 'n/a';
      g.appendChild(S('text', { x: 10, y: n.h - 27, 'font-size': 11.5, style: `fill:${tc}` }, `Backpressured (max): ${fmtP(v.bpMax)}`));
      g.appendChild(S('text', { x: 10, y: n.h - 10, 'font-size': 11.5, style: `fill:${tc}` }, `Busy (max): ${fmtP(v.busyMax)}`));
      if (res.bottleneck === v.id && res.limited) g.appendChild(S('text', { x: W - 8, y: 20, 'text-anchor': 'end', 'font-size': 15 }, '🔥'));
      if (opts.onSelect) g.addEventListener('click', () => opts.onSelect(v.id));
      svg.appendChild(g);
    });
    container.appendChild(svg);
  }

  function renderSlots(container, res) {
    container.innerHTML = '';
    const h = FA.h;
    const tms = res.cl.tms;
    const wrapEl = h('div', { class: 'row', style: { alignItems: 'stretch' } });
    const colors = ['#5b8ff9', '#5ad8a6', '#f6bd16', '#e8684a', '#6dc8ec', '#9270ca', '#ff9d4d', '#269a99', '#ff99c3'];
    for (let t = 0; t < tms; t++) {
      const tmBox = h('div', { class: 'card', style: { margin: 0, minWidth: '220px', flex: '1' } },
        h('div', { style: { fontWeight: 600, marginBottom: '6px' } }, `TaskManager ${t + 1}`),
        h('div', { class: 'muted', style: { fontSize: '12px', marginBottom: '8px' } }, `${res.cl.coresPerTm} CPU cores · CPU demand ${res.cpuDemand[t] ? res.cpuDemand[t].toFixed(2) : '0.00'} cores ${res.cpuDemand[t] > res.cl.coresPerTm ? '⚠️ oversubscribed' : ''}`));
      for (let s = 0; s < res.cl.slotsPerTm; s++) {
        const sl = res.slots.find(x => x.tm === t && x.slot === s);
        const slotEl = h('div', { style: { border: '1px dashed #44527a', borderRadius: '8px', padding: '6px', marginBottom: '6px', minHeight: '38px' } },
          h('div', { class: 'muted', style: { fontSize: '11px' } }, `Slot ${s + 1}${sl ? ' · group "' + sl.group + '"' : ' · free'}`));
        if (sl) sl.tasks.forEach(tk => {
          const v = res.vertices[tk.v];
          slotEl.appendChild(h('div', { style: { background: colors[tk.v % colors.length], color: '#111', borderRadius: '5px', padding: '2px 6px', margin: '3px 0', fontSize: '12px' }, title: v.name },
            `${shortName(v.name)} (${tk.s + 1}/${v.p})`));
        });
        tmBox.appendChild(slotEl);
      }
      wrapEl.appendChild(tmBox);
    }
    const unplaced = Math.max(0, res.slotsRequired - res.slotsAvailable);
    container.appendChild(h('div', { style: { marginBottom: '10px' } },
      h('b', null, `Slots required: ${res.slotsRequired}`), ` · available: ${res.slotsAvailable}`,
      unplaced ? h('span', { class: 'pill bad', style: { marginLeft: '8px' } }, `${unplaced} slot(s) missing → job cannot be scheduled`) : h('span', { class: 'pill ok', style: { marginLeft: '8px' } }, 'fits'),
      h('div', { class: 'muted', style: { fontSize: '12.5px', marginTop: '4px' } },
        'With slot sharing, slots needed = the highest parallelism in each slot sharing group (summed over groups). One slot can hold one subtask of EVERY task in its group, i.e. a whole pipeline slice. Operators without an explicit group inherit their input\'s group. (This lab spreads slots evenly over TaskManagers, like taskmanager.load-balance.mode: SLOTS.)')));
    container.appendChild(wrapEl);
  }
  function shortName(n) { const p = n.split(' -> '); return p.length > 2 ? `${p[0]} → … → ${p[p.length - 1]}` : p.join(' → '); }

  FA.SimView = { renderGraph, renderSlots, shortName };
})(window.FA);
