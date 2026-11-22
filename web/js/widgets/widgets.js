/* Interactive lesson widgets */
(function (FA) {
  'use strict';
  const h = FA.h;
  const KEYCOL = { A: '#ff7a8a', B: '#6cc3ff', C: '#ffd666', D: '#95de64', E: '#d3adf7', F: '#ffa940' };

  function seeded(seed) { let s = seed >>> 0; return () => { s = (s * 1664525 + 1013904223) >>> 0; return s / 4294967296; }; }

  /* ===== 1. Ordering across multiple workers ===== */
  FA.registerWidget('ordering', function (el, cfg) {
    const st = Object.assign({ up: 2, down: 2, strat: 'hash', pinned: true, seed: 7 }, cfg);
    el.appendChild(h('div', { class: 'wtitle' }, 'Ordering simulator: who receives what, in which order?'));
    const ctr = h('div', { class: 'row', style: { marginBottom: '10px' } });
    const out = h('div');
    el.append(ctr, out);
    const mk = (label, key, opts) => {
      const s = h('select', null, opts.map(([v, t]) => h('option', { value: v, selected: String(st[key]) === String(v) }, t)));
      s.addEventListener('change', () => { st[key] = key === 'pinned' ? s.value === 'true' : isNaN(+s.value) ? s.value : +s.value; run(); });
      ctr.appendChild(h('label', { class: 'f' }, label, s));
    };
    mk('Exchange / partitioner', 'strat', [['forward', 'FORWARD (one-to-one)'], ['hash', 'HASH (keyBy)'], ['rebalance', 'REBALANCE (round-robin)'], ['rescale', 'RESCALE'], ['broadcast', 'BROADCAST'], ['global', 'GLOBAL']]);
    mk('Upstream subtasks', 'up', [[1, '1'], [2, '2'], [3, '3']]);
    mk('Downstream subtasks', 'down', [[1, '1'], [2, '2'], [3, '3'], [4, '4']]);
    mk('How keys enter the upstream', 'pinned', [['true', 'Each key comes from ONE upstream subtask (e.g. Kafka partitioned by key)'], ['false', 'A key is spread over ALL upstream subtasks (e.g. round-robin producer)']]);
    ctr.appendChild(h('button', { class: 'btn sm', style: { alignSelf: 'end' }, onclick: () => { st.seed++; run(); } }, '🎲 New network timing'));

    function run() {
      out.innerHTML = '';
      if (st.strat === 'forward' && st.up !== st.down) {
        out.appendChild(h('div', { class: 'callout warn' }, h('b', null, 'Not allowed'), `FORWARD requires equal parallelism (${st.up} ≠ ${st.down}). Flink throws "Forward partitioning does not allow change of parallelism". Without an explicit call, Flink would use REBALANCE here.`));
        return;
      }
      const rnd = seeded(st.seed * 977 + st.up * 31 + st.down);
      const keys = ['A', 'B', 'C', 'D'];
      // generate: 8 ticks; each upstream subtask emits one event per tick
      const keySeq = {}; const events = [];
      for (let t = 0; t < 8; t++) for (let u = 0; u < st.up; u++) {
        let key;
        if (st.pinned) { const mine = keys.filter((k, i) => i % st.up === u); key = mine[Math.floor(rnd() * mine.length)]; }
        else key = keys[Math.floor(rnd() * keys.length)];
        keySeq[key] = (keySeq[key] || 0) + 1;
        events.push({ key, n: keySeq[key], u, t: t + u * 0.01 });
      }
      const hashOf = { A: 1, B: 0, C: 3, D: 2 };
      const lat = {}; const lastArr = {}; const rr = {};
      const inbox = Array.from({ length: st.down }, () => []);
      events.forEach(e => {
        let targets;
        switch (st.strat) {
          case 'forward': targets = [e.u]; break;
          case 'hash': targets = [hashOf[e.key] % st.down]; break;
          case 'rebalance': rr[e.u] = ((rr[e.u] === undefined ? e.u : rr[e.u]) + 1) % st.down; targets = [rr[e.u]]; break;
          case 'rescale': {
            const pool = [];
            if (st.down >= st.up) { for (let d = 0; d < st.down; d++) if (Math.floor(d * st.up / st.down) === e.u) pool.push(d); }
            else pool.push(Math.floor(e.u * st.down / st.up));
            rr[e.u] = ((rr[e.u] || 0) + 1) % pool.length; targets = [pool[rr[e.u]]]; break;
          }
          case 'broadcast': targets = [...Array(st.down).keys()]; break;
          case 'global': targets = [0]; break;
        }
        targets.forEach(d => {
          const ch = e.u + '>' + d;
          if (lat[ch] === undefined) lat[ch] = 0.3 + rnd() * 3.2; // each channel has its own speed
          const arrival = Math.max((lastArr[ch] || 0) + 0.01, e.t + lat[ch] + rnd() * 0.8); // FIFO per channel
          lastArr[ch] = arrival;
          inbox[d].push(Object.assign({ arrival, d }, e));
        });
      });
      let keyViolations = 0, channelViolations = 0;
      const grid = h('div', { style: { display: 'grid', gap: '8px' } });
      inbox.forEach((list, d) => {
        list.sort((a, b) => a.arrival - b.arrival);
        const lastSeen = {}; const lastFromU = {};
        const row = h('div', { style: { display: 'flex', gap: '4px', flexWrap: 'wrap', alignItems: 'center' } },
          h('span', { class: 'pill info', style: { minWidth: '118px', textAlign: 'center' } }, `downstream #${d + 1} receives:`));
        if (!list.length) row.appendChild(h('span', { class: 'muted' }, '(nothing: idle subtask)'));
        list.forEach(e => {
          const bad = lastSeen[e.key] !== undefined && e.n < lastSeen[e.key];
          if (bad) keyViolations++;
          if (lastFromU[e.u] !== undefined && e.t < lastFromU[e.u]) channelViolations++;
          lastSeen[e.key] = Math.max(lastSeen[e.key] || 0, e.n); lastFromU[e.u] = e.t;
          row.appendChild(h('span', {
            title: `key ${e.key}, #${e.n} of that key, sent by upstream #${e.u + 1}`,
            style: { background: KEYCOL[e.key], color: '#111', borderRadius: '6px', padding: '1px 6px', fontSize: '12px', fontFamily: 'Consolas,monospace', outline: bad ? '3px solid #ff2d2d' : 'none', outlineOffset: '1px' }
          }, `${e.key}${e.n}`, h('sub', { style: { opacity: .6 } }, `u${e.u + 1}`)));
        });
        grid.appendChild(row);
      });
      // upstream view
      const upView = h('div', { style: { display: 'grid', gap: '4px', marginBottom: '10px' } });
      for (let u = 0; u < st.up; u++) upView.appendChild(h('div', { style: { display: 'flex', gap: '4px', flexWrap: 'wrap', alignItems: 'center' } },
        h('span', { class: 'pill', style: { minWidth: '118px', textAlign: 'center' } }, `upstream #${u + 1} sends:`),
        events.filter(e => e.u === u).map(e => h('span', { style: { background: KEYCOL[e.key], color: '#111', borderRadius: '6px', padding: '1px 6px', fontSize: '12px', fontFamily: 'Consolas,monospace' } }, `${e.key}${e.n}`))));
      out.append(upView, h('div', { style: { textAlign: 'center', color: 'var(--muted)', margin: '2px 0 8px' } }, `⬇ ${st.strat.toUpperCase()} exchange, ${st.up}×${st.down} subtasks, each channel has its own network delay ⬇`), grid);

      let broadcastDiff = false;
      if (st.strat === 'broadcast' && st.down > 1) {
        const sig = inbox.map(l => l.map(e => e.key + e.n).join(','));
        broadcastDiff = sig.some(s => s !== sig[0]);
      }
      const verdict = [];
      verdict.push(`✅ FIFO per channel: every (upstream → downstream) pair delivered records in send order (${channelViolations} violations, and there can never be any).`);
      verdict.push(keyViolations ? `❌ Per-key order broken ${keyViolations} time(s) (red outline): a later record of a key arrived before an earlier one, because they travelled on DIFFERENT channels.` :
        `✅ Per-key order preserved in this run${st.pinned || st.strat === 'forward' || st.up === 1 ? '' : ' (by luck. Press 🎲 a few times)'}.`);
      if (st.strat === 'broadcast') verdict.push(broadcastDiff ? '⚠️ BROADCAST: the downstream subtasks saw DIFFERENT interleavings of the same records. Never assume broadcast elements arrive in the same order at every task.' : 'ℹ️ BROADCAST: every subtask got every record. Press 🎲: with several senders the interleaving can differ per receiver.');
      if (st.strat === 'global') verdict.push('⚠️ GLOBAL: everything goes to subtask #1, so the other subtasks idle. This is a classic bottleneck.');
      if (st.strat === 'rebalance' || st.strat === 'rescale') verdict.push('ℹ️ Round-robin ignores keys, so records of one key end up on different subtasks. Never use it before keyed logic.');
      if (st.strat === 'hash' && !st.pinned && st.up > 1) verdict.push('ℹ️ HASH puts every record of a key on the SAME downstream subtask, but records of that key come from several senders, so their relative order depends on network timing.');
      if (st.strat === 'hash' && (st.pinned || st.up === 1)) verdict.push('ℹ️ Each key travels on exactly ONE channel (one sender → one receiver), so its order is preserved. This is why "Kafka partition by key + keyBy on the same key" keeps per-key order.');
      if (st.strat === 'forward') verdict.push('ℹ️ FORWARD: subtask i only talks to subtask i, so the order of each partition is fully preserved. FORWARD is also what makes chaining possible.');
      out.appendChild(h('div', { class: 'callout tip', style: { marginTop: '12px' } }, verdict.map(v => h('div', null, v))));
    }
    run();
  });

  /* ===== 2. Key groups (real Flink hashing) ===== */
  function javaHash(s) { let x = 0; for (let i = 0; i < s.length; i++) x = (Math.imul(31, x) + s.charCodeAt(i)) | 0; return x; }
  function murmur(code) {
    code = Math.imul(code, 0xcc9e2d51); code = (code << 15) | (code >>> 17);
    code = Math.imul(code, 0x1b873593); code = (code << 13) | (code >>> 19);
    code = (Math.imul(code, 5) + 0xe6546b64) | 0; code ^= 4;
    code ^= code >>> 16; code = Math.imul(code, 0x85ebca6b); code ^= code >>> 13; code = Math.imul(code, 0xc2b2ae35); code ^= code >>> 16;
    if (code >= 0) return code; if (code !== -2147483648) return -code; return 0;
  }
  FA.keyGroupOf = (key, maxP) => murmur(javaHash(key)) % maxP;
  FA.registerWidget('keygroups', function (el) {
    const st = { keys: 'customer-1, customer-2, customer-3, customer-42, store-ny, store-tx', maxP: 128, p: 4 };
    el.appendChild(h('div', { class: 'wtitle' }, 'Key → key group → subtask (uses Flink\'s real murmur hash of String.hashCode)'));
    const keysIn = h('input', { value: st.keys, style: { width: '100%' } });
    const maxIn = h('input', { type: 'number', value: st.maxP, min: 1, max: 32768 });
    const pIn = h('input', { type: 'number', value: st.p, min: 1 });
    const out = h('div', { style: { marginTop: '10px' } });
    el.append(h('label', { class: 'f' }, 'Keys (comma separated)', keysIn), h('div', { class: 'row', style: { marginTop: '8px' } }, h('label', { class: 'f' }, 'Max parallelism (= number of key groups)', maxIn), h('label', { class: 'f' }, 'Parallelism', pIn)), out);
    function run() {
      const maxP = Math.max(1, +maxIn.value | 0), p = Math.max(1, +pIn.value | 0);
      out.innerHTML = '';
      if (p > maxP) { out.appendChild(h('div', { class: 'callout warn' }, 'Parallelism cannot exceed max parallelism: there would be subtasks without key groups. Flink rejects this.')); return; }
      const tbl = h('table', { class: 't' }, h('tr', null, ['Key', 'key group = murmur(hashCode) % maxP', 'subtask = kg × p / maxP'].map(x => h('th', null, x))));
      keysIn.value.split(',').map(s => s.trim()).filter(Boolean).forEach(k => {
        const kg = FA.keyGroupOf(k, maxP), sub = Math.floor(kg * p / maxP);
        tbl.appendChild(h('tr', null, h('td', null, h('code', null, k)), h('td', null, kg), h('td', null, `#${sub + 1}`)));
      });
      out.appendChild(tbl);
      const ranges = h('div', { style: { display: 'flex', height: '26px', borderRadius: '6px', overflow: 'hidden', marginTop: '10px', border: '1px solid var(--line)' } });
      const cols = ['#5b8ff9', '#5ad8a6', '#f6bd16', '#e8684a', '#6dc8ec', '#9270ca', '#ff9d4d', '#269a99'];
      const lines = [];
      for (let i = 0; i < p; i++) {
        const start = Math.floor((i * maxP + p - 1) / p), end = Math.floor(((i + 1) * maxP - 1) / p);
        ranges.appendChild(h('div', { style: { flex: (end - start + 1), background: cols[i % cols.length], color: '#111', fontSize: '11px', display: 'grid', placeItems: 'center' }, title: `subtask ${i + 1}: key groups ${start}–${end}` }, p <= 16 ? `#${i + 1}` : ''));
        lines.push(`#${i + 1}: KG ${start}–${end} (${end - start + 1})`);
      }
      out.append(ranges, h('div', { class: 'muted', style: { fontSize: '12px', marginTop: '6px' } }, lines.join(' · ')),
        h('div', { class: 'callout', style: { marginTop: '8px' } }, `Key groups are the atomic unit of keyed state. Rescaling only moves WHOLE key groups between subtasks. Since ${maxP} % ${p} ${maxP % p === 0 ? '= 0, every subtask gets the same number of key groups.' : '≠ 0, some subtasks get one more key group than others (mild built-in skew).'} Changing max parallelism after the first deployment breaks state compatibility.`));
    }
    [keysIn, maxIn, pIn].forEach(i => i.addEventListener('input', run)); run();
  });

  /* ===== 3. Watermarks & windows ===== */
  FA.registerWidget('watermark', function (el) {
    const arrivals = [[1.0, 'A'], [3.2, 'B'], [2.1, 'C'], [5.5, 'D'], [4.4, 'E'], [7.9, 'F'], [3.9, 'G'], [10.3, 'H'], [8.8, 'I'], [6.1, 'J'], [12.0, 'K'], [15.2, 'L'], [9.5, 'M']];
    el.appendChild(h('div', { class: 'wtitle' }, 'Watermarks: bounded out-of-orderness + 5 s tumbling event-time windows'));
    const ooo = h('input', { type: 'range', min: 0, max: 6, step: 0.5, value: 2 });
    const lateIn = h('input', { type: 'range', min: 0, max: 4, step: 1, value: 0 });
    const lbl = h('span'); const lbl2 = h('span');
    const out = h('div');
    el.append(h('div', { class: 'row' }, h('label', { class: 'f' }, h('span', null, 'forBoundedOutOfOrderness = ', lbl), ooo), h('label', { class: 'f' }, h('span', null, 'allowedLateness = ', lbl2), lateIn)), out);
    function run() {
      const b = +ooo.value, al = +lateIn.value; lbl.textContent = b + ' s'; lbl2.textContent = al + ' s';
      let maxTs = -Infinity, wm = -Infinity; const fired = {}; const purged = {}; const counts = {};
      const tbl = h('table', { class: 't' }, h('tr', null, ['#', 'Event', 'Event time', 'Window', 'Watermark after', 'What happens'].map(x => h('th', null, x))));
      arrivals.forEach(([ts, id], i) => {
        const w = Math.floor(ts / 5) * 5, wEnd = w + 5;
        let action;
        if (purged[w]) action = h('span', { class: 'pill bad' }, 'LATE → dropped (or side output)');
        else if (fired[w]) { counts[w] = (counts[w] || 0) + 1; action = h('span', { class: 'pill warn' }, `late but within allowedLateness → window [${w},${wEnd}) fires AGAIN (count=${counts[w]})`); }
        else { counts[w] = (counts[w] || 0) + 1; action = h('span', { class: 'pill' }, `added to [${w},${wEnd})`); }
        maxTs = Math.max(maxTs, ts); wm = maxTs - b - 0.001;
        const fires = [];
        Object.keys(counts).map(Number).sort((a, c) => a - c).forEach(ws => {
          if (!fired[ws] && wm >= ws + 5 - 0.001) { fired[ws] = true; fires.push(`🔔 window [${ws},${ws + 5}) FIRES (count=${counts[ws]})`); }
          if (fired[ws] && !purged[ws] && wm >= ws + 5 - 0.001 + al) purged[ws] = true;
        });
        tbl.appendChild(h('tr', null, h('td', null, i + 1), h('td', null, id), h('td', null, ts.toFixed(1) + ' s'), h('td', null, `[${w},${wEnd})`), h('td', null, wm.toFixed(3) + ' s'), h('td', null, action, fires.length ? h('div', { style: { color: 'var(--ok)', marginTop: '3px' } }, fires.join(' · ')) : null)));
      });
      out.innerHTML = '';
      out.append(tbl, h('div', { class: 'muted', style: { fontSize: '12.5px', marginTop: '6px' } }, 'Watermark = max event time seen − out-of-orderness − 1 ms. A window [start, end) fires when watermark ≥ end − 1 ms. Bigger bounds give fewer late events but more latency. Try 0 s versus 4 s.'));
    }
    ooo.addEventListener('input', run); lateIn.addEventListener('input', run); run();
  });

  /* ===== 4. Checkpoint barriers step-through ===== */
  FA.registerWidget('barriers', function (el) {
    const aligned = [
      ['Two input channels feed an operator. The JobManager triggered checkpoint 7; the sources injected barrier ‖7 into their streams.', ['a3', 'a2', '‖7', 'a1'], ['b4', 'b3', 'b2', '‖7', 'b1'], 'state: sum=10', []],
      ['a1 and b1 (before the barrier) are processed normally.', ['a3', 'a2', '‖7'], ['b4', 'b3', 'b2', '‖7'], 'state: sum=12', ['a1', 'b1']],
      ['Barrier ‖7 arrives on channel A first. In ALIGNED mode channel A is now BLOCKED. Records after the barrier (a2, a3) belong to checkpoint 8 and must wait.', ['a3', 'a2', '🚫'], ['b4', 'b3', 'b2', '‖7'], 'state: sum=12', ['a1', 'b1']],
      ['Channel B is still before its barrier, so b2 is processed. This waiting is "alignment time". With backpressure it can take a long time.', ['a3', 'a2', '🚫'], ['b4', 'b3', '‖7'], 'state: sum=14', ['a1', 'b1', 'b2']],
      ['‖7 arrives on B. All inputs aligned → the operator SNAPSHOTS its state (sum=14) asynchronously, then emits ‖7 downstream.', ['a3', 'a2'], ['b4', 'b3'], 'state: sum=14 📸 snapshot #7', ['a1', 'b1', 'b2', '‖7']],
      ['Channel A is unblocked and processing continues. The snapshot contains exactly the effect of the records before the barriers. That is exactly-once state.', [], ['b4'], 'state: sum=21', ['a1', 'b1', 'b2', '‖7', 'a2', 'a3', 'b3']]
    ];
    const unaligned = [
      ['UNALIGNED mode: same start. Barrier ‖7 is in both channels, behind buffered data.', ['a3', 'a2', '‖7', 'a1'], ['b4', 'b3', 'b2', '‖7', 'b1'], 'state: sum=10', []],
      ['As soon as the FIRST barrier reaches the input buffers, the operator reacts immediately: it does not wait for alignment.', ['a3', 'a2', '‖7', 'a1'], ['b4', 'b3', 'b2', '‖7', 'b1'], 'state: sum=10', []],
      ['The barrier OVERTAKES the in-flight records. The operator snapshots its state AND the in-flight buffers (a1, b1, b2) and forwards ‖7 at once.', ['a3', 'a2'], ['b4', 'b3'], 'state: sum=10 📸 + in-flight [a1,b1,b2]', ['‖7']],
      ['Processing continues without any blocking. Checkpoint time no longer depends on backpressure, but checkpoints are bigger and need more I/O.', [], ['b4'], 'state: sum=21', ['‖7', 'a1', 'b1', 'b2', 'a2', 'a3', 'b3']]
    ];
    let mode = 'aligned', i = 0;
    el.appendChild(h('div', { class: 'wtitle' }, 'Checkpoint barriers: aligned vs unaligned'));
    const bar = h('div', { class: 'row', style: { marginBottom: '10px' } });
    const view = h('div');
    el.append(bar, view);
    const chip = t => h('span', { style: { background: t.startsWith('‖') ? '#fadb14' : t === '🚫' ? '#ff4d4f' : t.startsWith('a') ? '#6cc3ff' : '#ff7a8a', color: '#111', borderRadius: '5px', padding: '2px 7px', fontFamily: 'Consolas,monospace', fontSize: '12.5px', fontWeight: t.startsWith('‖') ? 700 : 400 } }, t);
    function draw() {
      const steps = mode === 'aligned' ? aligned : unaligned, s = steps[i];
      bar.innerHTML = '';
      bar.append(
        h('button', { class: 'btn sm' + (mode === 'aligned' ? ' primary' : ''), onclick: () => { mode = 'aligned'; i = 0; draw(); } }, 'Aligned'),
        h('button', { class: 'btn sm' + (mode === 'unaligned' ? ' primary' : ''), onclick: () => { mode = 'unaligned'; i = 0; draw(); } }, 'Unaligned'),
        h('span', { class: 'muted' }, `step ${i + 1}/${steps.length}`),
        h('button', { class: 'btn sm', disabled: i === 0, onclick: () => { i--; draw(); } }, '◀ Prev'),
        h('button', { class: 'btn sm', disabled: i === steps.length - 1, onclick: () => { i++; draw(); } }, 'Next ▶'));
      view.innerHTML = '';
      const lane = (name, arr) => h('div', { class: 'row', style: { gap: '5px', margin: '4px 0' } }, h('span', { class: 'pill', style: { minWidth: '90px' } }, name), arr.map(chip), h('span', { class: 'muted' }, '→'));
      view.append(
        h('div', { style: { display: 'grid', gridTemplateColumns: '1fr auto 1fr', gap: '14px', alignItems: 'center' } },
          h('div', null, lane('channel A', s[1]), lane('channel B', s[2])),
          h('div', { style: { border: '2px solid var(--accent2)', borderRadius: '10px', padding: '10px 14px', textAlign: 'center' } }, h('b', null, 'Operator'), h('div', { style: { fontSize: '12px' } }, s[3])),
          h('div', null, h('div', { class: 'muted', style: { fontSize: '12px' } }, 'emitted downstream:'), h('div', { class: 'row', style: { gap: '4px' } }, s[4].map(chip)))),
        h('div', { class: 'callout', style: { marginTop: '10px' } }, s[0]));
    }
    draw();
  });

  /* ===== 5. TaskManager memory model ===== */
  FA.tmMemory = function (o) {
    const P = o.process;
    const overhead = Math.min(o.ohMax, Math.max(o.ohMin, P * o.ohFrac));
    const flink = P - o.metaspace - overhead;
    const network = Math.min(o.netMax, Math.max(o.netMin, flink * o.netFrac));
    const managed = flink * o.managedFrac;
    const taskHeap = flink - o.fwHeap - o.fwOff - o.taskOff - network - managed;
    return { overhead, flink, network, managed, taskHeap, metaspace: o.metaspace, fwHeap: o.fwHeap, fwOff: o.fwOff, taskOff: o.taskOff, heap: o.fwHeap + taskHeap, direct: o.fwOff + o.taskOff + network };
  };
  FA.registerWidget('memory', function (el) {
    const o = { process: 1728, managedFrac: 0.4, netFrac: 0.1, netMin: 64, netMax: 1024, metaspace: 256, ohFrac: 0.1, ohMin: 192, ohMax: 1024, fwHeap: 128, fwOff: 128, taskOff: 0 };
    el.appendChild(h('div', { class: 'wtitle' }, 'TaskManager memory calculator (Flink unified memory model, default values)'));
    const ctrls = h('div', { class: 'row' }); const out = h('div', { style: { marginTop: '12px' } });
    el.append(ctrls, out);
    const field = (label, key, step) => { const i = h('input', { type: 'number', value: o[key], step: step || 1 }); i.addEventListener('input', () => { o[key] = +i.value; run(); }); ctrls.appendChild(h('label', { class: 'f' }, label, i)); };
    field('taskmanager.memory.process.size (MB)', 'process', 64);
    field('managed.fraction', 'managedFrac', 0.05);
    field('network.fraction', 'netFrac', 0.05);
    field('jvm-metaspace.size (MB)', 'metaspace', 32);
    field('task.off-heap.size (MB)', 'taskOff', 32);
    function run() {
      const m = FA.tmMemory(o);
      out.innerHTML = '';
      if (m.taskHeap <= 0) { out.appendChild(h('div', { class: 'callout warn' }, h('b', null, 'IllegalConfigurationException'), `Derived task heap would be ${m.taskHeap.toFixed(0)} MB. The process is too small for these fractions. Increase process.size or lower the managed/network fractions.`)); return; }
      const parts = [['Framework heap', m.fwHeap, '#597ef7'], ['Task heap', m.taskHeap, '#2f54eb'], ['Managed (RocksDB, batch)', m.managed, '#13c2c2'], ['Network buffers', m.network, '#fa8c16'], ['Framework off-heap', m.fwOff, '#9254de'], ['Task off-heap', m.taskOff, '#b37feb'], ['JVM metaspace', m.metaspace, '#8c8c8c'], ['JVM overhead', m.overhead, '#595959']].filter(p => p[1] > 0);
      out.appendChild(h('div', { class: 'membar' }, parts.map(([n, v, c]) => h('div', { style: { flex: v, background: c }, title: `${n}: ${v.toFixed(0)} MB` }, v / o.process > 0.07 ? `${n.split(' (')[0]} ${v.toFixed(0)}` : ''))));
      const tbl = h('table', { class: 't', style: { marginTop: '10px' } }, h('tr', null, h('th', null, 'Component'), h('th', null, 'MB'), h('th', null, 'How it is derived')));
      const rows = [['Total process memory', o.process, 'what the container/pod must provide'], ['JVM overhead', m.overhead, `${o.ohFrac} × process, clamped to [${o.ohMin}, ${o.ohMax}]`], ['JVM metaspace', m.metaspace, 'fixed size'], ['= Total Flink memory', m.flink, 'process − metaspace − overhead'],
        ['Network memory', m.network, `${o.netFrac} × Flink memory, clamped to [${o.netMin}, ${o.netMax}]`], ['Managed memory', m.managed, `${o.managedFrac} × Flink memory (RocksDB & batch operators)`], ['Framework heap / off-heap', `${m.fwHeap} / ${m.fwOff}`, 'fixed defaults (128 MB each)'], ['Task heap (your objects)', m.taskHeap, 'whatever is left'],
        ['→ JVM -Xmx / -Xms', m.heap, 'framework heap + task heap'], ['→ -XX:MaxDirectMemorySize', m.direct, 'framework off-heap + task off-heap + network']];
      rows.forEach(r => tbl.appendChild(h('tr', null, h('td', null, r[0]), h('td', null, typeof r[1] === 'number' ? r[1].toFixed(0) : r[1]), h('td', { class: 'muted' }, r[2]))));
      out.appendChild(tbl);
    }
    run();
  });

  /* ===== 6. Mini job graph (embedded lab) ===== */
  FA.registerWidget('jobgraph', function (el, cfg) {
    let uid = 1000;
    const model = JSON.parse(JSON.stringify(cfg.model));
    model.ops.forEach(o => { o.id = uid++; const K = FA.Sim.KINDS[o.kind]; o.name = o.name || K.defName; o.cost = o.cost !== undefined ? o.cost : K.cost; o.sel = o.sel !== undefined ? o.sel : K.sel; o.part = o.part || 'auto'; o.group = o.group || ''; o.chain = o.chain || 'auto'; });
    el.appendChild(h('div', { class: 'wtitle' }, cfg.title || 'Job graph'));
    const ctr = h('div', { class: 'row', style: { marginBottom: '8px' } });
    const g = h('div', { class: 'graph-wrap' });
    const note = h('div', { class: 'muted', style: { fontSize: '12.5px', marginTop: '6px' } });
    el.append(ctr, g, note);
    function draw() {
      const res = FA.Sim.compile(model);
      FA.SimView.renderGraph(g, res, { animate: true });
      note.textContent = res.errors.length ? '⚠️ ' + res.errors[0] : `${res.vertices.length} task(s) · slots needed: ${res.slotsRequired} · throughput ${FA.fmt(res.throughput)} rec/s of ${FA.fmt(res.sourceRate)}${res.limited ? ' (backpressured!)' : ''}`;
    }
    (cfg.controls || []).forEach(c => {
      const target = c.op === 'cluster' ? model.cluster : model.ops[c.op];
      let input;
      if (c.type === 'select') { input = h('select', null, c.options.map(x => h('option', { value: x[0], selected: target[c.key] === x[0] }, x[1]))); input.addEventListener('change', () => { target[c.key] = input.value; draw(); }); }
      else if (c.type === 'check') { input = h('input', { type: 'checkbox', checked: !!target[c.key] }); input.addEventListener('change', () => { target[c.key] = input.checked; draw(); }); }
      else { input = h('input', { type: 'number', value: target[c.key], min: c.min || 1, max: c.max || 64, step: c.step || 1 }); input.addEventListener('input', () => { target[c.key] = +input.value; draw(); }); }
      ctr.appendChild(h('label', { class: 'f' }, c.label, input));
    });
    draw();
  });

  /* ===== 7. Slot calculator ===== */
  FA.registerWidget('slots', function (el) {
    const st = { tms: 2, slots: 3, pSrc: 2, pMap: 6, pWin: 6, pSink: 2, isolate: false };
    el.appendChild(h('div', { class: 'wtitle' }, 'Slot sharing playground'));
    const ctr = h('div', { class: 'row', style: { marginBottom: '10px' } }); const out = h('div');
    el.append(ctr, out);
    [['TaskManagers', 'tms'], ['Slots / TM', 'slots'], ['Source p', 'pSrc'], ['Map p', 'pMap'], ['Window p', 'pWin'], ['Sink p', 'pSink']].forEach(([l, k]) => {
      const i = h('input', { type: 'number', value: st[k], min: 1, max: 12 }); i.addEventListener('input', () => { st[k] = Math.max(1, +i.value | 0); run(); });
      ctr.appendChild(h('label', { class: 'f' }, l, i));
    });
    const iso = h('input', { type: 'checkbox' }); iso.addEventListener('change', () => { st.isolate = iso.checked; run(); });
    ctr.appendChild(h('label', { class: 'row', style: { gap: '6px', alignSelf: 'end' } }, iso, 'Window in its own slot sharing group'));
    function run() {
      const res = FA.Sim.compile({ cluster: { tms: st.tms, slotsPerTm: st.slots, coresPerTm: 8, sourceRate: 1000 }, ops: [
        { kind: 'source', p: st.pSrc, name: 'Kafka', cost: 2, sel: 1 }, { kind: 'map', p: st.pMap, name: 'Map', cost: 3, sel: 1, part: 'auto' },
        { kind: 'keyby', name: 'keyBy', skew: 0 }, { kind: 'window', p: st.pWin, name: 'Window', cost: 5, sel: 0.1, group: st.isolate ? 'windows' : '' }, { kind: 'sink', p: st.pSink, name: 'Sink', cost: 2, sel: 0, part: 'auto', group: '' }] });
      FA.SimView.renderSlots(out, res);
    }
    run();
  });

  /* ===== 8. Partitioner picture ===== */
  FA.registerWidget('partitioners', function (el) {
    el.appendChild(h('div', { class: 'wtitle' }, 'Which upstream subtask talks to which downstream subtask?'));
    const sel = h('select', null, ['FORWARD', 'REBALANCE', 'RESCALE', 'HASH', 'BROADCAST', 'GLOBAL', 'SHUFFLE'].map(x => h('option', null, x)));
    const upI = h('input', { type: 'number', value: 2, min: 1, max: 6 }), dnI = h('input', { type: 'number', value: 4, min: 1, max: 6 });
    const box = h('div'), txt = h('div', { class: 'callout' });
    el.append(h('div', { class: 'row' }, h('label', { class: 'f' }, 'Ship strategy', sel), h('label', { class: 'f' }, 'Upstream p', upI), h('label', { class: 'f' }, 'Downstream p', dnI)), box, txt);
    const DESC = {
      FORWARD: 'One-to-one: subtask i → subtask i. Needs equal parallelism. Order is fully preserved, no repartitioning, and it is the ONLY strategy that allows chaining. Default when parallelism is equal and you did not ask for anything else.',
      REBALANCE: 'Round-robin to ALL downstream subtasks (all-to-all channels). Evens out load and fixes skew from sources, but breaks per-key locality. Default when parallelism DIFFERS.',
      RESCALE: 'Round-robin to a SUBSET of downstream subtasks (local groups), so fewer channels and often no network hop. Good for fan-out/fan-in by a factor (2→6, 6→2).',
      HASH: 'keyBy(): key → key group → subtask. All records with the same key go to the same subtask, which is required for keyed state, windows and timers. Skewed keys cause hot subtasks.',
      BROADCAST: 'Every record goes to EVERY downstream subtask (records × parallelism). Use for small control/rule streams, e.g. the Broadcast State pattern.',
      GLOBAL: 'Everything goes to downstream subtask 0. Effectively parallelism 1. Use only for tiny streams.',
      SHUFFLE: 'Random uniform distribution to all downstream subtasks. Like REBALANCE but random instead of round-robin.'
    };
    function run() {
      const s = sel.value, up = Math.max(1, +upI.value | 0), dn = Math.max(1, +dnI.value | 0);
      const W = 520, H = 40 + Math.max(up, dn) * 46;
      const NS = 'http://www.w3.org/2000/svg';
      const svg = document.createElementNS(NS, 'svg'); svg.setAttribute('viewBox', `0 0 ${W} ${H}`); svg.setAttribute('class', 'viz-svg'); svg.style.maxWidth = '560px';
      const pos = (n, i) => 30 + (H - 60) * (n === 1 ? 0.5 : i / (n - 1));
      const line = (x1, y1, x2, y2, c) => { const l = document.createElementNS(NS, 'line'); Object.entries({ x1, y1, x2, y2, stroke: c, 'stroke-width': 2, opacity: .85 }).forEach(([k, v]) => l.setAttribute(k, v)); svg.appendChild(l); };
      const edges = [];
      if (s === 'FORWARD') { if (up === dn) for (let i = 0; i < up; i++) edges.push([i, i]); }
      else if (s === 'GLOBAL') for (let i = 0; i < up; i++) edges.push([i, 0]);
      else if (s === 'RESCALE') {
        if (dn >= up) for (let d = 0; d < dn; d++) edges.push([Math.floor(d * up / dn), d]);
        else for (let u = 0; u < up; u++) edges.push([u, Math.floor(u * dn / up)]);
      } else for (let i = 0; i < up; i++) for (let d = 0; d < dn; d++) edges.push([i, d]);
      const color = { FORWARD: '#52c41a', HASH: '#fa8c16', BROADCAST: '#eb2f96', GLOBAL: '#ff4d4f', RESCALE: '#40a9ff', REBALANCE: '#2f54eb', SHUFFLE: '#9254de' }[s];
      edges.forEach(([u, d]) => line(110, pos(up, u), 410, pos(dn, d), color));
      const node = (x, y, t) => { const r = document.createElementNS(NS, 'rect'); Object.entries({ x: x - 50, y: y - 15, width: 100, height: 30, rx: 6, fill: '#1c2538', stroke: '#5a6a90' }).forEach(([k, v]) => r.setAttribute(k, v)); svg.appendChild(r); const tt = document.createElementNS(NS, 'text'); Object.entries({ x, y: y + 4, 'text-anchor': 'middle', fill: '#e6ebf5', 'font-size': 12 }).forEach(([k, v]) => tt.setAttribute(k, v)); tt.textContent = t; svg.appendChild(tt); };
      for (let i = 0; i < up; i++) node(60, pos(up, i), `upstream #${i + 1}`);
      for (let d = 0; d < dn; d++) node(460, pos(dn, d), `downstream #${d + 1}`);
      box.innerHTML = ''; box.appendChild(svg);
      txt.innerHTML = (s === 'FORWARD' && up !== dn ? '<b>⚠️ Invalid:</b> FORWARD needs equal parallelism.<br>' : '') + `<b>${s}</b> · ${edges.length} channel(s). ${DESC[s]}`;
    }
    [sel, upI, dnI].forEach(x => x.addEventListener('input', run)); run();
  });
})(window.FA);
