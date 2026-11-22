/* Pipeline Lab: build a job, watch it run on a simulated cluster with a Flink-style dashboard */
(function (FA) {
  'use strict';
  const h = FA.h;
  let uid = 1;
  const op = (kind, p, extra) => Object.assign({ id: uid++, kind, p, name: FA.Sim.KINDS[kind].defName, cost: FA.Sim.KINDS[kind].cost, sel: FA.Sim.KINDS[kind].sel, part: 'auto', group: '', chain: 'auto' }, extra || {});

  FA.labPresets = {
    classic: () => ({ cluster: { tms: 2, slotsPerTm: 2, coresPerTm: 4, sourceRate: 20000, chaining: true, checkpointing: true, unaligned: false, maxParallelism: 0 },
      ops: [op('source', 2, { name: 'Kafka orders' }), op('map', 2, { name: 'Parse JSON' }), op('filter', 2, { name: 'Valid orders' }), op('keyby', 4, { name: 'keyBy(customerId)', skew: 0 }), op('window', 4, { name: 'Tumbling 1 min' }), op('sink', 4, { name: 'Kafka summaries' })] }),
    stateless: () => ({ cluster: { tms: 1, slotsPerTm: 4, coresPerTm: 4, sourceRate: 30000, chaining: true, checkpointing: true },
      ops: [op('source', 4, { name: 'Kafka clicks' }), op('map', 4, { name: 'Enrich' }), op('filter', 4, { name: 'Bots out' }), op('sink', 4, { name: 'Kafka clean' })] }),
    beamish: () => ({ cluster: { tms: 2, slotsPerTm: 4, coresPerTm: 4, sourceRate: 15000, chaining: true, checkpointing: true },
      ops: [op('source', 4, { name: 'KafkaIO.Read' }), op('map', 4, { name: 'ParDo(ParseFn)' }), op('keyby', 4, { name: 'WithKeys' }), op('reduce', 4, { name: 'GroupByKey + Combine.perKey' }), op('map', 4, { name: 'ParDo(FormatFn)' }), op('sink', 4, { name: 'KafkaIO.Write' })] }),
    broadcast: () => ({ cluster: { tms: 2, slotsPerTm: 2, coresPerTm: 4, sourceRate: 5000, chaining: true, checkpointing: true },
      ops: [op('source', 2, { name: 'Rules topic' }), op('map', 4, { name: 'Apply rule', part: 'broadcast' }), op('sink', 4, { name: 'Out' })] })
  };

  FA.labChallenges = [
    { id: 'fit', title: 'Make it fit the cluster', xp: 60, story: 'Your team set parallelism 6 everywhere, but the cluster has 2 TaskManagers × 2 slots. The job is stuck: NoResourceAvailableException.',
      setup: () => { const m = FA.labPresets.classic(); m.ops.forEach(o => o.p = 6); m.ops[0].p = 6; return m; },
      goals: [['Job status is RUNNING', r => r.status === 'RUNNING'], ['Keyed window parallelism is at least 4', (r, m) => m.ops.filter(o => o.kind === 'window').every(o => o.p >= 4)], ['Throughput ≥ 99% of the source rate', r => r.throughput >= r.sourceRate * 0.99]],
      hint: 'Slots needed = max parallelism in the slot sharing group. Either add TaskManagers/slots or lower the parallelism of the non-keyed part.' },
    { id: 'sink', title: 'Kill the backpressure', xp: 80, story: 'The JDBC sink is slow (150 µs/record). The source rate is 40k rec/s and everything upstream is black (backpressured).',
      setup: () => ({ cluster: { tms: 2, slotsPerTm: 8, coresPerTm: 8, sourceRate: 40000, chaining: true, checkpointing: true },
        ops: [op('source', 4, { name: 'Kafka payments' }), op('map', 4, { name: 'Validate' }), op('sink', 2, { name: 'JDBC upsert', cost: 150 })] }),
      goals: [['No task with HIGH or LOW backpressure (≤10%)', r => r.status === 'RUNNING' && r.vertices.every(v => v.bpMax <= 0.1)], ['Throughput ≥ 99% of the source rate', r => r.throughput >= r.sourceRate * 0.99]],
      hint: 'Find the red (busy) task right after the black ones. That is the bottleneck. Give it more parallel subtasks. 150 µs/record means about 6,600 rec/s per subtask.' },
    { id: 'chain', title: 'One chain to rule them all', xp: 70, story: 'This stateless job ships every record over the network 3 times. Make it a single chained task (one box in the UI).',
      setup: () => ({ cluster: { tms: 1, slotsPerTm: 4, coresPerTm: 4, sourceRate: 20000, chaining: true, checkpointing: true },
        ops: [op('source', 4, { name: 'Kafka' }), op('map', 4, { name: 'Parse' }), op('filter', 2, { name: 'Filter' }), op('map', 4, { name: 'Enrich', part: 'rebalance' }), op('sink', 4, { name: 'Kafka out', chain: 'disable' })] }),
      goals: [['Exactly 1 task (vertex) in the job graph', r => r.status === 'RUNNING' && r.vertices.length === 1], ['All operators still present (5)', (r, m) => m.ops.length === 5]],
      hint: 'Chaining needs FORWARD edges, equal parallelism, the same slot sharing group and no disableChaining()/startNewChain().' },
    { id: 'hot', title: 'Hot key on Black Friday', xp: 90, story: '70% of the traffic belongs to one customer. One window subtask is 100% busy while its siblings are idle, and adding parallelism does not help.',
      setup: () => ({ cluster: { tms: 2, slotsPerTm: 4, coresPerTm: 8, sourceRate: 60000, chaining: true, checkpointing: true },
        ops: [op('source', 4, { name: 'Kafka orders' }), op('map', 4, { name: 'Parse' }), op('keyby', 8, { name: 'keyBy(customerId)', skew: 0.7 }), op('window', 8, { name: 'Tumbling 1 min', cost: 30 }), op('sink', 8, { name: 'Out' })] }),
      goals: [['Throughput ≥ 99% of the source rate', r => r.throughput >= r.sourceRate * 0.99], ['Hottest window subtask busy < 80%', r => r.status === 'RUNNING' && r.vertices.filter(v => v.ops.some(o => o.kind === 'window')).every(v => v.busyMax < 0.8)]],
      hint: 'Parallelism cannot split ONE key. Use local pre-aggregation (two-phase aggregation) on the keyBy so the hot key is reduced before the shuffle. Tick "Pre-aggregate" on the keyBy.' },
    { id: 'global', title: 'The GLOBAL trap', xp: 60, story: 'Someone added .global() before an expensive enrichment. Parallelism 4, but only subtask #1 does any work.',
      setup: () => ({ cluster: { tms: 1, slotsPerTm: 4, coresPerTm: 4, sourceRate: 20000, chaining: true, checkpointing: true },
        ops: [op('source', 4, { name: 'Kafka' }), op('async', 4, { name: 'Enrich', cost: 120, part: 'global' }), op('sink', 4, { name: 'Out' })] }),
      goals: [['Throughput ≥ 99% of the source rate', r => r.throughput >= r.sourceRate * 0.99], ['No GLOBAL edge left', r => r.real.every(o => !o.edge || o.edge.strat !== 'GLOBAL')]],
      hint: 'GLOBAL sends everything to subtask 0 of the downstream operator. Use REBALANCE (or remove the override) and make sure there are enough subtasks.' },
    { id: 'cpu', title: 'Slots are not CPUs', xp: 80, story: '1 TaskManager with 8 slots but only 2 cores. Every slot has heavy work, the CPU is oversubscribed and throughput collapses.',
      setup: () => ({ cluster: { tms: 1, slotsPerTm: 8, coresPerTm: 2, sourceRate: 50000, chaining: true, checkpointing: true },
        ops: [op('source', 8, { name: 'Kafka' }), op('map', 8, { name: 'Heavy parse', cost: 40 }), op('sink', 8, { name: 'Out' })] }),
      goals: [['No TaskManager uses more CPU than it has', r => r.status === 'RUNNING' && r.cpuDemand.every(d => d <= r.cl.coresPerTm + 1e-6)], ['Throughput ≥ 99% of the source rate', r => r.throughput >= r.sourceRate * 0.99]],
      hint: 'Slots isolate managed memory, NOT CPU. Add TaskManagers or cores (a common rule of thumb is about 1 slot per core).' },
    { id: 'isolate', title: 'Isolate the slow I/O operator', xp: 60, story: 'Put the async enrichment into its own slot sharing group "io", so it gets dedicated slots, while keeping the job runnable.',
      setup: () => ({ cluster: { tms: 2, slotsPerTm: 2, coresPerTm: 4, sourceRate: 8000, chaining: true, checkpointing: true },
        ops: [op('source', 2, { name: 'Kafka' }), op('async', 2, { name: 'AsyncEnrich(REST)', cost: 100 }), op('sink', 2, { name: 'Out' })] }),
      goals: [['Async operator in slot sharing group "io"', (r, m) => m.ops.filter(o => o.kind === 'async').every(o => o.group === 'io')], ['Job status is RUNNING', r => r.status === 'RUNNING'], ['Throughput ≥ 99% of the source rate', r => r.throughput >= r.sourceRate * 0.99]],
      hint: 'A new group needs its OWN slots: required = max p in "default" + max p in "io". Check the Slots tab.' }
  ];

  FA.views.lab = function (main, challengeId) {
    const store = FA.store;
    const challenge = challengeId ? FA.labChallenges.find(c => c.id === challengeId) : null;
    let model;
    if (challenge) model = challenge.setup();
    else if (store.state.labSaved) { model = JSON.parse(JSON.stringify(store.state.labSaved)); model.ops.forEach(o => { o.id = uid++; }); }
    else model = FA.labPresets.classic();
    let selected = -1, tab = 'subtasks', res = null, lagAcc = 0, selOp = null;

    const c = h('div', { class: 'container', style: { maxWidth: '1400px' } });
    c.appendChild(h('div', { class: 'lesson-head', style: { marginBottom: '14px' } },
      h('div', null, h('h1', { style: { margin: 0 } }, challenge ? `🎯 Challenge: ${challenge.title}` : '🧪 Pipeline Lab'),
        h('div', { class: 'muted' }, challenge ? challenge.story : 'Build a job, set parallelism and the cluster, then read the result like the real Flink dashboard. Click a box for its subtasks.')),
      h('div', { class: 'row' },
        challenge ? h('a', { class: 'btn', href: '#/lab' }, '← Free play') : h('select', {
          onchange: e => { if (!e.target.value) return; model = FA.labPresets[e.target.value](); selected = -1; buildEditor(); update(); e.target.value = ''; }
        }, h('option', { value: '' }, 'Load preset…'), h('option', { value: 'classic' }, 'Kafka → keyBy → window'), h('option', { value: 'stateless' }, 'Stateless ETL (one chain)'), h('option', { value: 'beamish' }, 'Beam-style pipeline'), h('option', { value: 'broadcast' }, 'Broadcast edge')),
        h('button', { class: 'btn', onclick: () => { if (challenge) { model = challenge.setup(); } else model = FA.labPresets.classic(); buildEditor(); update(); } }, '↺ Reset'))));

    const left = h('div');
    const right = h('div');
    c.appendChild(h('div', { class: 'lab' }, left, right));
    main.appendChild(c);

    /* ---------- editor ---------- */
    function num(label, obj, key, attrs, after) {
      const inp = h('input', Object.assign({ type: 'number', value: obj[key] }, attrs || {}));
      inp.addEventListener('input', () => { const v = parseFloat(inp.value); if (!isNaN(v)) { obj[key] = v; update(); } });
      return h('label', { class: 'f' }, label, inp, after || null);
    }
    function sel(label, obj, key, options) {
      const s = h('select', null, options.map(([v, t]) => h('option', { value: v, selected: obj[key] === v }, t)));
      s.addEventListener('change', () => { obj[key] = s.value; update(); });
      return h('label', { class: 'f' }, label, s);
    }
    function chk(label, obj, key) {
      const i = h('input', { type: 'checkbox', checked: !!obj[key] });
      i.addEventListener('change', () => { obj[key] = i.checked; update(); });
      return h('label', { class: 'row', style: { gap: '6px', fontSize: '13px' } }, i, label);
    }

    function buildEditor() {
      left.innerHTML = '';
      const cl = model.cluster;
      left.appendChild(h('div', { class: 'card' },
        h('b', null, '⚙️ Cluster & job config'),
        h('div', { class: 'ob', style: { display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px', marginTop: '8px' } },
          num('TaskManagers', cl, 'tms', { min: 1, max: 8 }),
          num('Slots per TM', cl, 'slotsPerTm', { min: 1, max: 16 }),
          num('CPU cores per TM', cl, 'coresPerTm', { min: 1, max: 32 }),
          num('Source rate (rec/s)', cl, 'sourceRate', { min: 100, step: 1000 }),
          num('Max parallelism (0 = auto)', cl, 'maxParallelism', { min: 0 })),
        h('div', { style: { marginTop: '8px', display: 'grid', gap: '4px' } },
          chk('Operator chaining enabled', cl, 'chaining'),
          chk('Checkpointing enabled', cl, 'checkpointing'),
          chk('Unaligned checkpoints', cl, 'unaligned'))));

      const list = h('div', { class: 'ops-list' });
      model.ops.forEach((o, i) => {
        const K = FA.Sim.KINDS[o.kind];
        const box = h('div', { class: 'op' + (selOp === o.id ? ' selected' : '') });
        box.appendChild(h('div', { class: 'oh' },
          h('span', null, `${i + 1}. ${K.label}`),
          h('span', { class: 'tools' },
            h('button', { class: 'iconbtn', title: 'Move up', onclick: () => { if (i > 0) { [model.ops[i - 1], model.ops[i]] = [model.ops[i], model.ops[i - 1]]; buildEditor(); update(); } } }, '↑'),
            h('button', { class: 'iconbtn', title: 'Move down', onclick: () => { if (i < model.ops.length - 1) { [model.ops[i + 1], model.ops[i]] = [model.ops[i], model.ops[i + 1]]; buildEditor(); update(); } } }, '↓'),
            h('button', { class: 'iconbtn', title: 'Remove', onclick: () => { model.ops.splice(i, 1); buildEditor(); update(); } }, '✕'))));
        const body = h('div', { class: 'ob' });
        const nameInp = h('input', { value: o.name });
        nameInp.addEventListener('input', () => { o.name = nameInp.value; update(); });
        body.appendChild(h('label', { class: 'f', style: { gridColumn: '1 / -1' } }, 'Name (shown in the UI)', nameInp));
        if (K.pseudo) {
          body.appendChild(num('Hot-key share (0–1)', o, 'skew', { min: 0, max: 1, step: 0.05 }));
          body.appendChild(h('div', { style: { alignSelf: 'end' } }, chk('Pre-aggregate (2-phase)', o, 'preAgg')));
          body.appendChild(h('div', { class: 'muted', style: { gridColumn: '1 / -1', fontSize: '12px' } }, 'keyBy is not an operator. It changes the NEXT edge to HASH. Parallelism here is ignored.'));
        } else {
          body.appendChild(num('Parallelism', o, 'p', { min: 1, max: 64 }));
          body.appendChild(num('Cost µs/record', o, 'cost', { min: 0, step: 1 }));
          if (o.kind !== 'sink' && o.kind !== 'source') body.appendChild(num('Out/in ratio', o, 'sel', { min: 0, step: 0.1 }));
          if (o.kind !== 'source' && !(model.ops[i - 1] && model.ops[i - 1].kind === 'keyby'))
            body.appendChild(sel('Incoming partitioning', o, 'part', [['auto', 'auto (forward/rebalance)'], ['forward', 'forward()'], ['rebalance', 'rebalance()'], ['rescale', 'rescale()'], ['shuffle', 'shuffle()'], ['broadcast', 'broadcast()'], ['global', 'global()']]));
          const g = h('input', { value: o.group || '', placeholder: '(inherit from input)' });
          g.addEventListener('input', () => { o.group = g.value.trim(); update(); });
          body.appendChild(h('label', { class: 'f' }, 'Slot sharing group', g));
          body.appendChild(sel('Chaining', o, 'chain', [['auto', 'auto'], ['disable', 'disableChaining()'], ['startNew', 'startNewChain()']]));
        }
        box.appendChild(body);
        list.appendChild(box);
      });
      const adder = h('select', null, h('option', { value: '' }, '+ Add operator…'), Object.entries(FA.Sim.KINDS).map(([k, v]) => h('option', { value: k }, v.label)));
      adder.addEventListener('change', () => {
        if (!adder.value) return;
        const last = model.ops[model.ops.length - 1];
        const newOp = op(adder.value, last ? last.p : 2, adder.value === 'keyby' ? { skew: 0 } : {});
        const sinkIdx = model.ops.findIndex(x => x.kind === 'sink');
        if (sinkIdx >= 0 && adder.value !== 'sink') model.ops.splice(sinkIdx, 0, newOp); else model.ops.push(newOp);
        buildEditor(); update();
      });
      left.appendChild(h('div', { class: 'card' }, h('b', null, '🔧 Operators (in program order)'), h('div', { style: { marginTop: '8px' } }, list, adder)));
    }

    /* ---------- dashboard ---------- */
    const statusBar = h('div', { class: 'metrics-grid', style: { marginBottom: '12px' } });
    const errBox = h('div');
    const graph = h('div', { class: 'graph-wrap' });
    const legend = h('div', { class: 'legend' },
      h('span', null, h('i', { style: { background: 'rgb(61,139,253)' } }), 'Idle'),
      h('span', null, h('i', { style: { background: 'rgb(235,64,52)' } }), 'Busy'),
      h('span', null, h('i', { style: { background: 'rgb(20,20,20)' } }), 'Backpressured'),
      h('span', null, '🔥 = bottleneck · edge label = ship strategy · blue dashes = data flow'));
    const tabsEl = h('div', { class: 'tabs' });
    const tabBody = h('div', { class: 'card' });
    const chalBox = h('div');
    right.append(statusBar, errBox, h('div', { style: { background: '#f4f6fa', borderRadius: '12px', marginBottom: '12px' } }, graph, legend), tabsEl, tabBody, chalBox);

    const TABS = [['subtasks', 'SubTasks / BackPressure'], ['chain', 'Why chained / not chained'], ['slots', 'Slots & TaskManagers'], ['ckpt', 'Checkpoints'], ['java', 'Java DataStream code'], ['beam', 'Beam (FlinkRunner) code'], ['challenges', '🎯 Challenges']];
    function drawTabs() {
      tabsEl.innerHTML = '';
      TABS.forEach(([k, t]) => tabsEl.appendChild(h('button', { class: tab === k ? 'on' : '', onclick: () => { tab = k; drawTabs(); drawTab(); } }, t)));
    }

    function update() {
      res = FA.Sim.compile(model);
      if (!challenge) { store.state.labSaved = model; store.save(); }
      statusBar.innerHTML = '';
      const m = (k, v, extra) => h('div', { class: 'metric' }, h('div', { class: 'k' }, k), h('div', { class: 'v' }, v), extra ? h('div', { class: 'k' }, extra) : null);
      statusBar.append(
        m('Job status', h('span', { class: 'status-badge st-' + (res.status === 'RUNNING' ? 'RUNNING' : res.status === 'FAILED' ? 'FAILED' : 'CREATED') }, res.status)),
        m('Throughput', FA.fmt(res.throughput) + ' rec/s', `source wants ${FA.fmt(res.sourceRate)}/s`),
        m('Kafka consumer lag', res.lagGrowth > 1 ? `+${FA.fmt(res.lagGrowth)}/s 📈` : 'stable ✓', res.lagGrowth > 1 ? 'job cannot keep up' : 'keeping up'),
        m('Tasks / slots', `${res.vertices.length} / ${res.slotsRequired}`, `${res.slotsAvailable} slots available`));
      errBox.innerHTML = '';
      res.errors.forEach(e => errBox.appendChild(h('div', { class: 'callout warn' }, h('b', null, res.status === 'FAILED' ? 'Exception (job FAILED)' : 'Invalid program'), e)));
      res.warnings.forEach(e => errBox.appendChild(h('div', { class: 'callout' }, '⚠️ ', e)));
      if (selected >= res.vertices.length) selected = -1;
      FA.SimView.renderGraph(graph, res, { selected, onSelect: id => { selected = id; tab = 'subtasks'; drawTabs(); update(); } });
      drawTab();
      drawChallenge();
    }

    function drawTab() {
      tabBody.innerHTML = '';
      if (tab === 'subtasks') {
        const v = res.vertices[selected >= 0 ? selected : (res.bottleneck >= 0 ? res.bottleneck : 0)];
        if (!v) return;
        tabBody.appendChild(h('div', { style: { marginBottom: '8px' } }, h('b', null, v.name), h('span', { class: 'muted' }, ` · parallelism ${v.p} · back pressure status `),
          h('span', { class: 'pill ' + (v.bpStatus === 'HIGH' ? 'bad' : v.bpStatus === 'LOW' ? 'warn' : 'ok') }, v.bpStatus)));
        const tbl = h('table', { class: 't' }, h('tr', null, ['Subtask', 'Records received/s', 'Records sent/s', 'Busy', 'Backpressured', 'Idle', 'Status'].map(x => h('th', null, x))));
        v.sub.forEach(s => {
          const st = s.bp > 0.5 ? 'HIGH' : s.bp > 0.1 ? 'LOW' : 'OK';
          tbl.appendChild(h('tr', null, h('td', null, `#${s.s + 1}`), h('td', null, FA.fmt(s.recIn)), h('td', null, FA.fmt(s.recOut)),
            h('td', null, Math.round(s.busy * 100) + '%'), h('td', null, Math.round(s.bp * 100) + '%'), h('td', null, Math.round(s.idle * 100) + '%'),
            h('td', null, h('span', { class: 'pill ' + (st === 'HIGH' ? 'bad' : st === 'LOW' ? 'warn' : 'ok') }, st))));
        });
        tabBody.appendChild(tbl);
        tabBody.appendChild(h('div', { class: 'muted', style: { fontSize: '12.5px', marginTop: '8px' } },
          'Back pressure status: OK ≤ 10%, LOW 10–50%, HIGH > 50% of time back pressured (backPressuredTimeMsPerSecond). In a real job, busy + backpressured + idle ≈ 100% per subtask. Uneven "Busy" across subtasks means data skew.'));
      } else if (tab === 'chain') {
        const ul = h('ul');
        res.real.forEach((r, i) => {
          if (i === 0) { ul.appendChild(h('li', null, h('b', null, r.name), ' starts the first task.')); return; }
          ul.appendChild(h('li', null, h('b', null, r.name), r.chained ? ` is CHAINED to "${res.real[i - 1].name}" (FORWARD, same parallelism, same slot group). Same thread, no serialization.` :
            ` starts a NEW task because ${r.chainBreak.join(', ')}. Records are serialized and sent through network buffers (${r.edge.strat}).`));
        });
        tabBody.appendChild(ul);
      } else if (tab === 'slots') FA.SimView.renderSlots(tabBody, res);
      else if (tab === 'ckpt') {
        if (!res.ckpt) { tabBody.appendChild(h('div', null, 'Checkpointing is disabled. On failure this job would restart from scratch or from the Kafka committed offsets, with no exactly-once state.')); return; }
        tabBody.appendChild(h('div', { class: 'kv' },
          h('span', { class: 'muted' }, 'Mode'), h('span', null, res.cl.unaligned ? 'EXACTLY_ONCE, unaligned' : 'EXACTLY_ONCE, aligned barriers'),
          h('span', { class: 'muted' }, 'Estimated end-to-end duration'), h('span', null, res.ckpt.durationS.toFixed(1) + ' s' + (res.ckpt.durationS > 60 ? ' ⚠️ slow' : '')),
          h('span', { class: 'muted' }, 'Estimated alignment time'), h('span', null, res.ckpt.alignmentS.toFixed(1) + ' s'),
          h('span', { class: 'muted' }, 'Checkpointed data size'), h('span', null, res.ckpt.sizeNote)));
        tabBody.appendChild(h('div', { class: 'callout', style: { marginTop: '10px' } }, 'Barriers travel WITH the data. When tasks are backpressured, buffers are full, so barriers queue behind records and aligned checkpoints become slow or time out. Unaligned checkpoints let barriers overtake in-flight data, at the cost of persisting those buffers. (This is a simplified estimate.)'));
      } else if (tab === 'java') tabBody.appendChild(h('pre', null, h('code', null, FA.Sim.javaCode(model, res))));
      else if (tab === 'beam') {
        tabBody.appendChild(h('div', { class: 'callout beam' }, h('b', null, 'Beam view'), 'Beam on Flink normally runs every transform at the same job parallelism (--parallelism). Per-operator parallelism, slot groups and partitioning calls are Flink DataStream features you do not get directly in Beam. GroupByKey/Combine.perKey/stateful DoFns show up as HASH edges.'));
        tabBody.appendChild(h('pre', null, h('code', null, FA.Sim.beamCode(model))));
      } else if (tab === 'challenges') {
        FA.labChallenges.forEach(ch => {
          const done = !!store.state.challenges[ch.id];
          tabBody.appendChild(h('div', { class: 'chal' + (done ? ' done' : '') },
            h('div', { class: 'row', style: { justifyContent: 'space-between' } }, h('b', null, (done ? '✅ ' : '🎯 ') + ch.title), h('a', { class: 'btn sm', href: `#/lab/${ch.id}` }, done ? 'Replay' : `Start · +${ch.xp} XP`)),
            h('div', { class: 'muted', style: { fontSize: '13px', marginTop: '4px' } }, ch.story)));
        });
      }
    }

    function drawChallenge() {
      chalBox.innerHTML = '';
      if (!challenge) return;
      const results = challenge.goals.map(([t, f]) => [t, !!f(res, model)]);
      const all = results.every(x => x[1]);
      const box = h('div', { class: 'card', style: { borderColor: all ? 'var(--ok)' : 'var(--line)' } },
        h('b', null, '🎯 Goals'),
        results.map(([t, ok]) => h('div', { class: 'goal ' + (ok ? 'met' : 'unmet') }, (ok ? '✅ ' : '⬜ ') + t)),
        h('details', { style: { marginTop: '8px' } }, h('summary', { class: 'muted' }, 'Hint'), h('div', { style: { marginTop: '6px' } }, challenge.hint)));
      if (all) {
        if (!store.state.challenges[challenge.id]) { store.state.challenges[challenge.id] = true; store.addXp(challenge.xp, 'Challenge solved: ' + challenge.title); }
        box.appendChild(h('div', { style: { marginTop: '10px' } }, h('span', { class: 'pill ok' }, '🏆 Challenge solved!'), ' ', h('a', { class: 'btn sm', href: '#/lab' }, 'Back to lab')));
      }
      chalBox.appendChild(box);
    }

    buildEditor(); drawTabs(); update();
  };
})(window.FA);
