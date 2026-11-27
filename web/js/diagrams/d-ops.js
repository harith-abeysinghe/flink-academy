/* Animated diagrams for lessons 11-15 (Web UI, backpressure, memory, deployment, Beam) */
(function (FA) {
  'use strict';
  const D = FA.registerDiagram;
  const C = FA.DG.SOLID;

  /* L11: anatomy of one job-graph box */
  D('box-anatomy', {
    title: 'Reading one box in the job graph', h: 300,
    build(S) {
      const box = S.box(40, 40, 350, 128, '', { tone: 'bp', rx: 6 });
      const lines = ['Source: KafkaIO.Read -> ParDo(Parse)', 'Parallelism: 4', 'Backpressured (max): 87%', 'Busy (max): 9%']
        .map((t, i) => S.text(16, 26 + i * 27, t, { parent: box, anchor: 'start', size: i ? 12.5 : 13, weight: i ? 400 : 700, fill: '#ffffff' }));
      S.arrow(215, 168, 215, 226, { label: 'HASH', lx: -26, ly: 0, lcolor: '#e6ebf5' });
      S.box(40, 228, 350, 50, 'Window -> Sink: Kafka', { tone: 'idle', rx: 6, size: 12.5 });
      const ann = (y1, y2, txt, x1 = 392) => {
        const g = S.g({ hidden: true });
        S.line(x1, y1, 462, y2, { parent: g, color: C.warn, sw: 1.2 });
        S.rect(x1 - 3, y1 - 3, 6, 6, { parent: g, fill: C.warn, stroke: C.warn, rx: 3 });
        S.text(470, y2, txt, { parent: g, anchor: 'start', size: 12.5, tone: 'warn' });
        return g;
      };
      const a = [ann(66, 60, 'operators chained in this task'), ann(93, 92, 'number of subtasks (threads)'), ann(120, 124, 'worst subtask: % of time blocked on output'), ann(147, 156, 'worst subtask: % of time doing work'), ann(197, 197, 'ship strategy into the next task', 222)];
      const leg = S.g({ hidden: true });
      [['idle', 'idle'], ['busy', 'busy'], ['bp', 'backpressured']].forEach(([t, l], i) => {
        S.rect(470 + i * 100, 246, 14, 14, { parent: leg, tone: t, rx: 3 });
        S.text(490 + i * 100, 253, l, { parent: leg, anchor: 'start', size: 11.5, fill: '#93a0bb' });
      });
      const set = (tone, bp, busy) => { S.tone(box, tone); S.setText(lines[2], `Backpressured (max): ${bp}%`); S.setText(lines[3], `Busy (max): ${busy}%`); };
      return [
        { cap: 'One box is one <b>task</b>: a chain of operators that runs in one thread per subtask.', run: A => A.show(a[0]) },
        { cap: '<b>Parallelism</b>: how many subtasks (threads) run this task.', run: A => A.show(a[1]) },
        { cap: 'Backpressured and Busy show the <b>worst</b> subtask, and so does the colour: blue = idle, red = busy, black = backpressured. One hot subtask is enough to colour the whole box.',
          run: async A => {
            await A.show(a[2], a[3], leg);
            for (const [t, bp, b] of [['idle', 0, 4], ['busy', 0, 96], ['bp', 87, 9]]) { set(t, bp, b); await A.wait(900); }
          } },
        { cap: 'The label on the edge is the <b>ship strategy</b> into the next task: HASH, REBALANCE, FORWARD…', run: A => A.show(a[4]) }
      ];
    }
  });

  /* L12: backpressure propagates upstream */
  D('backpressure-propagation', {
    title: 'How backpressure travels upstream', h: 292,
    build(S) {
      const xs = [20, 215, 410, 605], ws = [130, 130, 130, 175];
      const names = ['Kafka source', 'Validate', 'Running balance', 'JDBC sink'];
      const t = xs.map((x, i) => S.box(x, 90, ws[i], 70, names[i], { tone: 'idle' }));
      const q = [0, 1, 2].map(g => [0, 1, 2].map(i => S.rect(xs[g] + ws[g] + 8 + i * 18, 100, 14, 26, { tone: 'ghost', rx: 3 })));
      [0, 1, 2].forEach(g => S.arrow(xs[g] + ws[g], 145, xs[g + 1] - 2, 145));
      S.text(20, 200, 'Kafka consumer lag', { anchor: 'start', size: 12, fill: '#93a0bb' });
      S.rect(20, 210, 320, 14, { tone: 'muted', rx: 4 });
      const lag = S.rect(20, 210, 0, 14, { tone: 'bad', rx: 4 });
      const lagT = S.text(350, 217, '0 records', { anchor: 'start', size: 12, tone: 'bad', weight: 700 });
      const vict = S.g({ hidden: true });
      S.line(20, 72, 540, 72, { parent: vict, color: '#6b7280' });
      S.text(280, 60, 'victims: backpressured (black)', { parent: vict, size: 12, fill: '#c4cad6', weight: 700 });
      const bn = S.g({ hidden: true });
      S.text(692, 50, 'bottleneck: busy (red)', { parent: bn, size: 12, tone: 'bad', weight: 700 });
      S.arrow(692, 62, 692, 86, { parent: bn, color: C.bad });
      const fillQ = (A, g) => A.stagger(q[g], r => { S.tone(r, 'warn'); return A.wait(200); }, 160);
      return [
        { cap: 'A healthy pipeline: every task keeps up, so every box is blue (idle part of the time).',
          run: A => A.stagger([0, 1, 2, 3], () => A.fly('', [150, 145], [603, 145], { r: 7, tone: 'warn', ms: 1700, ease: p => p, via: [[345, 145], [410, 145], [540, 145]] }), 350) },
        { cap: 'The JDBC sink gets slow (150 µs per record) and turns <b>red</b>: busy almost 100% of the time. Its input buffers start to fill.', run: async A => { S.tone(t[3], 'busy'); await fillQ(A, 2); } },
        { cap: 'With full buffers there are no credits left. Running balance cannot send, so it blocks: <b>backpressured</b> (black). Now its own input fills up.', run: async A => { S.tone(t[2], 'bp'); await fillQ(A, 1); } },
        { cap: 'The blocking spreads upstream until the source reads slower than Kafka produces, and <b>consumer lag</b> grows.',
          run: async A => { S.tone(t[1], 'bp'); await fillQ(A, 0); S.tone(t[0], 'bp'); await A.all(A.to(lag, { width: 300 }, 1600), A.count(lagT, 0, 1250000, 1600, v => FA.fmt(v) + ' records')); } },
        { cap: 'Walk from the source: black tasks are <b>victims</b>. The first red task after them is the <b>bottleneck</b>. Fix or scale that one.',
          run: async A => { await A.show(vict); await A.show(bn); S.stroke(t[3], C.warn, 3); } }
      ];
    }
  });

  /* L12: hot key */
  D('hot-key', {
    title: 'A hot key: why more parallelism does not help', h: 290,
    build(S) {
      S.box(20, 110, 130, 70, 'Orders', { tone: 'info', sub: 'keyBy(customer)' });
      const rowY = [40, 92, 144, 196];
      const fill = rowY.map((y, i) => {
        S.text(250, y + 13, 'window[' + i + ']', { anchor: 'start', size: 12, fill: '#93a0bb', mono: true });
        S.rect(330, y, 420, 26, { tone: 'muted', rx: 5 });
        return { r: S.rect(330, y, 0, 26, { tone: 'info', rx: 5 }), t: S.text(760, y + 13, '0%', { anchor: 'end', size: 12, weight: 700, fill: '#e6ebf5' }) };
      });
      const setBar = (A, i, pct, tone) => { if (tone) S.tone(fill[i].r, tone); A.text(fill[i].t, pct + '%'); return A.to(fill[i].r, { width: 420 * pct / 100 }, 700); };
      const to = (A, lbl, i, tone) => A.fly(lbl, [150, 145], [330, rowY[i] + 13], { tone: tone || 'warn', ms: 650 });
      const ghost = S.text(540, 248, 'parallelism 400? C7 still maps to one subtask', { tone: 'warn', weight: 700, size: 12.5, hidden: true });
      const fix = S.text(540, 248, 'then merge the partial results per customer', { tone: 'ok', weight: 700, size: 12.5, hidden: true });
      return [
        { cap: '<code>keyBy(customer)</code> with parallelism 4. Normal traffic spreads over the subtasks.',
          run: async A => { await A.stagger([1, 2, 3, 4, 5, 6, 7, 8], (k, i) => to(A, 'C' + k, i % 4, 'info'), 150); await A.all(fill.map((_, i) => setBar(A, i, 25))); } },
        { cap: 'Black Friday: one customer, C7, sends 70% of all orders. Every one of them hashes to the <b>same</b> subtask.',
          run: async A => { await A.stagger([...Array(7)], () => to(A, 'C7', 2), 130); await A.all(fill.map((_, i) => i === 2 ? setBar(A, i, 70, 'busy') : setBar(A, i, 10))); } },
        { cap: 'More parallelism adds subtasks, but a single key still goes to exactly one of them. The hot subtask stays the bottleneck.', run: A => A.show(ghost) },
        { cap: 'The fix: <b>salt</b> the key (C7#0 … C7#3) or <b>pre-aggregate</b> before the shuffle, so the work spreads out, then merge the partial results.',
          run: async A => {
            await A.hide(ghost);
            await A.stagger([0, 1, 2, 3, 0, 1, 2, 3], (i, n) => to(A, 'C7#' + i, i, 'ok'), 130);
            await A.all(fill.map((_, i) => setBar(A, i, 25, 'ok')));
            await A.show(fix);
          } }
      ];
    }
  });

  /* L13: TaskManager memory model */
  D('memory-tree', {
    title: 'TaskManager memory, built up from the container limit', h: 300,
    build(S) {
      const s1 = 740 / 1728, s2 = 740 / 1280;
      const total = S.box(30, 40, 740, 54, 'Total process memory · 1728m', { tone: 'muted', hidden: true, sub: 'taskmanager.memory.process.size (the container / pod limit)' });
      const flink = S.box(30, 40, 1280 * s1, 54, 'Total Flink memory · 1280m', { tone: 'info', hidden: true });
      const meta = S.box(30 + 1280 * s1, 40, 256 * s1, 54, 'Metaspace', { sub: '256m', tone: 'muted', hidden: true, size: 12 });
      const oh = S.box(30 + 1536 * s1, 40, 192 * s1, 54, 'Overhead', { sub: '192m (10%)', tone: 'muted', hidden: true, size: 12 });
      const conn = S.shape('polygon', { points: `30,94 ${30 + 1280 * s1},94 770,178 30,178`, fill: 'rgba(61,139,253,.07)', stroke: '#24365a' }, { hidden: true });
      let x = 30;
      const seg = (mb, label, sub, tone) => { const b = S.box(x, 180, mb * s2, 56, label, { sub, tone, hidden: true, size: 11.5 }); x += mb * s2; return b; };
      const fwh = seg(128, 'Fw heap', '128m', 'muted');
      const th = seg(384, 'Task heap · 384m', 'your objects, HashMap state', 'info');
      const mg = seg(512, 'Managed · 512m (40%)', 'RocksDB, batch sort/join, Python', 'purple');
      const nw = seg(128, 'Network', '128m (10%)', 'warn');
      const fwo = seg(128, 'Fw off-heap', '128m', 'muted');
      const br = (x1, x2, txt) => { const g = S.g({ hidden: true }); S.line(x1 + 2, 246, x2 - 2, 246, { parent: g, color: '#56627e' }); S.line(x1 + 2, 241, x1 + 2, 246, { parent: g, color: '#56627e' }); S.line(x2 - 2, 241, x2 - 2, 246, { parent: g, color: '#56627e' }); S.text((x1 + x2) / 2, 260, txt, { parent: g, size: 11.5, fill: '#93a0bb' }); return g; };
      const heapB = br(30, 30 + 512 * s2, 'JVM heap (-Xmx)'), offB = br(30 + 512 * s2, 770, 'off-heap (direct / native memory)');
      const sum = S.text(400, 285, '128 + 384 + 512 + 128 + 128 = 1280m  ✓  (task off-heap defaults to 0)', { tone: 'ok', size: 12.5, weight: 700, hidden: true });
      return [
        { cap: '<code>taskmanager.memory.process.size = 1728m</code>: everything the TaskManager container may use.', run: A => A.show(total) },
        { cap: 'JVM Metaspace (256m) and JVM Overhead (10%, here 192m) come off the top. The rest is <b>Total Flink memory</b> (1280m).', run: async A => { await A.hide(total); await A.stagger([flink, meta, oh], b => A.show(b), 250); } },
        { cap: 'Flink memory splits into the <b>JVM heap</b>: framework heap (128m) plus task heap (384m) for your objects and HashMap state…', run: async A => { await A.show(conn); await A.stagger([fwh, th], b => A.show(b), 250); await A.show(heapB); } },
        { cap: '…and <b>off-heap</b>: managed memory (40%: RocksDB, batch), network memory (10%: buffers between tasks) and framework off-heap (128m).', run: async A => { await A.stagger([mg, nw, fwo], b => A.show(b), 250); await A.show(offB); } },
        { cap: 'The pieces add up exactly to what a default TaskManager logs at startup.', run: A => A.show(sum) }
      ];
    }
  });

  /* L14: session vs application mode */
  D('session-vs-app', {
    title: 'Session mode vs application mode', h: 300,
    build(S) {
      S.box(15, 15, 380, 278, 'Session mode: one shared cluster', { corner: true, tone: 'ghost', dash: '5 4' });
      S.box(35, 42, 340, 42, 'JobManager (shared)');
      const tmS = [S.box(35, 100, 165, 172, 'TaskManager 1', { corner: true }), S.box(210, 100, 165, 172, 'TaskManager 2', { corner: true })];
      const chip = (x, y, job, w = 125) => S.box(x, y, w, 36, 'job ' + job + ' task', { tone: job === 'A' ? 'purple' : 'warn', size: 12, hidden: true });
      const sc = [[0, 'A', 140], [0, 'B', 196], [1, 'A', 140], [1, 'B', 196]].map(([i, j, y]) => ({ tm: i, job: j, b: chip(tmS[i].bx + 20, y, j) }));
      const right = S.g({ hidden: true });
      S.box(405, 15, 380, 278, 'Application mode: one cluster per job', { corner: true, tone: 'ghost', dash: '5 4', parent: right });
      const app = ['A', 'B'].map((j, k) => {
        const x = 420 + k * 185;
        S.box(x, 42, 170, 236, 'cluster ' + j, { corner: true, parent: right });
        S.box(x + 15, 66, 140, 40, 'JobManager ' + j, { parent: right, size: 12 });
        const tm = S.box(x + 15, 118, 140, 148, 'TaskManager', { corner: true, parent: right });
        const ch = [152, 206].map(y => chip(x + 30, y, j, 110));
        return { tm, ch };
      });
      return [
        { cap: '<b>Session mode</b>: one long-running cluster. Jobs A and B share its TaskManagers, even the same JVMs.', run: A => A.stagger(sc.map(c => c.b), b => A.show(b), 150) },
        { cap: '<b>Application mode</b>: each application gets its own cluster, and <code>main()</code> runs on its JobManager.', run: async A => { await A.show(right); await A.stagger(app.flatMap(a => a.ch), b => A.show(b), 120); } },
        { cap: 'A TaskManager crashes. In the session cluster <b>both</b> jobs lose tasks and restart. In application mode only job A is affected; job B keeps running.',
          run: async A => {
            S.tone(tmS[0], 'bad'); S.tone(app[0].tm, 'bad');
            sc.forEach(c => S.tone(c.b, c.tm === 0 ? 'bad' : 'muted'));
            app[0].ch.forEach(b => S.tone(b, 'bad'));
            app[1].ch.forEach(b => S.tone(b, 'ok'));
            await A.all(A.blink(tmS[0], 2), A.blink(app[0].tm, 2));
          } }
      ];
    }
  });

  /* L14: Kubernetes operator savepoint upgrade */
  D('k8s-upgrade', {
    title: 'A savepoint upgrade done by the Flink Kubernetes Operator', h: 270,
    build(S) {
      const yaml = S.box(15, 95, 225, 84, 'FlinkDeployment', { sub: 'image: orders-agg:1.4.3\nupgradeMode: savepoint', subMono: true });
      const op = S.box(290, 95, 185, 84, 'Flink Kubernetes\nOperator', { tone: 'purple' });
      const job = S.box(540, 25, 245, 72, 'Job · image 1.4.2', { tone: 'info', sub: 'RUNNING' });
      const sp = S.box(540, 190, 245, 60, 'Savepoint storage (S3)', { tone: 'muted' });
      S.arrow(240, 137, 288, 137, { label: 'apply', ly: -12 });
      S.arrow(475, 125, 538, 70);
      S.arrow(662, 97, 662, 188, { both: true });
      return [
        { cap: 'You change <code>spec.image</code> in the FlinkDeployment and apply it.', run: async A => { S.tone(yaml, 'warn'); await A.fly('spec changed', [240, 137], [op.cx, op.B + 12], { tone: 'warn', ms: 800 }); } },
        { cap: '<code>upgradeMode: savepoint</code>: the operator stops the job with a savepoint.',
          run: async A => { await A.fly('stop + savepoint', [op.R, 125], [job.cx, job.B], { tone: 'purple', ms: 900 }); S.sub(job, 'STOPPING'); await A.fly('state', [662, 100], [662, 190], { tone: 'purple', ms: 700 }); S.tone(sp, 'purple'); S.tone(job, 'muted'); S.sub(job, 'STOPPED'); } },
        { cap: 'It deploys the new image and restores the job from that savepoint.',
          run: async A => { S.label(job, 'Job · image 1.4.3'); S.tone(job, 'info'); S.sub(job, 'RESTORING'); await A.fly('state', [662, 190], [662, 100], { tone: 'purple', ms: 700 }); S.tone(job, 'ok'); S.sub(job, 'RUNNING'); S.tone(yaml, 'ok'); } },
        { cap: 'The other modes: <code>last-state</code> restores from the latest checkpoint found through HA metadata (faster, no savepoint), and <code>stateless</code> starts with empty state.' }
      ];
    }
  });

  /* L14: JobManager high availability */
  D('ha-failover', {
    title: 'JobManager failover with high availability', h: 272,
    build(S) {
      const lead = S.box(30, 30, 210, 64, 'JobManager', { sub: 'leader', tone: 'ok' });
      const stby = S.box(30, 176, 210, 64, 'JobManager', { sub: 'standby', tone: 'muted' });
      const ha = S.box(295, 98, 215, 76, 'HA store', { sub: 'storageDir + leader election:\njob graph, latest checkpoint' });
      S.box(570, 15, 215, 240, 'TaskManagers', { corner: true, tone: 'ghost', dash: '5 4' });
      const tm = [S.box(590, 45, 175, 60, 'TaskManager 1'), S.box(590, 176, 175, 60, 'TaskManager 2')];
      S.arrow(240, 62, 293, 118); S.arrow(240, 208, 293, 154, { dash: '4 4' });
      S.arrow(240, 50, 588, 70, { color: '#3a4766' });
      const nl = S.arrow(240, 222, 588, 210, { color: '#3a4766', hidden: true });
      return [
        { cap: 'With HA, the leader JobManager stores the job graph and a pointer to the latest completed checkpoint in the HA store, and keeps heartbeats with the TaskManagers.',
          run: async A => { await A.fly('checkpoint 41', [lead.R, 62], [ha.cx, ha.T], { tone: 'purple', ms: 900 }); await A.all(tm.map(t => A.fly('', [lead.R, 50], [t.L, t.cy], { r: 6, tone: 'ok', ms: 700 }))); } },
        { cap: 'The leader dies. Leader election (Kubernetes ConfigMaps or ZooKeeper) promotes the standby.',
          run: async A => { S.tone(lead, 'bad'); S.sub(lead, 'crashed'); await A.blink(lead, 2); await A.fade(lead, 0.45); S.tone(stby, 'ok'); S.sub(stby, 'new leader'); } },
        { cap: 'The new leader reads the job graph and the checkpoint pointer from the HA store and recovers the job from checkpoint 41. Nobody has to resubmit anything.',
          run: async A => { await A.fly('checkpoint 41', [ha.cx, ha.B], [stby.R, 208], { tone: 'purple', ms: 900 }); await A.show(nl); await A.all(tm.map(t => A.fly('deploy', [stby.R, 222], [t.L, t.cy], { tone: 'info', ms: 800 }))); tm.forEach(t => S.tone(t, 'ok')); } }
      ];
    }
  });

  /* L15: Beam steps fuse into Flink tasks */
  D('beam-fusion', {
    title: 'Beam steps → Flink boxes (fusion = chaining)', h: 318,
    build(S) {
      S.text(12, 20, 'YOUR BEAM PIPELINE', { anchor: 'start', size: 11, weight: 700, fill: '#93a0bb' });
      const names = ['KafkaIO.Read', 'ParDo(Parse)', 'Window.Into', 'WithKeys', 'Combine.perKey', 'ParDo(Format)', 'KafkaIO.Write'];
      names.forEach((n, i) => S.box(10 + i * 106, 36, 96, 40, n, { size: 11, tone: 'ghost', dash: '4 4' }));
      const ch = names.map((n, i) => S.box(10 + i * 106, 36, 96, 40, n, { size: 11, tone: 'accent', hidden: true }));
      const lk = [0, 1, 2, 3, 4, 5].map(i => S.line(10 + i * 106 + 96, 56, 10 + (i + 1) * 106, 56, { color: '#56627e', hidden: true }));
      const lab2 = S.text(12, 166, 'WHAT THE FLINK UI SHOWS', { anchor: 'start', size: 11, weight: 700, fill: '#93a0bb', hidden: true });
      const b1 = S.box(5, 178, 434, 112, 'Source: KafkaIO.Read -> … -> WithKeys', { corner: true, tone: 'idle', hidden: true });
      const b2 = S.box(464, 178, 328, 112, 'Combine.perKey -> … -> KafkaIO.Write', { corner: true, tone: 'idle', hidden: true });
      const p1 = S.text(222, 276, 'Parallelism: 4', { size: 12, fill: '#ffffff', hidden: true });
      const p2 = S.text(628, 276, 'Parallelism: 4', { size: 12, fill: '#ffffff', hidden: true });
      const hash = S.arrow(439, 232, 462, 232, { color: '#e6ebf5', hidden: true });
      const hl = S.text(451, 304, 'HASH', { size: 11.5, weight: 700, tone: 'info', hidden: true });
      const down = (A, idx, dx) => A.all(idx.map(i => A.move(ch[i], 10 + i * 106 + dx, 210, 800)));
      return [
        { cap: 'Your Beam pipeline: seven named steps.', run: async A => { await A.stagger(ch, c => A.show(c), 150); await A.show(lk); } },
        { cap: 'Beam fuses steps that need no shuffle. On Flink, fusion is operator chaining, so these four steps become <b>one task</b>, one box in the UI.',
          run: async A => { await A.hide(lk); await A.show(lab2); ch.slice(0, 4).forEach(c => c.parentNode.appendChild(c)); await A.all(A.show(b1), down(A, [0, 1, 2, 3], 0)); } },
        { cap: '<code>Combine.perKey</code> needs all values of a key in one place, so Beam shuffles there. In Flink that is a <b>HASH</b> edge.',
          run: async A => { await down(A, [4], 40); await A.show(hash, hl); await A.stagger([0, 1, 2], () => A.fly('', [420, 232], [478, 232], { r: 6, tone: 'warn', ms: 500 }), 150); } },
        { cap: 'The rest fuses into a second box. Beam gives every step the same <code>--parallelism</code>, so both boxes show "Parallelism: 4".',
          run: async A => { [ch[4], ch[5], ch[6]].forEach(c => c.parentNode.appendChild(c)); await A.all(A.show(b2), down(A, [5, 6], 40)); await A.show(p1, p2); } }
      ];
    }
  });

  /* L15: Beam bundles on Flink */
  D('bundles', {
    title: 'Bundles: when @FinishBundle runs', h: 270,
    build(S) {
      S.arrow(20, 140, 198, 140, { label: 'elements', ly: -12 });
      S.box(200, 40, 330, 200, 'DoFn', { corner: true });
      const bun = S.box(218, 66, 196, 156, 'current bundle', { corner: true, tone: 'muted', dash: '5 4' });
      const cnt = S.text(472, 100, 'elements: 0', { size: 12, weight: 700 });
      const tim = S.text(472, 128, 'elapsed: 0 ms', { size: 12, weight: 700 });
      S.text(472, 168, 'maxBundleSize 1000\nmaxBundleTimeMills\n1000', { size: 10.5, fill: '#93a0bb' });
      const db = S.box(640, 105, 145, 70, 'Database', { sub: 'batched writes' });
      S.arrow(530, 140, 638, 140, { label: '@FinishBundle', ly: -12 });
      const slot = k => [248 + (k % 4) * 44, 110 + Math.floor(k / 4) * 40];
      const add = async (A, n) => {
        const tks = await A.stagger([...Array(n)], (_, k) => A.fly('e', [20, 140], slot(k), { tone: 'warn', keep: true, ms: 700, via: [[200, 140]] }).then(t => { A.text(cnt, 'elements: ' + (k + 1)); return t; }), 260);
        A.text(cnt, 'elements: ' + n);
        return tks;
      };
      const flush = async (A, tks, n, tone) => {
        S.tone(bun, tone); await A.wait(250);
        await A.all(tks.map(t => A.move(t, 560, 140, 600)));
        tks.forEach(t => S.remove(t));
        await A.fly('batch of ' + n, [560, 140], [db.L, 140], { tone: 'ok', ms: 500 });
        S.tone(db, 'ok'); S.tone(bun, 'muted'); A.text(cnt, 'elements: 0'); A.text(tim, 'elapsed: 0 ms');
      };
      const st = {};
      return [
        { cap: 'On Flink streaming, Beam hands elements to your DoFn in <b>bundles</b>.' },
        { cap: 'Elements collect in the current bundle. Your <code>@ProcessElement</code> can buffer writes instead of calling the database each time.',
          run: async A => { st.t = await A.all(add(A, 5), A.count(tim, 0, 600, 1900, v => `elapsed: ${Math.round(v)} ms`)); } },
        { cap: 'The bundle closes after <code>maxBundleSize</code> elements or <code>maxBundleTimeMills</code>, whichever comes first. <code>@FinishBundle</code> then flushes the buffer as one batch.',
          run: async A => { await A.count(tim, 600, 1000, 900, v => `elapsed: ${Math.round(v)} ms`); await flush(A, st.t[0], 5, 'ok'); } },
        { cap: 'A checkpoint barrier also forces the bundle to finish early, so the output is consistent with the snapshot.',
          run: async A => {
            const tks = await add(A, 3);
            await A.fly('ckpt', [20, 140], [bun.L, 140], { shape: 'bar', tone: 'purple', ms: 900, h: 34 });
            await flush(A, tks, 3, 'purple');
          } }
      ];
    }
  });
})(window.FA);
