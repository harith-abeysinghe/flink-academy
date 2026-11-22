/* Animated diagrams for lessons 1-5 (foundations, architecture, chaining, slots, data exchange) */
(function (FA) {
  'use strict';
  const D = FA.registerDiagram;
  const C = FA.DG.SOLID;
  const lane = (S, x, y, txt) => S.text(x, y, txt, { anchor: 'start', size: 11, weight: 700, fill: '#93a0bb' });

  /* L1: batch vs streaming */
  D('batch-vs-stream', {
    title: 'Batch waits, streaming reacts', h: 270,
    build(S) {
      const st = {};
      lane(S, 20, 24, 'BATCH');
      lane(S, 20, 156, 'STREAMING');
      S.box(30, 64, 110, 52, 'Orders', { tone: 'info' });
      const store = S.box(220, 48, 220, 84, 'Storage (files / table)', { corner: true });
      const job = S.box(480, 62, 130, 56, 'Nightly job', { tone: 'muted' });
      const report = S.box(650, 62, 135, 56, 'Report', { sub: 'nothing yet' });
      S.arrow(140, 90, 218, 90); S.arrow(440, 90, 478, 90); S.arrow(610, 90, 648, 90);
      S.box(30, 184, 110, 52, 'Orders', { tone: 'info' });
      const op = S.box(300, 182, 170, 56, 'Flink operator', { sub: 'count orders' });
      const res = S.box(590, 182, 195, 56, 'Live result', { sub: 'orders so far: 0' });
      S.arrow(140, 210, 298, 210); S.arrow(470, 210, 588, 210);
      return [
        { cap: 'The same order events, handled two ways.' },
        { cap: '<b>Batch</b>: events pile up in storage. Nothing is computed yet.',
          run: async A => { st.bt = await A.stagger([1, 2, 3, 4], (n, i) => A.fly(String(n), [140, 90], [252 + i * 32, 104], { tone: 'warn', keep: true, ms: 650 }), 300); } },
        { cap: '<b>Streaming</b>: each event is processed the moment it arrives, so the result is always current.',
          run: async A => {
            await A.stagger([1, 2, 3, 4], async n => { await A.fly(String(n), [140, 210], [590, 210], { tone: 'warn', ms: 1100, ease: p => p }); S.sub(res, 'orders so far: ' + n); S.tone(res, 'ok'); }, 500);
            S.sub(res, 'orders so far: 4'); S.tone(op, 'info');
          } },
        { cap: 'At midnight the batch job finally reads everything and writes yesterday\'s report. All day long, its answer was hours old.',
          run: async A => {
            S.tone(job, 'busy');
            await A.all(st.bt.map(t => A.move(t, 545, 90, 800)));
            await A.hide(st.bt);
            S.tone(job, 'muted'); S.sub(report, 'total: 4 (at 00:00)'); S.tone(report, 'warn');
          } }
      ];
    }
  });

  /* L1: event time vs processing time */
  D('event-vs-processing-time', {
    title: 'One GPS ping, two timestamps', h: 275,
    build(S) {
      const X = s => 70 + s * 13;
      S.box(20, 50, 140, 60, 'Truck GPS', { sub: 'creates the ping' });
      S.box(280, 40, 200, 80, 'Tunnel', { tone: 'muted', sub: 'no signal: the ping waits', dash: '5 4' });
      S.box(620, 50, 160, 60, 'Flink', { tone: 'info', sub: 'receives the ping' });
      S.arrow(160, 80, 278, 80); S.arrow(480, 80, 618, 80);
      const tsl = S.text(90, 28, 'event time 10:00:05', { tone: 'ok', weight: 700, hidden: true });
      const prl = S.text(700, 28, 'processing time 10:00:40', { tone: 'warn', weight: 700, hidden: true });
      const ping = S.token(195, 80, 'ping', { tone: 'ok', hidden: true });
      S.line(60, 200, 740, 200, { color: '#3a4766' });
      for (let s = 0; s <= 50; s += 10) { S.line(X(s), 195, X(s), 205, { color: '#3a4766' }); S.text(X(s), 218, '10:00:' + String(s).padStart(2, '0'), { size: 10.5, fill: '#93a0bb' }); }
      const evM = S.g({ hidden: true }); S.line(X(5), 168, X(5), 200, { parent: evM, color: C.ok, sw: 2.5 }); S.text(X(5), 158, 'happened', { parent: evM, size: 11, tone: 'ok' });
      const prM = S.g({ hidden: true }); S.line(X(40), 168, X(40), 200, { parent: prM, color: C.warn, sw: 2.5 }); S.text(X(40), 158, 'arrived', { parent: prM, size: 11, tone: 'warn' });
      const br = S.arrow(X(5) + 6, 180, X(40) - 6, 180, { both: true, color: '#93a0bb', label: '35 s late', hidden: true });
      const evW = S.g({ hidden: true }); S.rect(X(0), 234, X(10) - X(0), 20, { parent: evW, tone: 'ok', rx: 4 }); S.text(X(10) + 8, 244, 'event-time window (correct)', { parent: evW, anchor: 'start', size: 11.5, tone: 'ok' });
      const prW = S.g({ hidden: true }); S.rect(X(40), 234, X(50) - X(40), 20, { parent: prW, tone: 'bad', rx: 4 }); S.text(X(40) - 8, 244, 'processing-time window (wrong bucket)', { parent: prW, anchor: 'end', size: 11.5, tone: 'bad' });
      return [
        { cap: 'A truck sends a GPS ping. The ping carries the time it was created: its <b>event time</b>.', run: A => A.show(tsl, ping, evM) },
        { cap: 'The truck drives through a tunnel. The ping waits in the phone\'s buffer.', run: async A => { await A.move(ping, 380, 80, 1000); await A.blink(ping, 2); } },
        { cap: 'It reaches Flink at 10:00:40. That is its <b>processing time</b>.', run: async A => { await A.move(ping, 600, 80, 900); await A.show(prl, prM, br); } },
        { cap: 'An event-time window puts the ping in 10:00:00 to 10:00:10, where it belongs. A processing-time window would count it in 10:00:40 to 10:00:50.', run: async A => { await A.show(evW); await A.show(prW); } }
      ];
    }
  });

  /* L1: Beam pipeline to Flink cluster */
  D('beam-to-flink', {
    title: 'From a Beam pipeline to running Flink tasks', h: 240,
    build(S) {
      const st = {};
      const b1 = S.box(15, 50, 130, 70, 'Beam pipeline', { sub: 'your code', tone: 'muted' });
      const b2 = S.box(175, 50, 130, 70, 'FlinkRunner', { sub: 'translates', tone: 'muted' });
      const b3 = S.box(335, 50, 130, 70, 'Flink JobGraph', { sub: 'operators + edges', tone: 'muted' });
      const b4 = S.box(495, 50, 130, 70, 'JobManager', { sub: 'schedules tasks', tone: 'muted' });
      const t1 = S.box(655, 30, 130, 45, 'TaskManager 1', { sub: 'slots', tone: 'muted' });
      const t2 = S.box(655, 95, 130, 45, 'TaskManager 2', { sub: 'slots', tone: 'muted' });
      const kf = S.box(655, 180, 130, 44, 'Kafka / DB', { tone: 'muted' });
      S.arrow(145, 85, 173, 85); S.arrow(305, 85, 333, 85); S.arrow(465, 85, 493, 85);
      S.arrow(625, 75, 653, 54); S.arrow(625, 95, 653, 117);
      S.arrow(720, 142, 720, 178, { both: true });
      return [
        { cap: 'You write a Beam pipeline: PCollections, ParDo, GroupByKey, windows.', run: async A => { S.tone(b1, 'accent'); await A.wait(400); } },
        { cap: 'With <code>--runner=FlinkRunner</code>, Beam translates the pipeline into a native Flink JobGraph.',
          run: async A => {
            const tk = st.tk = S.token(80, 150, 'pipeline', { tone: 'accent', hidden: true });
            await A.show(tk); await A.move(tk, 240, 150, 700); S.tone(b2, 'purple');
            A.text(tk, 'JobGraph'); await A.move(tk, 400, 150, 700); S.tone(b3, 'info');
          } },
        { cap: 'The JobGraph is submitted to the JobManager, which schedules its tasks into TaskManager slots.',
          run: async A => {
            await A.move(st.tk, 560, 150, 600); S.tone(b4, 'info'); await A.hide(st.tk);
            await A.all(A.fly('task', [560, 85], [720, 52], { tone: 'info' }), A.fly('task', [560, 85], [720, 117], { tone: 'info' }));
            [t1, t2].forEach(t => { S.tone(t, 'ok'); S.sub(t, 'running tasks'); });
          } },
        { cap: 'The tasks read from and write to Kafka or databases. From here on it is a normal Flink job: same Web UI, slots, backpressure and checkpoints.',
          run: async A => {
            S.tone(kf, 'info');
            for (let i = 0; i < 3; i++) { await A.fly('', [720, 202], [720, 125], { r: 6, tone: 'warn', ms: 450 }); await A.fly('', [720, 125], [720, 202], { r: 6, tone: 'ok', ms: 450 }); }
          } }
      ];
    }
  });

  /* L2: cluster architecture and job submission */
  D('cluster-arch', {
    title: 'Inside a Flink cluster: what happens when you submit a job', h: 340,
    build(S) {
      const client = S.box(15, 45, 115, 60, 'Client', { sub: 'flink run' });
      S.box(160, 15, 625, 122, 'JobManager (master process)', { corner: true, tone: 'ghost', dash: '6 4' });
      const disp = S.box(180, 45, 180, 72, 'Dispatcher', { sub: 'REST + Web UI' });
      const rm = S.box(380, 45, 180, 72, 'ResourceManager', { sub: 'manages slots' });
      const jm = S.box(580, 45, 185, 72, 'JobMaster', { sub: 'one per job:\nscheduling, checkpoints' });
      S.box(160, 195, 290, 128, 'TaskManager 1', { corner: true });
      S.box(495, 195, 290, 128, 'TaskManager 2', { corner: true });
      const slots = [178, 313, 513, 648].map(x => S.box(x, 225, 120, 82, 'slot', { tone: 'muted', sub: 'empty' }));
      S.arrow(130, 75, 178, 75);
      S.arrow(452, 266, 493, 266, { both: true, label: 'data', ly: -12 });
      S.arrow(470, 139, 400, 193, { dash: '4 4', label: 'slots', lx: 26 });
      S.arrow(672, 139, 640, 193, { dash: '4 4', label: 'tasks, barriers', lx: 52 });
      const sc = s => [s.cx, s.cy];
      return [
        { cap: 'A Flink cluster: one <b>JobManager</b> (the master) and <b>TaskManagers</b> (workers) that offer task slots.' },
        { cap: 'The client builds the JobGraph from your <code>main()</code> and submits it to the Dispatcher.', run: async A => { await A.fly('JobGraph', [client.cx, client.B + 14], [disp.cx, disp.B + 10], { tone: 'accent', ms: 900 }); S.tone(disp, 'info'); } },
        { cap: 'The Dispatcher starts a <b>JobMaster</b> for this job.', run: async A => { await A.fly('start job', [disp.cx, disp.B + 10], [jm.cx, jm.B + 10], { tone: 'info', via: [[470, 128]], ms: 900 }); S.tone(jm, 'info'); } },
        { cap: 'The JobMaster asks the ResourceManager for slots, and the ResourceManager finds free slots on the TaskManagers.',
          run: async A => {
            await A.fly('need 4 slots', [jm.cx, jm.T - 2], [rm.cx, rm.T - 2], { tone: 'purple', ms: 800 }); S.tone(rm, 'purple');
            await A.all(slots.map(s => A.fly('', [rm.cx, rm.B], sc(s), { r: 6, tone: 'purple', ms: 800 })));
            slots.forEach(s => { S.tone(s, 'ok'); S.sub(s, 'free'); });
          } },
        { cap: 'The JobMaster deploys tasks into those slots. Each slot now runs a slice of the pipeline.',
          run: async A => {
            await A.stagger(slots, s => A.fly('task', [jm.cx, jm.B], sc(s), { tone: 'info', ms: 900 }), 150);
            slots.forEach(s => { S.tone(s, 'info'); S.label(s, 'Source → Map'); S.sub(s, '→ Window → Sink'); });
          } },
        { cap: 'TaskManagers stream records to each other. The JobMaster coordinates checkpoints by injecting barriers at the sources.',
          run: async A => {
            const bar = A.fly('barrier', [jm.cx, jm.B], [slots[0].cx, slots[0].T + 6], { shape: 'bar', tone: 'purple', ms: 1100, via: [[640, 170], [250, 170]] });
            for (let i = 0; i < 3; i++) { await A.fly('', [slots[1].R - 6, 266], [slots[2].L + 6, 266], { r: 6, tone: 'warn', ms: 450 }); await A.fly('', [slots[2].L + 6, 278], [slots[1].R - 6, 278], { r: 6, tone: 'ok', ms: 450 }); }
            await bar;
          } }
      ];
    }
  });

  /* L2: StreamGraph -> JobGraph -> ExecutionGraph */
  D('graphs-pipeline', {
    title: 'StreamGraph → JobGraph → ExecutionGraph', h: 318,
    build(S) {
      const lbl = (y, t) => lane(S, 20, y, t);
      lbl(20, 'STREAMGRAPH: one node per operator');
      const names = ['Kafka source', 'Parse', 'Window', 'Sink'];
      const n = names.map((t, i) => S.box(60 + i * 180, 32, 130, 36, t, { tone: 'muted', size: 12, hidden: true }));
      const na = [0, 1, 2].map(i => S.arrow(n[i].R, 50, n[i + 1].L - 2, 50, { hidden: true, label: i === 1 ? 'keyBy' : null, ly: -11 }));
      lbl(108, 'JOBGRAPH: chains of operators = the boxes in the Web UI');
      const vA = S.box(60, 120, 310, 42, 'Source: Kafka source → Parse', { tone: 'purple', size: 12, hidden: true });
      const vB = S.box(420, 120, 310, 42, 'Window → Sink', { tone: 'info', size: 12, hidden: true });
      const eAB = S.arrow(372, 141, 418, 141, { hidden: true, label: 'HASH', lcolor: C.info });
      lbl(198, 'EXECUTIONGRAPH: one box per subtask');
      const sub = [[140, 212, 'Source → Parse [1/2]', 'purple'], [140, 262, 'Source → Parse [2/2]', 'purple'], [490, 212, 'Window → Sink [1/2]', 'info'], [490, 262, 'Window → Sink [2/2]', 'info']]
        .map(([x, y, t, tone]) => S.box(x, y, 170, 34, t, { tone, size: 12, hidden: true }));
      const downs = [S.arrow(335, 164, 300, 210, { dash: '4 4', hidden: true }), S.arrow(575, 164, 575, 210, { dash: '4 4', hidden: true })];
      const pairs = [[0, 2], [0, 3], [1, 2], [1, 3]];
      const lines = pairs.map(([a, b]) => S.arrow(sub[a].R, sub[a].cy, sub[b].L - 2, sub[b].cy, { color: '#3d8bfd', hidden: true }));
      return [
        { cap: 'Your DataStream calls become a <b>StreamGraph</b>: one node per operator.', run: async A => { await A.stagger(n, x => A.show(x), 220); await A.show(na); } },
        { cap: 'Chaining merges forward-connected operators with the same parallelism into one <b>JobVertex</b>. keyBy breaks the chain, so this job has two boxes in the Web UI.',
          run: async A => { S.tone(n[0], 'purple'); S.tone(n[1], 'purple'); S.tone(n[2], 'info'); S.tone(n[3], 'info'); await A.wait(400); await A.show(vA, vB); await A.show(eAB); } },
        { cap: 'The JobMaster expands each JobVertex by its parallelism into the <b>ExecutionGraph</b>: one ExecutionVertex per subtask.', run: async A => { await A.show(downs); await A.stagger(sub, x => A.show(x), 180); } },
        { cap: 'The HASH edge becomes all-to-all channels: every upstream subtask can send to every downstream subtask. Each ExecutionVertex is deployed into a slot.',
          run: async A => {
            await A.show(lines);
            await A.all(pairs.map(([a, b], i) => A.wait(i * 200).then(() => A.fly('', [sub[a].R, sub[a].cy], [sub[b].L, sub[b].cy], { r: 6, tone: 'warn', ms: 800 }))));
          } }
      ];
    }
  });

  /* L2: job lifecycle state machine */
  D('job-lifecycle', {
    title: 'Job status: the states you see in the UI', h: 250,
    build(S) {
      const P = { CREATED: [20, 105], RUNNING: [190, 105], FINISHED: [400, 25], CANCELLING: [400, 105], CANCELED: [600, 105], FAILING: [400, 190], RESTARTING: [190, 190], FAILED: [600, 190] };
      const b = {};
      for (const [k, [x, y]] of Object.entries(P)) b[k] = S.box(x, y, 130, 40, k, { tone: 'muted', mono: true, size: 12.5 });
      const E = {
        cr: [[150, 125], [188, 125]], rf: [[320, 115], [398, 48]], rc: [[320, 125], [398, 125]], cc: [[530, 125], [598, 125]],
        rx: [[320, 138], [398, 196]], xr: [[398, 220], [322, 220]], rr: [[255, 190], [255, 147]], xf: [[530, 210], [598, 210]]
      };
      for (const e of Object.values(E)) S.arrow(e[0][0], e[0][1], e[1][0], e[1][1]);
      S.text(360, 236, 'restart strategy', { size: 10.5, fill: '#93a0bb' });
      const go = (A, e) => A.fly('', E[e][0], E[e][1], { r: 6, tone: 'white', ms: 550 });
      return [
        { cap: 'A submitted job starts in <code>CREATED</code>. It stays there (or in SCHEDULED) while it waits for slots.', run: async A => { S.tone(b.CREATED, 'warn'); await A.wait(300); } },
        { cap: 'Slots found, tasks deployed, state restored: the job is <code>RUNNING</code>.', run: async A => { await go(A, 'cr'); S.tone(b.CREATED, 'muted'); S.tone(b.RUNNING, 'ok'); } },
        { cap: 'A task throws an exception. The job goes <code>FAILING</code>, then <code>RESTARTING</code>, as the restart strategy allows.',
          run: async A => { await go(A, 'rx'); S.tone(b.RUNNING, 'muted'); S.tone(b.FAILING, 'bad'); await A.wait(500); await go(A, 'xr'); S.tone(b.FAILING, 'muted'); S.tone(b.RESTARTING, 'warn'); } },
        { cap: '…and back to <code>RUNNING</code>, restored from the last completed checkpoint.', run: async A => { await go(A, 'rr'); S.tone(b.RESTARTING, 'muted'); S.tone(b.RUNNING, 'ok'); } },
        { cap: 'The end states: a bounded job that read all its input is <code>FINISHED</code>; a cancel goes through CANCELLING to <code>CANCELED</code>; when the restart strategy gives up, the job is <code>FAILED</code>.',
          run: async A => {
            await A.all(go(A, 'rf'), go(A, 'rc'), A.wait(300).then(() => go(A, 'rx')));
            S.tone(b.FINISHED, 'ok'); S.tone(b.CANCELLING, 'warn'); S.tone(b.FAILING, 'bad'); S.tone(b.RUNNING, 'muted');
            await A.all(go(A, 'cc'), go(A, 'xf'));
            S.tone(b.CANCELLING, 'muted'); S.tone(b.FAILING, 'muted'); S.tone(b.CANCELED, 'info'); S.tone(b.FAILED, 'bad');
          } }
      ];
    }
  });

  /* L3: chained vs unchained */
  D('chaining', {
    title: 'Chained vs not chained: what one record costs', h: 280,
    build(S) {
      lane(S, 20, 22, 'CHAINED: one task, one thread');
      S.box(110, 34, 560, 90, 'Task (thread 1)', { corner: true, tone: 'ghost', dash: '5 4' });
      const pA = S.box(150, 60, 150, 50, 'Parse'), eA = S.box(470, 60, 150, 50, 'Enrich');
      S.arrow(40, 85, 148, 85); S.arrow(300, 85, 468, 85, { label: 'method call', ly: -12 }); S.arrow(620, 85, 700, 85);
      const okT = S.text(745, 85, '≈ free', { tone: 'ok', weight: 700, hidden: true });
      lane(S, 20, 152, 'NOT CHAINED: two tasks, two threads');
      const pB = S.box(110, 180, 150, 54, 'Parse', { sub: 'thread 1' });
      const buf = S.box(320, 170, 180, 74, 'network buffer', { corner: true, tone: 'muted' });
      for (let i = 0; i < 3; i++) S.rect(335 + i * 52, 194, 44, 34, { tone: 'ghost', rx: 4 });
      const eB = S.box(560, 180, 150, 54, 'Enrich', { sub: 'thread 2' });
      S.arrow(40, 207, 108, 207); S.arrow(260, 207, 318, 207, { label: 'serialize', ly: -14 }); S.arrow(500, 207, 558, 207, { label: 'deserialize', ly: -14 });
      const badT = S.text(755, 207, 'CPU, copies\n+ latency', { tone: 'warn', weight: 700, size: 12, hidden: true });
      return [
        { cap: 'The same two operators, Parse then Enrich, run in two different ways.' },
        { cap: '<b>Chained</b>: Parse hands each record straight to Enrich with a method call in the same thread. No copy, no network.',
          run: async A => { await A.stagger([1, 2, 3, 4], () => A.fly('r', [60, 85], [690, 85], { tone: 'warn', ms: 1000, ease: p => p }), 260); await A.show(okT); } },
        { cap: '<b>Not chained</b>: every record is serialized into a network buffer; the full buffer is handed to another thread and deserialized again.',
          run: async A => {
            const sq = await A.stagger([0, 1, 2], async i => {
              await A.fly('r', [60, 207], [185, 207], { tone: 'warn', ms: 500 });
              const s = S.token(250, 207, '', { shape: 'sq', tone: 'muted' });
              await A.move(s, 357 + i * 52, 211, 500); return s;
            }, 450);
            S.tone(buf, 'warn'); await A.wait(250);
            await A.all(sq.map(s => A.move(s, 600, 207, 700)));
            sq.forEach(s => S.remove(s));
            S.tone(buf, 'muted');
            await A.stagger([0, 1, 2], () => A.fly('r', [635, 240], [635, 262], { tone: 'warn', ms: 400 }), 120);
            await A.show(badT);
          } },
        { cap: 'That extra work per record is why Flink chains whenever the rules allow it, and why you should not disable chaining in production without a reason.',
          run: async A => { S.tone(pA, 'ok'); S.tone(eA, 'ok'); S.tone(pB, 'warn'); S.tone(eB, 'warn'); await A.wait(300); } }
      ];
    }
  });

  /* L4: slot sharing */
  D('slot-sharing', {
    title: 'Slot sharing: why the job needs 4 slots, not 8', h: 300,
    build(S) {
      const rowY = { src: 55, map: 125, sink: 195 };
      S.text(20, 72, 'Source p=2', { anchor: 'start', size: 12, fill: '#93a0bb' });
      S.text(20, 142, 'Map p=4', { anchor: 'start', size: 12, fill: '#93a0bb' });
      S.text(20, 212, 'Sink p=2', { anchor: 'start', size: 12, fill: '#93a0bb' });
      S.box(378, 14, 202, 276, 'TaskManager 1', { corner: true, tone: 'ghost', dash: '5 4' });
      S.box(588, 14, 202, 276, 'TaskManager 2', { corner: true, tone: 'ghost', dash: '5 4' });
      const slotX = [390, 486, 600, 696];
      const slots = slotX.map((x, i) => { const r = S.rect(x, 40, 84, 238, { tone: 'muted', rx: 8 }); S.text(x + 42, 264, 'slot ' + (i + 1), { size: 11, fill: '#6f7c98' }); return r; });
      const chip = (x, y, t, tone) => S.box(x, y, 56, 32, t, { tone, size: 11.5 });
      const src = [0, 1].map(i => chip(100 + i * 62, rowY.src, 'Src ' + (i + 1), 'info'));
      const map = [0, 1, 2, 3].map(i => chip(100 + i * 62, rowY.map, 'Map ' + (i + 1), 'purple'));
      const sink = [0, 1].map(i => chip(100 + i * 62, rowY.sink, 'Sink ' + (i + 1), 'ok'));
      const into = (A, c, s, row) => A.move(c, slotX[s] + 14, row + 16, 600);
      const need = S.text(170, 258, 'slots needed = highest parallelism = 4', { tone: 'ok', weight: 700, size: 12.5, hidden: true });
      return [
        { cap: 'The job: Source with parallelism 2, Map with 4, Sink with 2. The cluster: 2 TaskManagers × 2 slots.' },
        { cap: 'With slot sharing, one slot can hold <b>one subtask of every task</b>: a whole slice of the pipeline.',
          run: async A => { await into(A, src[0], 0, rowY.src); await into(A, map[0], 0, rowY.map); await into(A, sink[0], 0, rowY.sink); } },
        { cap: 'Fill the rest. Map has the highest parallelism (4), so the job needs exactly 4 slots, not 2 + 4 + 2 = 8.',
          run: async A => {
            await A.all(into(A, src[1], 1, rowY.src), A.wait(150).then(() => into(A, map[1], 1, rowY.map)), A.wait(300).then(() => into(A, sink[1], 1, rowY.sink)));
            await A.all(into(A, map[2], 2, rowY.map), A.wait(150).then(() => into(A, map[3], 3, rowY.map)));
          } },
        { cap: 'Slots 3 and 4 run only a Map subtask. The heavy work is spread evenly, and the light source and sink subtasks ride along in slots 1 and 2.',
          run: async A => { S.tone(slots[2], 'warn'); S.tone(slots[3], 'warn'); await A.show(need); } }
      ];
    }
  });

  /* L4: slots are not CPUs */
  D('slots-vs-cpu', {
    title: '8 slots, 2 cores: who actually runs?', h: 262,
    build(S) {
      const tm = S.box(20, 18, 760, 112, 'TaskManager: 8 slots', { corner: true, tone: 'ghost', dash: '5 4' });
      const sl = [...Array(8)].map((_, i) => S.box(38 + i * 92, 46, 80, 60, 'task ' + (i + 1), { tone: 'muted', sub: 'waiting' }));
      const cores = [S.box(220, 196, 160, 50, 'CPU core 1'), S.box(420, 196, 160, 50, 'CPU core 2')];
      const ln = sl.map((s, i) => S.line(cores[i % 2].cx, 196, s.cx, 106, { color: C.busy, sw: 2, hidden: true }));
      const msg = S.text(400, 160, 'each task gets ≈ 25% of a core', { tone: 'warn', weight: 700, hidden: true });
      return [
        { cap: 'A TaskManager with 8 slots but only 2 CPU cores. Flink happily accepts 8 subtasks.' },
        { cap: 'Only 2 threads can run at any moment. The others wait for their turn.',
          run: async A => {
            for (let r = 0; r < 4; r++) {
              const pair = [2 * r, 2 * r + 1];
              pair.forEach(i => { S.tone(sl[i], 'busy'); S.sub(sl[i], 'running'); });
              await A.show(pair.map(i => ln[i])); await A.wait(550);
              pair.forEach(i => { S.tone(sl[i], 'muted'); S.sub(sl[i], 'waiting'); });
              await A.hide(pair.map(i => ln[i]));
            }
          } },
        { cap: 'Every task gets about 2/8 of a core. Everything looks busy and slow, and there is no error message to tell you why.', run: async A => { sl.forEach(s => { S.tone(s, 'warn'); S.sub(s, 'slow'); }); await A.show(msg); } },
        { cap: 'Start with <b>slots ≈ cores</b>. Here that means 2 slots, each with a full core.',
          run: async A => {
            await A.hide(sl.slice(2), msg);
            S.label(tm, 'TaskManager: 2 slots');
            [0, 1].forEach(i => { S.tone(sl[i], 'ok'); S.sub(sl[i], 'full core'); });
            ln[0].p.setAttribute('stroke', C.ok); ln[1].p.setAttribute('stroke', C.ok);
            await A.show(ln[0], ln[1]);
          } }
      ];
    }
  });

  /* L5: network buffers + credit-based flow control */
  D('network-exchange', {
    title: 'A non-forward edge: buffers and credits', h: 290,
    build(S) {
      const st = {};
      const snd = S.box(20, 100, 150, 70, 'Sender', { sub: 'map[0]' });
      const ob = S.box(215, 95, 170, 80, 'output buffer', { corner: true, tone: 'muted' });
      for (let i = 0; i < 4; i++) S.rect(228 + i * 38, 122, 32, 36, { tone: 'ghost', rx: 4 });
      S.arrow(385, 135, 503, 135, { label: 'TCP or local', ly: -12 });
      S.box(505, 95, 120, 80, 'input queue', { corner: true, tone: 'muted' });
      for (let i = 0; i < 2; i++) S.rect(517 + i * 54, 122, 44, 40, { tone: 'ghost', rx: 4 });
      const rcv = S.box(650, 100, 135, 70, 'Receiver', { sub: 'window[1]' });
      S.arrow(690, 172, 120, 174, { via: [[690, 252], [120, 252]], color: C.purple, dash: '5 4' });
      S.text(405, 266, 'credits = "I have room for N more buffers"', { size: 11.5, tone: 'purple' });
      const cred = S.text(95, 80, 'credits: 0', { tone: 'purple', size: 12.5, weight: 700 });
      const free = S.text(717, 80, 'free buffers: 2', { size: 12.5, fill: '#93a0bb', weight: 700 });
      const credit = A => A.fly('1', [690, 172], [120, 172], { via: [[690, 252], [120, 252]], tone: 'purple', ms: 1300 });
      const fill = (A, keep) => A.stagger([0, 1, 2, 3], i => A.fly('', [172, 135], [244 + i * 38, 140], { shape: 'sq', tone: 'warn', keep: true, ms: 420 }), 180);
      const ship = async (A, sq, j) => {
        await A.all(sq.map(s => A.move(s, 539 + j * 54, 142, 700)));
        sq.forEach(s => S.remove(s));
        return S.token(539 + j * 54, 142, 'buf', { tone: 'warn', w: 38 });
      };
      return [
        { cap: 'Sender and receiver are different subtasks, connected by a non-forward edge (HASH, REBALANCE…).' },
        { cap: 'The receiver announces <b>credits</b>: how many buffers it can accept right now.',
          run: async A => { await A.stagger([0, 1], () => credit(A), 500); S.setText(cred, 'credits: 2'); } },
        { cap: 'The sender serializes records into a network buffer. A buffer is sent when it is full, or when <code>execution.buffer-timeout</code> (100 ms) expires. Each buffer sent uses up one credit.',
          run: async A => {
            st.bufs = [];
            for (let j = 0; j < 2; j++) {
              const sq = await fill(A); st.bufs.push(await ship(A, sq, j));
              S.setText(cred, 'credits: ' + (1 - j)); S.setText(free, 'free buffers: ' + (1 - j));
            }
          } },
        { cap: 'The receiver is slow and its queue stays full, so no new credits come back. The sender cannot ship its next buffer: it is <b>backpressured</b>.',
          run: async A => { S.tone(rcv, 'busy'); S.sub(rcv, 'slow'); st.sq = await fill(A); S.tone(snd, 'bp'); S.sub(snd, 'BACKPRESSURED'); S.tone(cred, 'bad'); await A.blink(cred, 2); } },
        { cap: 'When the receiver finishes a buffer, it frees a slot and returns a credit. Data flows again, one buffer per credit.',
          run: async A => {
            await A.move(st.bufs[0], 717, 135, 600); await A.hide(st.bufs[0]); S.setText(free, 'free buffers: 1');
            await credit(A); S.setText(cred, 'credits: 1'); S.tone(cred, 'purple'); S.tone(snd, 'box'); S.sub(snd, 'map[0]');
            await A.move(st.bufs[1], 539, 142, 500);
            await ship(A, st.sq, 1);
            S.setText(cred, 'credits: 0'); S.setText(free, 'free buffers: 0');
          } }
      ];
    }
  });
})(window.FA);
