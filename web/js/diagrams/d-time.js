/* Animated diagrams for lessons 8-9 (event time, watermarks, windows, late data) */
(function (FA) {
  'use strict';
  const D = FA.registerDiagram;
  const C = FA.DG.SOLID;

  /* L8: watermarks flowing with the stream */
  D('watermark-flow', {
    title: 'Watermarks flow with the events and close windows', h: 290,
    build(S) {
      const st = { a: 0, b: 0 };
      const src = S.box(20, 95, 130, 64, 'Source', { sub: 'max ts: none yet' });
      S.arrow(150, 127, 478, 127);
      S.text(315, 108, 'bounded out-of-orderness = 2 s', { size: 11, fill: '#93a0bb' });
      S.box(480, 28, 300, 205, 'Window operator', { corner: true });
      const wm = S.text(700, 43, 'watermark: none', { tone: 'purple', weight: 700, size: 12 });
      const bA = S.box(493, 68, 140, 152, 'window [0,5)', { corner: true, tone: 'muted' });
      const bB = S.box(642, 68, 128, 152, 'window [5,10)', { corner: true, tone: 'muted' });
      const slot = (b, k) => [b.bx + 38 + (k % 2) * 56, b.by + 48 + Math.floor(k / 2) * 38];
      const ev = async (A, ts) => {
        const inA = ts < 5, b = inA ? bA : bB, k = inA ? st.a++ : st.b++;
        await A.fly(String(ts), [150, 127], slot(b, k), { via: [[470, 127]], keep: true, tone: 'warn', ms: 1000 });
      };
      const flag = async (A, w, max) => {
        S.sub(src, `max ts ${max} → W(${w})`);
        await A.fly('W' + w, [150, 127], [478, 127], { shape: 'bar', tone: 'purple', ms: 900, h: 26 });
        A.text(wm, 'watermark: ' + w);
      };
      return [
        { cap: 'The number on each event is its <b>event timestamp in seconds</b>, not a value. Window [0,5) collects timestamps from 0 s up to (not including) 5 s. The source tracks the highest timestamp seen and emits <b>watermark = max − 2 s</b> (the extra − 1 ms is ignored here).' },
        { cap: 'Events 1, 3 and 2 go into window [0,5). Watermark W(1) follows them: "no event with a timestamp ≤ 1 is still coming".',
          run: async A => { await A.stagger([1, 3, 2], ts => ev(A, ts), 350); await flag(A, 1, 3); } },
        { cap: 'Event 6 raises the max to 6, so the watermark advances to W(4). Window [0,5) is still open.', run: async A => { await ev(A, 6); await flag(A, 4, 6); } },
        { cap: 'Event 4 arrives after 6, so it is out of order. But the watermark is only 4, and window [0,5) needs it to pass 5, so the window is still open and 4 counts.', run: A => ev(A, 4) },
        { cap: 'Event 8 pushes the watermark to W(6). It has passed 5, so Flink now assumes every event of [0,5) has arrived: the window <b>fires</b> with 4 events (1, 3, 2, 4) and its state is deleted. Window [5,10) stays open.',
          run: async A => { await ev(A, 8); await flag(A, 6, 8); S.tone(bA, 'ok'); await A.fly('count = 4 → sink', [bA.cx, bA.B - 6], [bA.cx, 262], { tone: 'ok', keep: true, ms: 700 }); } }
      ];
    }
  });

  /* L8: idle source holds back the minimum watermark */
  D('idle-min', {
    title: 'One idle partition stalls every window', h: 260,
    build(S) {
      const p0 = S.box(20, 30, 175, 64, 'partition 0', { sub: 'W = 10:00' });
      const p1 = S.box(20, 166, 175, 64, 'partition 1', { sub: 'W = 10:00' });
      const op = S.box(370, 95, 230, 70, 'Window operator', { sub: 'W = min(10:00, 10:00) = 10:00' });
      S.arrow(195, 62, 368, 118); S.arrow(195, 198, 368, 142);
      S.arrow(600, 130, 668, 130);
      const out = S.text(728, 130, 'windows\nwaiting', { size: 12.5, fill: '#93a0bb', weight: 700 });
      return [
        { cap: 'An operator with two inputs uses the <b>minimum</b> of their watermarks.' },
        { cap: 'Partition 0 keeps receiving data, so its watermark advances: 10:01, 10:02, … 10:05. The operator\'s watermark does not move.',
          run: async A => {
            for (let m = 1; m <= 5; m++) {
              await A.fly('W', [195, 62], [368, 118], { shape: 'bar', tone: 'purple', ms: 500, h: 22 });
              S.sub(p0, 'W = 10:0' + m); S.sub(op, `W = min(10:0${m}, 10:00) = 10:00`);
            }
          } },
        { cap: 'Partition 1 has been quiet all night. Its watermark is stuck at 10:00, so the minimum is stuck, and <b>no window fires</b>. The Watermarks tab shows a value stuck in the past.',
          run: async A => { S.tone(p1, 'muted'); S.sub(p1, 'W = 10:00 (no data)'); S.tone(op, 'warn'); A.text(out, 'no output'); S.tone(out, 'bad'); await A.blink(p1, 2); } },
        { cap: '<code>withIdleness(1 min)</code> marks partition 1 idle, so it is left out of the minimum. The watermark jumps to 10:05 and the windows fire.',
          run: async A => {
            p1.r.setAttribute('stroke-dasharray', '5 4'); S.sub(p1, 'idle → ignored');
            S.sub(op, 'W = 10:05 (partition 1 idle)'); S.tone(op, 'ok'); A.text(out, 'windows\nfire'); S.tone(out, 'ok');
            await A.stagger([0, 1, 2], () => A.fly('', [600, 130], [668, 130], { r: 6, tone: 'ok', ms: 450 }), 200);
          } }
      ];
    }
  });

  /* L9: window types */
  D('window-types', {
    title: 'The same events, three kinds of windows', h: 312,
    build(S) {
      const X = t => 150 + t * 50;
      const evs = [0.5, 1.2, 2.1, 2.6, 5.4, 5.9, 6.3, 9.6, 10.2, 11];
      S.line(140, 288, 760, 288, { color: '#3a4766' });
      for (let t = 0; t <= 12; t += 2) { S.line(X(t), 283, X(t), 293, { color: '#3a4766' }); S.text(X(t), 303, t + (t === 0 ? ' min' : ''), { size: 10.5, fill: '#93a0bb' }); }
      const rl = (y, t) => S.text(12, y, t, { anchor: 'start', size: 12, fill: '#93a0bb', weight: 600 });
      rl(40, 'events (one key)'); rl(95, 'Tumbling 4 min'); rl(149, 'Sliding 4 min,'); rl(165, 'every 2 min'); rl(232, 'Session, 2 min gap');
      const guides = evs.map(t => S.line(X(t), 48, X(t), 283, { color: '#1f2940', dash: '2 4', hidden: true }));
      const dots = evs.map(t => S.token(X(t), 40, '', { r: 6, tone: 'warn', hidden: true }));
      const inW = (a, b) => evs.filter(t => t >= a && t < b).length;
      const win = (a, b, y, h, tone) => {
        const g = S.g({ hidden: true });
        const r = S.rect(X(a) + 2, y, 0, h, { parent: g, tone, rx: 5 });
        const t = S.text((X(a) + X(b)) / 2, y + h / 2, inW(a, b) + ' events', { parent: g, size: 11, tone, weight: 700, hidden: true });
        return { g, r, t, w: X(b) - X(a) - 4 };
      };
      const tum = [[0, 4], [4, 8], [8, 12]].map(([a, b]) => win(a, b, 82, 26, 'info'));
      const sli = [[0, 4], [2, 6], [4, 8], [6, 10], [8, 12]].map(([a, b], i) => win(a, b, i % 2 ? 160 : 130, 25, 'purple'));
      const ses = [[0.5, 4.6], [5.4, 8.3], [9.6, 12]].map(([a, b]) => win(a, b, 218, 28, 'ok'));
      const grow = async (A, w) => { await A.show(w.g); await A.to(w.r, { width: w.w }, 600); await A.show(w.t); };
      return [
        { cap: 'Ten events of one key, at their event times.', run: async A => { await A.show(guides); await A.stagger(dots, d => A.show(d), 120); } },
        { cap: '<b>Tumbling</b> (4 min): fixed size, no overlap. Each event belongs to exactly one window.', run: A => A.stagger(tum, w => grow(A, w), 300) },
        { cap: '<b>Sliding</b> (4 min, every 2 min): the windows overlap, so each event belongs to 2 windows and is counted twice.', run: A => A.stagger(sli, w => grow(A, w), 280) },
        { cap: '<b>Session</b> (2 min gap): a window stays open while events keep coming, and closes after 2 minutes of silence. Each key gets its own sessions.', run: A => A.stagger(ses, w => grow(A, w), 350) }
      ];
    }
  });

  /* L9: late data options */
  D('late-data', {
    title: 'A late event: three ways to handle it', h: 290,
    build(S) {
      const st = {};
      S.box(20, 115, 120, 60, 'Kafka', { tone: 'info' });
      const op = S.box(210, 90, 230, 110, 'window [0, 5)', { corner: true });
      const fired = S.text(325, 132, 'fired: count = 4', { tone: 'ok', weight: 700 });
      S.text(325, 162, 'watermark = 7', { tone: 'purple', weight: 700, size: 12 });
      S.arrow(140, 145, 208, 145);
      const out = [['Default: dropped', 'numLateRecordsDropped = 0'], ['allowedLateness(30 s)', 'window re-fires'], ['sideOutputLateData(tag)', 'dead-letter topic']]
        .map(([t, s], i) => S.box(540, 18 + i * 94, 245, 64, t, { sub: s, tone: 'muted' }));
      out.forEach(b => S.arrow(440, 145, 538, b.cy, { color: '#3a4766' }));
      const toOp = A => A.fly('ts 3', [140, 145], [210, 145], { tone: 'warn', keep: true, ms: 700 });
      return [
        { cap: 'Window [0,5) already fired (count = 4), because the watermark passed 5. Now an event with timestamp 3 shows up: it is <b>late</b>.',
          run: async A => { st.t = await toOp(A); S.tone(op, 'warn'); } },
        { cap: '<b>Default</b>: the late element is dropped and counted in the <code>numLateRecordsDropped</code> metric.',
          run: async A => { await A.move(st.t, 540, out[0].cy, 800); await A.hide(st.t); S.tone(out[0], 'bad'); S.sub(out[0], 'numLateRecordsDropped = 1'); } },
        { cap: '<b>allowedLateness</b> keeps the window state a while longer. The late element <b>re-fires</b> the window with an updated result, so downstream must handle updates.',
          run: async A => { const t = await toOp(A); await A.move(t, 325, 132, 500); await A.hide(t); A.text(fired, 'fired again: count = 5'); await A.fly('count = 5', [440, 145], [540, out[1].cy], { tone: 'ok', ms: 800 }); S.tone(out[1], 'ok'); } },
        { cap: 'Elements later than the allowed lateness can go to a <b>side output</b> instead, for example a dead-letter topic you can inspect or repair.',
          run: async A => { const t = await toOp(A); await A.move(t, 540, out[2].cy, 900); await A.hide(t); S.tone(out[2], 'purple'); } }
      ];
    }
  });

  /* L8: event time vs processing time */
  D('event-vs-processing', {
    title: 'Event time vs processing time: when it happened vs when Flink saw it', h: 300,
    build(S) {
      const X = t => 135 + t * 50, yE = 62, yP = 212;
      const axis = (y, name, sub) => {
        S.line(125, y, 760, y, { color: '#3a4766' });
        for (let t = 0; t <= 12; t += 2) { S.line(X(t), y - 4, X(t), y + 4, { color: '#3a4766' }); S.text(X(t), y + 22, '10:00:' + String(t).padStart(2, '0'), { size: 10, fill: '#6f7c98' }); }
        S.text(12, y - 7, name, { anchor: 'start', size: 12, weight: 700, fill: '#93a0bb' });
        S.text(12, y + 9, sub, { anchor: 'start', size: 10.5, fill: '#6f7c98' });
      };
      axis(yE, 'event time', 'stored in the record');
      axis(yP, 'processing time', 'Flink\'s wall clock');
      const evs = [['A', 1, 2], ['B', 2, 3.2], ['C', 3, 9.5], ['D', 5, 6], ['E', 7, 7.8], ['F', 9, 10.6]];
      const top = evs.map(([id, e]) => S.token(X(e), yE, id, { tone: 'warn', hidden: true }));
      const links = evs.map(([id, e, p]) => S.line(X(e), yE + 13, X(p), yP - 13, { color: id === 'C' ? C.bad : '#3a4766', dash: '4 4', hidden: true }));
      const late = S.g({ hidden: true });
      S.rect(X(6.6) - 132, 128, 264, 24, { parent: late, rx: 12, fill: '#0e1422', stroke: C.bad, sw: 1 });
      S.text(X(6.6), 140, 'C: placed in a tunnel, arrives 6.5 s late', { parent: late, tone: 'bad', size: 11.5, weight: 700 });
      const order = S.g({ hidden: true });
      S.text(270, 270, 'event order:  A B C D E F', { parent: order, size: 12, mono: true, fill: '#e6ebf5' });
      S.text(560, 270, 'arrival order:  A B D E C F', { parent: order, size: 12, mono: true, fill: C.bad });
      const down = (A, i) => { const c = evs[i][0] === 'C'; return A.show(links[i]).then(() => A.fly(evs[i][0], [X(evs[i][1]), yE], [X(evs[i][2]), yP], { tone: c ? 'bad' : 'info', keep: true, ms: c ? 1300 : 800 })); };
      return [
        { cap: 'Every record carries a timestamp of <b>when it happened</b>: its <b>event time</b>. Here, six orders placed on phones between 10:00:01 and 10:00:09.',
          run: A => A.stagger(top, t => A.show(t), 160) },
        { cap: 'Each record reaches Flink a little later (network, Kafka, retries). The moment Flink handles it is its <b>processing time</b>.',
          run: A => A.stagger([0, 1, 3, 4, 5], i => down(A, i), 220) },
        { cap: 'Order C was placed in a tunnel with no signal, so it arrives 6.5 s late, after D and E. <b>Arrival order is not event order.</b>',
          run: async A => { await down(A, 2); await A.show(late, order); } },
        { cap: '<b>Event-time</b> windows group records by the top line, so the result is the same whether data arrives late, out of order, or is replayed next week. <b>Processing-time</b> windows group by the bottom line: simpler and faster, but C would be counted in the wrong second.' }
      ];
    }
  });

  /* L8: what the numbers in [0,5) mean */
  D('window-boundaries', {
    title: 'Reading [0,5): a range of event time, start included, end excluded', h: 300,
    build(S) {
      const X = t => 75 + t * 44, yA = 240;
      S.line(65, yA, 750, yA, { color: '#3a4766' });
      for (let t = 0; t <= 15; t++) {
        const big = t % 5 === 0;
        S.line(X(t), yA - (big ? 6 : 3), X(t), yA + (big ? 6 : 3), { color: '#3a4766' });
        S.text(X(t), yA + 18, big ? t + ' s' : t, { size: big ? 11 : 9.5, fill: big ? '#93a0bb' : '#56627e', weight: big ? 700 : null });
      }
      S.text(750, yA + 40, 'event time (seconds) →', { anchor: 'end', size: 10.5, fill: '#6f7c98' });
      const bk = [0, 5, 10].map(a => {
        const b = S.box(X(a) + 3, 108, X(a + 5) - X(a) - 6, 102, `[${a},${a + 5})`, { corner: true, tone: 'muted' });
        b.rng = S.text(b.cx, 194, `${a} s ≤ t < ${a + 5} s`, { size: 11, fill: '#93a0bb', mono: true });
        b.n = 0;
        return b;
      });
      const edge = S.line(X(5), 100, X(5), yA, { color: C.warn, sw: 2.2, dash: '5 4', hidden: true });
      const rule = S.text(415, 26, 'window start = t − (t mod 5)    12.4 − 2.4 = 10  →  [10,15)', { size: 12.5, mono: true, tone: 'purple', weight: 700, hidden: true });
      const ev = async (A, ts, tone) => {
        const b = bk[Math.floor(ts / 5)], k = b.n++;
        const dot = S.token(X(ts), yA, '', { r: 5, tone: tone || 'warn', hidden: true });
        await A.fly(Number.isInteger(ts) ? ts.toFixed(1) : String(ts), [X(ts), 62], [b.bx + 38 + k * 66, 152], { tone: tone || 'warn', keep: true, ms: 800 });
        await A.show(dot);
        S.tone(b, 'info');
      };
      return [
        { cap: 'A window name is a <b>time range</b>, not a list of values and not a count. <code>[0,5)</code> means "every event whose <b>timestamp</b> t is 0 s ≤ t &lt; 5 s". The square bracket <b>[</b> includes the start; the round bracket <b>)</b> excludes the end.' },
        { cap: 'Events with timestamps 0.0 s, 1.3 s and 4.999 s all fall in [0,5). Even 4.999 s is inside, because it is still before 5.',
          run: A => A.stagger([0.0, 1.3, 4.999], ts => ev(A, ts), 260) },
        { cap: 'An event at exactly <b>5.0 s</b> goes to [5,10), not [0,5): the end is excluded, so consecutive windows never overlap and every timestamp has exactly one tumbling window.',
          run: async A => { await A.show(edge); await ev(A, 5.0, 'accent'); } },
        { cap: 'Flink computes the window from the timestamp alone: <code>start = t − (t mod size)</code>. What is <i>in</i> the record (amount, user) plays no part. A window that receives no events is never created and emits nothing.',
          run: async A => { await A.hide(edge); await A.show(rule); await ev(A, 12.4, 'purple'); } },
        { cap: 'In a real job timestamps are epoch milliseconds, so [0,5) reads like <code>[10:00:00.000, 10:00:05.000)</code>. Its last instant is 10:00:04.999 (<code>window.maxTimestamp()</code>), which is why it fires once the watermark reaches <b>end − 1 ms</b>.',
          run: async A => {
            await A.hide(rule);
            const p = n => String(n).padStart(2, '0');
            bk.forEach((b, i) => { A.text(b, `[10:00:${p(i * 5)}, 10:00:${p(i * 5 + 5)})`); A.text(b.rng, `last ms: 10:00:${p(i * 5 + 4)}.999`); });
            await A.blink(bk[0], 1);
          } }
      ];
    }
  });

  /* L9: one event lands in several sliding windows */
  D('sliding-membership', {
    title: 'Sliding windows of 10 min, every 2 min: one event, five windows', h: 320,
    build(S) {
      const X = m => 130 + (m - 56) * 28.5, yA = 292;   /* m = minutes after 9:00 */
      const fmt = m => `${9 + Math.floor(m / 60)}:${String(m % 60).padStart(2, '0')}`;
      S.line(120, yA, 768, yA, { color: '#3a4766' });
      for (let m = 56; m <= 78; m += 2) { S.line(X(m), yA - 4, X(m), yA + 4, { color: '#3a4766' }); S.text(X(m), yA + 16, fmt(m), { size: 10, fill: '#93a0bb' }); }
      const evLine = S.line(X(67), 44, X(67), yA - 4, { color: C.warn, dash: '4 4', hidden: true });
      const evTk = S.token(X(67), 30, '10:07', { tone: 'warn', hidden: true });
      const rows = [56, 58, 60, 62, 64, 66, 68].map((s, i) => {
        const y = 56 + i * 32, hit = s <= 67 && 67 < s + 10;
        const g = S.g({ hidden: true });
        const r = S.rect(X(s) + 1, y, 0, 24, { parent: g, tone: hit ? 'purple' : 'muted', rx: 5 });
        const t = S.text((X(s) + X(s + 10)) / 2, y + 12, `[${fmt(s)}, ${fmt(s + 10)})`, { parent: g, size: 11, tone: hit ? 'purple' : 'muted', weight: 700, hidden: true });
        S.text(12, y + 12, hit ? '✓ contains it' : '✗ misses it', { parent: g, anchor: 'start', size: 11, weight: 700, fill: hit ? C.ok : '#6f7c98' });
        return { g, r, t, w: X(s + 10) - X(s) - 2, hit };
      });
      const grow = async (A, w) => { await A.show(w.g); await A.to(w.r, { width: w.w }, 550); await A.show(w.t); };
      return [
        { cap: 'One event with event time <b>10:07</b>. The windows are 10 minutes long, and a new one starts every 2 minutes.',
          run: A => A.show(evTk, evLine) },
        { cap: 'The windows starting at 9:58, 10:00, 10:02, 10:04 and 10:06 all cover 10:07, so Flink adds the event to <b>each</b> of them.',
          run: A => A.stagger(rows.filter(r => r.hit), w => grow(A, w), 260) },
        { cap: 'The window starting at 9:56 ends at 10:06 (excluded), and the one starting at 10:08 begins after the event. Neither gets it.',
          run: A => A.stagger(rows.filter(r => !r.hit), w => grow(A, w), 260) },
        { cap: 'Each event belongs to <b>size ÷ slide = 10 ÷ 2 = 5</b> windows, and each window fires its own result. That is 5× the work and state of a tumbling window, so keep the ratio small.',
          run: async A => { rows.filter(r => r.hit).forEach(r => r.r.setAttribute('stroke', C.ok)); await A.blink(evTk, 1); } }
      ];
    }
  });

  /* L9: session windows merge */
  D('session-merge', {
    title: 'Session windows (gap 30 min): each event opens a window, overlapping ones merge', h: 305,
    build(S) {
      const X = m => 130 + m * 5.9, yA = 268;          /* m = minutes after 9:50 */
      const fmt = m => { const t = 590 + m; return `${Math.floor(t / 60)}:${String(t % 60).padStart(2, '0')}`; };
      S.line(120, yA, 770, yA, { color: '#3a4766' });
      for (let m = 10; m <= 110; m += 10) { S.line(X(m), yA - 4, X(m), yA + 4, { color: '#3a4766' }); S.text(X(m), yA + 16, fmt(m), { size: 10, fill: '#93a0bb' }); }
      const rl = (y, t) => S.text(12, y, t, { anchor: 'start', size: 11.5, weight: 700, fill: '#93a0bb' });
      rl(36, 'events (key u1)'); rl(118, 'own window'); S.text(12, 134, '[t, t + 30 min)', { anchor: 'start', size: 10.5, fill: '#6f7c98' }); rl(214, 'sessions');
      const own = (m, row, tone) => {
        const g = S.g({ hidden: true }), c = tone === 'warn' ? C.warn : '#56627e';
        S.rect(X(m), 84 + row * 28, X(30) - X(0), 22, { parent: g, fill: 'none', stroke: c, dash: '4 3', rx: 5 });
        S.text(X(m + 15), 95 + row * 28, `[${fmt(m)}, ${fmt(m + 30)})`, { parent: g, size: 10.5, fill: tone === 'warn' ? C.warn : '#93a0bb' });
        const dot = S.token(X(m), 36, fmt(m), { tone: tone || 'info', hidden: true });
        return { g, dot };
      };
      const sess = a => {
        const g = S.g({ hidden: true });
        const r = S.rect(X(a), 199, 0, 30, { parent: g, tone: 'ok', rx: 6 });
        const t = S.text(X(a), 214, '', { parent: g, size: 11.5, tone: 'ok', weight: 700 });
        return { g, r, t };
      };
      const span = async (A, s, a, b) => { await A.to(s.r, { x: X(a), width: X(b) - X(a) }, 650); s.t._x = (X(a) + X(b)) / 2; S.setText(s.t, `[${fmt(a)}, ${fmt(b)})`); };
      const e1 = own(10, 0), e2 = own(20, 1), e3 = own(70, 0), e4 = own(45, 2, 'warn');
      const s1 = sess(10), s2 = sess(70);
      const fire = S.line(X(100), 190, X(100), yA, { color: C.purple, sw: 2.2, hidden: true });
      const fireT = S.text(X(100) - 6, 248, 'fires when watermark ≥ 11:30', { anchor: 'end', size: 11.5, tone: 'purple', weight: 700, hidden: true });
      const add = async (A, e) => { await A.show(e.dot); await A.show(e.g); };
      return [
        { cap: 'Session windows have no fixed start or size. Each event of a key opens its own small window <b>[t, t + gap)</b>. With a 30 min gap, the 10:00 event opens [10:00, 10:30), which becomes a session.',
          run: async A => { await add(A, e1); await A.show(s1.g); await span(A, s1, 10, 40); } },
        { cap: 'The 10:10 event opens [10:10, 10:40). It overlaps the session, so Flink <b>merges</b> them: the session now runs [10:00, 10:40).',
          run: async A => { await add(A, e2); await span(A, s1, 10, 50); } },
        { cap: 'The 11:00 event opens [11:00, 11:30). The first session ended at 10:40, so there is no overlap and a <b>second session</b> starts.',
          run: async A => { await add(A, e3); await A.show(s2.g); await span(A, s2, 70, 100); } },
        { cap: 'Now an out-of-order event from <b>10:35</b> arrives. Its window [10:35, 11:05) overlaps both sessions, so all of them merge into one: <b>[10:00, 11:30)</b>. Two sessions become one.',
          run: async A => { await add(A, e4); await A.hide(s2.g); await span(A, s1, 10, 100); } },
        { cap: 'A session ends at <b>last event + gap</b>. It fires when the watermark passes 11:30, meaning 30 minutes of event time with no activity for this key. Every key has its own sessions.',
          run: A => A.show(fire, fireT) }
      ];
    }
  });

  /* L8: why a window must wait: closing too early loses events */
  D('close-too-early', {
    title: 'When may window [0,5) give its answer?', h: 330,
    build(S) {
      const order = [1, 3, 2, 6, 4, 8];
      S.text(12, 28, 'arrival order →', { anchor: 'start', size: 11.5, weight: 700, fill: '#93a0bb' });
      const q = order.map((t, i) => S.token(150 + i * 40, 28, String(t), { tone: 'warn' }));
      const row = (y, name, sub) => {
        S.text(12, y + 30, name, { anchor: 'start', size: 12.5, weight: 700, fill: '#e6ebf5' });
        S.text(12, y + 48, sub, { anchor: 'start', size: 10.5, fill: '#6f7c98' });
        const w0 = S.box(160, y, 250, 76, 'window [0,5)', { corner: true, tone: 'muted' });
        const w1 = S.box(420, y, 150, 76, 'window [5,10)', { corner: true, tone: 'muted' });
        const res = S.box(600, y + 8, 185, 60, 'waiting…', { tone: 'muted', size: 12.5, sub: 'result of [0,5)' });
        return { w0, w1, res, n0: 0, n1: 0, y };
      };
      const r1 = row(70, 'No waiting', 'close on first event ≥ 5');
      const r2 = row(190, 'Wait 2 s', 'close on first event ≥ 7');
      const note = S.text(472, 290, 'waiting: 6 is not ≥ 7', { size: 11, tone: 'warn', weight: 700, hidden: true });
      const formula = S.text(400, 314, 'This waiting rule is the watermark:  watermark = highest timestamp seen − bound', { size: 12.5, tone: 'purple', weight: 700, hidden: true });
      const put = (A, r, t) => {
        const early = t < 5, k = early ? r.n0++ : r.n1++, b = early ? r.w0 : r.w1;
        return A.fly(String(t), [150 + order.indexOf(t) * 40, 28], [b.bx + 28 + k * (early ? 52 : 44), r.y + 48], { tone: 'warn', keep: true, ms: 750 });
      };
      const fire = async (A, r, n, ok) => { S.tone(r.w0, ok ? 'ok' : 'warn'); S.tone(r.res, ok ? 'ok' : 'bad'); A.text(r.res, `fired: count = ${n}`); await A.blink(r.res, 1); };
      return [
        { cap: 'Events reach the window operator in this order: 1, 3, 2, 6, 4, 8. The numbers are event timestamps in seconds. The right answer for [0,5) is <b>4 events</b> (1, 2, 3, 4). But Flink never knows whether more are still on the way.' },
        { cap: '<b>No waiting</b>: close [0,5) as soon as any event ≥ 5 shows up. Event 6 closes it with count = 3.',
          run: async A => { for (const t of [1, 3, 2, 6]) await put(A, r1, t); await fire(A, r1, 3, false); } },
        { cap: 'Then event 4 arrives, but [0,5) has already given its answer. Event 4 is <b>lost</b>, and the count of 3 was wrong. Fast, but incorrect.',
          run: async A => { const t = await put(A, r1, 4); S.tone(t, 'bad'); await A.blink(t, 2); await A.hide(t); A.text(r1.res, 'count = 3 (wrong)'); } },
        { cap: '<b>Wait 2 s</b>: only close [0,5) once an event ≥ 5 + 2 = 7 shows up. Event 6 is not enough, so the window keeps waiting, and event 4 still gets in.',
          run: async A => { for (const t of [1, 3, 2, 6]) await put(A, r2, t); await A.show(note); await put(A, r2, 4); S.tone(r2.w0, 'info'); } },
        { cap: 'Event 8 is ≥ 7, so [0,5) closes with the correct count of 4. The answer came a little later, but it is right.',
          run: async A => { await A.hide(note); await put(A, r2, 8); await fire(A, r2, 4, true); } },
        { cap: 'A <b>watermark</b> is exactly this waiting rule, written down. With a bound of 2 s, the watermark trails the highest timestamp by 2 s, and a window closes when the watermark passes its end. A bigger bound loses fewer events but answers later.',
          run: A => A.show(formula) }
      ];
    }
  });

  /* L8: the watermark as a moving line on the event-time axis */
  D('watermark-timeline', {
    title: 'The watermark is a line on the event-time axis: left of it counts as complete', h: 300,
    build(S) {
      const X = t => 60 + t * 56, yA = 200, B = 2;
      const shade = S.rect(X(0), 92, 0, yA - 92, { fill: 'rgba(124,92,255,0.14)', stroke: 'none', rx: 0 });
      S.line(50, yA, 770, yA, { color: '#3a4766' });
      for (let t = 0; t <= 12; t++) S.text(X(t), yA + 18, t + (t === 0 ? ' s' : ''), { size: 10.5, fill: '#93a0bb' });
      const bands = [0, 5].map(a => {
        const b = S.box(X(a) + 2, 140, X(a + 5) - X(a) - 4, 34, `window [${a},${a + 5})`, { tone: 'muted', size: 12 });
        return b;
      });
      const mk = (color, label, y) => {
        const g = S.g({ hidden: true });
        S.line(0, y + 10, 0, yA, { parent: g, color, sw: 2.4 });
        g.t = S.text(0, y, label, { parent: g, size: 11.5, weight: 700, fill: color });
        return g;
      };
      const wm = mk(C.purple, 'watermark', 44), mx = mk(C.warn, 'highest seen', 44);
      S.arrow(4, 74, B * 56 - 4, 74, { parent: wm, color: C.purple, both: true, sw: 1.4 });
      S.text(B * 28, 64, 'bound 2 s', { parent: wm, size: 10.5, fill: C.purple, weight: 700 });
      const arr = S.text(788, 16, '', { anchor: 'end', size: 11.5, weight: 700, fill: '#93a0bb' });
      const clock = S.text(788, 284, '', { anchor: 'end', size: 11.5, weight: 700, fill: '#93a0bb' });
      const st = { max: -1, n0: 0 };
      const setMarks = async (A, max) => {
        const w = max - B;
        A.text(mx.t, 'highest seen: ' + max); A.text(wm.t, 'watermark: ' + w);
        await A.all(A.move(mx, X(max), 0, 700), A.move(wm, X(w), 0, 700), A.to(shade, { width: Math.max(0, X(w) - X(0)) }, 700));
      };
      const ev = async (A, t, o = {}) => {
        A.text(arr, 'arriving: event ' + t);
        const tk = await A.fly(String(t), [X(t), 16], [X(t), o.late ? 252 : yA - 14], { tone: o.late ? 'bad' : 'warn', keep: true, ms: 750 });
        if (t < 5 && !o.late) st.n0++;
        if (t > st.max) { st.max = t; await setMarks(A, t); }
        return tk;
      };
      return [
        { cap: 'Draw event time as a line. The watermark is a <b>marker on that line</b>. Everything to its left is treated as complete: "no more events from there are expected".' },
        { cap: 'Event 3 arrives. Flink tracks the <b>highest timestamp seen</b> (orange). The watermark (purple) sits the bound behind it: 3 − 2 = <b>1</b>. The shaded part is "done".',
          run: async A => { await ev(A, 3); await A.show(mx, wm); } },
        { cap: 'Events 2.2 and 1.5 arrive. They are older than 3, so the highest timestamp does not change, and neither does the watermark. <b>The watermark never moves backwards.</b> Both land right of the line, so they are on time.',
          run: async A => { await ev(A, 2.2); await ev(A, 1.5); } },
        { cap: 'Event 6 moves the highest timestamp to 6 and the watermark to 4. Window [0,5) is still right of the line, so it stays open. Then event 4.5 arrives: right of the line, on time.',
          run: async A => { await ev(A, 6); await ev(A, 4.5); } },
        { cap: 'Event 8 pushes the watermark to 6. The whole of window [0,5) is now left of the line, so it <b>fires</b> with 4 events (3, 2.2, 1.5, 4.5).',
          run: async A => { await ev(A, 8); S.tone(bands[0], 'ok'); A.text(bands[0], `[0,5) fired: count = ${st.n0}`); await A.blink(bands[0], 1); } },
        { cap: 'Event 3.5 arrives now. It lands <b>left of the watermark</b>, in a window that has already fired: it is <b>late</b>. Level 9 shows what you can do with late events.',
          run: async A => { const t = await ev(A, 3.5, { late: true }); await A.blink(t, 2); } },
        { cap: 'Then no events arrive for 30 seconds. The wall clock moves on, but nothing on this line moves. <b>Only newer events move the watermark</b>, not the clock. Window [5,10) waits until they come.',
          run: async A => { A.text(arr, 'arriving: (nothing)'); await A.count(clock, 0, 30, 1400, v => 'wall clock: +' + Math.round(v) + ' s'); await A.blink(wm, 2); } }
      ];
    }
  });

  /* L8: watermarks travel through the job, per subtask (not per key) */
  D('watermark-propagation', {
    title: 'Watermarks travel through the job: every subtask keeps its own', h: 300,
    build(S) {
      const src = [S.box(20, 42, 200, 70, 'source[0]', { sub: 'reads partition 0\nW = 10:05' }), S.box(20, 188, 200, 70, 'source[1]', { sub: 'reads partition 1\nW = 10:03' })];
      const win = [S.box(500, 18, 285, 124, 'window[0]', { corner: true }), S.box(500, 160, 285, 124, 'window[1]', { corner: true })];
      const ln = win.map(w => [0, 1, 2].map(i => S.text(w.bx + 16, w.by + 42 + i * 22, '', { anchor: 'start', size: 12, mono: true, fill: i === 2 ? C.purple : '#e6ebf5', weight: i === 2 ? 700 : null })));
      const keys = win.map((w, i) => S.text(w.bx + 16, w.by + 108, i ? 'keys here: carol, dave, …' : 'keys here: alice, bob, …', { anchor: 'start', size: 11.5, fill: '#93a0bb', hidden: true }));
      src.forEach((s, i) => win.forEach((w, j) => S.arrow(220, s.cy, 498, w.cy + (i ? 14 : -14), { color: '#2a3550', label: i === 0 && j === 0 ? 'keyBy: any source can feed any window' : null, ly: -12 })));
      const wmv = [['10:05', '10:03'], ['10:05', '10:03']];
      const fill = (j, srcW) => {
        const m = srcW[0] < srcW[1] ? srcW[0] : srcW[1];
        S.setText(ln[j][0], 'from source[0]: ' + srcW[0]); S.setText(ln[j][1], 'from source[1]: ' + srcW[1]); S.setText(ln[j][2], 'W = min = ' + m);
      };
      const send = (A, i, label) => A.all(win.map(w => A.fly('W ' + label, [220, src[i].cy], [498, w.cy + (i ? 14 : -14)], { shape: 'pill', tone: 'purple', ms: 900 })));
      return [
        { cap: 'Two source subtasks each read one Kafka partition. Each one makes its <b>own</b> watermark from the events it has read: 10:05 and 10:03. It sends a fresh one every 200 ms (<code>pipeline.auto-watermark-interval</code>).' },
        { cap: 'After <code>keyBy</code>, any source can send events to any window subtask. So every source sends its watermark to <b>every</b> downstream subtask.',
          run: async A => { await A.all(send(A, 0, '10:05'), A.wait(250).then(() => send(A, 1, '10:03'))); fill(0, wmv[0]); fill(1, wmv[1]); } },
        { cap: 'Each window subtask remembers the latest watermark from each input and uses the <b>minimum</b>: 10:03. Source[1] might still send events up to 10:03, so nothing later than that can be called complete.',
          run: async A => { win.forEach(w => S.tone(w, 'purple')); await A.blink(ln[0][2], 1); } },
        { cap: 'A watermark belongs to a <b>subtask</b>, not to a key. alice, bob and every other key on window[0] share the same watermark, so their windows that end by 10:03 fire together, even if alice had no events for an hour.',
          run: A => A.show(keys) },
        { cap: 'Source[1] reads newer data and its watermark moves to 10:06. Both window subtasks now take min(10:05, 10:06) = <b>10:05</b>, and every window ending by 10:05 fires.',
          run: async A => { S.sub(src[1], 'reads partition 1\nW = 10:06'); await send(A, 1, '10:06'); fill(0, ['10:05', '10:06']); fill(1, ['10:05', '10:06']); win.forEach(w => S.tone(w, 'ok')); } }
      ];
    }
  });
})(window.FA);
