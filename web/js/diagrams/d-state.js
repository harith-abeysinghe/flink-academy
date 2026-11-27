/* Animated diagrams for lessons 6-9 (ordering, broadcast, state, time, checkpoints) */
(function (FA) {
  'use strict';
  const D = FA.registerDiagram;
  const C = FA.DG.SOLID;
  const lane = (S, x, y, txt) => S.text(x, y, txt, { anchor: 'start', size: 11, weight: 700, fill: '#93a0bb' });

  /* L6: FIFO per channel */
  D('fifo-channels', {
    title: 'Order is guaranteed per channel, not per key', h: 275,
    build(S) {
      const st = {};
      const u1 = S.box(30, 40, 130, 54, 'Sender u1', { sub: 'map[1]' });
      const u2 = S.box(30, 176, 130, 54, 'Sender u2', { sub: 'map[2]' });
      const r = S.box(560, 108, 170, 54, 'Receiver', { sub: 'window[1] (owns key A)' });
      const c1 = S.arrow(160, 67, 558, 126, { label: 'channel u1 → receiver (FIFO)', ly: -14 });
      const c2 = S.arrow(160, 203, 558, 146, { label: 'channel u2 → receiver (FIFO)', ly: 16 });
      S.text(440, 238, 'arrival order:', { anchor: 'end', size: 12, fill: '#93a0bb' });
      const send = (A, lbl, from, i, tone, ms) => {
        const tk = S.token(from[0], from[1], lbl, { tone });
        return A.path(tk, [[558, 136], [475 + i * 44, 238]], ms).then(() => tk);
      };
      const warn = S.text(640, 238, 'A3 arrived before A2', { tone: 'bad', weight: 700, size: 12.5, anchor: 'start', hidden: true });
      return [
        { cap: 'Two sender subtasks feed one receiver. Each sender→receiver pair is a <b>channel</b>, and every channel is FIFO.' },
        { cap: 'u1 sends A1, then B1. They arrive in the same order, because a channel never reorders.',
          run: async A => { await A.stagger([['A1', 'info'], ['B1', 'warn']], ([l, t], i) => send(A, l, [160, 67], i, t, 1200), 500); } },
        { cap: 'Next, u1 sends A2 and, a moment later, u2 sends A3. They travel on <b>different channels</b>, and u1\'s channel is congested.',
          run: async A => {
            const p2 = send(A, 'A2', [160, 67], 3, 'info', 2600);
            await A.wait(350);
            st.a3 = await send(A, 'A3', [160, 203], 2, 'info', 1000);
            st.a2 = await p2;
          } },
        { cap: 'A3 overtook A2. Per-key order holds only when every record of the key comes from the <b>same</b> sender, for example when Kafka is partitioned by that key.',
          run: async A => { S.stroke(st.a2, C.bad, 3); S.stroke(st.a3, C.bad, 3); c2.p.setAttribute('stroke', C.warn); await A.show(warn); } }
      ];
    }
  });

  /* L6: broadcast state */
  D('broadcast-state', {
    title: 'Broadcast state: every instance keeps its own copy of the rules', h: 325,
    build(S) {
      S.box(20, 30, 140, 54, 'Rules topic', { tone: 'purple', sub: 'tiny, rare updates' });
      S.box(20, 232, 140, 54, 'Orders topic', { tone: 'info', sub: 'huge, constant' });
      const ins = [0, 1, 2].map(i => S.box(400, 30 + i * 101, 220, 70, `process[${i}]`, { sub: 'rules: { }' }));
      const alerts = S.box(680, 131, 105, 70, 'Alerts', { tone: 'muted' });
      ins.forEach((b, i) => {
        S.arrow(160, 57, 398, b.cy - 10, { color: C.purple, dash: '5 4', label: i === 0 ? 'BROADCAST' : null, lcolor: C.purple, ly: -12 });
        S.arrow(160, 259, 398, b.cy + 12, { color: '#3d6fbf', label: i === 2 ? 'HASH (keyBy customer)' : null, lcolor: C.info, ly: 14 });
        S.arrow(620, b.cy, 678, alerts.cy, { color: '#3a4766' });
      });
      return [
        { cap: 'A tiny rules stream and a big orders stream meet in a (Keyed)BroadcastProcessFunction with 3 parallel instances.' },
        { cap: 'A new rule arrives. The BROADCAST edge sends a copy to <b>every</b> instance, and each one stores it in its broadcast state.',
          run: async A => { await A.all(ins.map(b => A.fly('R1', [160, 57], [398, b.cy - 10], { tone: 'purple', ms: 1000 }))); ins.forEach(b => { S.sub(b, 'rules: { R1: amount > 1000 }'); S.tone(b, 'purple'); }); await A.wait(300); ins.forEach(b => S.tone(b, 'box')); } },
        { cap: 'Orders are keyed as usual: each order goes to <b>one</b> instance, which checks it against its read-only copy of the rules.',
          run: async A => {
            const orders = [['$50', 0], ['$4200', 1], ['$90', 2]];
            await A.stagger(orders, async ([l, i]) => {
              await A.fly(l, [160, 259], [398, ins[i].cy + 12], { tone: 'info', ms: 900 });
              if (i === 1) { S.tone(ins[1], 'warn'); await A.fly('alert', [620, ins[1].cy], [678, alerts.cy], { tone: 'bad', ms: 600 }); S.tone(alerts, 'bad'); }
            }, 400);
          } },
        { cap: 'Instances cannot talk to each other, so only the broadcast side may change broadcast state, and every instance must apply the same updates. Each instance also checkpoints its own copy: keep it small.',
          run: async A => { ins.forEach(b => { S.tone(b, 'purple'); S.sub(b, 'rules: { R1 } · own copy checkpointed'); }); await A.wait(300); } }
      ];
    }
  });

  /* L7: keyed state + operator state */
  D('keyed-state', {
    title: 'Keyed state lives with its key; operator state lives with the instance', h: 292,
    build(S) {
      const src = S.box(20, 105, 140, 70, 'Kafka source', { sub: 'offset p0 = 41' });
      S.text(90, 196, 'operator state\n(per instance)', { size: 11, fill: '#93a0bb' });
      const sub = [S.box(330, 18, 230, 124, 'window[0]', { corner: true }), S.box(330, 156, 230, 124, 'window[1]', { corner: true })];
      S.text(655, 150, 'keyed state\n(per key)', { size: 11, fill: '#93a0bb' });
      S.arrow(160, 140, 328, 80, { label: 'HASH', ly: -10 }); S.arrow(160, 140, 328, 218);
      const owner = { A: 0, C: 0, E: 0, G: 0, B: 1, D: 1, F: 1 };
      const row = {}, cnt = {}, used = [0, 0];
      for (const k of 'ACEGBDF') {
        const s = owner[k];
        row[k] = S.text(350, sub[s].by + 42 + used[s]++ * 22, '', { anchor: 'start', size: 12.5, mono: true, hidden: true, fill: '#e6ebf5' });
        cnt[k] = 0;
      }
      let off = 41;
      const rec = async (A, k, ms) => {
        const s = sub[owner[k]];
        await A.fly(k, [160, 140], [s.L + 200, s.cy + 8], { tone: k === 'A' ? 'warn' : 'info', ms: ms || 800 });
        cnt[k]++; off++;
        A.text(row[k], `${k} → count ${cnt[k]}`); await A.show(row[k]);
        S.sub(src, 'offset p0 = ' + off);
      };
      return [
        { cap: 'After <code>keyBy</code>, each key lives on exactly one subtask, and so does that key\'s state.' },
        { cap: 'Each record updates the state of <b>its own key</b>. Flink sets the current key before calling your function, so <code>state.value()</code> returns that key\'s value.',
          run: async A => { for (const k of 'ABACBA') await rec(A, k, 700); } },
        { cap: 'The source has state too, but it belongs to the parallel instance, not to a key: <b>operator state</b> such as Kafka offsets per partition.',
          run: async A => { S.tone(src, 'purple'); await A.blink(src.s, 2); } },
        { cap: 'Keys keep coming (new customers, new order IDs). Without State TTL or timers that call <code>clear()</code>, keyed state grows forever.',
          run: async A => { S.tone(src, 'box'); await A.stagger([...'DEFG'], k => rec(A, k, 500), 220); sub.forEach(s => S.tone(s, 'warn')); } }
      ];
    }
  });

  /* L7: one state field, a different value per key (Flink sets the current key) */
  D('current-key', {
    title: 'One state field, one value per key: Flink picks the row', h: 300,
    build(S) {
      lane(S, 20, 20, 'INCOMING RECORDS');
      S.arrow(20, 80, 188, 80);
      S.box(190, 30, 340, 104, 'Your Dedup function (one object for all keys)', { corner: true });
      const code = [
        S.text(206, 64, 'if (seen.value() == null) {', { anchor: 'start', mono: true, size: 12 }),
        S.text(206, 86, '  seen.update(true); out.collect(e);', { anchor: 'start', mono: true, size: 12 }),
        S.text(206, 108, '} // else: drop the duplicate', { anchor: 'start', mono: true, size: 12 })
      ];
      const hi = (t, on) => t.setAttribute('fill', on ? C.warn : '#e6ebf5');
      const ck = S.text(360, 151, 'current key: (none)', { tone: 'purple', weight: 700, size: 12.5 });
      S.box(190, 166, 340, 124, 'Keyed state "seen" (Flink keeps one row per key)', { corner: true, tone: 'muted' });
      const ROW = { A: 206, B: 234, C: 262 };
      const hl = S.rect(206, ROW.A - 12, 308, 24, { tone: 'purple', rx: 6, hidden: true });
      const row = {
        A: S.text(226, ROW.A, 'key A → true', { anchor: 'start', mono: true, size: 12.5 }),
        B: S.text(226, ROW.B, 'key B → (empty)', { anchor: 'start', mono: true, size: 12.5 }),
        C: S.text(226, ROW.C, 'key C → (empty)', { anchor: 'start', mono: true, size: 12.5 })
      };
      const outB = S.box(610, 40, 170, 56, 'forwarded', { tone: 'ok', sub: 'out.collect(e)' });
      const dropB = S.box(610, 200, 170, 56, 'dropped', { tone: 'bad', sub: 'duplicate' });
      S.arrow(530, 70, 608, 68); S.arrow(530, 110, 608, 222);
      let tk;
      const arrive = (A, k) => A.fly(k, [30, 80], [168, 80], { tone: 'warn', keep: true, ms: 800 });
      const leave = async (A, box) => { await A.move(tk, box.cx, box.cy, 700); await A.hide(tk); S.remove(tk); await A.blink(box, 1); };
      return [
        { cap: 'One subtask handles <b>many keys</b>, but your function has only <b>one</b> <code>seen</code> field. Behind that field, Flink keeps a table with a separate value for every key.' },
        { cap: 'A record with key <b>B</b> arrives. Before it calls <code>processElement</code>, Flink sets the <b>current key</b> to B, so every state call now points at row B.',
          run: async A => { tk = await arrive(A, 'B'); A.text(ck, 'current key: B'); hl.setAttribute('y', ROW.B - 12); await A.show(hl); } },
        { cap: '<code>seen.value()</code> reads row B: it is empty (null), so this is the first B. <code>seen.update(true)</code> writes to <b>row B only</b>, and the record is forwarded.',
          run: async A => {
            hi(code[0], 1); await A.fly('null', [470, ROW.B], [470, 64], { tone: 'muted', ms: 700 });
            hi(code[0], 0); hi(code[1], 1);
            await A.fly('true', [470, 86], [470, ROW.B], { tone: 'ok', ms: 700 });
            A.text(row.B, 'key B → true'); row.B.setAttribute('fill', C.ok);
            await leave(A, outB); hi(code[1], 0);
          } },
        { cap: 'Next comes a record with key <b>A</b>. Flink switches the current key to A, so the <b>same line of code</b> now reads row A.',
          run: async A => { tk = await arrive(A, 'A'); A.text(ck, 'current key: A'); await A.to(hl, { y: ROW.A - 12 }, 450); } },
        { cap: '<code>seen.value()</code> returns <b>true</b> for A, because an earlier A already set it. The record is a duplicate and is dropped. Rows B and C were not touched.',
          run: async A => {
            hi(code[0], 1); await A.fly('true', [470, ROW.A], [470, 64], { tone: 'ok', ms: 700 });
            hi(code[0], 0); hi(code[2], 1); await leave(A, dropB);
          } },
        { cap: 'Takeaway: you never pass the key yourself, and you cannot read another key\'s state from here. State calls always go to the key of the record (or timer) being processed.' }
      ];
    }
  });

  /* L7: the five keyed state types fed the same events */
  D('keyed-state-types', {
    title: 'Five keyed state types, fed the same three events', h: 350,
    build(S) {
      S.text(20, 28, 'events for key "user-7":', { anchor: 'start', size: 12, fill: '#93a0bb', weight: 600 });
      const EV = ['tea 5', 'cake 3', 'tea 4'];
      const chips = EV.map((e, i) => S.token(232 + i * 72, 28, e, { tone: 'muted' }));
      const TYPES = [
        ['ValueState<Integer>', 'update(price)', 'last write wins', ['5', '3', '4']],
        ['ListState<Order>', 'add(order)', 'grows until cleared', ['[tea 5]', '[tea 5, cake 3]', '[tea 5, cake 3, tea 4]']],
        ['MapState<String, Integer>', 'put(item, price)', 'get/put one entry', ['{tea=5}', '{tea=5, cake=3}', '{tea=4, cake=3}']],
        ['ReducingState<Integer>', 'add(price), reduce = sum', 'merged on every add', ['5', '8', '12']],
        ['AggregatingState<Integer, Double>', 'add(price), aggregate = avg', 'accumulator ≠ result', ['acc{sum 5, n 1} → 5.0', 'acc{sum 8, n 2} → 4.0', 'acc{sum 12, n 3} → 4.0']]
      ];
      const rows = TYPES.map((t, i) => {
        const y = 52 + i * 54;
        S.box(20, y, 258, 44, t[0], { sub: t[1], size: 12.5, subMono: true });
        const r = S.box(290, y, 292, 44, '(empty)', { tone: 'muted', mono: true, size: 12, weight: 500 });
        S.text(596, y + 22, t[2], { anchor: 'start', size: 11.5, fill: '#93a0bb' });
        return r;
      });
      const note = S.text(400, 336, 'Every row is per key: a record for "user-8" starts again from (empty) in all five.', { tone: 'ok', weight: 700, size: 12.5, hidden: true });
      const feed = async (A, i) => {
        S.tone(chips[i], 'warn');
        await A.stagger(rows, (r, j) => A.fly('', [chips[i]._tx, 40], [r.bx + 18, r.cy], { r: 6, tone: 'warn', ms: 650 })
          .then(() => { S.tone(r, 'info'); S.label(r, TYPES[j][3][i]); }), 90);
        S.tone(chips[i], 'ok');
      };
      return [
        { cap: 'Each type is a different <b>shape</b> of per-key memory. Watch what each one holds after the same three events for key user-7 (an item and a price).' },
        { cap: 'Event <code>tea 5</code>: every type now holds something for user-7.', run: A => feed(A, 0) },
        { cap: 'Event <code>cake 3</code>: ValueState is overwritten, ListState appends, MapState adds a <code>cake</code> entry, ReducingState sums to 8, and AggregatingState updates its (sum, count) accumulator.', run: A => feed(A, 1) },
        { cap: 'Event <code>tea 4</code>: ValueState keeps only the last price, and MapState <b>overwrites</b> its <code>tea</code> entry. The others keep accumulating.',
          run: async A => { await feed(A, 2); S.tone(rows[0], 'warn'); S.tone(rows[2], 'warn'); } },
        { cap: 'All five are scoped to the <b>current key</b>. Reducing and Aggregating state keep only the running result, not the events, so they stay small. ListState keeps every event, so clear it or use TTL.',
          run: async A => { S.tone(rows[0], 'info'); S.tone(rows[2], 'info'); await A.show(note); } }
      ];
    }
  });

  /* L7: operator state redistribution on rescale */
  D('operator-state-rescale', {
    title: 'Operator state on rescale: even-split vs union', h: 300,
    build(S) {
      lane(S, 30, 16, 'BEFORE: PARALLELISM 2 (each source instance keeps its own list of partition offsets)');
      const old = [S.box(30, 26, 360, 70, 'source[0]', { corner: true }), S.box(410, 26, 360, 70, 'source[1]', { corner: true })];
      const P = [['p0', 41], ['p1', 17], ['p2', 90], ['p3', 5], ['p4', 63], ['p5', 28]];
      const chips = P.map(([p, o], i) => S.token(old[i < 3 ? 0 : 1].bx + 80 + (i % 3) * 105, 68, `${p} @${o}`, { tone: 'purple' }));
      const l2 = lane(S, 30, 134, 'AFTER: PARALLELISM 3, restored from a savepoint'); l2.setAttribute('opacity', 0);
      const nw = [0, 1, 2].map(j => S.box(30 + j * 252, 144, 236, 76, 'source[' + j + ']', { corner: true, hidden: true }));
      const mode = S.text(400, 246, '', { weight: 700, size: 13.5, tone: 'warn' });
      const note = S.text(400, 270, '', { size: 12, fill: '#93a0bb' });
      return [
        { cap: 'Two source instances. Each keeps <b>its own list</b> of (partition, offset) pairs, e.g. p0 has been read up to offset 41. No key is involved: this is <b>operator state</b>, owned by the parallel instance.' },
        { cap: 'Rescale to 3 with <b>even-split</b> (the default, <code>getListState</code>): Flink joins all the lists and deals them out in equal pieces, so each new instance gets 2 partitions.',
          run: async A => {
            await A.show(l2, ...nw);
            A.text(mode, 'EVEN-SPLIT'); A.text(note, 'all lists are joined, then cut into equal pieces: 2 entries per instance');
            chips.forEach(c => c.parentNode.appendChild(c));
            await A.all(chips.map((c, i) => A.wait(i * 100).then(() => A.move(c, nw[i >> 1].bx + 62 + (i % 2) * 112, 190, 900))));
            await A.all(old.map(o => A.fade(o, 0.35)));
          } },
        { cap: 'With <b>union</b> (<code>getUnionListState</code>), every new instance receives <b>the whole list</b> and then keeps only the entries it is responsible for. It is used when an instance must see everything to decide, as the legacy Kafka consumer did.',
          run: async A => {
            await A.hide(...chips);
            A.text(mode, 'UNION'); A.text(note, 'every instance receives the full list, then keeps only what it needs');
            const small = [];
            nw.forEach((b, j) => P.forEach(([p], i) => small.push({ j, i, t: S.token(b.bx + 30 + i * 35, 190, p, { tone: 'purple', hidden: true }) })));
            await A.stagger(nw, (b, j) => A.show(small.filter(s => s.j === j).map(s => s.t)), 250);
            await A.wait(400);
            await A.all(small.filter(s => s.i % 3 !== s.j).map(s => A.fade(s.t, 0.18)));
          } },
        { cap: 'Compare with keyed state: there, whole <b>key groups</b> move and each key stays with its own state. Operator state has no keys, so on rescale Flink can only <b>split</b> the lists or <b>copy</b> them to everyone.' }
      ];
    }
  });

  /* L7: HashMap vs RocksDB backend */
  D('state-backends', {
    title: 'Where state lives: HashMap vs RocksDB', h: 330,
    build(S) {
      S.box(20, 16, 370, 196, 'HashMapStateBackend', { corner: true });
      S.box(410, 16, 370, 196, 'EmbeddedRocksDBStateBackend', { corner: true });
      const fnL = S.box(36, 42, 338, 42, 'your function', { sub: 'state.value()', subMono: true });
      const fnR = S.box(426, 42, 338, 42, 'your function', { sub: 'state.value()', subMono: true });
      const heap = S.box(36, 98, 338, 100, 'TaskManager JVM heap: live Java objects', { corner: true, tone: 'info' });
      S.box(426, 98, 150, 100, 'managed memory', { corner: true, tone: 'purple' });
      S.text(501, 150, 'block cache\n+ write buffers', { size: 11, fill: '#93a0bb' });
      const disk = S.box(586, 98, 178, 100, 'local disk (SST files)', { corner: true, tone: 'purple' });
      const OBJ = ['A=3', 'B=1', 'C=7', 'D=2'], SST = ['sst-1', 'sst-2', 'sst-3'];
      const objs = OBJ.map((o, i) => S.token(90 + i * 62, 145, o, { tone: 'info', hidden: i === 3 }));
      const ssts = SST.map((o, i) => S.token(630 + i * 52, 145, o, { tone: 'purple', hidden: i === 2 }));
      const heapSt = S.text(205, 182, '', { size: 11.5, fill: '#93a0bb' });
      const diskSt = S.text(675, 182, '', { size: 11, fill: '#93a0bb' });
      S.box(20, 232, 760, 88, 'checkpoint storage (S3 / HDFS / GCS)', { corner: true, tone: 'muted' });
      const ck = [[205, 266], [205, 292], [595, 266], [595, 292]].map(([x, y]) => S.text(x, y, '', { size: 12, mono: true, fill: '#e6ebf5' }));
      const up = (A, list, labels, tone, to) => A.stagger(list, (c, i) => A.fly(labels[i], [c._tx, c._ty], to, { tone, ms: 800 }), 150);
      return [
        { cap: 'Both backends keep the <b>working copy</b> of keyed state on the TaskManager. The difference is the <b>form</b>: live Java objects on the heap, or serialized bytes in RocksDB.' },
        { cap: 'Reading <code>state.value()</code>: HashMap hands back the object directly. RocksDB finds the <b>bytes</b> (in its cache, or on disk) and <b>deserializes</b> them on every access, which is slower.',
          run: async A => {
            await A.all(
              A.fly('A=3', [90, 145], [205, 63], { tone: 'info', ms: 500 }).then(() => S.sub(fnL, 'got A=3: object reference, no copy')),
              (async () => {
                await A.fly('A', [630, 145], [501, 150], { shape: 'sq', tone: 'purple', ms: 700 });
                await A.fly('A', [501, 150], [595, 63], { shape: 'sq', tone: 'purple', ms: 600 });
                S.sub(fnR, 'bytes → deserialize → A=3');
              })());
          } },
        { cap: 'Checkpoint 1: HashMap writes a <b>full snapshot</b> of all its state. RocksDB uploads its SST files.',
          run: async A => {
            await A.all(
              up(A, objs.slice(0, 3), OBJ, 'info', [205, 266]).then(() => A.text(ck[0], 'chk-1: full copy (3 entries)')),
              up(A, ssts.slice(0, 2), SST, 'purple', [595, 266]).then(() => A.text(ck[2], 'chk-1: sst-1, sst-2')));
          } },
        { cap: 'New data arrives (D=2, a new sst-3). Checkpoint 2: HashMap copies <b>everything again</b>. RocksDB uploads <b>only the new file</b> and reuses sst-1 and sst-2: an <b>incremental</b> checkpoint.',
          run: async A => {
            await A.all(A.show(objs[3]), A.show(ssts[2]));
            await A.all(
              up(A, objs, OBJ, 'info', [205, 292]).then(() => A.text(ck[1], 'chk-2: full copy again (4)')),
              up(A, [ssts[2]], ['sst-3'], 'purple', [595, 292]).then(() => A.text(ck[3], 'chk-2: only sst-3 (incremental)')));
          } },
        { cap: 'As state grows, HashMap hits the heap limit: long GC pauses, then OutOfMemoryError. RocksDB keeps hot data in managed memory and the rest on disk, so state can be far larger than RAM. Small state and lowest latency → HashMap. Large state → RocksDB.',
          run: async A => {
            S.tone(heap, 'bad'); A.text(heapSt, 'state > heap: long GC pauses, then OutOfMemoryError'); heapSt.setAttribute('fill', '#ffd6d6');
            S.tone(disk, 'ok'); A.text(diskSt, 'disk holds far more than RAM'); diskSt.setAttribute('fill', '#d2f5e4');
            await A.all(A.blink(heap, 2), A.blink(disk, 2));
          } }
      ];
    }
  });

  /* L7 + L13: key groups and rescaling */
  D('keygroup-rescale', {
    title: 'Key groups: the unit that moves when you rescale', h: 272,
    build(S) {
      lane(S, 30, 20, 'PARALLELISM 2 (max parallelism = 8 key groups)');
      const old = [S.box(30, 30, 360, 76, 'subtask 0', { corner: true }), S.box(410, 30, 360, 76, 'subtask 1', { corner: true })];
      const kg = [...Array(8)].map((_, i) => S.box(old[i < 4 ? 0 : 1].bx + 14 + (i % 4) * 86, 58, 74, 34, 'kg ' + i, { tone: i < 4 ? 'info' : 'purple', size: 12 }));
      const hashT = S.text(400, 128, 'murmurHash("customer-42") % 8 = 5', { mono: true, size: 12, tone: 'warn', hidden: true });
      const l2 = lane(S, 30, 152, 'PARALLELISM 4 (restored from a savepoint)'); l2.setAttribute('opacity', 0);
      const nw = [0, 1, 2, 3].map(j => S.box(30 + j * 190, 162, 172, 76, 'subtask ' + j, { corner: true, hidden: true }));
      const rule = S.text(400, 258, 'parallelism ≤ max parallelism: a key group is never split', { tone: 'ok', weight: 700, size: 12.5, hidden: true });
      return [
        { cap: 'Keys are hashed into a fixed number of <b>key groups</b> (= max parallelism, here 8). Each subtask owns a contiguous range of them.' },
        { cap: 'A key always maps to the same key group. "customer-42" lands in key group 5, which subtask 1 owns, so its state lives there.',
          run: async A => { await A.show(hashT); await A.fly('customer-42', [400, 128], [kg[5].cx, kg[5].cy], { tone: 'warn', ms: 900 }); S.tone(kg[5], 'warn'); await A.wait(300); S.tone(kg[5], 'purple'); } },
        { cap: 'Rescale to parallelism 4 from a savepoint: <b>whole key groups</b> move to their new owners. No key is re-hashed, and its state travels with its key group.',
          run: async A => {
            await A.hide(hashT); await A.show(l2, ...nw);
            kg.forEach(k => k.parentNode.appendChild(k));
            await A.all(kg.map((k, i) => A.wait(i * 90).then(() => A.move(k, nw[i >> 1].bx + 12 + (i % 2) * 80, 190, 900))));
            await A.fade(old[0], 0.35); await A.fade(old[1], 0.35);
          } },
        { cap: 'With 8 key groups you can run at most 8 subtasks, because there is no smaller unit to hand out. Set max parallelism once, before the first production deployment.', run: A => A.show(rule) }
      ];
    }
  });

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

  /* L8: window types */
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

  /* L8: late data options */
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

  /* L8: one event lands in several sliding windows */
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

  /* L8: session windows merge */
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

  /* L9: a checkpoint end to end */
  D('checkpoint-flow', {
    title: 'Checkpoint 7, from trigger to COMPLETED', h: 330,
    build(S) {
      const st = {};
      const co = S.box(270, 15, 260, 56, 'Checkpoint coordinator', { sub: 'inside the JobMaster' });
      const src = S.box(30, 140, 160, 64, 'Source', { sub: 'Kafka offset 1042' });
      const win = S.box(320, 140, 160, 64, 'Window', { sub: 'state: 12 keys' });
      const snk = S.box(610, 140, 160, 64, 'Sink', { sub: 'open Kafka txn' });
      S.arrow(190, 172, 318, 172); S.arrow(480, 172, 608, 172);
      const store = S.box(200, 262, 400, 58, 'Checkpoint storage (S3 / HDFS)', { corner: true, tone: 'muted' });
      const ent = ['source: offset 1042', 'window: state', 'sink: txn id'].map((t, i) => S.text(222 + i * 128, 299, t, { anchor: 'start', size: 11.5, tone: 'purple', hidden: true }));
      [[110, 204, 270, 260], [400, 204, 400, 260], [690, 204, 530, 260]].forEach(([a, b, c, d]) => S.line(a, b, c, d, { color: '#2a3550', dash: '3 4' }));
      const flow = (A, n) => A.stagger([...Array(n)], () => A.fly('', [190, 172], [608, 172], { r: 7, tone: 'warn', ms: 1500, ease: p => p }), 380);
      const snap = (A, task, i) => A.fly('', [task.cx, task.B], [task === win ? 400 : task === src ? 270 : 530, 262], { shape: 'sq', tone: 'purple', ms: 700 }).then(() => A.show(ent[i]));
      return [
        { cap: 'A running job: records flow from the source, through the window, to the sink.', run: A => flow(A, 3) },
        { cap: 'The coordinator triggers checkpoint 7. The source saves its Kafka offset and injects <b>barrier 7</b> into its output stream.',
          run: async A => {
            await A.fly('trigger 7', [co.cx, co.B], [src.cx, src.T], { tone: 'purple', ms: 800 });
            S.tone(src, 'purple'); await snap(A, src, 0);
            st.recs = [260, 296].map(x => S.token(x, 172, '', { r: 7, tone: 'warn' }));
            st.bar = S.token(214, 172, '7', { shape: 'bar', tone: 'purple', h: 34 });
            await A.blink(st.bar, 1);
          } },
        { cap: 'The barrier flows <b>with</b> the records and never overtakes them. When it reaches the window, the window snapshots its state (asynchronously) and passes the barrier on.',
          run: async A => {
            S.tone(src, 'box');
            await A.all(st.recs.map(r => A.move(r, 400, 172, 700).then(() => A.hide(r))), A.wait(150).then(() => A.move(st.bar, 318, 172, 800)));
            S.tone(win, 'purple'); await snap(A, win, 1);
            await A.move(st.bar, 496, 172, 500);
          } },
        { cap: 'The sink snapshots too (its pending Kafka transaction). Every task <b>acknowledges</b> to the coordinator.',
          run: async A => {
            S.tone(win, 'box');
            await A.move(st.bar, 606, 172, 700); await A.hide(st.bar);
            S.tone(snk, 'purple'); await snap(A, snk, 2);
            await A.all([src, win, snk].map((t, i) => A.fly('ack', [t.cx, t.T], [co.L + 60 + i * 70, co.B], { tone: 'ok', ms: 800 })));
          } },
        { cap: 'All acks are in: checkpoint 7 is <b>COMPLETED</b>. Transactional sinks commit now, and after a failure the job restarts from exactly this point.',
          run: async A => { S.tone(snk, 'box'); S.tone(co, 'ok'); S.label(co, 'Checkpoint 7 COMPLETED'); S.tone(store, 'ok'); S.sub(snk, 'txn committed'); await A.wait(300); } }
      ];
    }
  });

  /* L9: aligned vs unaligned under backpressure */
  D('aligned-vs-unaligned', {
    title: 'Under backpressure: aligned vs unaligned checkpoints', h: 312,
    build(S) {
      const mk = (y0, name) => {
        lane(S, 20, y0 - 16, name);
        S.box(140, y0, 312, 56, '', { tone: 'muted' });
        S.text(296, y0 + 70, 'full input buffers', { size: 11, fill: '#6f7c98' });
        const recs = [...Array(6)].map((_, i) => S.token(171 + i * 50, y0 + 28, 'r', { tone: 'warn' }));
        const bar = S.token(118, y0 + 28, '7', { shape: 'bar', tone: 'purple', h: 40 });
        const op = S.box(560, y0 - 2, 150, 60, 'Window', { tone: 'busy', sub: 'slow' });
        S.arrow(452, y0 + 28, 558, y0 + 28);
        const tm = S.text(635, y0 + 76, 'alignment: 0 s', { tone: 'warn', weight: 700, size: 12.5 });
        return { recs, bar, op, tm, y: y0 + 28 };
      };
      const a = mk(36, 'ALIGNED'), u = mk(186, 'UNALIGNED');
      const store = S.box(140, 268, 312, 34, 'checkpoint: state + 6 in-flight records', { tone: 'purple', size: 12, hidden: true });
      return [
        { cap: 'Under backpressure the input buffers are full. Barrier 7 sits at the back of the queue in both cases.' },
        { cap: '<b>Aligned</b>: the barrier moves only as fast as the slow operator drains the queue. The checkpoint waits for all of it, and alignment time keeps growing.',
          run: async A => {
            for (let k = 1; k <= 6; k++) {
              const f = a.recs.pop();
              await A.all(A.move(f, 635, a.y, 380).then(() => A.hide(f)), ...a.recs.map(r => A.move(r, r._tx + 50, a.y, 380)), A.move(a.bar, a.bar._tx + 50, a.y, 380));
              A.text(a.tm, `alignment: ${k * 20} s`);
              await A.wait(120);
            }
            await A.move(a.bar, 556, a.y, 500); S.tone(a.op, 'purple'); S.sub(a.op, 'snapshot (late)');
          } },
        { cap: '<b>Unaligned</b>: the barrier overtakes the queued records and reaches the operator right away.',
          run: async A => { await A.path(u.bar, [[140, u.y - 42], [452, u.y - 42], [556, u.y]], 1000); S.tone(u.op, 'purple'); S.sub(u.op, 'snapshot now'); A.text(u.tm, 'alignment: 0 s'); S.tone(u.tm, 'ok'); } },
        { cap: 'The overtaken in-flight records are stored as part of the checkpoint. The checkpoint is fast but bigger. Fixing the bottleneck is still the real cure.',
          run: async A => { await A.show(store); await A.all(u.recs.map((r, i) => A.wait(i * 90).then(() => A.fly('r', [r._tx, u.y], [180 + i * 46, 285], { tone: 'warn', ms: 600 })))); } }
      ];
    }
  });

  /* L9: failure, restore, replay */
  D('recovery-replay', {
    title: 'Crash, restore, replay: exactly-once state', h: 292,
    build(S) {
      lane(S, 20, 14, 'KAFKA PARTITION (offsets)');
      const cx = i => 50 + i * 44;
      const cells = [...Array(12)].map((_, i) => S.box(30 + i * 44, 52, 40, 40, String(100 + i), { size: 11, tone: i < 5 ? 'muted' : 'box', weight: 500 }));
      const ptr = S.token(cx(5), 33, 'read', { tone: 'info' });
      S.rect(cx(5) - 23, 46, 3, 54, { tone: 'purple', fill: C.purple, rx: 1 });
      S.text(cx(5) - 18, 114, 'checkpoint 7: offset 105, count = 5', { anchor: 'start', size: 11.5, tone: 'purple', weight: 700 });
      const job = S.box(580, 30, 200, 90, 'Flink job', { sub: 'count = 5' });
      const sink = S.box(580, 180, 200, 76, 'Non-transactional sink', { sub: 'writes: 0', tone: 'muted' });
      S.arrow(680, 120, 680, 178);
      let n = 5, w = 0;
      const read = async (A, i) => {
        await A.move(ptr, cx(i + 1), 33, 300);
        await A.fly(String(100 + i), [cx(i), 72], [680, 75], { tone: 'warn', ms: 650, w: 34 });
        n++; S.sub(job, 'count = ' + n);
        await A.fly('', [680, 120], [680, 178], { r: 6, tone: 'warn', ms: 350 }); w++; S.sub(sink, 'writes: ' + w);
      };
      return [
        { cap: 'The job counts records from a Kafka partition. Its last completed checkpoint (7) stored <b>offset 105</b> and <b>count = 5</b> together.' },
        { cap: 'Processing goes on: records 105, 106 and 107 are counted (count = 8) and written to an external sink.', run: async A => { for (let i = 5; i <= 7; i++) await read(A, i); } },
        { cap: 'Then the TaskManager crashes. Count = 8 was never checkpointed.', run: async A => { S.tone(job, 'bad'); S.sub(job, 'TaskManager lost!'); await A.blink(job, 2); } },
        { cap: 'Restore: the state goes back to count = 5 and the source <b>rewinds</b> to offset 105, both taken from checkpoint 7.',
          run: async A => { S.tone(job, 'warn'); n = 5; S.sub(job, 'restored: count = 5'); await A.move(ptr, cx(5), 33, 900); } },
        { cap: 'Records 105 to 107 are replayed. Flink state ends at count = 8, counted exactly once. The non-transactional sink got them twice: end-to-end exactly-once needs a transactional or idempotent sink.',
          run: async A => { for (let i = 5; i <= 7; i++) await read(A, i); S.tone(job, 'ok'); S.tone(sink, 'warn'); S.sub(sink, 'writes: 6 (3 duplicates)'); } }
      ];
    }
  });

  /* L9: savepoint + uid */
  D('savepoint-upgrade', {
    title: 'Upgrading with a savepoint: state is matched by uid', h: 262,
    build(S) {
      const v1 = S.box(20, 50, 200, 120, 'Job v1 · parallelism 2', { corner: true, tone: 'info' });
      const c1 = S.box(33, 92, 174, 36, 'uid: per-minute-total', { size: 11.5, mono: true });
      const sp = S.box(305, 85, 170, 60, 'savepoint-abc123', { sub: 'owned by you', tone: 'muted', mono: true, size: 12 });
      S.arrow(220, 115, 303, 115, { label: 'flink stop', ly: -14, lmono: true });
      const v2 = S.box(580, 30, 205, 180, 'Job v2 · parallelism 4', { corner: true, tone: 'info', hidden: true });
      const c2 = S.box(595, 70, 175, 36, 'uid: per-minute-total', { size: 11.5, mono: true, hidden: true });
      const c3 = S.box(595, 140, 175, 36, 'generated id 7f3a…', { size: 11.5, mono: true, tone: 'bad', hidden: true });
      const ar2 = S.arrow(475, 115, 578, 115, { label: 'flink run -s', ly: -14, lmono: true, hidden: true });
      const fail = S.text(682, 230, 'state cannot be mapped', { tone: 'bad', weight: 700, size: 12, hidden: true });
      return [
        { cap: '<code>flink stop --savepointPath</code>: the job writes a savepoint and stops cleanly. State is stored per operator <b>uid</b>.',
          run: async A => { await A.fly('state', [c1.cx, c1.cy], [sp.cx, sp.cy], { tone: 'purple', ms: 1000 }); S.tone(sp, 'purple'); S.tone(v1, 'muted'); S.label(v1, 'Job v1 · STOPPED'); } },
        { cap: 'You own the savepoint, so Flink never deletes it. Deploy new code, a new parallelism, or even a new Flink version.', run: A => A.show(v2, c2, ar2) },
        { cap: '<code>flink run -s …</code>: state is matched to operators by uid. Same uid, so the state is restored (and redistributed to 4 subtasks by key group).',
          run: async A => { await A.fly('state', [sp.cx, sp.cy], [c2.cx, c2.cy], { tone: 'purple', ms: 900 }); S.tone(c2, 'ok'); } },
        { cap: 'Without an explicit <code>.uid()</code>, the operator ID is generated from the graph. Insert one map and it changes, and the old state has nowhere to go.',
          run: async A => { await A.show(c3); const t = await A.fly('state', [sp.cx, sp.cy], [c3.L - 8, c3.cy], { tone: 'purple', keep: true, ms: 800 }); S.tone(t, 'bad'); await A.move(t, sp.R - 10, sp.B - 4, 700); await A.show(fail); } }
      ];
    }
  });
})(window.FA);
