/* Flink job simulator: turns a pipeline model into a JobGraph with chaining, ship strategies,
 * slot requirements and a steady-state busy / backpressure / idle model.
 * It is a teaching model (simplified), but the RULES mirror real Flink behaviour. */
(function (FA) {
  'use strict';

  const KINDS = {
    source: { label: 'Source', defName: 'Kafka Source', cost: 2, sel: 1, prefix: 'Source: ' },
    map: { label: 'Map', defName: 'Map', cost: 3, sel: 1 },
    filter: { label: 'Filter', defName: 'Filter', cost: 1, sel: 0.6 },
    flatmap: { label: 'FlatMap', defName: 'FlatMap', cost: 4, sel: 3 },
    keyby: { label: 'keyBy (partitioning, not an operator)', defName: 'keyBy(userId)', cost: 0, sel: 1, pseudo: true },
    window: { label: 'Window (keyed)', defName: 'TumblingEventTimeWindows(1 min)', cost: 6, sel: 0.02, keyed: true },
    reduce: { label: 'Reduce / Aggregate (keyed)', defName: 'Reduce', cost: 4, sel: 1, keyed: true },
    process: { label: 'KeyedProcessFunction (keyed)', defName: 'KeyedProcess', cost: 8, sel: 1, keyed: true },
    async: { label: 'Async I/O enrichment', defName: 'AsyncEnrich(REST)', cost: 60, sel: 1 },
    sink: { label: 'Sink', defName: 'Kafka Sink', cost: 4, sel: 0, prefix: 'Sink: ' }
  };
  const SERDE_US = 1.2; // serialization + network cost per record when an edge is NOT chained

  function opDisplayName(op) { const k = KINDS[op.kind]; return (k.prefix || '') + (op.name || k.defName); }

  function defaultMaxParallelism(p) {
    let x = 1; const t = p + Math.floor(p / 2); while (x < t) x *= 2;
    return Math.min(Math.max(x, 128), 32768);
  }

  function evenShares(p) { return Array.from({ length: p }, () => 1 / p); }

  function compile(model) {
    const cl = Object.assign({ tms: 2, slotsPerTm: 2, coresPerTm: 4, chaining: true, sourceRate: 20000, maxParallelism: 0, checkpointing: true, unaligned: false }, model.cluster || {});
    const errors = [], warnings = [];
    const real = []; // real operators with incoming edge info
    let pendingKeyBy = null;

    model.ops.forEach((op, idx) => {
      const k = KINDS[op.kind];
      if (!k) { errors.push(`Unknown operator kind ${op.kind}`); return; }
      if (k.pseudo) {
        if (!real.length) errors.push('keyBy() must come after a source.');
        pendingKeyBy = op; return;
      }
      const inherited = real.length ? real[real.length - 1].group : 'default';
      const r = { op, idx, kind: op.kind, name: opDisplayName(op), p: Math.max(1, op.p | 0), cost: +op.cost, sel: +op.sel, group: (op.group || '').trim() || inherited, chain: op.chain || 'auto' };
      if (real.length === 0) {
        if (op.kind !== 'source') errors.push('The first operator must be a Source.');
        r.edge = null;
      } else {
        const prev = real[real.length - 1];
        let strat;
        if (pendingKeyBy) { strat = 'HASH'; r.keyBy = pendingKeyBy; }
        else if (op.part && op.part !== 'auto') strat = op.part.toUpperCase();
        else strat = prev.p === r.p ? 'FORWARD' : 'REBALANCE';
        if (op.kind === 'source') errors.push('A Source cannot have an input.');
        if (k.keyed && !pendingKeyBy) errors.push(`"${r.name}" is a keyed operation. In Flink it needs a KeyedStream, so add a keyBy() directly before it.`);
        if (strat === 'FORWARD' && prev.p !== r.p) errors.push(`Forward partitioning does not allow change of parallelism. Upstream operation: ${prev.name} parallelism: ${prev.p}, downstream operation: ${r.name} parallelism: ${r.p}. Use rebalance()/rescale() or align the parallelism.`);
        r.edge = { strat, from: prev };
      }
      if (prev0(real) && prev0(real).kind === 'sink') errors.push('Nothing can follow a Sink in this simple linear lab.');
      pendingKeyBy = null;
      real.push(r);
    });
    function prev0(arr) { return arr[arr.length - 1]; }
    if (pendingKeyBy) errors.push('keyBy() at the end does nothing: it must be followed by a keyed operator.');
    if (!real.length) errors.push('Add at least a Source.');
    if (real.length && real[real.length - 1].kind !== 'sink') warnings.push('The job has no Sink. Flink will refuse to execute a job with no sinks ("No operators defined in streaming topology").');

    // max parallelism check (keyed state)
    real.forEach(r => {
      const mp = cl.maxParallelism > 0 ? cl.maxParallelism : defaultMaxParallelism(r.p);
      r.maxP = mp;
      if (r.p > mp) errors.push(`${r.name}: parallelism ${r.p} is greater than max parallelism ${mp}.`);
    });

    // ---- chaining -> vertices
    const vertices = [];
    real.forEach((r, i) => {
      let chained = false;
      if (i > 0 && cl.chaining) {
        const prev = real[i - 1];
        chained = r.edge.strat === 'FORWARD' && prev.p === r.p && prev.group === r.group && prev.chain !== 'disable' && r.chain !== 'disable' && r.chain !== 'startNew';
      }
      r.chained = chained;
      if (chained) vertices[vertices.length - 1].ops.push(r);
      else vertices.push({ id: vertices.length, ops: [r], p: r.p, group: r.group, inEdge: r.edge });
      r.vertex = vertices[vertices.length - 1];
    });
    vertices.forEach(v => { v.name = v.ops.map(o => o.name).join(' -> '); });
    // chain-break reasons for teaching
    real.forEach((r, i) => {
      if (i === 0 || r.chained) return;
      const prev = real[i - 1], why = [];
      if (!cl.chaining) why.push('chaining is disabled for the whole job');
      if (r.edge.strat !== 'FORWARD') why.push(`the exchange is ${r.edge.strat}, not FORWARD`);
      if (prev.p !== r.p) why.push(`parallelism differs (${prev.p} → ${r.p})`);
      if (prev.group !== r.group) why.push(`different slot sharing groups ("${prev.group}" vs "${r.group}")`);
      if (prev.chain === 'disable') why.push(`disableChaining() was called on "${prev.name}"`);
      if (r.chain === 'disable') why.push(`disableChaining() was called on "${r.name}"`);
      if (r.chain === 'startNew') why.push(`startNewChain() was called on "${r.name}"`);
      r.chainBreak = why;
    });

    // ---- slots
    const groups = {};
    vertices.forEach(v => { groups[v.group] = Math.max(groups[v.group] || 0, v.p); });
    const slotsRequired = Object.values(groups).reduce((a, b) => a + b, 0);
    const slotsAvailable = cl.tms * cl.slotsPerTm;
    const schedulable = slotsRequired <= slotsAvailable && !errors.length;
    if (!errors.length && slotsRequired > slotsAvailable) errors.push(`NoResourceAvailableException: could not acquire the minimum required resources. The job needs ${slotsRequired} slot(s) but the cluster has ${slotsAvailable} (${cl.tms} TaskManager(s) × ${cl.slotsPerTm} slot(s)).`);

    // ---- data volume and per-subtask shares
    let prevOut = 1, prevShares = null;
    real.forEach((r, i) => {
      const s = r.edge ? r.edge.strat : 'SOURCE';
      let inn = prevOut, shares;
      if (s === 'SOURCE') { inn = 1; shares = evenShares(r.p); }
      else if (s === 'FORWARD') shares = prevShares.slice(0, r.p);
      else if (s === 'BROADCAST') { inn = prevOut * r.p; shares = evenShares(r.p); }
      else if (s === 'GLOBAL') shares = Array.from({ length: r.p }, (_, k) => k === 0 ? 1 : 0);
      else if (s === 'HASH') {
        const kb = r.keyBy || {};
        let hot = Math.min(1, Math.max(0, +(kb.skew || 0)));
        if (kb.preAgg) { inn = inn * 0.25; hot = 0; }
        if (r.p === 1 || hot <= 1 / r.p) shares = evenShares(r.p);
        else shares = Array.from({ length: r.p }, (_, k) => k === 0 ? hot : (1 - hot) / (r.p - 1));
      } else shares = evenShares(r.p); // REBALANCE, RESCALE, SHUFFLE
      r.inPerSrc = inn; r.outPerSrc = r.kind === 'sink' ? 0 : inn * r.sel; r.shares = shares;
      prevOut = r.kind === 'sink' ? 0 : r.outPerSrc; prevShares = shares;
    });

    // per-vertex µs per source record for each subtask
    vertices.forEach((v, vi) => {
      v.usPerSrc = Array.from({ length: v.p }, (_, s) => {
        let t = 0;
        v.ops.forEach(o => { t += o.cost * o.inPerSrc * o.shares[s]; });
        const first = v.ops[0], last = v.ops[v.ops.length - 1];
        if (vi > 0) t += SERDE_US * first.inPerSrc * first.shares[s];
        if (vi < vertices.length - 1) t += SERDE_US * last.outPerSrc * last.shares[s];
        return t;
      });
    });

    // ---- slot placement: slot k hosts subtask k of every vertex in a group (slot sharing)
    const slots = []; // {tm, slot, group, tasks:[{v,s}]}
    let slotCursor = 0;
    Object.entries(groups).forEach(([g, n]) => {
      for (let k = 0; k < n; k++) {
        // spread slots evenly over TaskManagers (like taskmanager.load-balance.mode: SLOTS)
        const tm = slotCursor % Math.max(1, cl.tms);
        slots.push({ tm, slot: Math.floor(slotCursor / Math.max(1, cl.tms)), group: g, tasks: vertices.filter(v => v.group === g && v.p > k).map(v => ({ v: v.id, s: k })) });
        slotCursor++;
      }
    });

    // ---- throughput solve with CPU contention (slots do NOT isolate CPU)
    const R = Math.max(1, +cl.sourceRate);
    let eff = vertices.map(v => Array(v.p).fill(1));
    let rate = R, bottleneck = -1, busy = [];
    for (let iter = 0; iter < 6; iter++) {
      rate = R; bottleneck = -1;
      vertices.forEach((v, vi) => {
        v.usPerSrc.forEach((us, s) => {
          if (us <= 0) return;
          const cap = eff[vi][s] * 1e6 / us; // source records/s this subtask can sustain
          if (cap < rate) { rate = cap; bottleneck = vi; }
        });
      });
      busy = vertices.map((v, vi) => v.usPerSrc.map((us, s) => Math.min(1, rate * us / (eff[vi][s] * 1e6))));
      // CPU demand per TM
      const demand = Array(Math.max(1, cl.tms)).fill(0);
      slots.forEach(sl => sl.tasks.forEach(t => { if (sl.tm < demand.length) demand[sl.tm] += busy[t.v][t.s] * eff[t.v][t.s]; }));
      const next = vertices.map(v => Array(v.p).fill(1));
      slots.forEach(sl => sl.tasks.forEach(t => {
        const d = demand[sl.tm] || 0;
        next[t.v][t.s] = d > cl.coresPerTm ? cl.coresPerTm / d : 1;
      }));
      eff = next.map((arr, vi) => arr.map((x, s) => 0.5 * eff[vi][s] + 0.5 * x));
    }
    const cpuDemand = Array(Math.max(1, cl.tms)).fill(0);
    slots.forEach(sl => sl.tasks.forEach(t => { if (sl.tm < cpuDemand.length) cpuDemand[sl.tm] += busy[t.v][t.s]; }));
    const limited = rate < R - 1e-6;

    vertices.forEach((v, vi) => {
      v.sub = v.usPerSrc.map((us, s) => {
        const b = busy[vi][s];
        let bp = 0, idle = 0;
        if (limited && vi < bottleneck) bp = Math.max(0, 1 - b); else idle = Math.max(0, 1 - b);
        if (limited && vi === bottleneck && b < 0.999) idle = 1 - b; // non-hot subtasks of the bottleneck vertex
        const first = v.ops[0];
        return { s, busy: b, bp, idle, recIn: rate * first.inPerSrc * first.shares[s], recOut: rate * v.ops[v.ops.length - 1].outPerSrc * v.ops[v.ops.length - 1].shares[s], eff: eff[vi][s] };
      });
      v.busyMax = Math.max(...v.sub.map(x => x.busy));
      v.bpMax = Math.max(...v.sub.map(x => x.bp));
      v.idleMax = Math.max(...v.sub.map(x => x.idle));
      v.recIn = vi === 0 ? 0 : rate * v.ops[0].inPerSrc;
      v.recOut = vi === vertices.length - 1 ? 0 : rate * v.ops[v.ops.length - 1].outPerSrc;
      v.bpStatus = v.bpMax > 0.5 ? 'HIGH' : v.bpMax > 0.1 ? 'LOW' : 'OK';
    });

    const maxBp = Math.max(0, ...vertices.map(v => v.bpMax));
    const ckpt = cl.checkpointing ? {
      durationS: cl.unaligned ? 0.3 + 1.5 * maxBp : 0.3 + 90 * maxBp * maxBp,
      alignmentS: cl.unaligned ? 0 : 85 * maxBp * maxBp,
      sizeNote: cl.unaligned && maxBp > 0.1 ? 'larger (in-flight buffers are persisted)' : 'normal'
    } : null;

    const status = errors.length ? (slotsRequired > slotsAvailable && errors.length === 1 ? 'FAILED' : 'INVALID') : 'RUNNING';
    return {
      cl, real, vertices, errors, warnings, slotsRequired, slotsAvailable, groups, slots, schedulable,
      sourceRate: R, throughput: errors.length ? 0 : rate, limited: !errors.length && limited, bottleneck: errors.length ? -1 : bottleneck,
      lagGrowth: errors.length ? R : Math.max(0, R - rate), cpuDemand, ckpt, status, maxBp
    };
  }

  /* ---------- code generation ---------- */
  function javaCode(model, res) {
    const cl = model.cluster || {};
    const L = [];
    L.push('StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();');
    if (cl.chaining === false) L.push('env.disableOperatorChaining();');
    if (cl.maxParallelism > 0) L.push(`env.setMaxParallelism(${cl.maxParallelism});`);
    if (cl.checkpointing !== false) {
      L.push('env.enableCheckpointing(60_000); // execution.checkpointing.interval');
      if (cl.unaligned) L.push('env.getCheckpointConfig().enableUnalignedCheckpoints();');
    }
    L.push('');
    let expr = '';
    let pendingKey = null;
    model.ops.forEach((op, i) => {
      const k = KINDS[op.kind], name = op.name || k.defName;
      const tail = [];
      if (k.pseudo) { pendingKey = op; return; }
      const partCall = pendingKey ? `\n    .keyBy(e -> e.getKey())   // ${pendingKey.name || 'keyBy'} → HASH` :
        op.part && op.part !== 'auto' && op.part !== 'forward' ? `\n    .${op.part}()` : op.part === 'forward' ? '\n    .forward()' : '';
      tail.push(`.name("${name}")`, `.setParallelism(${op.p})`);
      if (op.group && op.group.trim()) tail.push(`.slotSharingGroup("${op.group.trim()}")`);
      if (op.chain === 'disable') tail.push('.disableChaining()');
      if (op.chain === 'startNew') tail.push('.startNewChain()');
      const t = tail.join('');
      switch (op.kind) {
        case 'source': expr = `DataStream<Event> stream = env\n    .fromSource(kafkaSource, watermarkStrategy, "${name}")${tail.slice(1).join('')}`; break;
        case 'map': expr += `${partCall}\n    .map(new MyMapper())${t}`; break;
        case 'filter': expr += `${partCall}\n    .filter(e -> e.isValid())${t}`; break;
        case 'flatmap': expr += `${partCall}\n    .flatMap(new Splitter())${t}`; break;
        case 'window': expr += `${partCall}\n    .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))\n    .aggregate(new CountAgg())${t}`; break;
        case 'reduce': expr += `${partCall}\n    .reduce((a, b) -> a.merge(b))${t}`; break;
        case 'process': expr += `${partCall}\n    .process(new MyKeyedProcessFunction())${t}`; break;
        case 'async': expr += `${partCall}\n    ; stream = AsyncDataStream.unorderedWait(stream, new RestEnricher(), 2, TimeUnit.SECONDS, 100)${t}`; break;
        case 'sink': expr += `${partCall}\n    .sinkTo(kafkaSink)${t}`; break;
      }
      pendingKey = null;
    });
    L.push(expr + ';');
    L.push('');
    L.push('env.execute("lab-job");');
    return L.join('\n');
  }

  function beamCode(model) {
    const L = [];
    L.push('FlinkPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(FlinkPipelineOptions.class);');
    L.push('options.setRunner(FlinkRunner.class);');
    const maxP = Math.max(...model.ops.map(o => o.p || 1));
    L.push(`options.setParallelism(${maxP});            // Beam sets ONE parallelism for the whole job`);
    if ((model.cluster || {}).checkpointing !== false) L.push('options.setCheckpointingInterval(60_000L);');
    if ((model.cluster || {}).chaining === false) L.push('options.setOperatorChaining(false);');
    L.push('Pipeline p = Pipeline.create(options);', '');
    let line = 'p';
    let pendingKey = null;
    model.ops.forEach(op => {
      const k = KINDS[op.kind], name = op.name || k.defName;
      if (k.pseudo) { pendingKey = op; return; }
      if (op.part === 'rebalance' || op.part === 'shuffle' || op.part === 'rescale') line += `\n  .apply("Reshuffle", Reshuffle.viaRandomKey())   // ≈ redistribution (implemented with a GroupByKey → HASH)`;
      switch (op.kind) {
        case 'source': line += `\n  .apply("${name}", KafkaIO.<String, Event>read()...withoutMetadata())`; break;
        case 'map': line += `\n  .apply("${name}", MapElements.via(new MyMapper()))`; break;
        case 'filter': line += `\n  .apply("${name}", Filter.by(e -> e.isValid()))`; break;
        case 'flatmap': line += `\n  .apply("${name}", FlatMapElements.via(new Splitter()))`; break;
        case 'window':
          line += `\n  .apply("WithKeys", WithKeys.of(e -> e.getKey()))   // ${pendingKey ? pendingKey.name : 'key'}`;
          line += `\n  .apply("${name}", Window.into(FixedWindows.of(Duration.standardMinutes(1))))`;
          line += `\n  .apply("Count", Count.perKey())              // GroupByKey inside → HASH edge in Flink UI`; break;
        case 'reduce':
          line += `\n  .apply("WithKeys", WithKeys.of(e -> e.getKey()))`;
          line += `\n  .apply("${name}", Combine.perKey(new MergeFn()))  // GroupByKey → HASH`; break;
        case 'process':
          line += `\n  .apply("WithKeys", WithKeys.of(e -> e.getKey()))`;
          line += `\n  .apply("${name}", ParDo.of(new MyStatefulDoFn()))  // @StateId/@TimerId → keyed → HASH`; break;
        case 'async': line += `\n  .apply("${name}", ParDo.of(new EnrichDoFn()))   // no native async I/O; use batching/@StartBundle`; break;
        case 'sink': line += `\n  .apply("${name}", KafkaIO.<String, String>write()...)`; break;
      }
      pendingKey = null;
    });
    L.push(line + ';', '', 'p.run().waitUntilFinish();');
    return L.join('\n');
  }

  FA.Sim = { KINDS, compile, javaCode, beamCode, defaultMaxParallelism, opDisplayName };
})(window.FA);
