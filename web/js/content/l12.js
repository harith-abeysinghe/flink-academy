FA.registerLevel({
  id: 12, track: 'Operations', icon: '🔥', minutes: 35,
  title: 'Backpressure & performance tuning',
  summary: 'Find the real bottleneck, fix it (parallelism, skew, async I/O, chaining, serialization), and avoid the classic traps.',
  objectives: ['Locate the bottleneck from colours and metrics', 'Fix sink/IO bottlenecks, CPU limits and data skew', 'Understand why more parallelism does not fix a hot key', 'Apply a systematic tuning checklist'],
  sections: [
    { title: 'How backpressure propagates', html: `
<p>A slow task cannot consume its input fast enough → its input buffers fill → its upstream senders run out of credits and block (backpressured) → their inputs fill → … → the <b>source</b> slows down, and Kafka consumer lag grows.</p>
<p><b>The rule</b>: walk the graph from the source. The tasks that are <b>black</b> (backpressured) are victims. The first <b>red</b> (busy) task after them is the likely <b>bottleneck</b>. Tasks after the bottleneck are mostly blue (idle, starving).</p>
${FA.D('backpressure-propagation')}
${FA.W('jobgraph', { title: 'Raise the sink parallelism or lower its cost until nothing is black', model: { cluster: { tms: 2, slotsPerTm: 8, coresPerTm: 8, sourceRate: 40000 }, ops: [{ kind: 'source', p: 4, name: 'Kafka payments' }, { kind: 'map', p: 4, name: 'Validate' }, { kind: 'keyby', name: 'keyBy(account)' }, { kind: 'reduce', p: 4, name: 'Running balance' }, { kind: 'sink', p: 2, name: 'JDBC upsert', cost: 150 }] }, controls: [{ op: 4, key: 'p', label: 'Sink parallelism', max: 16 }, { op: 4, key: 'cost', label: 'Sink µs/record', min: 1, max: 500 }, { op: 'cluster', key: 'sourceRate', label: 'Source rate', min: 1000, max: 200000, step: 5000 }] })}
<p>Capacity check: 1 subtask spending 150 µs/record handles about 6,600 rec/s. At 40,000 rec/s you need at least 7 sink subtasks (plus headroom).</p>` },
    { title: 'Typical bottlenecks and fixes', html: `
<table class="t"><tr><th>Bottleneck</th><th>Signals</th><th>Fixes</th></tr>
<tr><td>Slow external I/O (DB, REST)</td><td>Sink/enrich task red, FlameGraph off-CPU waits</td><td>Batching, Async I/O (more in-flight requests), more parallelism, caching, idempotent bulk writes</td></tr>
<tr><td>CPU-heavy logic</td><td>Red task, FlameGraph hot method</td><td>Optimize code, more parallelism, more cores per slot</td></tr>
<tr><td>Serialization</td><td>Kryo in FlameGraph, many unchained edges</td><td>POJOs/Avro with proper TypeInformation, avoid generic types (<code>pipeline.generic-types: false</code> to detect), chain more</td></tr>
<tr><td>Key skew</td><td>One subtask much busier in SubTasks tab</td><td>Two-phase/local pre-aggregation, key salting, better key choice</td></tr>
<tr><td>State backend</td><td>RocksDB: high busy time, disk I/O, slow checkpoints</td><td>More managed memory, incremental checkpoints, local SSDs, reduce state size/TTL</td></tr>
<tr><td>GC</td><td>Busy but low throughput, GC time high in TM metrics</td><td>Bigger heap, fewer objects, RocksDB instead of heap state</td></tr>
<tr><td>CPU oversubscribed</td><td>Everything moderately busy, TM CPU at 100%</td><td>Fewer slots per TM / more cores / more TMs</td></tr></table>` },
    { title: 'Why parallelism does not fix a hot key', html: `
<p>All records with the same key go to the <b>same</b> subtask (HASH). If 70% of traffic is one customer, that subtask must handle 70% regardless of parallelism 4 or 400. Fixes:</p>
<ul>
<li><b>Two-phase (local/global) aggregation</b>: pre-aggregate before the shuffle (e.g. per source subtask), then keyBy and merge partial results. Only possible for associative aggregations (sum, count, min/max).</li>
<li><b>Salting</b>: key by <code>(customer, random 0..N)</code>, aggregate, then keyBy customer and merge.</li>
<li>Rethink the key: is per-customer state really needed, or per (customer, region)?</li>
</ul>
${FA.D('hot-key')}
${FA.C('tip', 'Try it', 'The <a href="#/lab/hot">Hot key on Black Friday</a> challenge reproduces this.')}` },
    { title: 'Tuning checklist', html: `
<ol>
<li>Is the job actually behind? Check consumer lag and the throughput trend, not just colours.</li>
<li>Find the first red task after black ones. Open SubTasks: skew or uniform?</li>
<li>FlameGraph the red task: CPU (on-CPU) or waiting (off-CPU)?</li>
<li>Fix the cause (code, I/O pattern, key), then scale that task (enough slots and cores).</li>
<li>Re-check checkpoint duration and alignment, which usually improve once backpressure is gone.</li>
<li>Only then consider network tuning (buffer timeout, buffer debloating) or unaligned checkpoints.</li>
</ol>
${FA.C('beam', 'Beam specifics', 'In Beam all transforms share one parallelism, so you scale the whole job. To isolate a slow DoFn, batch external calls with <code>GroupIntoBatches</code> or @StartBundle/@FinishBundle, and tune <code>--maxBundleSize</code>/<code>--maxBundleTimeMills</code>. Breaking fusion with <code>Reshuffle</code> can also help.')}` }
  ],
  refs: [['How to identify the source of backpressure', 'https://flink.apache.org/2021/07/07/how-to-identify-the-source-of-backpressure/'], ['Monitoring back pressure', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/monitoring/back_pressure/'], ['Network stack deep dive', 'https://flink.apache.org/2019/06/05/a-deep-dive-into-flinks-network-stack/'], ['Large state tuning', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/state/large_state_tuning/']],
  quiz: [
    { q: 'Graph: Source (black) → Map (black) → Window (red) → Sink (blue). Bottleneck?', options: ['Source', 'Map', 'Window', 'Sink'], answer: 2, explain: 'First red task after the black ones.' },
    { q: 'Why is the Sink blue in that example?', options: ['It is the bottleneck', 'It is starving: it only gets what the slow window emits', 'Its checkpoint failed', 'It is chained'], answer: 1, explain: 'Downstream of the bottleneck tasks are idle.' },
    { q: 'A sink takes 200 µs per record. Minimum subtasks to sustain 20,000 rec/s?', type: 'number', answer: 4, explain: '1 subtask = 5,000 rec/s; 20,000 / 5,000 = 4 (plus headroom in practice).' },
    { q: '70% of records share one key. You raise window parallelism from 8 to 32. Result?', options: ['4× faster', 'Barely changes: the hot key still lands on one subtask', 'Job fails', 'Key is split automatically'], answer: 1, explain: 'HASH sends a key to exactly one subtask.' },
    { q: 'Fixes for a hot key (select all):', type: 'multi', options: ['Local/two-phase pre-aggregation', 'Key salting then merge', 'global()', 'Choosing a finer-grained key if the logic allows'], answer: [0, 1, 3], explain: 'global() makes it worse.' },
    { q: 'FlameGraph of a red sink shows most time in PreparedStatement.executeBatch / socket reads. Best direction?', options: ['Optimize CPU-heavy parsing', 'Treat it as I/O-bound: bigger batches, async/more in-flight requests, more parallelism', 'Disable checkpoints', 'Increase max parallelism'], answer: 1, explain: 'The time is spent waiting on the database.' },
    { q: 'A task is red; its SubTasks show equal records but all busy ~100%. Likely?', options: ['Key skew', 'Uniform overload: needs more parallelism or cheaper code', 'Idle source', 'Broadcast'], answer: 1, explain: 'Uniform load means skew is not the issue.' },
    { q: 'Seeing Kryo serialization dominating CPU suggests…', options: ['Use generic types more', 'Register proper TypeInformation/POJOs/Avro and chain more', 'Increase network buffers', 'Use GLOBAL'], answer: 1, explain: 'Kryo fallback is slow; give Flink proper types.' },
    { q: 'After fixing the bottleneck, what usually also improves?', options: ['Aligned checkpoint duration', 'Max parallelism', 'Number of key groups', 'Kafka partitions'], answer: 0, explain: 'Barriers no longer wait behind full buffers.' },
    { q: 'In Beam on Flink, a common way to batch external calls in a DoFn is…', options: ['GroupIntoBatches or @StartBundle/@FinishBundle buffering', 'Window.into(Global)', 'Flatten', 'setParallelism on the DoFn'], answer: 0, explain: 'Bundle-level or GroupIntoBatches batching reduces per-record I/O overhead.' }
  ]
});
