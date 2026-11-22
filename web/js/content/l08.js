FA.registerLevel({
  id: 8, track: 'Time & reliability', icon: '⏱️', minutes: 30,
  title: 'Event time, watermarks & windows',
  summary: 'Timestamp assigners, watermark strategies, idle sources, tumbling/sliding/session windows, allowed lateness and side outputs.',
  objectives: ['Configure a WatermarkStrategy', 'Predict when a window fires', 'Handle late data and idle partitions', 'Choose the right window type'],
  sections: [
    { title: 'Watermarks', html: `
<p>A <b>watermark W(t)</b> is a statement flowing in the stream: <i>"I do not expect any more events with timestamp ≤ t"</i>. Event-time windows and timers fire when the watermark passes their end time.</p>
<pre><code>WatermarkStrategy
    .&lt;Order&gt;forBoundedOutOfOrderness(Duration.ofSeconds(5))   // tolerate 5 s disorder
    .withTimestampAssigner((order, kafkaTs) -&gt; order.eventTimeMillis())
    .withIdleness(Duration.ofMinutes(1));                          // see "idle sources"</code></pre>
<ul>
<li><code>forBoundedOutOfOrderness(B)</code>: watermark = <i>max timestamp seen − B − 1 ms</i>.</li>
<li><code>forMonotonousTimestamps()</code>: B = 0, for strictly ordered data.</li>
<li>Watermarks are emitted periodically (<code>pipeline.auto-watermark-interval</code>, default 200 ms).</li>
<li>An operator with several inputs uses the <b>minimum</b> of its input watermarks. The slowest input holds everyone back.</li>
</ul>
${FA.D('watermark-flow')}
${FA.W('watermark', {})}` },
    { title: 'Idle sources: the stuck-watermark classic', html: `
<p>If one Kafka partition (or one source subtask) receives no data, its watermark never advances. Because downstream takes the <b>minimum</b>, <b>all windows stop firing</b>. In the UI the Watermarks tab shows "No Watermark" or a value stuck in the past.</p>
<p>Fix: <code>.withIdleness(Duration)</code> marks a split idle after that duration so it is ignored for the minimum. Also check source parallelism vs partition count (Level 4).</p>
${FA.D('idle-min')}
${FA.C('beam', 'Beam', 'In Beam, watermarks come from the source IO (e.g. KafkaIO\'s timestamp policy: <code>withLogAppendTime()</code>, <code>withCreateTime(maxDelay)</code>, or a custom policy). The same stuck-watermark symptom appears in the Flink UI when a partition is idle.')}` },
    { title: 'Window types', html: `
<table class="t"><tr><th>Window</th><th>Shape</th><th>Example</th></tr>
<tr><td>Tumbling</td><td>fixed, non-overlapping</td><td>orders per minute: <code>TumblingEventTimeWindows.of(Duration.ofMinutes(1))</code></td></tr>
<tr><td>Sliding</td><td>fixed size, overlapping by slide</td><td>last 10 min, updated every minute (each element belongs to 10 windows)</td></tr>
<tr><td>Session</td><td>dynamic, closed by an inactivity gap</td><td>user sessions with a 30 min gap</td></tr>
<tr><td>Global</td><td>one window per key, custom trigger</td><td>count-based or custom logic</td></tr></table>
${FA.D('window-types')}
<p>Windows are <b>keyed</b> (after keyBy, parallel). <code>windowAll</code> on a non-keyed stream runs with <b>parallelism 1</b>, which is a bottleneck by design.</p>
<p>Prefer <code>aggregate()</code>/<code>reduce()</code> (incremental: one accumulator per window) over <code>process()</code> on a full window (buffers all elements until it fires).</p>` },
    { title: 'Late data', html: `
<ul>
<li><b>Late element</b>: arrives after the watermark already passed its window end.</li>
<li>Default: dropped (counted in the <code>numLateRecordsDropped</code> metric).</li>
<li><code>.allowedLateness(Duration)</code>: keep window state longer; each late element <b>re-fires</b> the window with an updated result, so downstream must handle updates.</li>
<li><code>.sideOutputLateData(tag)</code>: route too-late elements to a side output for logging or repair.</li>
</ul>
${FA.D('late-data')}
<pre><code>OutputTag&lt;Order&gt; late = new OutputTag&lt;&gt;("late") {};
var result = orders.keyBy(Order::customer)
    .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
    .allowedLateness(Duration.ofSeconds(30))
    .sideOutputLateData(late)
    .aggregate(new SumAgg());
result.getSideOutput(late).sinkTo(deadLetterSink);</code></pre>` }
  ],
  refs: [['Timely stream processing', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/concepts/time/'], ['Generating watermarks', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/event-time/generating_watermarks/'], ['Windows', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/windows/'], ['Beam programming guide: windowing & triggers', 'https://beam.apache.org/documentation/programming-guide/']],
  quiz: [
    { q: 'With <code>forBoundedOutOfOrderness(5 s)</code>, max event time seen is 12:00:20.000. What is the watermark?', options: ['12:00:20.000', '12:00:15.000', '12:00:14.999', '12:00:25.000'], answer: 2, explain: 'max − bound − 1 ms = 12:00:14.999.' },
    { q: 'An operator has two inputs with watermarks 10:05 and 10:01. Its watermark is…', options: ['10:05', '10:01 (the minimum)', '10:03 (average)', 'undefined'], answer: 1, explain: 'Multi-input operators take the minimum.' },
    { q: 'One of 8 Kafka partitions receives no data. Windows stopped firing. Best fix?', options: ['Increase parallelism', 'WatermarkStrategy.withIdleness(...)', 'Disable checkpointing', 'Use processing time everywhere'], answer: 1, explain: 'Idleness lets the idle split be excluded from the minimum.' },
    { q: 'A tumbling event-time window [10:00, 10:01) fires when…', options: ['the first element arrives', 'the watermark reaches ≥ 10:00:59.999', 'processing time is 10:01', 'the window has 100 elements'], answer: 1, explain: 'Event-time windows fire on watermark ≥ end − 1 ms.' },
    { q: 'What happens by default to an element whose window has already fired (no allowed lateness)?', options: ['It re-fires the window', 'It is dropped', 'It goes to the next window', 'The job fails'], answer: 1, explain: 'Dropped by default; use sideOutputLateData to capture it.' },
    { q: 'Effect of <code>allowedLateness(30 s)</code>?', options: ['Delays the first firing by 30 s', 'Keeps window state 30 s longer and re-fires the window for each late element', 'Drops anything later than 30 s after arrival', 'Changes the watermark bound'], answer: 1, explain: 'Late firings produce updated results.' },
    { q: 'Why prefer aggregate()/reduce() over a full ProcessWindowFunction?', options: ['They support more types', 'Incremental: one accumulator per window instead of buffering all elements', 'They are processing-time only', 'They avoid watermarks'], answer: 1, explain: 'Incremental aggregation keeps state small.' },
    { q: 'Which window type is closed by a gap of inactivity?', options: ['Tumbling', 'Sliding', 'Session', 'Global'], answer: 2, explain: 'Session windows merge until a gap is seen.' },
    { q: '<code>windowAll()</code> on a non-keyed stream runs with parallelism…', type: 'number', answer: 1, explain: 'Non-keyed all-windows are not parallel.' },
    { q: 'Bigger out-of-orderness bound means… (select all)', type: 'multi', options: ['Fewer late (dropped) events', 'Higher latency before windows fire', 'Less state', 'More parallelism'], answer: [0, 1], explain: 'It is a completeness vs latency trade-off. State is held longer, not less.' }
  ]
});
