FA.registerLevel({
  id: 3, track: 'Foundations', icon: '🔗', minutes: 25,
  title: 'DataStream API, operators & chaining',
  summary: 'Sources, transformations and sinks; operator names; why some operators share one box and others do not.',
  objectives: ['Write the skeleton of a DataStream job', 'Know common transformations', 'State the exact conditions for operator chaining', 'Use startNewChain(), disableChaining() and naming for debugging'],
  sections: [
    { title: 'A complete DataStream job', html: `
<pre><code>StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(60_000);

KafkaSource&lt;Order&gt; source = KafkaSource.&lt;Order&gt;builder()
    .setBootstrapServers("kafka:9092").setTopics("orders")
    .setValueOnlyDeserializer(new OrderSchema()).build();

DataStream&lt;Order&gt; orders = env.fromSource(source,
    WatermarkStrategy.&lt;Order&gt;forBoundedOutOfOrderness(Duration.ofSeconds(5)), "Kafka orders");

orders
    .filter(o -&gt; o.amount() &gt; 0).name("Valid orders")
    .map(o -&gt; new Total(o.customerId(), o.amount())).name("To total")
    .keyBy(Total::customerId)
    .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
    .reduce(Total::add).name("Per-minute total").uid("per-minute-total")
    .sinkTo(kafkaSink).name("Kafka totals");

env.execute("orders-per-minute");</code></pre>
<ul>
<li><b>Sources</b>: <code>fromSource(KafkaSource / FileSource …)</code>.</li>
<li><b>Transformations</b>: <code>map</code> (1→1), <code>filter</code> (1→0/1), <code>flatMap</code> (1→n), <code>keyBy</code> (partition by key, giving a <i>KeyedStream</i>), <code>window</code>, <code>reduce/aggregate</code>, <code>process</code> (ProcessFunction with timers/state), <code>connect</code>/<code>union</code>, async I/O.</li>
<li><b>Sinks</b>: <code>sinkTo(KafkaSink …)</code>. Nothing runs until <code>env.execute()</code>.</li>
</ul>
${FA.C('tip', 'Always set .name() and .uid()', '<code>.name()</code> is what you see in the UI boxes. <code>.uid()</code> is a stable operator ID used to map state in savepoints. Without it, changing the job graph can break state restore (Level 9).')}` },
    { title: 'Operator chaining: the rules', html: `
<p>Flink <b>chains</b> consecutive operators into one task so they run in the <b>same thread</b> and records are passed by method call: no serialization, no network buffer, no thread handover. Two operators are chained only if <b>all</b> of these hold:</p>
${FA.D('chaining')}
<ol>
<li>The connection is <b>FORWARD</b> (no keyBy/rebalance/broadcast… in between).</li>
<li>They have the <b>same parallelism</b>.</li>
<li>They are in the <b>same slot sharing group</b>.</li>
<li>Chaining is not disabled: globally (<code>env.disableOperatorChaining()</code>), or on either operator (<code>disableChaining()</code>), and the downstream operator did not call <code>startNewChain()</code>.</li>
<li>(Also: the downstream operator has a single input, and both operators allow chaining. Most built-in ones do.)</li>
</ol>
${FA.W('jobgraph', { title: 'Break and fix the chain: change parallelism or chaining options', model: { cluster: { tms: 1, slotsPerTm: 4, coresPerTm: 4, sourceRate: 10000, chaining: true }, ops: [{ kind: 'source', p: 2, name: 'Kafka' }, { kind: 'map', p: 2, name: 'Parse' }, { kind: 'filter', p: 2, name: 'Valid', chain: 'auto' }, { kind: 'map', p: 2, name: 'Enrich' }, { kind: 'sink', p: 2, name: 'Out' }] }, controls: [{ op: 2, key: 'p', label: 'Filter parallelism', max: 8 }, { op: 3, key: 'chain', type: 'select', label: 'Enrich chaining', options: [['auto', 'auto'], ['startNew', 'startNewChain()'], ['disable', 'disableChaining()']] }, { op: 'cluster', key: 'chaining', type: 'check', label: 'Global chaining' }] })}
<p>Notice: when the Filter's parallelism changes, Flink silently switches the edges to <b>REBALANCE</b> and the chain breaks into several boxes.</p>` },
    { title: 'Why you sometimes break chains on purpose', html: `
<ul>
<li><b>Debugging</b>: a chain is one box, so busy/backpressure metrics are per <i>task</i>, not per operator. Disabling chaining temporarily shows each operator separately.</li>
<li><b>Isolating heavy work</b>: <code>startNewChain()</code> puts an expensive operator into its own thread so it can overlap with upstream work (a form of pipelining).</li>
<li><b>Different parallelism</b>: e.g. a slow external call needs more subtasks than the source.</li>
</ul>
${FA.C('warn', 'Cost', 'Every unchained edge adds serialization, network buffers and a thread handover. Do not disable chaining in production "just because".')}` },
    { title: 'How names appear in the UI', html: `
<p>A chained task is displayed with operator names joined by arrows, e.g. <code>Source: Kafka orders -&gt; Valid orders -&gt; To total</code>, plus <code>Parallelism: 4</code>. Sources are prefixed <code>Source:</code> and sinks <code>Sink:</code> (exact sink naming varies by connector/version, e.g. <code>Kafka totals: Writer</code> for Sink V2 writers).</p>` }
  ],
  refs: [['DataStream operators, chaining & resource groups', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/overview/'], ['Flink architecture: tasks and operator chains', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/concepts/flink-architecture/']],
  quiz: [
    { q: 'Which edge type is <b>required</b> for two operators to be chained?', options: ['HASH', 'FORWARD', 'REBALANCE', 'BROADCAST'], answer: 1, explain: 'Only forward (one-to-one) connections can be chained.' },
    { q: 'Map (p=4) is followed by Filter (p=2) with no partitioning call. What happens?', options: ['They are chained', 'Flink uses REBALANCE and they become separate tasks', 'The job fails to compile', 'Flink uses HASH'], answer: 1, explain: 'Different parallelism → default REBALANCE → no chaining.' },
    { q: 'Conditions for chaining (select ALL that apply):', type: 'multi', options: ['Same parallelism', 'Same slot sharing group', 'FORWARD connection', 'Both operators must be keyed', 'Chaining not disabled on either operator'], answer: [0, 1, 2, 4], explain: 'Keyed-ness is not a requirement. keyBy actually creates a HASH edge which prevents chaining.' },
    { q: '<code>filter(...).map(...).startNewChain().map(...)</code>: what is chained?', options: ['All three together', 'filter alone; the two maps chained together', 'Nothing', 'filter + first map; second map alone'], answer: 1, explain: 'startNewChain() starts a new chain at that map, so the first map is not chained to filter, and the two maps chain together.' },
    { q: 'Why does chaining improve performance?', options: ['It increases parallelism', 'Records pass by method call in the same thread: no serialization, buffering or thread handover', 'It compresses state', 'It enables exactly-once'], answer: 1, explain: 'Chained operators run in one thread inside one task.' },
    { q: 'You want per-operator busy/backpressure metrics while debugging a long chain. Quickest option?', options: ['Increase parallelism', 'Temporarily disable operator chaining', 'Enable unaligned checkpoints', 'Switch to RocksDB'], answer: 1, explain: 'Without chaining every operator is its own task/box with its own metrics.' },
    { q: 'What is <code>.uid("…")</code> mainly for?', options: ['The box label in the Web UI', 'A stable operator ID used to match state in savepoints/checkpoints across job changes', 'Kafka consumer group ID', 'Setting parallelism'], answer: 1, explain: '.name() is the label; .uid() is the stable state mapping ID.' },
    { q: 'When does a DataStream program actually start running?', options: ['When the source is created', 'When env.execute() is called', 'When the first map is defined', 'Immediately on JVM start'], answer: 1, explain: 'The API builds a graph lazily; execute() submits it.' },
    { q: 'A job has 6 operators all at parallelism 3 with only FORWARD edges and default settings. How many boxes in the UI?', type: 'number', answer: 1, explain: 'All are chainable → one task.' },
    { q: 'Which call prevents ONLY that operator from chaining with neighbours?', options: ['env.disableOperatorChaining()', 'disableChaining()', 'rebalance()', 'slotSharingGroup("x") on all operators'], answer: 1, explain: 'disableChaining() affects only that operator; env.disableOperatorChaining() affects the whole job.' }
  ]
});
