FA.registerLevel({
  id: 7, track: 'Parallel execution', icon: '🗄️', minutes: 30,
  title: 'Keyed state, key groups & state backends',
  summary: 'ValueState/ListState/MapState, operator state, key groups and rescaling, HashMap vs RocksDB, state TTL.',
  objectives: ['Choose the right state primitive', 'Explain key groups and how rescaling moves state', 'Pick between HashMap and RocksDB backends', 'Avoid unbounded state with TTL and timers'],
  sections: [
    { title: 'Keyed state vs operator state', html: `
<p>"State" is anything your operator remembers between records. The question is always: <b>who does this memory belong to?</b></p>
<ul>
<li><b>Keyed state</b> belongs to a <b>key</b>. Think of a locker per customer: whichever worker serves customer A opens A's locker. It is available after <code>keyBy</code>. Every key has its own value, and Flink sets the current key before your function is called. Types: <code>ValueState</code>, <code>ListState</code>, <code>MapState</code>, <code>ReducingState</code>, <code>AggregatingState</code>.</li>
<li><b>Operator state</b> belongs to a <b>parallel instance</b> (a subtask), not to any key. Think of each worker's own notebook. Examples: Kafka source offsets per partition, buffered elements in a sink. On rescale it is redistributed by <i>even-split</i> or <i>union</i>.</li>
<li><b>Broadcast state</b> (Level 6) is a third type: the same copy on every instance.</li>
</ul>
<table class="t"><tr><th></th><th>Keyed state</th><th>Operator state</th><th>Broadcast state</th></tr>
<tr><td>Belongs to</td><td>One key</td><td>One parallel instance</td><td>Every instance (identical copies)</td></tr>
<tr><td>How you get it</td><td>After <code>keyBy</code>: <code>getRuntimeContext().getState(…)</code></td><td><code>CheckpointedFunction</code>, sources and sinks</td><td><code>broadcast()</code> + <code>connect</code></td></tr>
<tr><td>Typical use</td><td>Per-user counters, dedup flags, window contents</td><td>Kafka offsets, sink buffers</td><td>Rules, config, small lookup tables</td></tr>
<tr><td>On rescale</td><td>Whole key groups move</td><td>Lists are even-split or union-copied</td><td>Each new instance gets a copy</td></tr></table>
${FA.D('keyed-state')}` },
    { title: 'How one state field holds a value per key', html: `
<p>This is the part that usually confuses people. You declare <b>one</b> field, <code>seen</code>, in your function. One subtask processes thousands of keys. So how can <code>seen.value()</code> be different for each key?</p>
<p>Answer: the field is not the value. It is a <b>handle</b> into a table that Flink manages. Before every <code>processElement</code> call, Flink sets the <b>current key</b> to the key of that record, and the handle reads and writes only that key's row.</p>
${FA.D('current-key')}
<pre><code>class Dedup extends KeyedProcessFunction&lt;String, Event, Event&gt; {
  private transient ValueState&lt;Boolean&gt; seen;
  public void open(OpenContext ctx) {
    ValueStateDescriptor&lt;Boolean&gt; d = new ValueStateDescriptor&lt;&gt;("seen", Types.BOOLEAN);
    d.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofHours(24)).build());
    seen = getRuntimeContext().getState(d);
  }
  public void processElement(Event e, Context ctx, Collector&lt;Event&gt; out) throws Exception {
    if (seen.value() == null) { seen.update(true); out.collect(e); }
  }
}</code></pre>
${FA.C('warn', 'Unbounded state', 'Keys keep coming (user IDs, order IDs), so state grows forever unless you clear it: use <b>State TTL</b>, or register a <b>timer</b> and call <code>state.clear()</code>. Window operators clean up automatically after the window plus allowed lateness.')}` },
    { title: 'Choosing a keyed state type', html: `
<p>All five types are per key. They differ in the <b>shape</b> of what each key remembers:</p>
${FA.D('keyed-state-types')}
<table class="t"><tr><th>Type</th><th>Holds per key</th><th>Reach for it when…</th></tr>
<tr><td><code>ValueState&lt;T&gt;</code></td><td>One value</td><td>A flag, a last-seen timestamp, a running count you update yourself</td></tr>
<tr><td><code>ListState&lt;T&gt;</code></td><td>A list you append to</td><td>You must keep the individual events (buffer until a timer fires)</td></tr>
<tr><td><code>MapState&lt;K, V&gt;</code></td><td>A map of entries</td><td>Many sub-items per key (e.g. count per page per user). With RocksDB only the touched entry is (de)serialized, unlike <code>ValueState&lt;HashMap&gt;</code></td></tr>
<tr><td><code>ReducingState&lt;T&gt;</code></td><td>One value, merged on <code>add</code></td><td>Sum, min, max: input and output have the same type</td></tr>
<tr><td><code>AggregatingState&lt;IN, OUT&gt;</code></td><td>An accumulator</td><td>Average and similar: the accumulator type differs from the result type</td></tr></table>` },
    { title: 'Operator state on rescale', html: `
<p>Keyed state can be moved key group by key group, because every value has a key. Operator state has <b>no key</b>: it is just a list owned by one instance. When the parallelism changes, Flink has two options for these lists:</p>
<ul>
<li><b>Even-split</b> (<code>getListState</code>, the default): join all lists and deal them out in equal pieces.</li>
<li><b>Union</b> (<code>getUnionListState</code>): give <b>every</b> instance the whole list and let each one keep what it needs.</li>
</ul>
${FA.D('operator-state-rescale')}
${FA.C('tip', 'In practice', 'You rarely write operator state yourself. It lives inside connectors (Kafka source, file sink) and Flink handles it. Most of your own state will be <b>keyed</b> state.')}` },
    { title: 'Key groups: how keyed state is partitioned', html: `
<p>Flink does not assign keys directly to subtasks. It assigns them to <b>key groups</b>:</p>
<pre><code>keyGroup = murmurHash(key.hashCode()) % maxParallelism
subtask  = keyGroup * parallelism / maxParallelism</code></pre>
<p>Each subtask owns a <b>contiguous range of key groups</b>. Key groups are the <b>atomic unit</b> of state redistribution: when you rescale from a savepoint, whole key groups move. That makes rescaling efficient, and it is why parallelism can never exceed max parallelism.</p>
${FA.D('keygroup-rescale')}
${FA.W('keygroups', {})}
<ul>
<li>Default max parallelism = <code>roundUpToPowerOfTwo(p + p / 2)</code> (integer division, so about 1.5 × parallelism), then clamped to at least 128 and at most 32768.</li>
<li>If max parallelism is not a multiple of parallelism, some subtasks own one more key group, which is mild built-in skew. Example: maxP 128, p 96 → 32 subtasks get 2 key groups and 64 get 1.</li>
<li><b>Set max parallelism explicitly before the first production deployment</b> and never change it afterwards. A different value is incompatible with existing state.</li>
</ul>` },
    { title: 'State backends', html: `
<p>The <b>state backend</b> decides where the working copy of state lives on each TaskManager and in what form. Your code is the same either way.</p>
${FA.D('state-backends')}
<table class="t"><tr><th></th><th>HashMapStateBackend (<code>hashmap</code>)</th><th>EmbeddedRocksDBStateBackend (<code>rocksdb</code>)</th></tr>
<tr><td>Where state lives</td><td>Java objects on the TM heap</td><td>Serialized bytes in RocksDB on local disk, cached in <b>managed memory</b></td></tr>
<tr><td>Speed</td><td>Very fast (no serialization per access)</td><td>Slower (serialize/deserialize each access)</td></tr>
<tr><td>State size</td><td>Limited by heap; GC pressure</td><td>Can be far larger than memory</td></tr>
<tr><td>Checkpoints</td><td>Full snapshots</td><td><b>Incremental</b> possible (only new SST files)</td></tr>
<tr><td>Typical use</td><td>Small/medium state, low latency</td><td>Large state, long windows, big joins</td></tr></table>
<p>Configure with <code>state.backend.type: hashmap|rocksdb</code>. The checkpoint <i>storage</i> (where snapshots go: S3/HDFS/GCS) is configured separately (<code>state.checkpoints.dir</code>). Beam: <code>--stateBackend=rocksdb</code>.</p>
${FA.C('tip', 'Memory link', 'RocksDB uses the slot\'s share of <b>managed memory</b> (Level 13). With the HashMap backend in a streaming job, managed memory is mostly unused, so you can lower <code>taskmanager.memory.managed.fraction</code> and give the heap more room.')}` }
  ],
  refs: [['Working with state', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/fault-tolerance/state/'], ['Stateful stream processing (key groups)', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/concepts/stateful-stream-processing/'], ['State backends', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/state/state_backends/'], ['Parallel execution: max parallelism', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/execution/parallel/'], ['Ten Flink gotchas (key group skew)', 'https://engineering.contentsquare.com/2021/ten-flink-gotchas/']],
  quiz: [
    { q: 'What is the atomic unit Flink moves when rescaling keyed state?', options: ['A single key', 'A key group', 'A Kafka partition', 'A slot'], answer: 1, explain: 'Key groups are redistributed as a whole.' },
    { q: 'Parallelism is 4 and max parallelism is 128. How many key groups does each subtask own?', type: 'number', answer: 32, explain: '128 / 4 = 32.' },
    { q: 'What is the default max parallelism for a job with parallelism 4?', type: 'number', answer: 128, explain: 'roundUpToPowerOfTwo(6) = 8, but the lower bound is 128.' },
    { q: 'Why must you not change max parallelism after going to production?', options: ['It slows checkpoints', 'Key → key group mapping changes, so existing keyed state can no longer be restored', 'It changes Kafka partitions', 'It disables chaining'], answer: 1, explain: 'The key group count is baked into the state layout.' },
    { q: 'Which backend supports incremental checkpoints and state larger than memory?', options: ['HashMap', 'RocksDB', 'Both equally', 'Neither'], answer: 1, explain: 'RocksDB stores state on disk and can upload only new SST files.' },
    { q: 'Which memory pool does RocksDB use for its block cache and write buffers by default?', options: ['Task heap', 'Managed memory', 'Network memory', 'Metaspace'], answer: 1, explain: 'state.backend.rocksdb.memory.managed: true by default.' },
    { q: 'You key a dedup operator by orderId and keep a ValueState forever. What goes wrong over months?', options: ['Nothing', 'State grows unbounded (every new orderId adds an entry): checkpoints and memory keep growing', 'Watermarks stop', 'Chaining breaks'], answer: 1, explain: 'Use State TTL or timers to clear old keys.' },
    { q: 'Kafka source offsets per partition are an example of…', options: ['Keyed state', 'Operator state', 'Broadcast state', 'Window state'], answer: 1, explain: 'Per-instance, non-keyed state.' },
    { q: 'A KeyedProcessFunction has one ValueState field. Records for keys A and B reach the same subtask. What does seen.value() return while processing a B record?', options: ['The last value written by any key', 'B\'s own value: Flink set the current key to B before the call', 'A list of values for all keys on the subtask', 'An error: one field cannot serve two keys'], answer: 1, explain: 'The field is a handle. Flink sets the current key before each call, so it reads and writes only that key\'s value.' },
    { q: 'You rescale from 2 to 3 instances. Which operator state mode gives every new instance the complete list of entries?', options: ['Even-split', 'Union', 'Key groups', 'Broadcast join'], answer: 1, explain: 'Union redistribution sends the whole list to every instance. Even-split deals it out in equal pieces.' },
    { q: 'Formula for the key group of a key?', options: ['key.hashCode() % parallelism', 'murmurHash(key.hashCode()) % maxParallelism', 'random() % maxParallelism', 'key % slots'], answer: 1, explain: 'Then subtask = keyGroup * parallelism / maxParallelism.' },
    { q: 'Can a keyed operator\'s parallelism be greater than its max parallelism?', options: ['Yes', 'No: there would be subtasks without key groups', 'Only with RocksDB', 'Only in batch'], answer: 1, explain: 'Parallelism ≤ max parallelism always.' }
  ]
});
