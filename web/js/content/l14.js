FA.registerLevel({
  id: 14, track: 'Apache Beam on Flink', icon: '🛰️', minutes: 40,
  title: 'Apache Beam on Flink',
  summary: 'How the FlinkRunner translates Beam, how Beam steps show up in the Flink dashboard, key pipeline options, versions, state/timers and operational gotchas.',
  objectives: ['Map Beam concepts to Flink concepts', 'Recognise Beam transforms in the Flink job graph', 'Configure FlinkPipelineOptions correctly', 'Debug and upgrade Beam jobs on Flink'],
  sections: [
    { title: 'Concept mapping', html: `
<table class="t"><tr><th>Beam</th><th>Flink</th><th>What you see in the UI</th></tr>
<tr><td>Pipeline</td><td>Job</td><td>Job name from <code>--jobName</code></td></tr>
<tr><td>PCollection</td><td>DataStream (stream of <i>WindowedValue</i>s)</td><td>Edges</td></tr>
<tr><td>ParDo / MapElements / Filter</td><td>Operator (DoFnOperator)</td><td>Named after the transform, e.g. <code>ParDo(Parse)</code>, usually chained</td></tr>
<tr><td>Fusion</td><td>Operator chaining</td><td>Several Beam steps inside one box</td></tr>
<tr><td>GroupByKey / Combine.perKey / CoGroupByKey</td><td>keyBy + windowed state operator</td><td><b>HASH</b> edge into the GBK box</td></tr>
<tr><td>Stateful DoFn (@StateId / @TimerId)</td><td>Keyed operator with keyed state + timers</td><td>HASH edge (input is keyed by the KV key)</td></tr>
<tr><td>Reshuffle</td><td>keyBy on a random/explicit key + regroup</td><td>HASH edge (breaks fusion)</td></tr>
<tr><td>Side input (View.asMap…)</td><td>Broadcast of the side input to all subtasks</td><td>BROADCAST edge into the consumer</td></tr>
<tr><td>Flatten</td><td>union</td><td>Multiple input edges</td></tr>
<tr><td>Windowing / triggers</td><td>Beam\'s own window logic inside Flink operators, driven by Flink watermarks</td><td>Watermarks tab</td></tr>
<tr><td>--parallelism</td><td>Job parallelism (all operators)</td><td>Same "Parallelism: N" on every box</td></tr></table>
${FA.D('beam-fusion')}
${FA.W('jobgraph', { title: 'A typical Beam pipeline as Flink draws it', model: { cluster: { tms: 2, slotsPerTm: 4, coresPerTm: 4, sourceRate: 15000 }, ops: [{ kind: 'source', p: 4, name: 'KafkaIO.Read' }, { kind: 'map', p: 4, name: 'ParDo(ParseOrderFn)' }, { kind: 'map', p: 4, name: 'Window.Into(FixedWindows 1m)', cost: 1 }, { kind: 'keyby', name: 'WithKeys(customerId)' }, { kind: 'reduce', p: 4, name: 'Combine.perKey(SumFn) [GroupByKey]' }, { kind: 'map', p: 4, name: 'ParDo(FormatFn)' }, { kind: 'sink', p: 4, name: 'KafkaIO.Write' }] }, controls: [{ op: 0, key: 'p', label: '--parallelism (source)', max: 16 }, { op: 1, key: 'p', label: 'ParDo', max: 16 }, { op: 2, key: 'p', label: 'Window', max: 16 }, { op: 4, key: 'p', label: 'Combine', max: 16 }, { op: 5, key: 'p', label: 'Format', max: 16 }, { op: 6, key: 'p', label: 'Sink', max: 16 }, { op: 'cluster', key: 'chaining', type: 'check', label: '--operatorChaining' }] })}
<p>Beam sets the same parallelism everywhere (set all fields to the same value to mimic it). Notice the single HASH edge where GroupByKey/Combine shuffles, and how the rest fuses into two boxes.</p>
${FA.C('', 'Note', 'Real Beam-on-Flink graphs show longer, more technical names (e.g. <code>Combine.perKey(SumFn)/GroupByKey -&gt; …/ParDo(Anonymous)/ParMultiDo(Anonymous)</code>) and sometimes extra internal steps (Impulse, SDF read wrappers, PrepareWrite). The exact naming depends on Beam and IO versions. Use your step names in the Beam code to find yourself in the graph.')}` },
    { title: 'Versions must match', html: `
<p>The classic Java FlinkRunner artifact is built per Flink minor version: <code>beam-runners-flink-1.19</code>, <code>-1.20</code>, <code>-2.0</code> … and the <b>cluster's Flink minor version must match the runner artifact</b>. From the Beam docs table: Flink 1.19 needs Beam ≥ 2.61, Flink 1.20 needs Beam ≥ 2.70, Flink 2.0 needs Beam ≥ 2.72; 1.17/1.18 support ended at Beam 2.74.</p>
<pre><code>&lt;dependency&gt;
  &lt;groupId&gt;org.apache.beam&lt;/groupId&gt;
  &lt;artifactId&gt;beam-runners-flink-1.19&lt;/artifactId&gt;
  &lt;version&gt;\${beam.version}&lt;/version&gt;
&lt;/dependency&gt;</code></pre>
${FA.C('beam', 'Action item', 'Check your project\'s pom/gradle for the <code>beam-runners-flink-1.x</code> artifact and compare it to the Flink version in the dashboard header. They must agree on the minor version.')}` },
    { title: 'The options that matter', html: `
<pre><code>--runner=FlinkRunner
--flinkMaster=jobmanager:8081         # or [auto] when submitted with "flink run"
--parallelism=8
--maxParallelism=720                  # set once; keep stable for savepoints
--streaming=true
--checkpointingInterval=60000         # DEFAULT IS -1 → NO CHECKPOINTS!
--checkpointingMode=EXACTLY_ONCE
--stateBackend=rocksdb --stateBackendStoragePath=s3://…/ckpt
--externalizedCheckpointsEnabled=true
--tolerableCheckpointFailureNumber=3  # default 0 → one failed checkpoint fails the job
--unalignedCheckpointEnabled=false
--maxBundleSize=1000 --maxBundleTimeMills=1000
--objectReuse=false --fasterCopy=false
--operatorChaining=true               # false only while debugging</code></pre>
<p>See the full list in the <a href="#/config">Config explorer</a>. Options appear in the Flink UI under the job's <b>Configuration</b> tab.</p>` },
    { title: 'Bundles, latency and checkpoints', html: `
<ul>
<li>Beam processes elements in <b>bundles</b>. On Flink streaming, a bundle closes after <code>maxBundleSize</code> elements or <code>maxBundleTimeMills</code> ms (defaults 1000 / 1000 ms). <code>@FinishBundle</code> runs then: a natural place to flush batched writes.</li>
<li>Bundles are also finished on checkpoints so that output is consistent with the snapshot.</li>
<li>Large bundle times increase latency; tiny bundles add per-bundle overhead.</li>
</ul>
${FA.D('bundles')}` },
    { title: 'State, timers and upgrades in Beam on Flink', html: `
<ul>
<li>Stateful DoFns (<code>@StateId</code> ValueState/BagState/MapState, <code>@TimerId</code> event/processing-time timers) are backed by Flink keyed state and timers, so the Flink state backend (HashMap/RocksDB) and checkpoints apply.</li>
<li>Savepoint restore maps state by <b>operator identity derived from transform names</b>. Give every transform an explicit, stable name (<code>.apply("ParseOrders", …)</code>) and don't rename steps casually.</li>
<li>Coder changes = state serializer changes. Changing a coder for a stateful step or GBK can break restore.</li>
<li>Upgrade: <code>flink stop --savepointPath</code> → submit new jar with <code>--savepointPath=…</code> (Beam option) or <code>flink run -s …</code>.</li>
</ul>` },
    { title: 'Debugging checklist for Beam jobs', html: `
<ol>
<li>Job graph: which box is red, which are black? Read the Beam step names inside the boxes.</li>
<li>If everything is one huge box, temporarily run with <code>--operatorChaining=false</code> in a test env to see per-step metrics.</li>
<li>HASH edges = your GroupByKey/Combine/stateful steps. Check the SubTasks tab for <b>key skew</b> (hot customer/site/product).</li>
<li>Watermarks tab: stuck? Check KafkaIO timestamp policy and idle partitions.</li>
<li>Checkpoints: is checkpointing even enabled (<code>--checkpointingInterval</code>)? Long alignment means backpressure.</li>
<li>Exceptions tab: Beam user-code exceptions surface here with the DoFn in the stack trace.</li>
<li>Beam metrics (Counters/Distributions) are reported as Flink metrics/accumulators unless <code>--disableMetrics</code>.</li>
</ol>` }
  ],
  refs: [['Beam Flink runner (options & compatibility table)', 'https://beam.apache.org/documentation/runners/flink/'], ['Beam Flink runner roadmap', 'https://beam.apache.org/roadmap/flink-runner/'], ['Beam programming guide (state & timers, windowing)', 'https://beam.apache.org/documentation/programming-guide/'], ['Beam capability matrix', 'https://beam.apache.org/documentation/runners/capability-matrix/']],
  quiz: [
    { q: 'Which Beam transform creates a <b>HASH</b> edge in the Flink job graph?', options: ['ParDo', 'GroupByKey (and Combine.perKey)', 'MapElements', 'Filter'], answer: 1, explain: 'GBK is a shuffle by key, so Flink uses keyBy → HASH.' },
    { q: 'Beam\'s "fusion" corresponds to which Flink concept?', options: ['Slot sharing', 'Operator chaining', 'Key groups', 'Watermarks'], answer: 1, explain: 'Fused steps run in one Flink task/thread.' },
    { q: 'Default of <code>--checkpointingInterval</code>?', options: ['60000', '-1 (disabled)', '1000', '300000'], answer: 1, explain: 'Without setting it, a Beam streaming job on Flink takes no checkpoints.' },
    { q: 'Your cluster runs Flink 1.19.x. Which runner artifact must the Beam job use?', options: ['beam-runners-flink-1.18', 'beam-runners-flink-1.19', 'beam-runners-flink-2.0', 'Any'], answer: 1, explain: 'Runner minor version must match the cluster.' },
    { q: 'What happens with <code>--tolerableCheckpointFailureNumber</code> left at its default 0?', options: ['Failures are ignored', 'One failed checkpoint fails the job (triggering restart)', 'Checkpoints are disabled', 'Only savepoints fail'], answer: 1, explain: '0 tolerable failures means any checkpoint failure fails the job.' },
    { q: 'In Beam on Flink, how do you set different parallelism for a slow DoFn only?', options: ['.setParallelism on the ParDo', 'Not directly: --parallelism is job-wide; batch I/O, tune bundles or split into separate jobs', 'Use slotSharingGroup', 'Use Reshuffle.withParallelism'], answer: 1, explain: 'The classic FlinkRunner applies one parallelism to all transforms.' },
    { q: 'A side input (View.asMap) consumed by a ParDo usually shows up as…', options: ['FORWARD edge', 'BROADCAST of the side input to all consumer subtasks', 'GLOBAL edge', 'No edge'], answer: 1, explain: 'Side inputs must be available to every parallel instance.' },
    { q: 'What closes a bundle on the Flink streaming runner by default? (select all)', type: 'multi', options: ['1000 elements (maxBundleSize)', '1000 ms (maxBundleTimeMills)', 'A checkpoint', 'Every GroupByKey'], answer: [0, 1, 2], explain: 'Size, time, and checkpoints finish bundles.' },
    { q: 'Why give every Beam transform an explicit stable name?', options: ['Prettier UI only', 'State in savepoints is mapped via operator identity derived from names; renaming can break restore', 'Required for compilation', 'It sets parallelism'], answer: 1, explain: 'Stable names = stable state mapping across upgrades.' },
    { q: 'Everything is fused into one huge box and you cannot tell which step is slow. Debug approach?', options: ['--operatorChaining=false in a test environment to see each step separately', 'Increase maxParallelism', 'Disable checkpoints', 'Use GLOBAL'], answer: 0, explain: 'Unchained tasks give per-step busy/backpressure metrics (with some overhead).' },
    { q: 'Beam stateful DoFn state is stored in…', options: ['Beam\'s own database', 'Flink keyed state (HashMap or RocksDB backend), checkpointed by Flink', 'Kafka', 'Driver memory'], answer: 1, explain: 'The FlinkRunner maps Beam state & timers to Flink keyed state & timers.' }
  ]
});
