FA.registerLevel({
  id: 4, track: 'Parallel execution', icon: '⚡', minutes: 30,
  title: 'Parallelism, task slots & CPU',
  summary: 'Operator vs job parallelism, slot sharing, how many slots a job needs, slot sharing groups, and why slots are not CPUs.',
  objectives: ['Set parallelism at all four levels and know which wins', 'Compute the slots a job needs with slot sharing', 'Use slot sharing groups deliberately', 'Size slots against CPU cores'],
  sections: [
    { title: 'Where parallelism comes from', html: `
<p>Parallelism can be set at four levels. The <b>most specific wins</b>:</p>
<ol>
<li><b>Operator level</b>: <code>.map(...).setParallelism(8)</code></li>
<li><b>Execution environment</b>: <code>env.setParallelism(4)</code> (all operators without an explicit value)</li>
<li><b>Client / submission</b>: <code>flink run -p 4 …</code> (or the UI/REST field, or Beam <code>--parallelism=4</code>)</li>
<li><b>System default</b>: <code>parallelism.default</code> in config.yaml (default 1)</li>
</ol>
<p>Separately, <b>max parallelism</b> (<code>setMaxParallelism</code> / <code>pipeline.max-parallelism</code>) is the upper limit for rescaling keyed state, i.e. the number of <i>key groups</i> (Level 7).</p>
${FA.C('beam', 'Beam', 'With Beam on Flink you normally have ONE knob: <code>--parallelism</code>, applied to every transform. You don\'t call setParallelism per transform as in the DataStream API.')}` },
    { title: 'Slot sharing: how many slots does a job need?', html: `
<p>By default, all operators of a job are in the slot sharing group <code>"default"</code>, and Flink lets <b>one slot hold one subtask of every task</b> in the group, i.e. a whole slice of the pipeline. Consequences:</p>
<ul>
<li><b>Slots needed = the highest parallelism in the job</b> (per slot sharing group, summed over groups). You do not add up all operators.</li>
<li>Better utilization: light tasks (source, sink) share slots with heavy tasks (window), so heavy work is spread fairly across slots.</li>
</ul>
${FA.D('slot-sharing')}
${FA.W('slots', {})}
<p>Try: Source 2, Map 6, Window 6, Sink 2 with 2×3 slots → needs 6 and fits. Now tick <b>Window in its own slot sharing group</b>: you need 6 + 6 = 12 slots.</p>` },
    { title: 'Slot sharing groups', html: `
<pre><code>stream.map(new HeavyMl()).slotSharingGroup("ml").setParallelism(4);</code></pre>
<ul>
<li>Operators in different groups <b>never share a slot</b>, which isolates them (e.g. an expensive ML operator or slow external I/O).</li>
<li>An operator <b>inherits</b> the group of its inputs if all inputs share one, so everything downstream of "ml" is also in "ml" unless you set it back to <code>"default"</code>.</li>
<li>Different group → different slot → <b>no chaining</b> across the boundary.</li>
</ul>` },
    { title: 'Slots are not CPUs', html: `
<p>A TaskManager with 8 slots and 2 cores <b>accepts</b> 8 subtasks per pipeline slice, but they all fight for 2 cores. Each slot runs several threads (one per task in the slot), so CPU oversubscription shows up as <b>everything slow and busy</b>, not as an error.</p>
${FA.D('slots-vs-cpu')}
<ul>
<li>Start with <b>slots ≈ cores</b> (or slightly fewer if there is heavy GC, RocksDB compaction or I/O threads).</li>
<li>IO-bound jobs (waiting on external services) can use more slots than cores; CPU-bound jobs should not.</li>
<li>On Kubernetes, the TM pod's CPU request/limit must match what the slots will really use.</li>
</ul>
${FA.C('tip', 'Try it', 'In the <a href="#/lab/cpu">"Slots are not CPUs" challenge</a> you fix an oversubscribed TaskManager.')}` },
    { title: 'Choosing parallelism', html: `
<ul>
<li><b>Kafka sources</b>: source parallelism above the number of partitions leaves idle source subtasks (they also need idleness handling for watermarks). Usually partitions ≥ source parallelism.</li>
<li><b>Bottleneck first</b>: raise the parallelism of the slow (red) task, not everything.</li>
<li><b>Key skew</b>: parallelism cannot split a single hot key (Level 11).</li>
<li><b>Headroom</b>: size for peak load plus catch-up after a restart (lag replay).</li>
</ul>` }
  ],
  refs: [['Parallel execution', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/execution/parallel/'], ['Flink architecture: task slots & resources', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/concepts/flink-architecture/'], ['Chaining & slot sharing groups', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/overview/']],
  quiz: [
    { q: 'env.setParallelism(4), flink run -p 2, and .map().setParallelism(8). What is the map\'s parallelism?', options: ['2', '4', '8', '1'], answer: 2, explain: 'Operator-level setting wins.' },
    { q: 'A job: Source p=2, Map p=6, Window p=6, Sink p=2, all in the default slot sharing group. How many slots does it need?', type: 'number', answer: 6, explain: 'With slot sharing, slots needed = max parallelism in the group = 6.' },
    { q: 'A job: Source p=2, Map p=6, Window p=6, Sink p=2. Source and Map are in the "default" slot sharing group; Window and Sink are in group "w". How many slots does it need?', type: 'number', answer: 12, explain: 'default: max(2,6)=6; w: max(6,2)=6; total 12.' },
    { q: 'A TM has 8 slots and 2 CPU cores and all slots run CPU-heavy work. What happens?', options: ['Flink refuses to start', 'Subtasks compete for 2 cores, so throughput drops and tasks look busy', 'Flink automatically reduces slots to 2', 'Each slot gets 0.25 cores guaranteed'], answer: 1, explain: 'Slots do not isolate CPU; oversubscription just slows everything.' },
    { q: 'Benefits of slot sharing (select all):', type: 'multi', options: ['Slots needed equals the highest parallelism, so no need to count all tasks', 'Heavy and light subtasks are mixed, which improves utilization', 'It guarantees CPU isolation', 'It allows a whole pipeline slice in one slot'], answer: [0, 1, 3], explain: 'No CPU isolation is ever guaranteed by slots.' },
    { q: 'An operator follows a map in group "ml" and has no explicit group. Its group is…', options: ['"default"', '"ml" (inherited from its input)', 'A new random group', 'None'], answer: 1, explain: 'Operators inherit the slot sharing group of their inputs when all inputs share it.' },
    { q: 'What does max parallelism limit?', options: ['Number of TaskManagers', 'The upper bound for rescaling keyed operators (number of key groups)', 'Checkpoint frequency', 'Number of Kafka partitions'], answer: 1, explain: 'Max parallelism = key groups = rescale limit.' },
    { q: 'Kafka topic has 6 partitions; Kafka source parallelism is 10. Consequence?', options: ['Each partition is read by two subtasks', '4 source subtasks have no partition and are idle', 'The job fails', 'Flink creates 4 new partitions'], answer: 1, explain: 'Partitions are assigned to subtasks; extra subtasks are idle (watch out for watermarks: use withIdleness).' },
    { q: 'In Beam on Flink, how is parallelism usually controlled?', options: ['setParallelism per PTransform', 'One job-wide --parallelism pipeline option', 'Number of DoFns', 'It cannot be controlled'], answer: 1, explain: 'FlinkPipelineOptions.parallelism applies to the whole job.' },
    { q: 'Good starting point for slots per TaskManager?', options: ['Always 1', 'About the number of CPU cores of the TM', '10× the cores', 'Equal to max parallelism'], answer: 1, explain: 'Slots ≈ cores is the common rule of thumb; adjust for I/O-bound work.' }
  ]
});
