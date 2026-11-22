FA.registerLevel({
  id: 10, track: 'Operations', icon: '🖥️', minutes: 35,
  title: 'The Flink Web UI in depth',
  summary: 'Every screen of the dashboard: overview, job graph boxes and arrows, vertex tabs, checkpoints, exceptions, TaskManagers and Job Manager.',
  objectives: ['Read each line inside a job-graph box', 'Use SubTasks, BackPressure, Watermarks, Metrics and FlameGraph tabs', 'Read the Checkpoints tab', 'Know where to look for each kind of problem'],
  sections: [
    { title: 'Guided tour', html: `
<p>This mock follows the real Flink 1.x dashboard layout (labels can differ slightly between versions). Click around: every dashed red element explains itself. Open <b>Running Jobs</b>, then try each tab.</p>
${FA.W('dashboard', {})}` },
    { title: 'Reading one box', html: `
${FA.D('box-anatomy')}
<ul>
<li><b>Colour</b>: blue = idle, red = busy, black = backpressured, with blends in between.</li>
<li>"(max)" means the <b>worst subtask</b>. One hot subtask (skew) is enough to make the box look busy, so check the <b>SubTasks</b> tab.</li>
<li><b>Records Received/Sent</b> in the table below the graph count only <b>network</b> traffic between tasks. A source shows 0 received and a sink 0 sent; chained hand-offs are invisible there.</li>
</ul>` },
    { title: 'Where to look for what', html: `
<table class="t"><tr><th>Symptom</th><th>Look at</th></tr>
<tr><td>Job keeps restarting</td><td>Exceptions tab (root cause, deepest "Caused by"), TaskManager logs</td></tr>
<tr><td>Lag growing / slow</td><td>Job graph colours → BackPressure tab → FlameGraph of the red task</td></tr>
<tr><td>Windows never fire</td><td>Watermarks tab ("No Watermark", stuck values) and idle partitions</td></tr>
<tr><td>Checkpoints failing</td><td>Checkpoints → History (expand row: per-task sync/async/alignment), Summary</td></tr>
<tr><td>One subtask much slower</td><td>SubTasks tab: records received per subtask → key skew</td></tr>
<tr><td>OOM / container killed</td><td>Task Managers → Metrics (heap, managed, network), logs; Level 12 memory model</td></tr>
<tr><td>"Is my config really applied?"</td><td>Job Manager → Configuration, Job → Configuration</td></tr>
<tr><td>Long startup after restart</td><td>TimeLine (INITIALIZING = state restore)</td></tr></table>` },
    { title: 'Useful metrics (Metrics tab / Prometheus)', html: `
<ul>
<li><code>numRecordsInPerSecond</code>, <code>numRecordsOutPerSecond</code>: throughput per task/operator.</li>
<li><code>busyTimeMsPerSecond</code>, <code>backPressuredTimeMsPerSecond</code>, <code>idleTimeMsPerSecond</code>: they sum to about 1000 per subtask.</li>
<li><code>currentInputWatermark</code> / <code>currentOutputWatermark</code>.</li>
<li>Kafka source: <code>pendingRecords</code> / records-lag-max: consumer lag.</li>
<li><code>lastCheckpointDuration</code>, <code>numberOfFailedCheckpoints</code> (job level).</li>
<li>TM: <code>Status.JVM.Memory.Heap.Used</code>, <code>Status.Flink.Memory.Managed.Used</code>, GC time.</li>
</ul>
${FA.C('tip', 'REST API', 'Everything in the UI comes from the REST API on the same port (8081): e.g. <code>/jobs/overview</code>, <code>/jobs/&lt;id&gt;/vertices/&lt;vid&gt;/backpressure</code>, <code>/jobs/&lt;id&gt;/checkpoints</code>. That is useful for scripts and alerts.')}` }
  ],
  refs: [['Monitoring back pressure', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/monitoring/back_pressure/'], ['Monitoring checkpointing', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/monitoring/checkpoint_monitoring/'], ['Flame graphs', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/debugging/flame_graphs/'], ['Metrics', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/metrics/'], ['Confluent Flink 101: Web UI exercise', 'https://developer.confluent.io/courses/apache-flink/web-ui-exercise']],
  quiz: [
    { q: 'In the job graph, a fully backpressured task is coloured…', options: ['Red', 'Blue', 'Black', 'Green'], answer: 2, explain: 'Blue idle, red busy, black backpressured.' },
    { q: 'What does "Busy (max): 95%" in a box mean?', options: ['Average busy time of all subtasks', 'The busiest subtask of that task was working 95% of the time', 'CPU of the TaskManager', '95% of records processed'], answer: 1, explain: '(max) is the worst subtask.' },
    { q: 'Why does a source task show 0 for "Records Received"?', options: ['Bug', 'The table counts records received over network edges from other tasks; a source has none', 'The source is idle', 'Metrics are disabled'], answer: 1, explain: 'Only inter-task network traffic is counted there.' },
    { q: 'Job keeps restarting. First place to look?', options: ['FlameGraph', 'Exceptions tab', 'Watermarks', 'Submit New Job'], answer: 1, explain: 'Exceptions tab shows the root cause and history.' },
    { q: 'Windows never fire. Which vertex tab helps most?', options: ['Accumulators', 'Watermarks', 'TaskManagers', 'Detail'], answer: 1, explain: 'Look for "No Watermark" or stuck values (idle splits).' },
    { q: 'One subtask of a window task receives 10× more records than the others. What is it?', options: ['Backpressure', 'Key skew', 'GC', 'Checkpoint alignment'], answer: 1, explain: 'Uneven per-subtask input under HASH = skewed keys.' },
    { q: 'Which tab shows which method burns CPU in a busy task?', options: ['FlameGraph', 'TimeLine', 'Configuration', 'Checkpoints'], answer: 0, explain: 'Requires rest.flamegraph.enabled: true.' },
    { q: 'Subtasks stay long in INITIALIZING after a restart. Likely reason?', options: ['Slot shortage', 'Large state being restored', 'Watermark idleness', 'Too many network buffers'], answer: 1, explain: 'INITIALIZING includes state restore.' },
    { q: 'Where do you verify the effective value of taskmanager.numberOfTaskSlots?', options: ['Job Manager → Configuration', 'Exceptions', 'Watermarks', 'Completed Jobs'], answer: 0, explain: 'The Job Manager page shows effective cluster config.' },
    { q: 'busyTimeMsPerSecond + backPressuredTimeMsPerSecond + idleTimeMsPerSecond ≈', type: 'number', answer: 1000, tolerance: 1, explain: 'They partition each second of a subtask\'s time.' }
  ]
});
