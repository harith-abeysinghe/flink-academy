FA.registerLevel({
  id: 10, track: 'Time & reliability', icon: '📸', minutes: 30,
  title: 'Checkpoints, savepoints & recovery',
  summary: 'Barriers, alignment, unaligned checkpoints, exactly-once vs end-to-end, savepoints, uids, restart strategies.',
  objectives: ['Explain how barriers create consistent snapshots', 'Know why backpressure slows aligned checkpoints', 'Use savepoints for upgrades and rescaling', 'Configure restart and checkpoint settings sensibly'],
  sections: [
    { title: 'How a checkpoint works', html: `
<ol>
<li>The JobMaster's checkpoint coordinator triggers checkpoint <i>n</i>; sources record their position (e.g. Kafka offsets) and inject <b>barrier n</b> into their output streams.</li>
<li>Barriers flow <b>with the data</b>, never overtaking records (in aligned mode). Everything before barrier n belongs to checkpoint n.</li>
<li>An operator with several inputs <b>aligns</b>: it waits until barrier n has arrived on <b>all</b> inputs, then snapshots its state (asynchronously) and forwards the barrier.</li>
<li>When all sinks have acknowledged, checkpoint n is <b>complete</b>; transactional sinks then commit (two-phase commit, e.g. KafkaSink EXACTLY_ONCE).</li>
</ol>
${FA.D('checkpoint-flow')}
${FA.W('barriers', {})}
<p>This is a variant of the Chandy-Lamport algorithm ("asynchronous barrier snapshotting").</p>` },
    { title: 'Checkpoints under backpressure', html: `
<p>When tasks are backpressured, network buffers are full, so barriers wait behind all the queued data. Alignment can take minutes, and checkpoints time out (<code>execution.checkpointing.timeout</code>, default 10 min). The Checkpoints tab shows a long <b>alignment duration</b> and expired checkpoints.</p>
${FA.D('aligned-vs-unaligned')}
<ul>
<li><b>Fix the bottleneck</b> first (Level 12).</li>
<li><b>Unaligned checkpoints</b> (<code>execution.checkpointing.unaligned: true</code>): barriers overtake in-flight data, which is stored as part of the checkpoint. Checkpoint time becomes independent of backpressure, but size and I/O grow.</li>
<li><b>Buffer debloating</b> (<code>taskmanager.network.memory.buffer-debloat.enabled</code>): keeps in-flight data small so aligned barriers move faster.</li>
</ul>` },
    { title: 'Exactly-once: what it really means', html: `
<ul>
<li><b>Exactly-once state</b>: after recovery, every record's effect on Flink state is counted once. Records after the last checkpoint are <i>replayed</i> from the source.</li>
<li><b>End-to-end exactly-once</b> also needs <b>replayable sources</b> (Kafka) and <b>transactional or idempotent sinks</b> (KafkaSink with EXACTLY_ONCE + a transactional id prefix; upserts by key into a DB). Otherwise replays cause duplicates in the external system: <b>at-least-once output</b>.</li>
<li>Transactional Kafka output becomes visible only when the checkpoint completes, so consumers with <code>isolation.level=read_committed</code> see results with checkpoint-interval latency.</li>
</ul>
${FA.D('recovery-replay')}` },
    { title: 'Savepoints', html: `
<table class="t"><tr><th></th><th>Checkpoint</th><th>Savepoint</th></tr>
<tr><td>Purpose</td><td>Automatic failure recovery</td><td>Planned operations: upgrade code, change parallelism, migrate cluster/version, A/B</td></tr>
<tr><td>Owner</td><td>Flink (deleted when superseded, unless retained)</td><td>You (never deleted automatically)</td></tr>
<tr><td>Format</td><td>Backend-specific, may be incremental</td><td>Canonical (portable), or native format if requested</td></tr></table>
<pre><code># stop gracefully with a savepoint (preferred over cancel)
flink stop --savepointPath s3://bucket/savepoints &lt;jobId&gt;
# restart new code / new parallelism from it
flink run -s s3://bucket/savepoints/savepoint-abc123 -p 8 my-job.jar
# if you removed an operator that had state:
flink run -s ... --allowNonRestoredState my-job.jar</code></pre>
${FA.D('savepoint-upgrade')}
${FA.C('warn', 'Assign .uid() to stateful operators', 'State in a savepoint is matched to operators by <b>operator ID</b>. Without explicit <code>.uid()</code>, IDs are generated from the graph structure, so inserting a map can change them and the restore fails or silently drops state. In Beam, state is mapped via transform names, so keep step names stable across upgrades.')}` },
    { title: 'Restart strategies', html: `
<ul>
<li><code>exponential-delay</code>: the default when checkpointing is enabled (Flink 1.19+). Delays grow between attempts, up to a max.</li>
<li><code>fixed-delay</code>: N attempts with a fixed delay, then FAILED.</li>
<li><code>failure-rate</code>: fail if more than N failures happen within an interval.</li>
<li><code>none</code>: fail immediately.</li>
</ul>
<p><code>jobmanager.execution.failover-strategy: region</code> restarts only the affected pipelined region. In typical streaming jobs with keyBy (all-to-all), that region is the whole job.</p>
<p>Beam: <code>--checkpointingInterval</code> defaults to <b>-1 = disabled</b>. A Beam streaming job without it has <b>no checkpoints</b>! Also check <code>--numberOfExecutionRetries</code>.</p>` }
  ],
  refs: [['Stateful stream processing: checkpointing & barriers', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/concepts/stateful-stream-processing/'], ['Checkpoints', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/state/checkpoints/'], ['Savepoints', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/state/savepoints/'], ['Checkpointing under backpressure', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/state/checkpointing_under_backpressure/'], ['Task failure recovery', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/state/task_failure_recovery/'], ['Lightweight Asynchronous Snapshots paper', 'https://arxiv.org/abs/1506.08603']],
  quiz: [
    { q: 'In aligned checkpointing, what does a two-input operator do when the barrier arrives on input 1 only?', options: ['Snapshots immediately', 'Blocks input 1 (buffers records after the barrier) and keeps processing input 2 until its barrier arrives', 'Drops input 2', 'Fails the checkpoint'], answer: 1, explain: 'That waiting is alignment.' },
    { q: 'Why do aligned checkpoints get slow under backpressure?', options: ['RocksDB compaction', 'Barriers queue behind full network buffers of in-flight data', 'The JobManager is busy', 'Watermarks are late'], answer: 1, explain: 'Barriers do not overtake data in aligned mode.' },
    { q: 'What do unaligned checkpoints trade?', options: ['Correctness for speed', 'Faster checkpoints under backpressure, but bigger checkpoints (in-flight data persisted)', 'Lower latency for no state', 'Exactly-once for at-least-once'], answer: 1, explain: 'Still exactly-once, but more data to store.' },
    { q: 'For end-to-end exactly-once into Kafka you need… (select all)', type: 'multi', options: ['Checkpointing enabled', 'A replayable source', 'A transactional (or idempotent) sink', 'Parallelism 1'], answer: [0, 1, 2], explain: 'Parallelism is irrelevant.' },
    { q: 'Main purpose of a savepoint?', options: ['Automatic crash recovery', 'Planned upgrades, rescaling and migrations: user-owned and portable', 'Speeding up checkpoints', 'Storing logs'], answer: 1, explain: 'Checkpoints are for recovery; savepoints are for operations.' },
    { q: 'You added a new map before a stateful window, and restore from a savepoint now fails to map state. Missing practice?', options: ['Using RocksDB', 'Setting explicit .uid() on stateful operators', 'Using allowedLateness', 'Using rebalance'], answer: 1, explain: 'Stable uids keep the operator → state mapping across graph changes.' },
    { q: 'You removed a stateful operator. Which flag allows restoring the rest?', options: ['--force', '--allowNonRestoredState', '--ignoreState', '-p 0'], answer: 1, explain: 'It skips state that has no matching operator.' },
    { q: 'Default of Beam\'s <code>--checkpointingInterval</code> on the FlinkRunner?', options: ['60 s', '-1 (checkpointing disabled)', '10 s', '5 min'], answer: 1, explain: 'You must set it explicitly for streaming jobs.' },
    { q: 'Best way to stop a job for an upgrade?', options: ['Kill the TaskManagers', 'flink stop --savepointPath … (stop with savepoint)', 'Cancel without savepoint', 'Delete the checkpoint dir'], answer: 1, explain: 'Stop-with-savepoint produces a consistent snapshot and finishes cleanly.' },
    { q: 'Default checkpoint timeout?', options: ['1 min', '10 min', '1 hour', 'no timeout'], answer: 1, explain: 'execution.checkpointing.timeout defaults to 10 minutes.' }
  ]
});
