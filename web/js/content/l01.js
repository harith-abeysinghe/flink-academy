FA.registerLevel({
  id: 1, track: 'Foundations', icon: '🌊', minutes: 20,
  title: 'Stream processing fundamentals',
  summary: 'Bounded vs unbounded data, event vs processing time, what Flink is and how Apache Beam relates to it.',
  objectives: ['Explain bounded vs unbounded streams', 'Distinguish event time, ingestion time and processing time', 'Describe what Flink adds: state, time, exactly-once', 'Explain how a Beam pipeline ends up running on Flink'],
  sections: [
    { title: 'Why stream processing?', html: `
<p>Classic batch jobs wait until all the data is collected (e.g. "yesterday's orders"), then process it. Stream processing handles each event <b>as it arrives</b>: order placed, truck GPS ping, price change. That makes results continuous and low-latency.</p>
${FA.D('batch-vs-stream')}
<p>Apache Flink is a distributed engine for <b>stateful computations over unbounded and bounded data streams</b>. The three things that make it more than "a fast loop over Kafka":</p>
<ul>
<li><b>State</b>: operators remember things (counts per customer, last seen location) and Flink manages that state, scales it and snapshots it.</li>
<li><b>Time</b>: Flink understands <i>event time</i> (when something happened) and uses <i>watermarks</i> to reason about out-of-order data.</li>
<li><b>Fault tolerance</b>: periodic <i>checkpoints</i> give <b>exactly-once state consistency</b>. After a crash the job resumes as if nothing happened.</li>
</ul>` },
    { title: 'Bounded vs unbounded', html: `
<table class="t"><tr><th></th><th>Bounded stream</th><th>Unbounded stream</th></tr>
<tr><td>Has an end?</td><td>Yes (a file, a table snapshot)</td><td>No (Kafka topic, sensor feed)</td></tr>
<tr><td>Can sort/see all data first?</td><td>Yes</td><td>No: must produce results incrementally</td></tr>
<tr><td>Flink mode</td><td>BATCH or STREAMING</td><td>STREAMING only</td></tr></table>
<p>Flink treats batch as a <b>special case of streaming</b>: a bounded stream. The same DataStream program can run in <code>execution.runtime-mode: BATCH</code> when all sources are bounded.</p>` },
    { title: 'Three notions of time', html: `
<ul>
<li><b>Event time</b>: the timestamp carried by the event itself (when the order was placed). Gives deterministic, correct results even when data arrives late or out of order. Needs <b>watermarks</b>.</li>
<li><b>Processing time</b>: the wall clock of the machine processing the event. Fastest and simplest, but results depend on load and delays and are <i>not reproducible</i>.</li>
<li><b>Ingestion time</b>: the timestamp assigned when the event enters Flink (a middle ground, rarely used now).</li>
</ul>
${FA.D('event-vs-processing-time')}
${FA.C('tip', 'Rule of thumb', 'If a business user would complain that "orders from 10:59 were counted in the 11:00 window because the network was slow", you need <b>event time</b>.')}` },
    { title: 'Where Apache Beam fits', html: `
<p><b>Apache Beam</b> is a <i>programming model + SDKs</i> (Java, Python, Go). You write a pipeline once (PCollections, ParDo, GroupByKey, windows, triggers) and run it on a <b>runner</b>: Google Cloud Dataflow, <b>Apache Flink</b>, Spark, or the local DirectRunner.</p>
<p>With <code>--runner=FlinkRunner</code>, Beam <b>translates</b> your pipeline into a native Flink job. That job runs on the Flink cluster, so everything in this course applies to your Beam jobs: the Flink dashboard, slots, parallelism, backpressure, checkpoints and savepoints.</p>
${FA.C('beam', 'For your Beam jobs', 'When a Beam job is slow or stuck, you debug it in the <b>Flink</b> UI. Beam transform names (e.g. <code>ParDo(EnrichFn)</code>) appear inside the Flink task boxes. Level 14 covers this mapping in detail.')}
${FA.D('beam-to-flink')}` },
    { title: 'Flink in one picture', html: `
<p>A Flink program is a <b>dataflow</b>: <i>sources → transformations → sinks</i>. When you submit it, Flink turns it into a graph of tasks that run in parallel across workers. Here is a tiny job, drawn the way the Flink dashboard draws it:</p>
${FA.W('jobgraph', { title: 'Your first job graph (try changing the source rate)', model: { cluster: { tms: 1, slotsPerTm: 2, coresPerTm: 4, sourceRate: 10000 }, ops: [{ kind: 'source', p: 2, name: 'Kafka orders' }, { kind: 'map', p: 2, name: 'Parse' }, { kind: 'keyby', name: 'keyBy(customer)' }, { kind: 'reduce', p: 2, name: 'Sum per customer' }, { kind: 'sink', p: 2, name: 'Kafka totals' }] }, controls: [{ op: 'cluster', key: 'sourceRate', label: 'Source rate (rec/s)', min: 1000, max: 2000000, step: 5000 }] })}
<p>Do not worry about every detail yet. By Level 10 you will read every line in those boxes.</p>` }
  ],
  refs: [['Flink: Timely stream processing (event time)', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/concepts/time/'], ['Flink concepts overview', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/concepts/overview/'], ['Beam Flink runner', 'https://beam.apache.org/documentation/runners/flink/'], ['The Dataflow Model paper', 'https://www.vldb.org/pvldb/vol8/p1792-Akidau.pdf']],
  quiz: [
    { q: 'Which statement best describes an <b>unbounded</b> stream?', options: ['A stream with no defined end, so results must be computed incrementally', 'A stream that is too large to fit in memory', 'A stream without a schema', 'A stream read with parallelism greater than 1'], answer: 0, explain: 'Unbounded = no end. You cannot wait for "all data"; you compute continuously.' },
    { q: 'A truck sends a GPS ping at 10:00:05 but it reaches Flink at 10:00:40 due to a tunnel. Which timestamp is the <b>event time</b>?', options: ['10:00:40', '10:00:05', 'The time the window fires', 'The Kafka log-append time always'], answer: 1, explain: 'Event time is when the event happened at the source: 10:00:05.' },
    { q: 'Why are processing-time results generally <b>not reproducible</b>?', options: ['Because processing time is stored in UTC', 'Because they depend on when the machine happens to process each record (load, delays, restarts)', 'Because Flink randomizes timestamps', 'Because watermarks are disabled'], answer: 1, explain: 'Processing time depends on the wall clock at processing, so replaying the same data gives different groupings.' },
    { q: 'Which three capabilities make Flink a <i>stateful</i> stream processor? (select all)', type: 'multi', options: ['Managed state', 'Event-time handling with watermarks', 'Checkpoint-based fault tolerance (exactly-once state)', 'A built-in SQL database server'], answer: [0, 1, 2], explain: 'State, time and fault tolerance are Flink\'s core. It is not a database server.' },
    { q: 'What is Apache Beam relative to Flink?', options: ['A Flink plugin for the Web UI', 'A unified programming model/SDK whose pipelines can be executed by Flink through the FlinkRunner', 'A replacement for the JobManager', 'A message broker like Kafka'], answer: 1, explain: 'Beam is the model + SDKs; FlinkRunner translates the pipeline into a Flink job.' },
    { q: 'Your Beam job runs with <code>--runner=FlinkRunner</code> and is slow. Where do you look at backpressure and task-level metrics?', options: ['Only in the Beam SDK logs', 'In the Flink Web UI, because the pipeline runs as a regular Flink job', 'In the Kafka UI', 'Nowhere, since Beam hides it'], answer: 1, explain: 'Beam on Flink = a Flink job, so you use the Flink dashboard.' },
    { q: 'How does Flink treat batch processing?', options: ['As a completely separate engine', 'As a special case of streaming: processing a bounded stream', 'It does not support batch at all', 'Only via Apache Spark'], answer: 1, explain: 'Bounded streams can run in BATCH runtime mode with batch-specific optimizations.' },
    { q: 'Which time notion should you use for "orders per minute by the minute the order was placed", with network delays possible?', options: ['Processing time', 'Event time', 'Ingestion time', 'Any, they are equivalent'], answer: 1, explain: 'Grouping by when the order happened requires event time.' },
    { q: 'The basic shape of every Flink program is:', options: ['Source → transformations → sink', 'Sink → source → map', 'JobManager → TaskManager → Kafka', 'Window → keyBy → source'], answer: 0, explain: 'Dataflow: sources feed transformations which feed sinks.' },
    { q: 'Which runtime mode can a job use <b>only</b> if all its sources are bounded?', options: ['STREAMING', 'BATCH', 'PIPELINED', 'EXACTLY_ONCE'], answer: 1, explain: 'BATCH mode requires bounded inputs.' }
  ]
});
