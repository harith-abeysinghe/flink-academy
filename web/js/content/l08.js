FA.registerLevel({
  id: 8, track: 'Time & reliability', icon: '⏱️', minutes: 40,
  title: 'Event time & watermarks',
  summary: 'Out-of-order events, window time ranges like [0,5), why windows must wait, what a watermark is, WatermarkStrategy, how watermarks travel through a job, idle sources and troubleshooting.',
  objectives: ['Read time ranges such as [0,5)', 'Explain why a window must wait, and what a watermark is', 'Compute a watermark and predict when a window fires', 'Configure a WatermarkStrategy', 'Explain why the minimum watermark is used, and fix idle sources', 'Troubleshoot stuck or racing watermarks'],
  sections: [
    { title: 'Events arrive out of order', html: `
<p>Level 1 introduced the two clocks: <b>event time</b> (when something happened, a timestamp inside the record) and <b>processing time</b> (when Flink handles it). This level is about what goes wrong between them: records arrive <b>late and out of order</b>.</p>
${FA.D('event-vs-processing')}
<table class="t"><tr><th></th><th>Event time</th><th>Processing time</th></tr>
<tr><td>Comes from</td><td>A timestamp in the record</td><td>The machine clock</td></tr>
<tr><td>Result if data arrives late or is replayed</td><td>Same result every time</td><td>Different result each run</td></tr>
<tr><td>Needs watermarks</td><td>Yes</td><td>No</td></tr>
<tr><td>Late data possible</td><td>Yes (Level 9)</td><td>No: "now" is never late</td></tr></table>
<p>With event time, results are correct no matter when data arrives. The price is one hard question: <i>"How long should we wait for stragglers?"</i> This whole level answers it.</p>` },
    { title: 'Time ranges: what [0,5) means', html: `
<p>Watermarks are easiest to explain with windows, so first, how windows are written: <b>[start, end)</b>. Both numbers are <b>points in event time</b>. They are not values inside the records, and they are not a number of records.</p>
<ul>
<li><b>[</b> means the start is <b>included</b>; <b>)</b> means the end is <b>excluded</b>.</li>
<li><code>[0,5)</code> = "all events whose timestamp t satisfies 0 s ≤ t &lt; 5 s". An event at 4.999 s is in it; an event at 5.000 s belongs to the next window, <code>[5,10)</code>.</li>
</ul>
${FA.D('window-boundaries')}
<p>In this level's diagrams, times are small numbers of <b>seconds</b> (0, 1, 2, …) to keep them readable. In a real job a timestamp is <b>epoch milliseconds</b>, so for 1-minute windows it looks like this:</p>
<table class="t"><tr><th>Event timestamp</th><th>Window it belongs to</th><th>Why</th></tr>
<tr><td>10:07:00.000</td><td>[10:07:00, 10:08:00)</td><td>The start is included</td></tr>
<tr><td>10:07:42.300</td><td>[10:07:00, 10:08:00)</td><td>Between start and end</td></tr>
<tr><td>10:07:59.999</td><td>[10:07:00, 10:08:00)</td><td>The last millisecond inside (<code>maxTimestamp</code>)</td></tr>
<tr><td>10:08:00.000</td><td>[10:08:00, 10:09:00)</td><td>The end is excluded, so it starts the next window</td></tr></table>
<p>Level 9 covers the different window types. For now you only need this: a window collects the events in its time range, and at some point it must <b>fire</b>, i.e. output its result.</p>` },
    { title: 'The problem: when is a window complete?', html: `
<p>Window <code>[0,5)</code> has to give its answer at some point. Because events arrive out of order, Flink can never be <i>sure</i> that the last event with a timestamp below 5 has arrived. There are only two ways to go wrong:</p>
<ul>
<li><b>Answer too early</b>: fast, but stragglers miss the window and the result is wrong.</li>
<li><b>Wait too long</b>: correct, but results come late and state is held longer.</li>
</ul>
${FA.D('close-too-early')}
<p>So Flink needs a rule for <b>how long to wait</b>, and the rule has to work without knowing the future. That rule is the watermark.</p>` },
    { title: 'What a watermark is', html: `
<p>A <b>watermark</b> is Flink's current answer to "how far along in event time are we?". A watermark with value t means: <i>"I do not expect any more events with a timestamp ≤ t."</i> Picture it as a line on the event-time axis that only moves to the right. Windows that end left of the line are complete and fire.</p>
${FA.D('watermark-timeline')}
${FA.C('', 'An everyday comparison', 'A teacher collects postal entries for a contest that closes on Friday. Letters can take up to 2 days in the post. When a letter postmarked Sunday arrives, the teacher reasons: "Anything postmarked Friday or earlier should be here by now", and announces the results. The postmark is event time, the 2 days is the bound, and "Friday or earlier is complete" is the watermark.')}
<p>The facts to remember:</p>
<table class="t"><tr><th>Fact</th><th>What it means in practice</th></tr>
<tr><td>watermark = highest timestamp seen − bound − 1 <b>millisecond</b></td><td>With a 2 s bound, after seeing 8 s: 8 s − 2 s − 0.001 s = <b>5.999 s</b>. The 1 ms is tiny; it exists so that an event at exactly 6 s still counts as on time</td></tr>
<tr><td>A window <code>[start, end)</code> fires when watermark ≥ end − 1 ms</td><td>[0,5) fires at watermark 4.999 s, i.e. once its last millisecond is covered</td></tr>
<tr><td>It never moves backwards</td><td>An older event does not lower it</td></tr>
<tr><td>Only <b>newer events</b> move it, not the clock</td><td>No data means a frozen watermark and no firing windows</td></tr>
<tr><td>Events left of it are <b>late</b></td><td>Their window may already have fired (Level 9)</td></tr></table>
${FA.C('tip', 'Rule of thumb', 'With bound B, window [start, end) fires as soon as an event with timestamp ≥ end + B has been seen. Window [0,5) with B = 2 s fires on the first event at 7 s or later.')}` },
    { title: 'Setting up a WatermarkStrategy', html: `
<p>You tell Flink two things: <b>where the timestamp is</b>, and <b>how out of order</b> your data can be. Do it on the source, so every operator downstream gets watermarks:</p>
<pre><code>WatermarkStrategy&lt;Order&gt; wm = WatermarkStrategy
    .&lt;Order&gt;forBoundedOutOfOrderness(Duration.ofSeconds(5))      // events can be up to 5 s behind the newest
    .withTimestampAssigner((order, kafkaTs) -&gt; order.createdAtMillis())  // event time, in epoch ms
    .withIdleness(Duration.ofMinutes(1));                             // see "Idle sources"

DataStream&lt;Order&gt; orders = env.fromSource(kafkaSource, wm, "orders");</code></pre>
<ul>
<li><b>Timestamp assigner</b>: returns the event time in <b>epoch milliseconds</b>. Without one, the Kafka source uses the Kafka record timestamp.</li>
<li><code>forBoundedOutOfOrderness(B)</code>: the usual choice. You promise that no event arrives more than B behind the newest one seen.</li>
<li><code>forMonotonousTimestamps()</code>: B = 0, for data that is already strictly in order. Anything out of order becomes late.</li>
<li>Watermarks are emitted <b>periodically</b>, not after every record: every <code>pipeline.auto-watermark-interval</code> (default 200 ms of wall-clock time).</li>
</ul>
<h4>Choosing the bound</h4>
<p>Look at how late your events really are, for example the gap between event time and Kafka append time, and pick a bound that covers nearly all of them (say the 99th percentile). Then watch <code>numLateRecordsDropped</code>. If it keeps growing, the bound is too small. If results come out much later than you need, it is too big.</p>
${FA.C('warn', 'Seconds vs milliseconds', 'If your assigner returns epoch <b>seconds</b>, Flink reads them as milliseconds. Every window becomes 1,000 times too long, so a 1-minute window covers about 17 hours of your data and seems never to fire. Convert with <code>× 1000</code> or <code>Instant.toEpochMilli()</code>.')}` },
    { title: 'Step by step: watermarks closing windows', html: `
<p>Here are the moving parts together: events, a watermark that follows them, and two windows.</p>
${FA.D('watermark-flow')}
<p>The same example as a table. Bound B = 2 s, windows of 5 s, and for simplicity a watermark after every event:</p>
<table class="t"><tr><th>#</th><th>Event timestamp</th><th>Highest seen</th><th>Watermark (max − 2 s − 0.001 s)</th><th>What happens</th></tr>
<tr><td>1</td><td>1 s</td><td>1 s</td><td>−1.001 s</td><td>Goes into [0,5)</td></tr>
<tr><td>2</td><td>3 s</td><td>3 s</td><td>0.999 s</td><td>Goes into [0,5)</td></tr>
<tr><td>3</td><td>2 s</td><td>3 s</td><td>0.999 s</td><td>Out of order. The highest timestamp does not change, so neither does the watermark. Goes into [0,5)</td></tr>
<tr><td>4</td><td>6 s</td><td>6 s</td><td>3.999 s</td><td>Goes into [5,10). [0,5) needs the watermark at 4.999 s, so it stays open</td></tr>
<tr><td>5</td><td>4 s</td><td>6 s</td><td>3.999 s</td><td>Out of order, but [0,5) is still open: counted</td></tr>
<tr><td>6</td><td>8 s</td><td>8 s</td><td>5.999 s</td><td>Goes into [5,10). Watermark ≥ 4.999 s, so <b>[0,5) fires: count = 4</b></td></tr>
<tr><td>7</td><td>3 s</td><td>8 s</td><td>5.999 s</td><td><b>Late</b>: [0,5) has already fired. Dropped by default</td></tr></table>
<p>Try it yourself. Move the sliders and watch which events are counted, which windows fire, and which events are dropped. Compare a bound of 0 s with 4 s:</p>
${FA.W('watermark', {})}` },
    { title: 'How watermarks travel through a job', html: `
<p>So far there was one stream and one watermark. A real job has several source subtasks, each reading its own Kafka partitions, and each makes its <b>own</b> watermark. Watermarks flow downstream with the records, and every operator subtask keeps its own current watermark.</p>
${FA.D('watermark-propagation')}
<ul>
<li>After a <code>keyBy</code>, every upstream subtask sends its watermark to <b>every</b> downstream subtask.</li>
<li>An operator with several inputs uses the <b>minimum</b> of their latest watermarks. If it took the maximum, it would close windows while a slower input still has events for them, and those events would all be late.</li>
<li>A watermark belongs to a <b>subtask, not to a key</b>. Every key on that subtask shares it. A key that has been silent for an hour still has its windows fired, because other keys moved the watermark.</li>
</ul>` },
    { title: 'Idle sources: the stuck-watermark classic', html: `
<p>The minimum has a downside: <b>the slowest input holds everyone back</b>. In the worst case an input receives no data at all, so its watermark never moves and <b>no window fires anywhere</b>. In the UI the Watermarks tab shows "No Watermark" or a value stuck in the past. Typical causes: a Kafka partition no producer writes to, a quiet region or tenant at night, or more source subtasks than partitions (Level 4).</p>
${FA.D('idle-min')}
<p>Fix: <code>.withIdleness(Duration.ofMinutes(1))</code>. A split that has sent no data for one minute is marked <b>idle</b> and left out of the minimum. As soon as it receives data again, it rejoins.</p>
${FA.C('warn', 'Idleness is a trade-off', 'When an idle partition wakes up, its first events may already be behind the watermark set by the others, so they can be late. Pick an idle timeout that is clearly longer than the normal gaps in that partition.')}
${FA.C('beam', 'Beam', 'In Beam, watermarks come from the source IO (e.g. KafkaIO\'s timestamp policy: <code>withLogAppendTime()</code>, <code>withCreateTime(maxDelay)</code>, or a custom policy). The same stuck-watermark symptom appears in the Flink UI when a partition is idle.')}` },
    { title: 'Troubleshooting watermarks', html: `
<p>Where to look: in the Web UI, open an operator and select the <b>Watermarks</b> tab. It shows the current watermark of every subtask. The metrics <code>currentInputWatermark</code> and <code>currentOutputWatermark</code> show the same per operator. <b>Watermark lag</b>, the wall clock minus the watermark, tells you how far behind real time your windows are.</p>
<table class="t"><tr><th>Symptom</th><th>Likely cause</th><th>Fix</th></tr>
<tr><td>"No Watermark" on every subtask</td><td>No WatermarkStrategy on the source, or no data yet</td><td>Pass a strategy to <code>fromSource</code>; check the source is reading</td></tr>
<tr><td>One subtask far behind the others; nothing fires</td><td>Idle or slow partition holds the minimum</td><td><code>withIdleness</code>; match source parallelism to partitions</td></tr>
<tr><td>Watermark stuck although data flows</td><td>All new events are older than the highest one seen</td><td>Check timestamps; a replayed or misordered topic can do this</td></tr>
<tr><td>Watermark jumped years ahead; everything is late</td><td>One event with a bad timestamp in the future (e.g. year 2099)</td><td>Filter or clamp impossible timestamps before assigning them</td></tr>
<tr><td>Windows seem to never fire, watermark in 1970</td><td>Timestamps in seconds instead of milliseconds</td><td>Multiply by 1000</td></tr>
<tr><td>Many late events (<code>numLateRecordsDropped</code> grows)</td><td>Bound smaller than the real disorder</td><td>Raise the bound, or use allowed lateness (Level 9)</td></tr></table>
${FA.C('tip', 'Common misunderstandings', 'The watermark is <b>not a clock</b>: it moves only with newer events. It is <b>not per key</b>: it is per subtask. It is <b>not a guarantee</b>: it is a guess based on your bound, so late events can still happen.')}` }
  ],
  refs: [['Timely stream processing', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/concepts/time/'], ['Generating watermarks', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/event-time/generating_watermarks/'], ['Built-in watermark generators', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/event-time/built_in/'], ['Beam programming guide: watermarks', 'https://beam.apache.org/documentation/programming-guide/']],
  quiz: [
    { q: 'In "window [0,5)" in this lesson, what do 0 and 5 refer to?', options: ['Record values 0 to 4', 'Event time: from 0 s up to, but not including, 5 s', 'The first five records', 'Processing time on the TaskManager'], answer: 1, explain: 'Window bounds are points in event time. [ includes the start, ) excludes the end.' },
    { q: 'Windows of 5 s. An event has timestamp exactly 5.000 s. Which window does it go to?', options: ['[0,5)', '[5,10)', 'Both', 'Neither: it is dropped'], answer: 1, explain: 'The end of a window is excluded, so 5.000 s starts the next window.' },
    { q: 'Why can a window not simply fire as soon as an event past its end arrives?', options: ['It would use too much memory', 'Out-of-order events for that window may still be on the way and would be lost', 'Flink forbids it', 'Checkpoints would fail'], answer: 1, explain: 'Firing too early loses stragglers. A watermark encodes how long to wait.' },
    { q: 'With <code>forBoundedOutOfOrderness(5 s)</code>, max event time seen is 12:00:20.000. What is the watermark?', options: ['12:00:20.000', '12:00:15.000', '12:00:14.999', '12:00:25.000'], answer: 2, explain: 'max − bound − 1 ms = 12:00:14.999.' },
    { q: 'Bound 2 s, windows of 5 s. Which arriving event first makes window [0,5) fire?', options: ['The first event at 5 s or later', 'The first event at 7 s or later', 'The fifth event', 'An event at 4.999 s'], answer: 1, explain: 'The window fires at watermark ≥ 4.999 s, and watermark = max − 2 s − 1 ms, so it needs max ≥ 7 s.' },
    { q: 'The watermark is 10:00:30. An event with timestamp 10:00:10 arrives. What happens to the watermark?', options: ['It goes back to 10:00:10', 'Nothing: it never moves backwards', 'It jumps forward by the bound', 'It resets to "No Watermark"'], answer: 1, explain: 'Only a newer highest timestamp moves the watermark, and only forwards.' },
    { q: 'No events arrive for 10 minutes. What does the watermark do?', options: ['It advances with the wall clock', 'It stays where it is', 'It resets', 'It jumps to the current time'], answer: 1, explain: 'Watermarks are driven by event timestamps, not by the clock.' },
    { q: 'A window subtask handles keys alice and bob. Which is true?', options: ['Each key has its own watermark', 'Both keys share the subtask\'s watermark', 'Only the busiest key has a watermark', 'Keys have no watermarks; only sources do'], answer: 1, explain: 'Watermarks are per subtask, not per key.' },
    { q: 'An operator has two inputs with watermarks 10:05 and 10:01. Its watermark is…', options: ['10:05', '10:01 (the minimum)', '10:03 (average)', 'undefined'], answer: 1, explain: 'Multi-input operators take the minimum.' },
    { q: 'One of 8 Kafka partitions receives no data. Windows stopped firing. Best fix?', options: ['Increase parallelism', 'WatermarkStrategy.withIdleness(...)', 'Disable checkpointing', 'Use processing time everywhere'], answer: 1, explain: 'Idleness lets the idle split be excluded from the minimum.' },
    { q: 'A window [10:00, 10:01) fires when…', options: ['the first element arrives', 'the watermark reaches ≥ 10:00:59.999', 'processing time is 10:01', 'the window has 100 elements'], answer: 1, explain: 'Event-time windows fire on watermark ≥ end − 1 ms.' },
    { q: 'The Watermarks tab shows a watermark in January 1970 and 1-minute windows never fire. Most likely cause?', options: ['The cluster clock is wrong', 'The timestamp assigner returns seconds instead of milliseconds', 'Checkpointing is disabled', 'Parallelism is too high'], answer: 1, explain: 'Seconds read as milliseconds land in 1970 and make every window 1,000 times too long.' },
    { q: 'Bigger out-of-orderness bound means… (select all)', type: 'multi', options: ['Fewer late (dropped) events', 'Higher latency before windows fire', 'Less state', 'More parallelism'], answer: [0, 1], explain: 'It is a completeness vs latency trade-off. State is held longer, not less.' }
  ]
});
