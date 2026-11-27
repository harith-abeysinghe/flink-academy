FA.registerLevel({
  id: 8, track: 'Time & reliability', icon: '⏱️', minutes: 45,
  title: 'Event time, watermarks & windows',
  summary: 'Event time vs processing time, window notation, timestamp assigners, watermark strategies, idle sources, tumbling/sliding/session windows, allowed lateness and side outputs.',
  objectives: ['Tell event time from processing time', 'Read window notation such as [0,5)', 'Configure a WatermarkStrategy', 'Predict when a window fires', 'Handle late data and idle partitions', 'Choose the right window type'],
  sections: [
    { title: 'Event time vs processing time', html: `
<p>Every record has two different "times", and windows can be built on either one:</p>
<ul>
<li><b>Event time</b>: when the thing <b>happened</b>. It is a field inside the record, for example <code>order.createdAt</code>, set by the phone, sensor or service that produced it.</li>
<li><b>Processing time</b>: when Flink <b>handles</b> the record, read from the TaskManager's wall clock.</li>
</ul>
<p>They are never equal. Records spend time in networks and Kafka, phones go offline, and producers retry. So records arrive a bit late, and often <b>out of order</b>:</p>
${FA.D('event-vs-processing')}
<table class="t"><tr><th></th><th>Event time</th><th>Processing time</th></tr>
<tr><td>Comes from</td><td>A timestamp in the record</td><td>The machine clock</td></tr>
<tr><td>Result if data arrives late or is replayed</td><td>Same result every time</td><td>Different result each run</td></tr>
<tr><td>Needs watermarks</td><td>Yes</td><td>No</td></tr>
<tr><td>Late data possible</td><td>Yes (handled in the last section)</td><td>No: "now" is never late</td></tr>
<tr><td>Typical use</td><td>Business metrics, billing, anything that must be correct</td><td>Rough monitoring where speed matters more than accuracy</td></tr></table>
<p>The rest of this level is about <b>event time</b>, because that is what you want almost always. Its one hard question is: <i>"How long should a window wait for stragglers before it gives its result?"</i> Watermarks answer it.</p>` },
    { title: 'Reading window notation: what [0,5) means', html: `
<p>Windows are written as <b>[start, end)</b>. Both numbers are <b>points in event time</b>. They are not values inside the records, and they are not a number of records.</p>
<ul>
<li><b>[</b> means the start is <b>included</b>; <b>)</b> means the end is <b>excluded</b>.</li>
<li><code>[0,5)</code> = "all events whose timestamp t satisfies 0 s ≤ t &lt; 5 s". An event at 4.999 s is in it; an event at 5.000 s belongs to the next window, <code>[5,10)</code>.</li>
<li>Because the end is excluded, back-to-back windows never overlap, and each timestamp lands in exactly one tumbling window.</li>
</ul>
${FA.D('window-boundaries')}
<p>In this lesson's diagrams, times are small numbers of <b>seconds</b> (0, 1, 2, …) to keep them readable. In a real job a timestamp is <b>epoch milliseconds</b>, so the same idea looks like this, for 1-minute tumbling windows:</p>
<table class="t"><tr><th>Event timestamp</th><th>Window it belongs to</th><th>Why</th></tr>
<tr><td>10:07:00.000</td><td>[10:07:00, 10:08:00)</td><td>The start is included</td></tr>
<tr><td>10:07:42.300</td><td>[10:07:00, 10:08:00)</td><td>Between start and end</td></tr>
<tr><td>10:07:59.999</td><td>[10:07:00, 10:08:00)</td><td>The last millisecond inside (<code>maxTimestamp</code>)</td></tr>
<tr><td>10:08:00.000</td><td>[10:08:00, 10:09:00)</td><td>The end is excluded, so it starts the next window</td></tr></table>
<p>Flink finds the window with simple arithmetic: <code>start = timestamp − (timestamp mod size)</code>, then <code>end = start + size</code>. Windows line up with the epoch (UTC), so hourly windows start at :00. Pass an offset to shift them, for example <code>TumblingEventTimeWindows.of(Duration.ofDays(1), Duration.ofHours(-8))</code> for days that start at midnight in UTC+8.</p>
${FA.C('tip', 'Windows are created lazily', 'Flink creates a window when its first event arrives, separately for each key. If customer Alice has no orders between 10:02 and 10:03, there is no window [10:02, 10:03) for Alice and nothing is emitted for it. To get zero rows you need extra logic, such as a timer.')}` },
    { title: 'Watermarks', html: `
<p>Window <code>[0,5)</code> must eventually give its result. But since events arrive out of order, Flink can never be sure that the last event with a timestamp below 5 has arrived. It needs a rule for <b>when to stop waiting</b>. That rule is the watermark.</p>
<p>A <b>watermark W(t)</b> is a special marker that flows through the stream alongside the records and says: <i>"I do not expect any more events with timestamp ≤ t."</i> When the watermark passes the end of a window, the window fires.</p>
${FA.C('info', 'An everyday comparison', 'A teacher collects postal entries for a contest closing on Friday. Letters can take up to 2 days in the post. When a letter postmarked Sunday arrives, the teacher reasons: "Anything postmarked Friday or earlier should be here by now", and announces the results. The postmark is event time, the 2 days is the out-of-orderness bound, and "Friday or earlier is complete" is the watermark.')}
<pre><code>WatermarkStrategy
    .&lt;Order&gt;forBoundedOutOfOrderness(Duration.ofSeconds(5))   // tolerate 5 s disorder
    .withTimestampAssigner((order, kafkaTs) -&gt; order.eventTimeMillis())  // which field is event time
    .withIdleness(Duration.ofMinutes(1));                          // see "idle sources"</code></pre>
<ul>
<li>The <b>timestamp assigner</b> tells Flink which field holds the event time. Without it, Kafka sources use the Kafka record timestamp.</li>
<li><code>forBoundedOutOfOrderness(B)</code>: watermark = <i>highest timestamp seen so far − B − 1 ms</i>. You are promising that no event arrives more than B behind the newest one.</li>
<li><code>forMonotonousTimestamps()</code>: B = 0, for data that is already strictly in order.</li>
<li>The watermark is driven by <b>event timestamps, not by the clock</b>. It only moves when a newer event arrives, and it never moves backwards. If no data arrives, it stays where it is.</li>
<li>It is emitted periodically, not after every record (<code>pipeline.auto-watermark-interval</code>, default 200 ms of wall-clock time).</li>
<li>A window <code>[start, end)</code> fires when <b>watermark ≥ end − 1 ms</b>, i.e. once its last millisecond is covered.</li>
</ul>
${FA.D('watermark-flow')}
<h4>The same example as a table</h4>
<p>Bound B = 2 s, tumbling windows of 5 s. For simplicity, assume a watermark is emitted after each event.</p>
<table class="t"><tr><th>#</th><th>Event timestamp</th><th>Highest seen</th><th>Watermark (max − 2 s − 1 ms)</th><th>What happens</th></tr>
<tr><td>1</td><td>1 s</td><td>1 s</td><td>−1.001 s</td><td>Goes into [0,5)</td></tr>
<tr><td>2</td><td>3 s</td><td>3 s</td><td>0.999 s</td><td>Goes into [0,5)</td></tr>
<tr><td>3</td><td>2 s</td><td>3 s</td><td>0.999 s</td><td>Out of order. The highest timestamp does not change, so neither does the watermark. Goes into [0,5)</td></tr>
<tr><td>4</td><td>6 s</td><td>6 s</td><td>3.999 s</td><td>Goes into [5,10). [0,5) needs the watermark at 4.999 s, so it stays open</td></tr>
<tr><td>5</td><td>4 s</td><td>6 s</td><td>3.999 s</td><td>Out of order, but [0,5) is still open: counted</td></tr>
<tr><td>6</td><td>8 s</td><td>8 s</td><td>5.999 s</td><td>Goes into [5,10). Watermark ≥ 4.999 s, so <b>[0,5) fires: count = 4</b></td></tr>
<tr><td>7</td><td>3 s</td><td>8 s</td><td>5.999 s</td><td><b>Late</b>: [0,5) has already fired. Dropped by default</td></tr></table>
${FA.C('tip', 'Rule of thumb', 'With bound B, window [start, end) fires as soon as an event with timestamp ≥ end + B has been seen. Window [0,5) with B = 2 s fires on the first event at 7 s or later. A bigger B means fewer late events, but every result comes out later.')}
<p>Try it yourself. Move the sliders and watch which events are counted, which windows fire, and which events are dropped:</p>
${FA.W('watermark', {})}` },
    { title: 'Parallel inputs and idle sources', html: `
<p>A real job reads from many Kafka partitions with several source subtasks. Each one tracks its own watermark. An operator downstream receives watermarks from all of its inputs and uses the <b>minimum</b>.</p>
<p>Why the minimum? Say partition 0 has reached 10:05, but partition 1 is slower and has only reached 10:01. If the operator took 10:05, it would close windows up to 10:05 while partition 1 still has events from 10:02 to 10:05 to deliver, and they would all be late. The minimum is the only value that is true for <b>every</b> input.</p>
<p>The downside: the <b>slowest input holds everyone back</b>. In the worst case an input receives no data at all, so its watermark never moves and <b>no window fires anywhere</b>. In the UI the Watermarks tab shows "No Watermark" or a value stuck in the past. Typical causes: a Kafka partition no producer writes to, a quiet region or tenant at night, or more source subtasks than partitions (Level 4).</p>
${FA.D('idle-min')}
<p>Fix: <code>.withIdleness(Duration.ofMinutes(1))</code>. A split that has sent no data for one minute is marked <b>idle</b> and left out of the minimum. As soon as it receives data again, it rejoins.</p>
${FA.C('warn', 'Idleness is a trade-off', 'When an idle partition wakes up, its first events may already be behind the watermark set by the others, so they can be late. Pick an idle timeout that is clearly longer than normal gaps in that partition.')}
${FA.C('beam', 'Beam', 'In Beam, watermarks come from the source IO (e.g. KafkaIO\'s timestamp policy: <code>withLogAppendTime()</code>, <code>withCreateTime(maxDelay)</code>, or a custom policy). The same stuck-watermark symptom appears in the Flink UI when a partition is idle.')}` },
    { title: 'Window types', html: `
<p>The window type decides <b>which window(s) each event is put into</b>. All types except the global window are defined in event time (or processing time) and use the [start, end) notation.</p>
<table class="t"><tr><th>Window</th><th>Shape</th><th>Windows per event</th><th>Example</th></tr>
<tr><td>Tumbling</td><td>Fixed size, no overlap, no gaps</td><td>Exactly 1</td><td>Orders per minute: <code>TumblingEventTimeWindows.of(Duration.ofMinutes(1))</code></td></tr>
<tr><td>Sliding</td><td>Fixed size, a new window every <i>slide</i></td><td>size ÷ slide</td><td>Last 10 min, updated every 2 min: <code>SlidingEventTimeWindows.of(Duration.ofMinutes(10), Duration.ofMinutes(2))</code></td></tr>
<tr><td>Session</td><td>Variable, closed by a gap of inactivity</td><td>1 (after merging)</td><td>User visits with a 30 min gap: <code>EventTimeSessionWindows.withGap(Duration.ofMinutes(30))</code></td></tr>
<tr><td>Global</td><td>One endless window per key</td><td>1</td><td>Every 100 events: <code>GlobalWindows.create()</code> + <code>.trigger(CountTrigger.of(100))</code></td></tr></table>
${FA.D('window-types')}
<h4>Tumbling: fixed buckets</h4>
<p>Time is cut into equal buckets that touch but do not overlap. With 1-minute windows, an event at 10:07:42 goes into [10:07, 10:08), and only there. Use it for "per minute / per hour / per day" reports.</p>
<h4>Sliding: overlapping buckets</h4>
<p>A sliding window has a <b>size</b> (how much time it covers) and a <b>slide</b> (how often a new one starts). Each event is copied into every window that covers its timestamp, so it is counted several times, once per window. That is what you want for a "moving" figure, such as "orders in the last 10 minutes, refreshed every 2 minutes".</p>
${FA.D('sliding-membership')}
<p>Keep <i>size ÷ slide</i> small. A 1-hour window sliding every second puts each event into 3,600 windows, which means 3,600× the state and output of a tumbling window.</p>
<h4>Session: buckets that follow activity</h4>
<p>A session window has no fixed start or length. Each event opens a small window <code>[t, t + gap)</code>, and windows of the same key that overlap are <b>merged</b>. The session ends at <i>last event + gap</i>. An out-of-order event can even glue two sessions together:</p>
${FA.D('session-merge')}
<p>Example with a 30 min gap: user u1 has events at 10:00, 10:20 and 11:00. The first two form [10:00, 10:50). 11:00 is more than 30 minutes after 10:20, so it starts a second session, [11:00, 11:30). Each user has their own sessions.</p>
<h4>Global: you decide when</h4>
<p>One window per key that never ends by itself. It does nothing until you add a <b>trigger</b> that decides when to fire, for example every 100 elements. <code>keyBy(…).countWindow(100)</code> is a shortcut for exactly that.</p>
${FA.C('info', 'Processing-time versions', 'Each time-based type also exists for processing time (<code>TumblingProcessingTimeWindows</code>, …). They use the machine clock and fire when it passes the window end, with no watermarks and no late data, but the results depend on when the data arrived.')}` },
    { title: 'Computing the result', html: `
<p>Windows are almost always <b>keyed</b>: after <code>keyBy</code>, every key has its own windows, and different keys run in parallel on different subtasks. When a window fires, Flink calls your window function <b>once per key per window</b>.</p>
<pre><code>orders
    .keyBy(Order::customer)                                       // windows per customer
    .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))   // [10:00,10:01), [10:01,10:02), …
    .aggregate(new CountOrders(), new AddWindowBounds());         // count as events arrive, label on fire

class CountOrders implements AggregateFunction&lt;Order, Long, Long&gt; {
    public Long createAccumulator()          { return 0L; }       // a new window starts at 0
    public Long add(Order o, Long acc)       { return acc + 1; }  // called for every event
    public Long getResult(Long acc)          { return acc; }      // called when the window fires
    public Long merge(Long a, Long b)        { return a + b; }    // needed for session windows
}

class AddWindowBounds extends ProcessWindowFunction&lt;Long, String, String, TimeWindow&gt; {
    public void process(String customer, Context ctx, Iterable&lt;Long&gt; counts, Collector&lt;String&gt; out) {
        TimeWindow w = ctx.window();                              // w.getStart(), w.getEnd() in epoch ms
        out.collect(customer + " [" + w.getStart() + ", " + w.getEnd() + ") orders=" + counts.iterator().next());
    }
}</code></pre>
<p>If Alice places 3 orders between 10:00:00 and 10:00:59.999 and Bob places 1, the window [10:00, 10:01) produces two results once the watermark passes 10:00:59.999: <code>alice … orders=3</code> and <code>bob … orders=1</code>.</p>
<ul>
<li>Prefer <code>aggregate()</code>/<code>reduce()</code>: they are <b>incremental</b>, keeping one small accumulator per window and key. A plain <code>process()</code> buffers <b>every element</b> until the window fires.</li>
<li>Passing both, as above, gives you the best of both: incremental counting, plus access to the window bounds when it fires.</li>
<li><code>windowAll</code> on a non-keyed stream puts everything in one window, which runs with <b>parallelism 1</b>. It is a bottleneck by design.</li>
</ul>` },
    { title: 'Late data', html: `
<p>An event is <b>late</b> when it arrives after the watermark has already passed the end of its window. "Late" is judged by the <b>watermark</b>, not by the clock. An event that is 10 minutes old is not late if the watermark is even further behind, for example while a job catches up on a backlog.</p>
<ul>
<li><b>Default</b>: a late element is dropped and counted in the <code>numLateRecordsDropped</code> metric.</li>
<li><code>.allowedLateness(Duration)</code>: keep the window's state for a while after it fires. Each late element is added and the window <b>fires again</b> with an updated result.</li>
<li><code>.sideOutputLateData(tag)</code>: send elements that are too late to a side output for logging or repair, instead of dropping them silently.</li>
</ul>
${FA.D('late-data')}
<h4>The timeline of one window</h4>
<p>Window [10:00:00, 10:01:00) with <code>allowedLateness(30 s)</code>. What happens to an event of this window depends on the watermark at the moment it arrives:</p>
<table class="t"><tr><th>Watermark when the event arrives</th><th>Status</th><th>What happens</th></tr>
<tr><td>below 10:00:59.999</td><td>On time</td><td>Added to the window. Nothing is emitted yet</td></tr>
<tr><td>reaches 10:00:59.999</td><td>(window end)</td><td><b>Main firing</b>: the first result is emitted</td></tr>
<tr><td>from 10:00:59.999 to below 10:01:29.999</td><td>Late, but allowed</td><td>Added; the window fires <b>again</b> with an updated result</td></tr>
<tr><td>10:01:29.999 or later</td><td>Too late</td><td>The window state is deleted: dropped, or sent to the side output</td></tr></table>
${FA.C('warn', 'Late firings mean updates', 'With allowed lateness, the same window can produce several results: first <code>alice orders=3</code>, then later <code>alice orders=4</code>. Downstream must treat them as updates, for example by upserting on (key, window start) instead of appending.')}
<pre><code>OutputTag&lt;Order&gt; late = new OutputTag&lt;&gt;("late") {};
var result = orders.keyBy(Order::customer)
    .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
    .allowedLateness(Duration.ofSeconds(30))
    .sideOutputLateData(late)
    .aggregate(new CountOrders());
result.getSideOutput(late).sinkTo(deadLetterSink);</code></pre>
<p>Which knob to turn: raise the <b>out-of-orderness bound</b> if many events are a little late (it delays every result). Use <b>allowed lateness</b> if a few events are much later and an updated result is acceptable. Use a <b>side output</b> so nothing disappears without a trace.</p>` }
  ],
  refs: [['Timely stream processing', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/concepts/time/'], ['Generating watermarks', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/event-time/generating_watermarks/'], ['Windows', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/windows/'], ['Beam programming guide: windowing & triggers', 'https://beam.apache.org/documentation/programming-guide/']],
  quiz: [
    { q: 'In "window [0,5)" in this lesson, what do 0 and 5 refer to?', options: ['Record values 0 to 4', 'Event time: from 0 s up to, but not including, 5 s', 'The first five records', 'Processing time on the TaskManager'], answer: 1, explain: 'Window bounds are points in event time. [ includes the start, ) excludes the end.' },
    { q: 'Tumbling windows of 5 s. An event has timestamp exactly 5.000 s. Which window does it go to?', options: ['[0,5)', '[5,10)', 'Both', 'Neither: it is dropped'], answer: 1, explain: 'The end of a window is excluded, so 5.000 s starts the next window.' },
    { q: 'An order is placed at 10:00:03 and reaches Flink at 10:00:41. Which is its event time?', options: ['10:00:03', '10:00:41', 'The time the window fires', 'The Kafka offset'], answer: 0, explain: 'Event time is when it happened, taken from the record. 10:00:41 is processing time.' },
    { q: 'With <code>forBoundedOutOfOrderness(5 s)</code>, max event time seen is 12:00:20.000. What is the watermark?', options: ['12:00:20.000', '12:00:15.000', '12:00:14.999', '12:00:25.000'], answer: 2, explain: 'max − bound − 1 ms = 12:00:14.999.' },
    { q: 'Bound 2 s, tumbling windows of 5 s. Which arriving event first makes window [0,5) fire?', options: ['The first event at 5 s or later', 'The first event at 7 s or later', 'The fifth event', 'An event at 4.999 s'], answer: 1, explain: 'The window fires at watermark ≥ 4.999 s, and watermark = max − 2 s − 1 ms, so it needs max ≥ 7 s.' },
    { q: 'An operator has two inputs with watermarks 10:05 and 10:01. Its watermark is…', options: ['10:05', '10:01 (the minimum)', '10:03 (average)', 'undefined'], answer: 1, explain: 'Multi-input operators take the minimum.' },
    { q: 'One of 8 Kafka partitions receives no data. Windows stopped firing. Best fix?', options: ['Increase parallelism', 'WatermarkStrategy.withIdleness(...)', 'Disable checkpointing', 'Use processing time everywhere'], answer: 1, explain: 'Idleness lets the idle split be excluded from the minimum.' },
    { q: 'A tumbling event-time window [10:00, 10:01) fires when…', options: ['the first element arrives', 'the watermark reaches ≥ 10:00:59.999', 'processing time is 10:01', 'the window has 100 elements'], answer: 1, explain: 'Event-time windows fire on watermark ≥ end − 1 ms.' },
    { q: 'Sliding windows with size 10 min and slide 2 min. How many windows does each event belong to?', type: 'number', answer: 5, explain: 'size ÷ slide = 10 ÷ 2 = 5.' },
    { q: 'Session windows with a 30 min gap. One key has events at 10:00, 10:20 and 11:00. How many sessions?', type: 'number', answer: 2, explain: '10:00 and 10:20 form [10:00, 10:50). 11:00 is after 10:50, so it opens a second session.' },
    { q: 'What happens by default to an element whose window has already fired (no allowed lateness)?', options: ['It re-fires the window', 'It is dropped', 'It goes to the next window', 'The job fails'], answer: 1, explain: 'Dropped by default; use sideOutputLateData to capture it.' },
    { q: 'Effect of <code>allowedLateness(30 s)</code>?', options: ['Delays the first firing by 30 s', 'Keeps window state 30 s longer and re-fires the window for each late element', 'Drops anything later than 30 s after arrival', 'Changes the watermark bound'], answer: 1, explain: 'Late firings produce updated results.' },
    { q: 'Why prefer aggregate()/reduce() over a full ProcessWindowFunction?', options: ['They support more types', 'Incremental: one accumulator per window instead of buffering all elements', 'They are processing-time only', 'They avoid watermarks'], answer: 1, explain: 'Incremental aggregation keeps state small.' },
    { q: 'Which window type is closed by a gap of inactivity?', options: ['Tumbling', 'Sliding', 'Session', 'Global'], answer: 2, explain: 'Session windows merge until a gap is seen.' },
    { q: '<code>windowAll()</code> on a non-keyed stream runs with parallelism…', type: 'number', answer: 1, explain: 'Non-keyed all-windows are not parallel.' },
    { q: 'Bigger out-of-orderness bound means… (select all)', type: 'multi', options: ['Fewer late (dropped) events', 'Higher latency before windows fire', 'Less state', 'More parallelism'], answer: [0, 1], explain: 'It is a completeness vs latency trade-off. State is held longer, not less.' }
  ]
});
