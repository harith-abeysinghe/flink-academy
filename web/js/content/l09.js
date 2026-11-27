FA.registerLevel({
  id: 9, track: 'Time & reliability', icon: '🪟', minutes: 35,
  title: 'Windows & late data',
  summary: 'Tumbling, sliding, session and global windows, computing results incrementally, allowed lateness, side outputs, and choosing the right window.',
  objectives: ['Predict which window(s) an event lands in', 'Choose between tumbling, sliding, session and global windows', 'Compute window results incrementally', 'Handle late data with allowed lateness and side outputs'],
  sections: [
    { title: 'Windows in one picture', html: `
<p>A window groups a stream into finite chunks so you can compute something per chunk: a count, a sum, an average. Two things from Level 8 carry over:</p>
<ul>
<li>A time window is a range of event time, written <b>[start, end)</b>: start included, end excluded.</li>
<li>It <b>fires</b> (outputs its result) when the watermark reaches its end − 1 ms.</li>
</ul>
<p>The window <b>type</b> decides which window(s) each event is put into:</p>
<table class="t"><tr><th>Window</th><th>Shape</th><th>Windows per event</th><th>Example</th></tr>
<tr><td>Tumbling</td><td>Fixed size, no overlap, no gaps</td><td>Exactly 1</td><td>Orders per minute: <code>TumblingEventTimeWindows.of(Duration.ofMinutes(1))</code></td></tr>
<tr><td>Sliding</td><td>Fixed size, a new window every <i>slide</i></td><td>size ÷ slide</td><td>Last 10 min, updated every 2 min: <code>SlidingEventTimeWindows.of(Duration.ofMinutes(10), Duration.ofMinutes(2))</code></td></tr>
<tr><td>Session</td><td>Variable, closed by a gap of inactivity</td><td>1 (after merging)</td><td>User visits with a 30 min gap: <code>EventTimeSessionWindows.withGap(Duration.ofMinutes(30))</code></td></tr>
<tr><td>Global</td><td>One endless window per key</td><td>1</td><td>Every 100 events: <code>GlobalWindows.create()</code> + <code>.trigger(CountTrigger.of(100))</code></td></tr></table>
${FA.D('window-types')}` },
    { title: 'Tumbling windows: fixed buckets', html: `
<p>Time is cut into equal buckets that touch but do not overlap. With 1-minute windows, an event at 10:07:42 goes into [10:07, 10:08), and only there. Use it for "per minute / per hour / per day" figures.</p>
<p>Flink finds the bucket with arithmetic: <code>start = timestamp − (timestamp mod size)</code>, then <code>end = start + size</code>. Windows line up with the epoch (UTC), so hourly windows start at :00. Pass an offset to shift them, for example <code>TumblingEventTimeWindows.of(Duration.ofDays(1), Duration.ofHours(-8))</code> for days that start at midnight in UTC+8.</p>
${FA.C('tip', 'Windows are created lazily', 'Flink creates a window when its first event arrives, separately for each key. If customer Alice has no orders between 10:02 and 10:03, there is no window [10:02, 10:03) for Alice and nothing is emitted for it. To get rows with zero, you need extra logic, such as a timer.')}` },
    { title: 'Sliding windows: overlapping buckets', html: `
<p>A sliding window has a <b>size</b> (how much time it covers) and a <b>slide</b> (how often a new one starts). Each event is added to every window that covers its timestamp, so it is counted several times, once per window. That is what you want for a moving figure, such as "orders in the last 10 minutes, refreshed every 2 minutes".</p>
${FA.D('sliding-membership')}
<p>Keep <i>size ÷ slide</i> small. A 1-hour window sliding every second puts each event into 3,600 windows: 3,600 times the state and output of a tumbling window.</p>` },
    { title: 'Session windows: buckets that follow activity', html: `
<p>A session window has no fixed start or length. Each event opens a small window <code>[t, t + gap)</code>, and windows of the same key that overlap are <b>merged</b>. A session ends at <i>last event + gap</i>. An out-of-order event can even glue two sessions together:</p>
${FA.D('session-merge')}
<p>Example with a 30 min gap: user u1 has events at 10:00, 10:20 and 11:00. The first two form [10:00, 10:50). 11:00 is more than 30 minutes after 10:20, so it starts a second session, [11:00, 11:30). Each user has their own sessions.</p>` },
    { title: 'Global and processing-time windows', html: `
<ul>
<li><b>Global window</b>: one window per key that never ends by itself. It does nothing until you add a <b>trigger</b> that decides when to fire, for example every 100 elements. <code>keyBy(…).countWindow(100)</code> is a shortcut for exactly that.</li>
<li><b>Processing-time windows</b> (<code>TumblingProcessingTimeWindows</code>, …): the same shapes, but on the machine clock. They fire when the clock passes the window end, with no watermarks and no late data, but results depend on when the data happened to arrive.</li>
</ul>` },
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
<li>Passing both, as above, gives you incremental counting plus access to the window bounds when it fires.</li>
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
<p>Which knob to turn: raise the <b>out-of-orderness bound</b> (Level 8) if many events are a little late; it delays every result. Use <b>allowed lateness</b> if a few events are much later and an updated result is acceptable. Use a <b>side output</b> so nothing disappears without a trace.</p>` },
    { title: 'Which window should I use?', html: `
<table class="t"><tr><th>You need…</th><th>Use</th></tr>
<tr><td>Orders per minute for a report</td><td>Tumbling, 1 min</td></tr>
<tr><td>A moving average over the last 5 min, refreshed every 10 s</td><td>Sliding, size 5 min, slide 10 s (30 windows per event)</td></tr>
<tr><td>Pages viewed per visit, where a visit ends after 30 min of silence</td><td>Session, gap 30 min</td></tr>
<tr><td>A batch of every 100 events per key</td><td><code>countWindow(100)</code> (global window + count trigger)</td></tr>
<tr><td>An alert the moment a count passes a limit, without waiting for a window end</td><td>No window: a <code>KeyedProcessFunction</code> with keyed state and timers (Level 7)</td></tr>
<tr><td>A rough live figure where speed beats accuracy</td><td>A processing-time window</td></tr></table>
${FA.C('beam', 'Beam', 'Beam has the same shapes: <code>FixedWindows</code> (tumbling), <code>SlidingWindows</code> and <code>Sessions</code>, plus <code>withAllowedLateness</code> and triggers. On the Flink runner, Beam\'s watermarks are Flink watermarks, so the Watermarks tab reads the same. Beam counts dropped late elements in its own <code>droppedDueToLateness</code> metric.')}` }
  ],
  refs: [['Windows', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/windows/'], ['Side outputs', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/side_output/'], ['Beam programming guide: windowing & triggers', 'https://beam.apache.org/documentation/programming-guide/']],
  quiz: [
    { q: 'Tumbling windows of 1 minute. An event has timestamp 10:07:42. Which window is it in?', options: ['[10:07, 10:08)', '[10:06, 10:08)', '[10:07:42, 10:08:42)', 'Every window up to 10:08'], answer: 0, explain: 'start = t − (t mod 1 min) = 10:07, end = 10:08.' },
    { q: 'Sliding windows with size 10 min and slide 2 min. How many windows does each event belong to?', type: 'number', answer: 5, explain: 'size ÷ slide = 10 ÷ 2 = 5.' },
    { q: 'A 1-hour window sliding every second. How many windows does each event land in?', type: 'number', answer: 3600, explain: '3,600 s ÷ 1 s = 3,600 windows: very expensive.' },
    { q: 'Session windows with a 30 min gap. One key has events at 10:00, 10:20 and 11:00. How many sessions?', type: 'number', answer: 2, explain: '10:00 and 10:20 form [10:00, 10:50). 11:00 is after 10:50, so it opens a second session.' },
    { q: 'Which window type is closed by a gap of inactivity?', options: ['Tumbling', 'Sliding', 'Session', 'Global'], answer: 2, explain: 'Session windows merge until a gap is seen.' },
    { q: 'A global window with no trigger…', options: ['fires every minute', 'never fires', 'fires on every element', 'fires when the job stops'], answer: 1, explain: 'A global window only fires when a trigger you add tells it to.' },
    { q: 'Why prefer aggregate()/reduce() over a full ProcessWindowFunction?', options: ['They support more types', 'Incremental: one accumulator per window instead of buffering all elements', 'They are processing-time only', 'They avoid watermarks'], answer: 1, explain: 'Incremental aggregation keeps state small.' },
    { q: '<code>windowAll()</code> on a non-keyed stream runs with parallelism…', type: 'number', answer: 1, explain: 'Non-keyed all-windows are not parallel.' },
    { q: 'What happens by default to an element whose window has already fired (no allowed lateness)?', options: ['It re-fires the window', 'It is dropped', 'It goes to the next window', 'The job fails'], answer: 1, explain: 'Dropped by default; use sideOutputLateData to capture it.' },
    { q: 'Effect of <code>allowedLateness(30 s)</code>?', options: ['Delays the first firing by 30 s', 'Keeps window state 30 s longer and re-fires the window for each late element', 'Drops anything later than 30 s after arrival', 'Changes the watermark bound'], answer: 1, explain: 'Late firings produce updated results.' },
    { q: 'Whether an event is late is decided by…', options: ['How old it is by the wall clock', 'The watermark when it arrives, compared with its window end', 'Its Kafka offset', 'The checkpoint interval'], answer: 1, explain: 'An old event is not late if the watermark has not yet passed its window end.' },
    { q: 'With allowed lateness, a window emits orders=3 and later orders=4. What must the sink do?', options: ['Append both rows', 'Treat the second as an update of the first, e.g. upsert on (key, window start)', 'Ignore the second', 'Fail the job'], answer: 1, explain: 'Late firings are updates of an earlier result.' }
  ]
});
