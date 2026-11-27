FA.registerLevel({
  id: 5, track: 'Parallel execution', icon: '🔀', minutes: 25,
  title: 'Data exchange: the arrows in the job graph',
  summary: 'FORWARD, HASH, REBALANCE, RESCALE, BROADCAST, GLOBAL, SHUFFLE: what each one does, when Flink chooses it, and what it costs.',
  objectives: ['Read every edge label in the job graph', 'Predict which ship strategy Flink will choose', 'Pick the right partitioning call for a use case', 'Understand network buffers and credit-based flow control at a high level'],
  sections: [
    { title: 'The edge labels', html: `
<table class="t">
<tr><th>Label</th><th>Created by</th><th>Pattern</th><th>Use it for</th></tr>
<tr><td><code>FORWARD</code></td><td>default when parallelism is equal; <code>forward()</code></td><td>subtask i → subtask i</td><td>Cheapest; allows chaining; order preserved</td></tr>
<tr><td><code>HASH</code></td><td><code>keyBy()</code> (Beam: GroupByKey, Combine.perKey, stateful DoFn)</td><td>key → key group → subtask</td><td>Anything keyed: state, windows, timers, joins</td></tr>
<tr><td><code>REBALANCE</code></td><td>default when parallelism differs; <code>rebalance()</code></td><td>round-robin, all-to-all</td><td>Even out skewed/idle sources</td></tr>
<tr><td><code>RESCALE</code></td><td><code>rescale()</code></td><td>round-robin to a local subset</td><td>Cheap fan-out/fan-in (2→6, 6→2)</td></tr>
<tr><td><code>BROADCAST</code></td><td><code>broadcast()</code></td><td>every record to every subtask</td><td>Small rule/config streams (Level 6)</td></tr>
<tr><td><code>GLOBAL</code></td><td><code>global()</code></td><td>everything to subtask 0</td><td>Tiny final aggregations only</td></tr>
<tr><td><code>SHUFFLE</code></td><td><code>shuffle()</code></td><td>random, all-to-all</td><td>Rarely; like REBALANCE but random</td></tr>
<tr><td><code>CUSTOM</code></td><td><code>partitionCustom()</code></td><td>your Partitioner</td><td>Special routing (does not give a KeyedStream)</td></tr>
</table>
${FA.W('partitioners', {})}` },
    { title: 'What Flink picks when you say nothing', html: `
<ul>
<li>Same parallelism, no partitioning call → <b>FORWARD</b> (and usually chained).</li>
<li>Different parallelism, no partitioning call → <b>REBALANCE</b>.</li>
<li>You called <code>keyBy</code> → <b>HASH</b>, always (even with the same parallelism, so it is never chained).</li>
<li>You called <code>forward()</code> but parallelism differs → <b>error</b>: <i>"Forward partitioning does not allow change of parallelism"</i>.</li>
</ul>
${FA.C('warn', 'keyBy is not an operator', 'In the job graph you will NOT find a "keyBy" box. keyBy only changes how records are routed to the NEXT operator, so you see a <b>HASH</b> edge into the window/process box.')}` },
    { title: 'What happens on a non-forward edge', html: `
<ol>
<li>The sender serializes each record into a <b>network buffer</b> (a memory segment from the TM's network memory) for the target subtask's channel.</li>
<li>A buffer is sent when it is full or when <code>execution.buffer-timeout</code> (default 100 ms) expires, which trades latency against throughput.</li>
<li>If both subtasks are in the same TM, the buffer is handed over locally; otherwise it goes over TCP (multiplexed per TM pair).</li>
<li><b>Credit-based flow control</b>: a receiver announces how many buffers it can accept (credits). When a receiver is slow, credits run out and the sender cannot get output buffers. That waiting time is <b>backpressure</b> (Level 12).</li>
</ol>
${FA.D('network-exchange')}
<p>All-to-all exchanges (HASH, REBALANCE, BROADCAST) create <b>sender × receiver</b> channels: 100 × 100 = 10,000 logical channels, each needing buffers. This is one reason very high parallelism needs more network memory.</p>` },
    { title: 'Choosing in practice', html: `
<ul>
<li>Need per-key state, windows or timers → <code>keyBy</code>. There is no alternative.</li>
<li>Source partitions are skewed (one Kafka partition hot) before a stateless heavy map → <code>rebalance()</code>.</li>
<li>Scale out a stateless stage by an integer factor cheaply → <code>rescale()</code>.</li>
<li>Rules, thresholds or configuration that all subtasks need → <code>broadcast()</code> with broadcast state.</li>
<li>Avoid <code>global()</code> on anything non-trivial: it is effectively parallelism 1 (see <a href="#/lab/global">the GLOBAL trap challenge</a>).</li>
</ul>` }
  ],
  refs: [['Physical partitioning (DataStream operators)', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/overview/'], ['A deep-dive into Flink\'s network stack', 'https://flink.apache.org/2019/06/05/a-deep-dive-into-flinks-network-stack/'], ['Network memory tuning', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/memory/network_mem_tuning/']],
  quiz: [
    { q: 'Which edge label does <code>keyBy()</code> produce in the Web UI?', options: ['FORWARD', 'HASH', 'KEYED', 'REBALANCE'], answer: 1, explain: 'keyBy hashes the key into key groups, shown as HASH.' },
    { q: 'Upstream p=4, downstream p=8, no explicit partitioning. Edge label?', options: ['FORWARD', 'RESCALE', 'REBALANCE', 'HASH'], answer: 2, explain: 'Differing parallelism defaults to REBALANCE.' },
    { q: 'Which strategy sends every record to <b>all</b> downstream subtasks?', options: ['GLOBAL', 'BROADCAST', 'SHUFFLE', 'REBALANCE'], answer: 1, explain: 'BROADCAST replicates records to every subtask.' },
    { q: 'You call <code>.forward()</code> between p=2 and p=4 operators. Result?', options: ['Works, uses RESCALE silently', 'Fails: forward partitioning does not allow change of parallelism', 'Works, but only 2 downstream subtasks get data', 'Chained anyway'], answer: 1, explain: 'Forward requires equal parallelism.' },
    { q: 'Difference between RESCALE and REBALANCE?', options: ['RESCALE is random', 'RESCALE round-robins to a local subset of downstream subtasks; REBALANCE round-robins to all', 'REBALANCE keeps keys together', 'No difference'], answer: 1, explain: 'Rescale uses fewer channels (subset), often staying local.' },
    { q: 'Why is there no "keyBy" box in the job graph?', options: ['It is hidden by default', 'keyBy is a partitioning, not an operator: it shows up as the HASH edge', 'It is chained into the source', 'It runs on the JobManager'], answer: 1, explain: 'keyBy configures routing for the next operator.' },
    { q: 'What causes the sender to wait (backpressure) under credit-based flow control?', options: ['The receiver has no credits (free buffers) left because it is too slow', 'Checkpoints are disabled', 'The buffer timeout is 100 ms', 'Too many key groups'], answer: 0, explain: 'No credit → no buffer to send into → the sender blocks.' },
    { q: 'What does <code>execution.buffer-timeout</code> trade off?', options: ['State size vs speed', 'Latency vs throughput', 'CPU vs memory', 'Exactly-once vs at-least-once'], answer: 1, explain: 'Flushing earlier reduces latency but sends smaller, less efficient buffers.' },
    { q: 'Which strategies create all-to-all channels between sender and receiver subtasks? (select all)', type: 'multi', options: ['HASH', 'REBALANCE', 'BROADCAST', 'FORWARD'], answer: [0, 1, 2], explain: 'FORWARD is one-to-one; the others connect every sender to every receiver.' },
    { q: 'A stateless heavy map is slow because one Kafka partition carries most traffic. Best fix?', options: ['keyBy on a random key', 'rebalance() before the map', 'global()', 'Disable chaining'], answer: 1, explain: 'rebalance spreads records evenly across the map subtasks.' }
  ]
});
