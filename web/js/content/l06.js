FA.registerLevel({
  id: 6, track: 'Parallel execution', icon: '🧵', minutes: 30,
  title: 'Ordering with multiple workers & broadcast state',
  summary: 'What Flink really guarantees about record order across parallel subtasks for FORWARD, HASH, REBALANCE and BROADCAST, plus the Broadcast State pattern.',
  objectives: ['State Flink\'s only ordering guarantee (per channel FIFO)', 'Predict when per-key order is preserved or lost', 'Explain why broadcast elements can arrive in different orders per task', 'Implement the Broadcast State pattern correctly'],
  sections: [
    { title: 'The one real guarantee', html: `
<p>Flink guarantees exactly one thing about order: <b>records sent from one subtask to another subtask arrive in the order they were sent</b>. In the docs' words, for redistributing exchanges, order is only preserved <b>for each pair of sending and receiving subtasks</b> (e.g. map[1] → window[2]). Each such pair is a <b>channel</b>, and every channel is FIFO.</p>
${FA.D('fifo-channels')}
<p>Everything else follows from that:</p>
<table class="t">
<tr><th>Exchange</th><th>Channels per receiver</th><th>Order you get</th></tr>
<tr><td><b>FORWARD</b> (one-to-one)</td><td>exactly 1 (from its peer)</td><td>The <b>whole partition</b> keeps its order. A chained operator sees records in exactly the order the upstream emitted them.</td></tr>
<tr><td><b>HASH</b> (keyBy)</td><td>one from every sender</td><td>Per key: preserved <b>only if all records of that key come from the same sender subtask</b>. Across keys: no guarantee.</td></tr>
<tr><td><b>REBALANCE / RESCALE / SHUFFLE</b></td><td>one or more senders</td><td>Records of a key are spread over different receivers, so <b>no per-key order</b> downstream.</td></tr>
<tr><td><b>BROADCAST</b></td><td>one from every sender</td><td>Each receiver sees each sender's records in order, but the <b>interleaving of different senders can differ per receiver</b>.</td></tr>
<tr><td><b>GLOBAL</b></td><td>all senders → subtask 0</td><td>Per-sender order only; interleaving of senders is arbitrary.</td></tr>
</table>` },
    { title: 'See it happen', html: `
<p>Each chip is a record: key letter + sequence number of that key (A1 was produced before A2). The small <code>uN</code> is the sending subtask. Red outline = a later record of a key arrived <b>before</b> an earlier one. Press 🎲 to change network timing.</p>
${FA.W('ordering', { strat: 'hash', up: 2, down: 2, pinned: false })}
<p><b>Experiments to try:</b></p>
<ol>
<li>HASH + "key spread over ALL upstream subtasks" + 🎲 a few times: per-key order breaks, because A1 (from u1) and A2 (from u2) travel on different channels.</li>
<li>HASH + "each key from ONE upstream subtask": order holds every time, because each key uses one channel.</li>
<li>FORWARD with 2→2: always in order.</li>
<li>BROADCAST 2→3 + 🎲: receivers see different interleavings.</li>
<li>REBALANCE: a key is split across receivers.</li>
</ol>` },
    { title: 'Practical consequences', html: `
<ul>
<li><b>Kafka → keyBy on the same key</b>: if the producer partitioned the topic by that key, each key comes from one Kafka partition → one source subtask → one channel, so <b>per-key order is preserved</b>. If the producer used round-robin, or you keyed by a different field, order is not guaranteed.</li>
<li><b>A rebalance() before keyBy destroys per-key order</b>, because records of one key are now emitted by several subtasks.</li>
<li><b>Changing parallelism</b> between source and keyed operator inserts a REBALANCE if you did not keyBy, so watch out in long pipelines.</li>
<li><b>Event time fixes most ordering problems</b>: instead of relying on arrival order, use event-time timestamps, watermarks, windows or timers (Levels 8 and 9) and sort/buffer in state when the business logic needs strict order.</li>
<li><b>Async I/O</b>: <code>AsyncDataStream.orderedWait</code> preserves order; <code>unorderedWait</code> emits results as they complete (faster, order lost).</li>
<li>Kafka itself: per-partition order on the producer side also needs idempotent producers / <code>max.in.flight.requests.per.connection</code> settings.</li>
</ul>
${FA.C('beam', 'Beam', 'Beam\'s model gives <b>no ordering guarantees</b> for elements in a PCollection at all. Even on Flink, never write a DoFn that depends on arrival order. Use event-time timestamps, windows and stateful DoFns with timers, or sort within a key in state.')}` },
    { title: 'The Broadcast State pattern', html: `
<p>Use it when a <b>low-volume</b> stream (rules, thresholds, config, a product catalog delta) must be available to <b>every</b> parallel instance processing a high-volume stream.</p>
${FA.D('broadcast-state')}
<pre><code>MapStateDescriptor&lt;String, Rule&gt; RULES = new MapStateDescriptor&lt;&gt;(
    "rules", Types.STRING, TypeInformation.of(Rule.class));

BroadcastStream&lt;Rule&gt; ruleStream = rules.broadcast(RULES);   // BROADCAST edge

orders.keyBy(Order::customerId)
      .connect(ruleStream)
      .process(new KeyedBroadcastProcessFunction&lt;String, Order, Rule, Alert&gt;() {
          public void processBroadcastElement(Rule r, Context ctx, Collector&lt;Alert&gt; out) throws Exception {
              ctx.getBroadcastState(RULES).put(r.id(), r);          // read-WRITE
          }
          public void processElement(Order o, ReadOnlyContext ctx, Collector&lt;Alert&gt; out) throws Exception {
              for (Map.Entry&lt;String, Rule&gt; e : ctx.getBroadcastState(RULES).immutableEntries())  // read-ONLY
                  if (e.getValue().matches(o)) out.collect(new Alert(o, e.getKey()));
          }
      });</code></pre>
<ul>
<li>Non-keyed main stream → <code>BroadcastProcessFunction</code>; keyed main stream → <code>KeyedBroadcastProcessFunction</code> (which also has keyed state and timers on the main side).</li>
<li><b>Only the broadcast side may modify broadcast state</b>; the main side gets a <b>read-only</b> view. Reason: there is <b>no cross-task communication</b>. Every task must apply the same updates in the same deterministic way so all copies stay identical.</li>
<li><b>Order of broadcast events may differ across tasks</b> (you saw why above). Updates must not depend on arrival order across different senders; otherwise copies diverge.</li>
<li>Broadcast state lives <b>on the heap</b> (no RocksDB) and <b>every task checkpoints its copy</b>. Keep it small.</li>
<li>Rules and events race: an order may be processed before the rule that should apply to it arrives. If that matters, buffer events in keyed state until rules are loaded.</li>
</ul>
${FA.W('jobgraph', { title: 'A broadcast edge in the job graph (parallelism 2 → 4 means every rule is sent 4 times)', model: { cluster: { tms: 2, slotsPerTm: 2, coresPerTm: 4, sourceRate: 2000 }, ops: [{ kind: 'source', p: 2, name: 'Rules topic' }, { kind: 'map', p: 4, name: 'Apply rules (BroadcastProcessFunction)', part: 'broadcast' }, { kind: 'sink', p: 4, name: 'Alerts' }] }, controls: [{ op: 1, key: 'p', label: 'Receiver parallelism', max: 12 }] })}` }
  ],
  refs: [['Flink concepts: parallel dataflows, ordering in redistributing streams', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/concepts/overview/'], ['The Broadcast State pattern', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/fault-tolerance/broadcast_state/'], ['Ververica: Broadcast State, 4 important considerations', 'https://ververica.com/blog/broadcast-state-pattern-flink-considerations'], ['Async I/O (ordered vs unordered)', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/asyncio/'], ['Beam programming guide', 'https://beam.apache.org/documentation/programming-guide/']],
  quiz: [
    { q: 'What ordering does Flink guarantee for a redistributing exchange (e.g. keyBy)?', options: ['Global order across all records', 'Order per pair of sending and receiving subtask (per channel)', 'Order per key always', 'No order at all'], answer: 1, explain: 'Each sender→receiver channel is FIFO; nothing more.' },
    { q: 'Source p=2 reads a Kafka topic produced <b>round-robin</b> (a key appears in all partitions). Then keyBy(key). Is per-key order guaranteed at the keyed operator?', options: ['Yes, keyBy always preserves per-key order', 'No: records of one key arrive from different source subtasks on different channels', 'Only with checkpointing', 'Only if parallelism is 1 downstream'], answer: 1, explain: 'Different senders = different channels, so relative order depends on timing.' },
    { q: 'Source p=2 reads a Kafka topic that is partitioned <b>by key</b> (each key lives in exactly one partition). Then keyBy(key). Is per-key order preserved at the keyed operator?', options: ['Preserved: each key comes from one partition → one source subtask → one channel', 'Not preserved', 'Preserved only with BROADCAST', 'Random'], answer: 0, explain: 'One channel per key → FIFO → order preserved.' },
    { q: 'Which exchange keeps the full order of each partition, and is the only one that permits chaining?', options: ['HASH', 'FORWARD', 'REBALANCE', 'BROADCAST'], answer: 1, explain: 'FORWARD is one-to-one.' },
    { q: 'Why can different parallel instances see broadcast elements in different orders?', options: ['Flink shuffles them on purpose', 'Elements from different upstream senders interleave differently on each receiver', 'Because of RocksDB', 'They cannot; order is identical everywhere'], answer: 1, explain: 'Per-channel FIFO only; multiple senders interleave arbitrarily per receiver.' },
    { q: 'In a (Keyed)BroadcastProcessFunction, where can broadcast state be <b>modified</b>?', options: ['processElement', 'processBroadcastElement', 'Both', 'onTimer'], answer: 1, explain: 'Only the broadcast side has read-write access; the main side is read-only.' },
    { q: 'Why is the non-broadcast side read-only?', options: ['Performance only', 'There is no cross-task communication, so only deterministic updates from the broadcast side keep all copies identical', 'RocksDB limitation', 'Licensing'], answer: 1, explain: 'If one task changed its copy based on its own data, copies would diverge.' },
    { q: 'Where is broadcast state stored?', options: ['RocksDB', 'On the heap; every task checkpoints its own copy', 'In Kafka', 'In the JobManager only'], answer: 1, explain: 'Broadcast state is in-memory and checkpointed by every task, so keep it small.' },
    { q: 'You put <code>rebalance()</code> right before <code>keyBy()</code>. Effect on per-key order?', options: ['Improves it', 'Destroys it: one key is now emitted by several subtasks', 'No effect', 'Makes it global'], answer: 1, explain: 'After rebalance, a key\'s records come from multiple senders.' },
    { q: 'Which async I/O variant preserves input order?', options: ['unorderedWait', 'orderedWait', 'Both', 'Neither'], answer: 1, explain: 'orderedWait emits in input order (with some latency cost).' },
    { q: 'In Apache Beam, may a DoFn rely on the order in which elements arrive?', options: ['Yes on Flink', 'No: Beam gives no ordering guarantees; use event time, windows, state/timers', 'Only in batch', 'Only with GroupByKey'], answer: 1, explain: 'PCollections are unordered in Beam\'s model.' }
  ]
});
