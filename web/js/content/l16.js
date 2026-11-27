FA.registerLevel({
  id: 16, track: 'Certification', icon: '🏆', minutes: 30,
  title: 'Final exam: Flink Engineer',
  summary: '25 mixed questions drawn from every level plus scenario questions. Pass with 80% to earn the Flink Architect badge.',
  finalExam: true, examSize: 25,
  objectives: ['Prove you can reason across architecture, parallelism, time, state, operations and Beam'],
  sections: [
    { title: 'How the exam works', html: `
<ul>
<li>25 questions: 2 random questions from each level plus scenario questions that combine topics.</li>
<li>Options and question order are shuffled on every attempt, so you cannot memorise positions.</li>
<li>Pass mark: 80%. Passing awards +500 XP.</li>
<li>Before you start: solve the <a href="#/lab">Pipeline Lab challenges</a>. They are the best practice for the scenario questions.</li>
</ul>` },
    { title: 'Quick revision sheet', html: `
<ul>
<li>Chaining = FORWARD + same parallelism + same slot sharing group + not disabled.</li>
<li>Slots needed = Σ over slot sharing groups of (max parallelism in group).</li>
<li>Edges: keyBy → HASH; parallelism change → REBALANCE; same → FORWARD.</li>
<li>Order: FIFO per (sender, receiver) channel only.</li>
<li>Key group = murmur(hashCode) % maxP; maxP default ≥ 128; never change it.</li>
<li>Watermark = max ts − bound − 1 ms; operators take the min of inputs; idle splits → withIdleness.</li>
<li>Bottleneck = first red after black. A hot key is not fixed by parallelism.</li>
<li>Aligned checkpoints are slow under backpressure → fix the bottleneck / unaligned / debloating.</li>
<li>Memory: process = Flink + metaspace + overhead; managed 0.4 and network 0.1 of Flink memory.</li>
<li>Beam: one --parallelism; GBK → HASH; --checkpointingInterval default -1; runner minor = cluster minor.</li>
</ul>` }
  ],
  quiz: [
    { q: '<b>Scenario.</b> A Beam job on Flink: Source and ParDo boxes are black, the Combine.perKey box is red, and in its SubTasks one subtask receives 8× more records than the others. Best next step?', options: ['Raise --parallelism to 64', 'Address key skew: pre-combine (Combine has combiner lifting) / salt the hot key / rethink the key', 'Enable unaligned checkpoints only', 'Switch to processing time'], answer: 1, explain: 'Uneven per-subtask input under HASH = skew; parallelism will not split one key.' },
    { q: '<b>Scenario.</b> A job needs 10 slots; the cluster has 3 TMs × 3 slots. What will you see?', options: ['Job runs with 9 subtasks', 'Job waits for resources, then fails with NoResourceAvailableException', 'Flink reduces parallelism automatically (default scheduler)', 'Job runs but drops data'], answer: 1, explain: '9 < 10 slots. The default scheduler cannot shrink the job.' },
    { q: '<b>Scenario.</b> Checkpoints expire after 10 min only during peak hours; alignment duration is huge. Most effective root-cause fix?', options: ['Increase checkpoint timeout to 1 hour', 'Remove the backpressure bottleneck (and optionally use unaligned checkpoints)', 'Disable checkpoints at peak', 'Lower max parallelism'], answer: 1, explain: 'Alignment is slow because barriers queue behind backpressured buffers.' },
    { q: '<b>Scenario.</b> After deploying a new version from a savepoint, the job fails: "Cannot map checkpoint/savepoint state for operator …". The developer inserted a new map step. Likely cause?', options: ['Kafka offsets lost', 'Stateful operators had no explicit uid (or Beam step names changed), so generated IDs shifted', 'RocksDB version', 'Too few slots'], answer: 1, explain: 'Stable uids/names keep the state mapping intact.' },
    { q: '<b>Scenario.</b> Windows stopped producing output after one Kafka partition went quiet overnight. Fix?', options: ['WatermarkStrategy.withIdleness (or the IO\'s idle handling)', 'Increase slots', 'Use BROADCAST', 'allowedLateness'], answer: 0, explain: 'The idle partition holds back the minimum watermark.' },
    { q: '<b>Scenario.</b> You need every parallel instance of a fraud detector to know the latest rules from a tiny "rules" topic. Pattern?', options: ['keyBy on rule id', 'Broadcast state pattern', 'GLOBAL partitioning', 'Side output'], answer: 1, explain: 'Broadcast the rules stream; keep rules in broadcast state.' },
    { q: '<b>Scenario.</b> Upstream p=3 → keyBy → downstream p=3, records of key K come from all 3 upstream subtasks. Which ordering holds at the downstream subtask that owns K?', options: ['Full per-key order', 'Only per-sender order: K\'s records from the same upstream subtask stay in order', 'No order at all, even per sender', 'Global order'], answer: 1, explain: 'FIFO per channel only.' },
    { q: '<b>Scenario.</b> A TaskManager pod (8 GB limit) using RocksDB is OOMKilled while JVM heap is at 50%. Which settings to look at first? (select all)', type: 'multi', options: ['taskmanager.memory.jvm-overhead.*', 'state.backend.rocksdb.memory.managed (keep true) / managed memory size', 'parallelism.default', 'execution.buffer-timeout'], answer: [0, 1], explain: 'Native memory beyond the budget: RocksDB and overhead.' }
  ]
});
