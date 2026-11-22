/* Config explorer, real cluster lab, glossary, references, progress */
(function (FA) {
  'use strict';
  const h = FA.h;

  FA.views.config = function (main) {
    const c = h('div', { class: 'container' });
    c.appendChild(h('h1', null, '⚙️ Configuration Explorer'));
    c.appendChild(h('div', { class: 'callout' }, h('b', null, 'Where config lives'), 'Cluster settings go in conf/config.yaml (Flink 1.19+; the legacy flink-conf.yaml is no longer supported in Flink 2.0), in Docker FLINK_PROPERTIES, or in the K8s operator\'s flinkConfiguration. Job-level settings can also be set in code or via -D at submission. Cluster-level options (memory, slots) need a process restart.'));
    const search = h('input', { class: 'search', placeholder: 'Search keys or descriptions… e.g. checkpoint, slots, memory, chaining' });
    const list = h('div');
    c.append(search, h('div', { class: 'card', style: { marginTop: '12px' } }, list));
    function draw() {
      const q = search.value.toLowerCase();
      list.innerHTML = '';
      const cats = {};
      FA.CONFIG.filter(x => !q || (x.k + x.t + x.c).toLowerCase().includes(q)).forEach(x => (cats[x.c] = cats[x.c] || []).push(x));
      for (const [cat, items] of Object.entries(cats)) {
        list.appendChild(h('h3', { style: { marginBottom: '4px' } }, cat));
        items.forEach(x => list.appendChild(h('div', { class: 'cfg-item' }, h('div', null, h('span', { class: 'k' }, x.k), h('span', { class: 'pill', style: { marginLeft: '8px' } }, 'default: ' + x.d)), h('div', { class: 'muted', style: { marginTop: '3px' } }, x.t))));
      }
      if (!list.children.length) list.appendChild(h('div', { class: 'muted' }, 'No matches.'));
    }
    search.addEventListener('input', draw); draw();
    c.appendChild(h('h2', null, 'Example config.yaml (Flink 1.19+ standard YAML, nested or flat keys both work)'));
    c.appendChild(h('pre', null, h('code', null, `jobmanager:
  rpc:
    address: flink-jobmanager
  memory:
    process:
      size: 1600m
taskmanager:
  numberOfTaskSlots: 4
  memory:
    process:
      size: 4096m
    managed:
      fraction: 0.4
parallelism:
  default: 4
state:
  backend:
    type: rocksdb
    incremental: true
  checkpoints:
    dir: s3://my-bucket/flink/checkpoints
execution:
  checkpointing:
    interval: 60s
    timeout: 10min
    min-pause: 10s
    externalized-checkpoint-retention: RETAIN_ON_CANCELLATION
restart-strategy:
  type: exponential-delay
rest:
  flamegraph:
    enabled: true`)));
    const mem = h('div', { 'data-widget': 'memory' });
    c.appendChild(h('h2', null, 'TaskManager memory calculator'));
    c.appendChild(mem);
    c.appendChild(h('h2', null, 'Beam FlinkRunner pipeline options (FlinkPipelineOptions)'));
    const t = h('table', { class: 't' }, h('tr', null, h('th', null, 'Option'), h('th', null, 'Meaning')));
    FA.BEAM_OPTIONS.forEach(([k, v]) => t.appendChild(h('tr', null, h('td', null, h('code', null, k)), h('td', null, v))));
    c.appendChild(h('div', { class: 'card' }, t));
    main.appendChild(c);
    FA.mountWidgets(c);
  };

  FA.views.cluster = function (main) {
    const c = h('div', { class: 'container lesson' });
    c.innerHTML = `<h1>🐳 Real Cluster Lab (optional)</h1>
    <div class="callout tip"><b>Why</b>The simulator teaches the rules; this lab lets you see the <b>real</b> Flink dashboard at <a href="http://localhost:8081" target="_blank">http://localhost:8081</a>. It needs Docker Desktop. The files are in the <code>real-cluster</code> folder next to this app.</div>
    <h2>1. Start the cluster</h2>
    <pre><code>cd D:\\Personal\\Claude\\flink-academy\\real-cluster
docker compose up -d
docker compose ps        # 1 jobmanager + 2 taskmanagers (2 slots each = 4 slots)</code></pre>
    <p>Open <a href="http://localhost:8081" target="_blank">localhost:8081</a> → Overview should show <b>Total Task Slots: 4</b> and <b>Task Managers: 2</b>.</p>
    <h2>2. Run a streaming job and read the graph</h2>
    <pre><code>docker compose exec jobmanager ./bin/flink run -d -p 2 ./examples/streaming/TopSpeedWindowing.jar
docker compose exec jobmanager ./bin/flink list</code></pre>
    <p>Open the job. Identify: how many boxes (tasks)? Which edges are FORWARD and which are HASH? Which operators are chained into one box? Open a box → <b>SubTasks</b>, <b>BackPressure</b>, <b>Watermarks</b>, <b>FlameGraph</b> (enabled in the compose file).</p>
    <h2>3. Exhaust the slots</h2>
    <pre><code>docker compose exec jobmanager ./bin/flink run -d -p 4 ./examples/streaming/StateMachineExample.jar</code></pre>
    <p>The first job uses 2 slots, this one needs 4 but only 2 are free. Watch the job stay in CREATED/SCHEDULED until it fails after <code>slot.request.timeout</code> (5 min). Cancel it, then try <code>-p 2</code>.</p>
    <h2>4. Savepoint, rescale, restore</h2>
    <pre><code>docker compose exec jobmanager ./bin/flink stop --savepointPath /tmp/flink-savepoints &lt;jobId&gt;
# note the printed savepoint path, then restart with a different parallelism:
docker compose exec jobmanager ./bin/flink run -d -p 4 -s /tmp/flink-savepoints/savepoint-xxxx ./examples/streaming/StateMachineExample.jar</code></pre>
    <p>The job resumes from its state with more subtasks: key groups were redistributed (Level 6).</p>
    <h2>5. Kill a worker and watch recovery</h2>
    <pre><code>docker compose stop taskmanager-2
# watch: Exceptions tab, job RESTARTING, then FAILED or stuck waiting for slots
docker compose start taskmanager-2</code></pre>
    <p>Look at the <b>Exceptions</b> tab, the restart strategy behaviour and the <b>Checkpoints → Overview → Latest Restore</b>.</p>
    <h2>6. Change cluster config</h2>
    <p>Edit <code>FLINK_PROPERTIES</code> in <code>docker-compose.yml</code> (e.g. <code>taskmanager.numberOfTaskSlots: 4</code>), then <code>docker compose up -d --force-recreate</code>. Check <b>Job Manager → Configuration</b> to confirm the effective value and <b>Task Managers → (a TM) → Metrics</b> for the memory model.</p>
    <h2>7. Run a Beam pipeline on it</h2>
    <p>The Flink runner minor version must match the cluster. The compose file uses Flink <b>1.20</b>, so use <code>beam-runners-flink-1.20</code> (Beam ≥ 2.70). With Maven + JDK 17 installed:</p>
    <pre><code>mvn archetype:generate -DarchetypeGroupId=org.apache.beam -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-examples ^
    -DarchetypeVersion=2.70.0 -DgroupId=org.example -DartifactId=word-count-beam -Dversion="0.1" -Dpackage=org.apache.beam.examples -DinteractiveMode=false
cd word-count-beam
# make sure the flink-runner profile uses beam-runners-flink-1.20, then:
mvn package -Pflink-runner -DskipTests
docker cp target\\word-count-beam-bundled-0.1.jar flink-jobmanager:/tmp/beam.jar
docker compose exec jobmanager ./bin/flink run -c org.apache.beam.examples.WordCount /tmp/beam.jar --runner=FlinkRunner --inputFile=/opt/flink/README.txt --output=/tmp/counts</code></pre>
    <p>In the UI, see how Beam transform names (e.g. <code>ReadLines</code>, <code>CountWords/…</code>, <code>WriteCounts</code>) become Flink tasks and where GroupByKey creates a HASH edge. (Exact archetype names can differ between Beam versions; check <code>pom.xml</code>.)</p>
    <h2>8. Stop everything</h2>
    <pre><code>docker compose down</code></pre>`;
    main.appendChild(c);
  };

  FA.GLOSSARY = [
    ['JobManager', 'Coordinator process: Dispatcher (accepts jobs, runs the Web UI), ResourceManager (slots) and one JobMaster per job (scheduling, checkpoints, recovery).'],
    ['TaskManager (TM)', 'Worker JVM that runs subtasks in threads, owns task slots, network buffers and local state.'],
    ['Task slot', 'Unit of resource scheduling in a TM. Isolates managed memory but NOT CPU. With slot sharing one slot can run one subtask of every task of a job.'],
    ['Operator', 'One transformation (map, filter, window…).'],
    ['Task / JobVertex', 'One box in the UI: one or more operators chained together.'],
    ['Subtask', 'One parallel instance of a task, i.e. one thread. "Parallelism: 4" = 4 subtasks.'],
    ['Operator chaining', 'Fusing operators with FORWARD connection + same parallelism into one task (same thread, no serialization).'],
    ['Parallelism', 'Number of subtasks of an operator. Set per operator, per job, via -p, or parallelism.default.'],
    ['Max parallelism', 'Number of key groups. Upper bound for rescaling keyed state.'],
    ['Key group', 'Atomic unit of keyed state. key → murmurHash(key.hashCode()) % maxParallelism.'],
    ['Ship strategy', 'How records move between tasks: FORWARD, HASH, REBALANCE, RESCALE, BROADCAST, GLOBAL, SHUFFLE, CUSTOM.'],
    ['Backpressure', 'A task cannot hand off output because downstream buffers are full, so it slows down and the slowness propagates upstream to the source.'],
    ['busyTimeMsPerSecond', 'Metric: ms per second a subtask is doing work. Busy + idle + backpressured ≈ 1000.'],
    ['backPressuredTimeMsPerSecond', 'Metric: ms per second a subtask is blocked waiting for output buffers.'],
    ['Watermark', 'Event-time progress marker: "no more events with timestamp ≤ W expected". Triggers event-time windows/timers.'],
    ['Checkpoint', 'Automatic, periodic, consistent snapshot of all operator state + source positions, used for failure recovery.'],
    ['Savepoint', 'Manually triggered, user-owned snapshot in a canonical format for upgrades, rescaling and migrations.'],
    ['Checkpoint barrier', 'Marker injected at sources that flows with the data and splits the stream into pre/post-checkpoint parts.'],
    ['Alignment', 'Waiting at a multi-input operator until a barrier arrived on all inputs (aligned checkpoints).'],
    ['State backend', 'Where working state lives: HashMap (heap objects) or RocksDB (local disk, serialized).'],
    ['Broadcast state', 'Operator state replicated to every parallel instance via a broadcast stream. Only the broadcast side may write it.'],
    ['Slot sharing group', 'Named group whose operators may share slots. Different groups need different slots.'],
    ['Pipelined region', 'Set of tasks connected by pipelined exchanges that must be restarted together on failure.'],
    ['FlinkRunner', 'Apache Beam runner that translates a Beam pipeline into a Flink job.'],
    ['Fusion (Beam)', 'Beam\'s term for combining consecutive transforms into one execution stage. On Flink, this corresponds to operator chaining.'],
    ['Bundle (Beam)', 'Group of elements processed together by a DoFn (@StartBundle/@FinishBundle). Controlled by maxBundleSize/maxBundleTimeMills on Flink.'],
    ['GroupByKey (Beam)', 'Groups by key and window: a shuffle. On Flink it becomes a keyBy, so you see a HASH edge.']
  ];
  FA.views.glossary = function (main) {
    const c = h('div', { class: 'container' });
    c.appendChild(h('h1', null, '📖 Glossary'));
    const s = h('input', { class: 'search', placeholder: 'Filter…' });
    const t = h('div', { class: 'card', style: { marginTop: '12px' } });
    c.append(s, t);
    const draw = () => { t.innerHTML = ''; const q = s.value.toLowerCase(); FA.GLOSSARY.filter(g => (g[0] + g[1]).toLowerCase().includes(q)).forEach(g => t.appendChild(h('div', { class: 'cfg-item' }, h('b', null, g[0]), h('div', { class: 'muted' }, g[1])))); };
    s.addEventListener('input', draw); draw();
    main.appendChild(c);
  };

  FA.REFERENCES = [
    ['Official Apache Flink documentation (1.20 docs are used for most links; Beam-on-Flink jobs typically run on Flink 1.x)', [
      ['Flink concepts overview: parallel dataflows, ordering in one-to-one vs redistributing streams', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/concepts/overview/'],
      ['Flink architecture: JobManager, TaskManagers, task slots, slot sharing, chaining', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/concepts/flink-architecture/'],
      ['Stateful stream processing: keyed state, key groups, checkpoint barriers, alignment', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/concepts/stateful-stream-processing/'],
      ['Timely stream processing: event time, watermarks, lateness', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/concepts/time/'],
      ['DataStream operators: physical partitioning, task chaining, slot sharing groups', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/overview/'],
      ['Parallel execution & max parallelism', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/execution/parallel/'],
      ['Working with state (ValueState, ListState, MapState, TTL)', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/fault-tolerance/state/'],
      ['The Broadcast State pattern', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/fault-tolerance/broadcast_state/'],
      ['Generating watermarks (WatermarkStrategy, idleness)', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/event-time/generating_watermarks/'],
      ['Windows (assigners, triggers, allowed lateness)', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/windows/'],
      ['Checkpoints', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/state/checkpoints/'],
      ['Savepoints', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/state/savepoints/'],
      ['Checkpointing under backpressure (unaligned checkpoints, buffer debloating)', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/state/checkpointing_under_backpressure/'],
      ['State backends (HashMap vs RocksDB)', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/state/state_backends/'],
      ['Tuning checkpoints and large state', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/state/large_state_tuning/'],
      ['Task failure recovery (restart strategies, failover strategies)', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/state/task_failure_recovery/'],
      ['Monitoring back pressure (busy/idle/backpressured, colours, OK/LOW/HIGH)', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/monitoring/back_pressure/'],
      ['Monitoring checkpointing (Checkpoints tab explained)', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/monitoring/checkpoint_monitoring/'],
      ['Flame graphs', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/debugging/flame_graphs/'],
      ['Metrics (system metrics list)', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/metrics/'],
      ['Configuration (config.yaml, all options)', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/config/'],
      ['Set up Flink\'s process memory', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/memory/mem_setup/'],
      ['Set up TaskManager memory (detailed model)', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/memory/mem_setup_tm/'],
      ['Network memory tuning guide', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/memory/network_mem_tuning/'],
      ['Deployment overview (session vs application mode)', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/overview/'],
      ['Flink on Docker (docker-compose, FLINK_PROPERTIES)', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/resource-providers/standalone/docker/'],
      ['Elastic scaling (adaptive scheduler, reactive mode)', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/elastic_scaling/'],
      ['Flink Operations Playground (official hands-on)', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/try-flink/flink-operations-playground/'],
      ['Flink Kubernetes Operator documentation', 'https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/'],
      ['Flink downloads & current release (Flink 2.2.x is the latest stable at the time of writing)', 'https://flink.apache.org/downloads/']
    ]],
    ['Apache Beam (FlinkRunner)', [
      ['Beam Flink Runner: pipeline options and the Flink version compatibility table', 'https://beam.apache.org/documentation/runners/flink/'],
      ['Beam Flink Runner roadmap (Flink 2.x support)', 'https://beam.apache.org/roadmap/flink-runner/'],
      ['Beam programming guide (PCollections, ParDo, GroupByKey, windowing, triggers, state & timers)', 'https://beam.apache.org/documentation/programming-guide/'],
      ['Beam capability matrix (which features each runner supports)', 'https://beam.apache.org/documentation/runners/capability-matrix/'],
      ['The Dataflow Model paper (Akidau et al., VLDB 2015): the basis of Beam\'s model', 'https://www.vldb.org/pvldb/vol8/p1792-Akidau.pdf']
    ]],
    ['Blogs, papers & deep dives', [
      ['Flink blog: How to identify the source of backpressure? (2021)', 'https://flink.apache.org/2021/07/07/how-to-identify-the-source-of-backpressure/'],
      ['Flink blog: A Deep-Dive into Flink\'s Network Stack (credit-based flow control)', 'https://flink.apache.org/2019/06/05/a-deep-dive-into-flinks-network-stack/'],
      ['Lightweight Asynchronous Snapshots for Distributed Dataflows (the checkpoint algorithm paper)', 'https://arxiv.org/abs/1506.08603'],
      ['Ververica: Broadcast State Pattern, 4 important considerations', 'https://ververica.com/blog/broadcast-state-pattern-flink-considerations'],
      ['Contentsquare: Ten Flink gotchas (max parallelism / key group skew and more)', 'https://engineering.contentsquare.com/2021/ten-flink-gotchas/'],
      ['FLIP-49: Unified memory configuration for TaskExecutors', 'https://cwiki.apache.org/confluence/display/FLINK/FLIP-49+Unified+Memory+Configuration+for+TaskExecutors'],
      ['FLIP-366: Standard YAML for Flink configuration (config.yaml)', 'https://cwiki.apache.org/confluence/display/FLINK/FLIP-366:+Support+standard+YAML+for+FLINK+configuration'],
      ['Confluent Apache Flink 101: using the Flink Web UI (exercise)', 'https://developer.confluent.io/courses/apache-flink/web-ui-exercise'],
      ['Official Flink Docker image', 'https://hub.docker.com/_/flink']
    ]],
    ['Books', [
      ['Stream Processing with Apache Flink (Fabian Hueske & Vasiliki Kalavri, O\'Reilly 2019)', 'https://www.oreilly.com/library/view/stream-processing-with/9781491974285/'],
      ['Streaming Systems (Akidau, Chernyak, Lax, O\'Reilly 2018): watermarks, triggers and the Beam model', 'https://www.oreilly.com/library/view/streaming-systems/9781491983867/']
    ]]
  ];
  FA.views.references = function (main) {
    const c = h('div', { class: 'container' });
    c.appendChild(h('h1', null, '📚 References'));
    c.appendChild(h('div', { class: 'muted', style: { marginBottom: '14px' } }, 'Everything taught in Flink Academy is based on these sources. Each level also lists its own references at the bottom of the lesson. Where the simulator simplifies reality (cost model, checkpoint estimates), the lesson says so.'));
    FA.REFERENCES.forEach(([cat, items]) => {
      c.appendChild(h('div', { class: 'card' }, h('h3', { style: { marginTop: 0 } }, cat),
        h('ol', null, items.map(([t, u]) => h('li', { style: { marginBottom: '4px' } }, h('a', { href: u, target: '_blank', rel: 'noopener' }, t), h('div', { class: 'muted', style: { fontSize: '12px' } }, u))))));
    });
    main.appendChild(c);
  };

  FA.views.progress = function (main) {
    const st = FA.store;
    const c = h('div', { class: 'container' });
    c.appendChild(h('h1', null, '🏅 Progress & settings'));
    const t = h('table', { class: 't' }, h('tr', null, ['Level', 'Status', 'Best', 'Attempts'].map(x => h('th', null, x))));
    FA.levels.forEach(l => { const p = st.lvl(l.id); t.appendChild(h('tr', null, h('td', null, `${l.id}. ${l.title}`), h('td', null, p.passed ? '✅ passed' : st.isUnlocked(l.id) ? (p.read ? '📖 read' : '🔓 open') : '🔒 locked'), h('td', null, p.attempts ? p.best + '%' : '-'), h('td', null, p.attempts))); });
    c.appendChild(h('div', { class: 'card' }, h('div', null, `XP: ${st.state.xp} · Rank: ${st.rank().name} · Challenges solved: ${Object.keys(st.state.challenges).length}/${FA.labChallenges.length}`), t));
    const ta = h('textarea', { style: { width: '100%', height: '120px' }, placeholder: 'Paste exported progress JSON here to import' });
    c.appendChild(h('div', { class: 'card' }, h('b', null, 'Backup / restore'),
      h('div', { class: 'muted', style: { margin: '4px 0 8px' } }, st.serverAvailable ? 'Progress is saved automatically to data/progress.json (and in the browser).' : 'Running without the Python server, so progress lives in this browser only. Export it now and then.'),
      h('div', { class: 'row' },
        h('button', { class: 'btn', onclick: () => { ta.value = st.exportJson(); ta.select(); FA.toast('Exported. Copy the text.'); } }, '⬇ Export'),
        h('button', { class: 'btn', onclick: () => { try { st.importJson(ta.value); FA.toast('Imported ✓'); FA.render(); } catch (e) { FA.toast('Invalid JSON'); } } }, '⬆ Import'),
        h('button', { class: 'btn', style: { borderColor: 'var(--bad)' }, onclick: async () => { if (confirm('Reset ALL progress? (the server keeps a .bak.json backup)')) { await st.reset(); FA.toast('Progress reset'); FA.render(); } } }, '🗑 Reset progress')),
      h('div', { style: { marginTop: '8px' } }, ta)));
    main.appendChild(c);
  };
})(window.FA);
