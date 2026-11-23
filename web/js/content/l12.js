FA.registerLevel({
  id: 12, track: 'Operations', icon: '⚙️', minutes: 30,
  title: 'Configuration & the memory model',
  summary: 'config.yaml vs flink-conf.yaml, config precedence, the TaskManager memory model, and the settings that matter most.',
  objectives: ['Know where each setting is configured and what overrides what', 'Compute TaskManager memory components', 'Diagnose common memory errors', 'Know the top settings for production'],
  sections: [
    { title: 'Where configuration lives', html: `
<ul>
<li><b>conf/config.yaml</b>: the cluster configuration file since Flink 1.19 (standard YAML 1.2, nested or flat keys). The legacy <code>flink-conf.yaml</code> still worked in 1.19/1.20 (and took priority if present) but is <b>not supported in Flink 2.0+</b>.</li>
<li><b>Docker</b>: the <code>FLINK_PROPERTIES</code> environment variable is appended to the config.</li>
<li><b>Kubernetes Operator</b>: <code>spec.flinkConfiguration</code> in the FlinkDeployment.</li>
<li><b>Per job</b>: <code>-D key=value</code> on <code>flink run</code>, or in code (<code>env.configure(...)</code>, <code>env.enableCheckpointing(...)</code>). Beam: pipeline options.</li>
</ul>
<p>Cluster-level options (memory, slots, HA, ports) are read when the JobManager/TaskManager processes start, so changing them requires a <b>restart</b> of those processes. Job-level options (parallelism, checkpointing, restart strategy) can be set per job; code usually wins over cluster defaults.</p>
${FA.C('tip', 'Explore all settings', 'The <a href="#/config">Config explorer</a> lists the important keys with defaults and tips, plus the Beam pipeline options.')}` },
    { title: 'TaskManager memory model', html: `
${FA.D('memory-tree')}
<pre><code>Total Process Memory (taskmanager.memory.process.size)  ← container/pod limit
├── JVM Metaspace (256m)
├── JVM Overhead (10%, 192m..1g)        ← threads, code cache, native
└── Total Flink Memory (taskmanager.memory.flink.size)
    ├── JVM Heap  (-Xmx)
    │   ├── Framework Heap (128m)
    │   └── Task Heap (rest)             ← your objects, HashMap state
    └── Off-heap
        ├── Managed Memory (40%)         ← RocksDB, batch sort/join, Python
        ├── Network Memory (10%, min 64m) ← buffers between tasks
        ├── Framework Off-Heap (128m)
        └── Task Off-Heap (0)            ← your direct/native memory</code></pre>
${FA.W('memory', {})}
<p>Check the default: process 1728m → overhead 192 → Flink memory 1280 → network 128, managed 512, task heap 384. That is exactly what a default Docker TaskManager logs at startup.</p>` },
    { title: 'Memory errors and what they mean', html: `
<table class="t"><tr><th>Error</th><th>Meaning</th><th>Typical fix</th></tr>
<tr><td><code>OutOfMemoryError: Java heap space</code></td><td>Task heap too small (heap state, big windows, buffering)</td><td>More task heap; RocksDB; incremental aggregation; TTL</td></tr>
<tr><td><code>OutOfMemoryError: Metaspace</code></td><td>Too many classes loaded (restarts, leaks)</td><td>Raise jvm-metaspace.size; fix classloader leaks</td></tr>
<tr><td><code>OutOfMemoryError: Direct buffer memory</code></td><td>Direct memory exceeded (connectors, Netty, user code)</td><td>Raise task.off-heap.size</td></tr>
<tr><td><code>Insufficient number of network buffers</code></td><td>Too many channels for network memory (high parallelism, all-to-all)</td><td>Raise network fraction/min/max</td></tr>
<tr><td>Container OOMKilled (exit 137), heap looks fine</td><td>Native memory above the limit (RocksDB, glibc arenas)</td><td>Raise jvm-overhead, keep RocksDB in managed memory, check <code>MALLOC_ARENA_MAX</code></td></tr></table>` },
    { title: 'Top production settings', html: `
<pre><code>taskmanager.numberOfTaskSlots: 4              # ≈ cores
parallelism.default: 4
pipeline.max-parallelism: 720                 # set once (divisible by many p values)
taskmanager.memory.process.size: 8g
state.backend.type: rocksdb
state.backend.incremental: true
state.checkpoints.dir: s3://…/checkpoints
execution.checkpointing.interval: 60s
execution.checkpointing.min-pause: 10s
execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION
restart-strategy.type: exponential-delay
high-availability.type: kubernetes
metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory</code></pre>
<p>Flink 2.x renamed several checkpoint keys (e.g. <code>execution.checkpointing.dir</code>, <code>execution.checkpointing.incremental</code>). Always check the docs for <b>your</b> version.</p>` }
  ],
  refs: [['Configuration', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/config/'], ['Set up Flink\'s process memory', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/memory/mem_setup/'], ['Set up TaskManager memory', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/memory/mem_setup_tm/'], ['FLIP-366 config.yaml', 'https://cwiki.apache.org/confluence/display/FLINK/FLIP-366:+Support+standard+YAML+for+FLINK+configuration'], ['FLIP-49 unified memory', 'https://cwiki.apache.org/confluence/display/FLINK/FLIP-49+Unified+Memory+Configuration+for+TaskExecutors']],
  quiz: [
    { q: 'Since which version is <code>config.yaml</code> the default config file, and in which major version is flink-conf.yaml no longer supported?', options: ['1.15 / 1.18', '1.19 / 2.0', '1.17 / 1.20', '2.0 / 3.0'], answer: 1, explain: 'FLIP-366: config.yaml default in 1.19; flink-conf.yaml unsupported in 2.0.' },
    { q: 'Default <code>taskmanager.memory.managed.fraction</code>?', options: ['0.1', '0.25', '0.4', '0.7'], answer: 2, explain: '40% of total Flink memory.' },
    { q: 'Which component holds RocksDB\'s block cache by default?', options: ['Task heap', 'Managed memory', 'Network memory', 'Metaspace'], answer: 1, explain: 'RocksDB is bounded by managed memory.' },
    { q: 'Process size 1728m with defaults. Total Flink memory (MB)?', type: 'number', answer: 1280, tolerance: 1, explain: '1728 − 256 metaspace − 192 overhead = 1280.' },
    { q: 'A TaskManager has process size 1728m with all other memory settings at their defaults (total Flink memory = 1280 MB). Network memory (MB)?', type: 'number', answer: 128, tolerance: 1, explain: '0.1 × 1280 = 128 (within 64m..1g).' },
    { q: '"Insufficient number of network buffers" at deployment. Fix?', options: ['Increase managed fraction', 'Increase network memory (fraction/min/max)', 'Increase metaspace', 'Disable checkpointing'], answer: 1, explain: 'Each channel needs buffers; give more network memory.' },
    { q: 'Container OOMKilled while heap usage looks healthy with RocksDB. Suspect…', options: ['Task heap', 'Native/off-heap memory (RocksDB, overhead) beyond limits', 'Metaspace only', 'Too few slots'], answer: 1, explain: 'Increase overhead; keep RocksDB within managed memory.' },
    { q: 'You change taskmanager.numberOfTaskSlots in the config. What else is needed?', options: ['Nothing, it applies live', 'Restart the TaskManagers', 'Restart only the job', 'Clear checkpoints'], answer: 1, explain: 'Cluster-level options are read at process start.' },
    { q: 'How do you pass Flink config to the official Docker image?', options: ['FLINK_PROPERTIES environment variable', 'Kafka headers', 'JAVA_OPTS only', 'It cannot be configured'], answer: 0, explain: 'FLINK_PROPERTIES is appended to the configuration.' },
    { q: 'JVM -Xmx of a TaskManager equals…', options: ['Total process memory', 'Framework heap + task heap', 'Managed memory', 'Network + task off-heap'], answer: 1, explain: 'JVM heap = framework heap + task heap.' }
  ]
});
