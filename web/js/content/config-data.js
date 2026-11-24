/* Configuration reference used by the Config Explorer. Keys are Flink 1.19/1.20 names unless noted; 2.x renames are mentioned. */
window.FA = window.FA || {};
FA.CONFIG = [
  // ---- Cluster & parallelism
  { k: 'taskmanager.numberOfTaskSlots', d: '1', c: 'Cluster & parallelism', t: 'Slots per TaskManager. Cluster capacity = TMs × slots. Rule of thumb: about 1 slot per CPU core; slots do NOT isolate CPU, only managed memory.' },
  { k: 'parallelism.default', d: '1', c: 'Cluster & parallelism', t: 'Parallelism used when neither the operator nor the job sets one. With Beam, --parallelism overrides it for the job.' },
  { k: 'pipeline.max-parallelism', d: '-1 (auto: roundUpToPowerOfTwo(p + p/2), min 128, max 32768)', c: 'Cluster & parallelism', t: 'Number of key groups for keyed state = upper limit for rescaling. Set it EXPLICITLY before the first production deploy. Changing it later breaks state restore.' },
  { k: 'jobmanager.scheduler', d: 'default', c: 'Cluster & parallelism', t: '"adaptive" enables the Adaptive Scheduler (rescales to available slots; used by reactive mode and autoscaling).' },
  { k: 'slot.request.timeout', d: '300000 ms (5 min)', c: 'Cluster & parallelism', t: 'How long the scheduler waits for slots before failing with NoResourceAvailableException.' },
  { k: 'taskmanager.load-balance.mode', d: 'NONE', c: 'Cluster & parallelism', t: 'Flink 1.20+: SLOTS spreads slots evenly across TaskManagers (replaces the older cluster.evenly-spread-out-slots: true). Helps avoid one hot TM.' },
  { k: 'execution.runtime-mode', d: 'STREAMING', c: 'Cluster & parallelism', t: 'STREAMING / BATCH / AUTOMATIC. BATCH works only with bounded sources and uses blocking shuffles + sort-based operators.' },
  // ---- Memory
  { k: 'taskmanager.memory.process.size', d: '(none) · official Docker image config uses 1728m', c: 'Memory', t: 'Total TM process memory, i.e. what your container/pod limit must cover. Set this OR taskmanager.memory.flink.size in containers.' },
  { k: 'taskmanager.memory.flink.size', d: '(none)', c: 'Memory', t: 'Total Flink memory (process minus JVM metaspace and overhead). Typical for standalone bare-metal setups.' },
  { k: 'taskmanager.memory.managed.fraction', d: '0.4', c: 'Memory', t: 'Share of Flink memory given to managed memory: RocksDB block cache/write buffers, batch sorting, Python workers. With the HashMap backend in streaming you can lower it to give the heap more room.' },
  { k: 'taskmanager.memory.network.fraction', d: '0.1 (min 64mb, max 1gb in 1.x docs; check your version)', c: 'Memory', t: 'Network buffers for exchanges between tasks (not used by chained operators). Too few gives "Insufficient number of network buffers" at deploy.' },
  { k: 'taskmanager.memory.jvm-metaspace.size', d: '256mb', c: 'Memory', t: 'Class metadata. Raise it if you see "OutOfMemoryError: Metaspace" (frequent with many job restarts or classloader leaks).' },
  { k: 'taskmanager.memory.jvm-overhead.fraction', d: '0.1 (min 192mb, max 1gb)', c: 'Memory', t: 'Native memory for threads, code cache, GC, etc. Container killed with heap looking fine? Increase overhead (or check RocksDB).' },
  { k: 'taskmanager.memory.task.heap.size', d: '(derived)', c: 'Memory', t: 'Heap for YOUR objects and the HashMap state backend. Normally derived as the remainder.' },
  { k: 'taskmanager.memory.framework.heap.size', d: '128mb', c: 'Memory', t: 'Heap reserved for Flink framework. Rarely changed.' },
  { k: 'jobmanager.memory.process.size', d: '(none) · Docker image config uses 1600m', c: 'Memory', t: 'JobManager JVM total. Raise it for very large jobs, many checkpoints/metadata or big UI histories.' },
  // ---- State & checkpointing
  { k: 'state.backend.type', d: 'hashmap', c: 'State & checkpoints', t: '"hashmap" = state as Java objects on the heap (fast, limited by heap). "rocksdb" = serialized on local disk + managed memory (large state, incremental checkpoints). Flink 2.x also adds "forst" (disaggregated state).' },
  { k: 'execution.checkpointing.interval', d: '(none) = checkpointing disabled', c: 'State & checkpoints', t: 'How often to checkpoint (e.g. 60s). Without it there is no exactly-once state and Kafka offsets are not committed on checkpoints. Beam: --checkpointingInterval.' },
  { k: 'execution.checkpointing.mode', d: 'EXACTLY_ONCE', c: 'State & checkpoints', t: 'EXACTLY_ONCE aligns barriers (or uses unaligned); AT_LEAST_ONCE skips alignment (lower latency, possible duplicates in state after recovery).' },
  { k: 'execution.checkpointing.timeout', d: '10 min', c: 'State & checkpoints', t: 'A checkpoint not finished in time is declared failed ("Checkpoint expired before completing"). Usually a symptom of backpressure or slow uploads.' },
  { k: 'execution.checkpointing.min-pause', d: '0 ms', c: 'State & checkpoints', t: 'Minimum gap between the end of one checkpoint and the start of the next, which guarantees processing time between checkpoints.' },
  { k: 'execution.checkpointing.max-concurrent-checkpoints', d: '1', c: 'State & checkpoints', t: 'Number of checkpoints that may be in flight at once.' },
  { k: 'execution.checkpointing.unaligned', d: 'false', c: 'State & checkpoints', t: 'Flink 2.x name: execution.checkpointing.unaligned.enabled. Barriers overtake in-flight data, so checkpoint time no longer depends on backpressure; checkpoints get bigger. Beam: --unalignedCheckpointEnabled.' },
  { k: 'execution.checkpointing.aligned-checkpoint-timeout', d: '0 ms', c: 'State & checkpoints', t: 'Start aligned and switch to unaligned if alignment takes longer than this.' },
  { k: 'execution.checkpointing.tolerable-failed-checkpoints', d: '0', c: 'State & checkpoints', t: 'How many consecutive checkpoint failures are tolerated before the JOB fails. Beam: --tolerableCheckpointFailureNumber.' },
  { k: 'state.checkpoints.dir', d: '(none)', c: 'State & checkpoints', t: 'Durable storage for checkpoints, e.g. s3://bucket/checkpoints. Flink 2.x name: execution.checkpointing.dir.' },
  { k: 'state.savepoints.dir', d: '(none)', c: 'State & checkpoints', t: 'Default target for savepoints. Flink 2.x name: execution.checkpointing.savepoint-dir.' },
  { k: 'execution.checkpointing.externalized-checkpoint-retention', d: 'NO_EXTERNALIZED_CHECKPOINTS', c: 'State & checkpoints', t: 'RETAIN_ON_CANCELLATION keeps the last checkpoint after cancel, so you can restart from it like a savepoint. Beam: --externalizedCheckpointsEnabled.' },
  { k: 'state.backend.incremental', d: 'false', c: 'State & checkpoints', t: 'RocksDB incremental checkpoints upload only new SST files, which shrinks checkpoint size for large state. Flink 2.x name: execution.checkpointing.incremental.' },
  { k: 'state.backend.rocksdb.memory.managed', d: 'true', c: 'State & checkpoints', t: 'RocksDB uses the slot\'s managed memory budget for block cache + write buffers.' },
  // ---- Restart / failure
  { k: 'restart-strategy.type', d: 'exponential-delay when checkpointing is on (1.19+); none otherwise', c: 'Fault tolerance', t: 'fixed-delay, failure-rate, exponential-delay or none. Controls how often the job restarts after a task failure before going FAILED.' },
  { k: 'restart-strategy.fixed-delay.attempts', d: '1', c: 'Fault tolerance', t: 'With fixed-delay: number of restart attempts (+ restart-strategy.fixed-delay.delay, default 1 s).' },
  { k: 'jobmanager.execution.failover-strategy', d: 'region', c: 'Fault tolerance', t: 'Restart only the pipelined region containing the failed task. In streaming jobs with all-to-all exchanges the region is usually the whole job.' },
  { k: 'heartbeat.timeout', d: '50000 ms', c: 'Fault tolerance', t: 'After this a silent TaskManager is considered lost and its tasks fail ("Heartbeat of TaskManager timed out"). Often caused by long GC pauses.' },
  { k: 'high-availability.type', d: 'NONE', c: 'Fault tolerance', t: 'zookeeper or kubernetes for JobManager HA. Needs high-availability.storageDir. The Flink K8s operator configures this for you.' },
  // ---- Network & latency
  { k: 'execution.buffer-timeout', d: '100 ms', c: 'Network & latency', t: 'Max time an output buffer waits before being flushed even if not full. Lower gives lower latency but less throughput; 0 flushes after every record.' },
  { k: 'taskmanager.network.memory.buffers-per-channel', d: '2', c: 'Network & latency', t: 'Exclusive buffers per input channel. Fewer in-flight buffers means faster aligned checkpoints under backpressure.' },
  { k: 'taskmanager.network.memory.floating-buffers-per-gate', d: '8', c: 'Network & latency', t: 'Shared buffers per input gate, distributed to channels on demand (credit-based flow control).' },
  { k: 'taskmanager.network.memory.buffer-debloat.enabled', d: 'false', c: 'Network & latency', t: 'Buffer debloating automatically shrinks in-flight data to a target time, which speeds up aligned checkpoints under backpressure.' },
  // ---- Pipeline / API behaviour
  { k: 'pipeline.operator-chaining.enabled', d: 'true', c: 'Pipeline', t: 'Global switch for chaining (older name: pipeline.operator-chaining). Disabling it is great for DEBUGGING in the UI (every operator gets its own box and metrics) but costs performance. Beam: --operatorChaining=false.' },
  { k: 'pipeline.object-reuse', d: 'false', c: 'Pipeline', t: 'Reuse objects between chained operators instead of copying. Faster, but unsafe if your functions keep references. Beam: --objectReuse.' },
  { k: 'pipeline.auto-watermark-interval', d: '200 ms', c: 'Pipeline', t: 'How often periodic watermark generators emit watermarks. Beam: --autoWatermarkInterval.' },
  { k: 'pipeline.name', d: '(none)', c: 'Pipeline', t: 'Job name shown in the UI.' },
  // ---- REST / UI / metrics
  { k: 'rest.port', d: '8081', c: 'Web UI & metrics', t: 'Port of the REST API and Web UI.' },
  { k: 'rest.flamegraph.enabled', d: 'false', c: 'Web UI & metrics', t: 'Enables the FlameGraph tab on tasks. Sampling has some overhead; enable in dev or temporarily.' },
  { k: 'web.submit.enable', d: 'true', c: 'Web UI & metrics', t: 'Allow uploading/submitting jars from the UI. Commonly false in production.' },
  { k: 'web.cancel.enable', d: 'true', c: 'Web UI & metrics', t: 'Show the Cancel Job button.' },
  { k: 'metrics.reporter.<name>.factory.class', d: '(none)', c: 'Web UI & metrics', t: 'e.g. org.apache.flink.metrics.prometheus.PrometheusReporterFactory (port 9249) to scrape metrics into Prometheus/Grafana.' },
  { k: 'metrics.latency.interval', d: '0 (disabled)', c: 'Web UI & metrics', t: 'Latency markers: useful for debugging, expensive in production.' }
];

FA.BEAM_OPTIONS = [
  ['--runner=FlinkRunner', 'Use the Flink runner (classic Java runner submits a native Flink job).'],
  ['--flinkMaster', 'host:port of the JobManager REST endpoint, [local], [auto] (default) or [collection].'],
  ['--parallelism', 'Job-wide parallelism (-1 = Flink default). All Beam transforms get this value.'],
  ['--maxParallelism', 'Key groups / rescale limit. Set it once and keep it stable for savepoint compatibility.'],
  ['--streaming', 'Force streaming mode (unbounded sources switch to streaming automatically).'],
  ['--checkpointingInterval', 'Checkpoint interval in ms (-1 = disabled, the default!).'],
  ['--checkpointingMode', 'EXACTLY_ONCE (default) or AT_LEAST_ONCE.'],
  ['--checkpointTimeoutMillis / --minPauseBetweenCheckpoints / --numConcurrentCheckpoints', 'Same meaning as the Flink checkpoint settings.'],
  ['--tolerableCheckpointFailureNumber', 'Default 0: one failed checkpoint fails the job.'],
  ['--externalizedCheckpointsEnabled', 'Retain checkpoints on cancellation (default false).'],
  ['--unalignedCheckpointEnabled', 'Use unaligned checkpoints (default false).'],
  ['--stateBackend', '"rocksdb" or "hashmap" (also --stateBackendStoragePath).'],
  ['--objectReuse / --fasterCopy', 'Avoid object copies between operators (default false).'],
  ['--operatorChaining', 'Default true. Set false to see every transform as its own box when debugging.'],
  ['--maxBundleSize / --maxBundleTimeMills', 'Beam bundles: defaults 1000 elements / 1000 ms in streaming. This affects latency and how often @FinishBundle runs.'],
  ['--numberOfExecutionRetries', '-1 = system default; 0 effectively disables fault tolerance.'],
  ['--autoWatermarkInterval', 'Interval for automatic watermark emission.'],
  ['--latencyTrackingInterval', '≤0 disables latency markers (default 0).'],
  ['--savepointPath', 'Restore the job from a savepoint/retained checkpoint on submission.'],
  ['--disableMetrics', 'Turn off Beam metric reporting to Flink (default false).']
];
