FA.registerLevel({
  id: 13, track: 'Operations', icon: '🚀', minutes: 25,
  title: 'Deployment, scaling & upgrades',
  summary: 'Session vs application clusters, Kubernetes Operator, HA, rescaling, autoscaling and safe upgrade procedures.',
  objectives: ['Choose a deployment mode', 'Describe a FlinkDeployment on Kubernetes', 'Rescale and upgrade a stateful job safely', 'Know what HA protects against'],
  sections: [
    { title: 'Deployment modes', html: `
<table class="t"><tr><th></th><th>Session</th><th>Application</th></tr>
<tr><td>Cluster lifetime</td><td>Long-running, shared by many jobs</td><td>One cluster per application, torn down with it</td></tr>
<tr><td>main() runs</td><td>On the client</td><td>On the JobManager</td></tr>
<tr><td>Isolation</td><td>Weak (shared TMs; a crash affects all jobs)</td><td>Strong</td></tr>
<tr><td>Good for</td><td>Dev, many small/short jobs, SQL gateway</td><td>Production streaming jobs (K8s standard)</td></tr></table>
${FA.D('session-vs-app')}
<p>Resource providers: Standalone (Docker/VMs), native Kubernetes, YARN. Standalone cannot add TaskManagers by itself.</p>` },
    { title: 'Kubernetes Operator', html: `
<pre><code>apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata: { name: orders-agg }
spec:
  image: registry/orders-agg:1.4.2
  flinkVersion: v1_19
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "4"
    state.backend.type: rocksdb
    execution.checkpointing.interval: 60s
    state.checkpoints.dir: s3://bucket/ckpt
    state.savepoints.dir: s3://bucket/sp
    high-availability.type: kubernetes
    high-availability.storageDir: s3://bucket/ha
  jobManager: { resource: { memory: "2048m", cpu: 1 } }
  taskManager: { resource: { memory: "8192m", cpu: 4 } }
  job:
    jarURI: local:///opt/flink/usrlib/orders-agg.jar
    parallelism: 8
    upgradeMode: savepoint        # stateless | last-state | savepoint
    state: running</code></pre>
<ul>
<li>Change <code>image</code>/<code>parallelism</code> → the operator performs the upgrade according to <code>upgradeMode</code>: <b>savepoint</b> (stop with savepoint → redeploy → restore), <b>last-state</b> (uses the latest checkpoint via HA metadata; fast), <b>stateless</b> (start fresh).</li>
<li>The operator can also trigger periodic savepoints and offers an <b>autoscaler</b> that adjusts per-vertex parallelism from busy time and backpressure metrics.</li>
<li>Number of TM pods ≈ ceil(parallelism / slots per TM) for the default slot sharing group.</li>
</ul>
${FA.D('k8s-upgrade')}` },
    { title: 'High availability', html: `
<p>Without HA, losing the JobManager means losing the job's coordination; it must be resubmitted (from a retained checkpoint or savepoint). With HA (<code>kubernetes</code> or <code>zookeeper</code>), a new JobManager takes over leadership, reads job metadata and the latest completed checkpoint pointer from <code>high-availability.storageDir</code>, and recovers the job automatically.</p>
${FA.D('ha-failover')}` },
    { title: 'Rescaling & safe upgrades', html: `
<ol>
<li>Confirm checkpoints are healthy and stateful operators have stable <code>uid</code>s (Beam: stable transform names).</li>
<li><code>flink stop --savepointPath …</code> (or let the operator do it).</li>
<li>Deploy the new version/parallelism with <code>-s &lt;savepoint&gt;</code>. Keyed state is redistributed by key groups (new p ≤ max parallelism).</li>
<li>Watch: INITIALIZING time, first checkpoint success, lag catch-up, no restore errors.</li>
<li>Keep the savepoint until the new version is proven.</li>
</ol>
${FA.D('keygroup-rescale', { title: 'Rescaling from a savepoint: whole key groups move' })}
<p><b>Adaptive scheduler / reactive mode</b> (<code>jobmanager.scheduler: adaptive</code>): the job adapts its parallelism to the available slots, e.g. scaling with the number of TaskManagers.</p>
${FA.C('warn', 'Compatibility', 'State schema changes need compatible serializers (POJO/Avro evolution rules). Changing key types, max parallelism or removing uids breaks restore. Beam: changing coders or step names can break restore too.')}` }
  ],
  refs: [['Deployment overview', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/overview/'], ['Flink Kubernetes Operator', 'https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/'], ['Elastic scaling', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/elastic_scaling/'], ['Savepoints', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/state/savepoints/']],
  quiz: [
    { q: 'Best deployment mode for an isolated production streaming job on Kubernetes?', options: ['Session', 'Application', 'Local', 'Per-job on K8s'], answer: 1, explain: 'Application mode: one cluster per app.' },
    { q: 'In application mode, main() runs on…', options: ['Client', 'JobManager', 'TaskManager', 'Kafka'], answer: 1, explain: 'That is the defining property of application mode.' },
    { q: 'Operator <code>upgradeMode: last-state</code> uses…', options: ['A new savepoint', 'The latest checkpoint (via HA metadata)', 'No state', 'Kafka offsets only'], answer: 1, explain: 'Fast upgrades from the last checkpoint.' },
    { q: 'Parallelism 12 and 4 slots per TaskManager in one slot sharing group. How many TMs are needed?', type: 'number', answer: 3, explain: 'ceil(12/4) = 3.' },
    { q: 'What does JobManager HA provide?', options: ['Faster checkpoints', 'Automatic JobManager failover with recovery from the latest checkpoint', 'More slots', 'Exactly-once sinks'], answer: 1, explain: 'Leader election + persisted job metadata.' },
    { q: 'Safe rescale of a stateful job from p=4 to p=8 (max p 128)?', options: ['Change -p and restart without state', 'Stop with savepoint, then run with -p 8 -s <savepoint>', 'Edit the checkpoint files', 'Impossible'], answer: 1, explain: 'Key groups are redistributed on restore.' },
    { q: 'What prevents restore after an upgrade? (select all)', type: 'multi', options: ['Changing max parallelism', 'Removing/changing operator uids without --allowNonRestoredState', 'Incompatible state serializer changes', 'Changing the job name'], answer: [0, 1, 2], explain: 'Job name does not matter for state.' },
    { q: 'Which scheduler adapts parallelism to available slots (used by reactive mode)?', options: ['default', 'adaptive', 'region', 'fifo'], answer: 1, explain: 'jobmanager.scheduler: adaptive.' },
    { q: 'Standalone cluster, job needs more slots. What must happen?', options: ['ResourceManager starts new TMs automatically', 'You start more TaskManagers (or add slots) yourself', 'Flink lowers parallelism silently', 'Nothing, it queues forever successfully'], answer: 1, explain: 'Standalone RM cannot provision TMs.' },
    { q: 'The Flink Kubernetes Operator autoscaler decides mainly from…', options: ['Kafka partition count only', 'Per-vertex busy time / backpressure / throughput metrics', 'CPU temperature', 'Number of uids'], answer: 1, explain: 'It uses job metrics to compute target parallelism per vertex.' }
  ]
});
