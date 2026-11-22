FA.registerLevel({
  id: 2, track: 'Foundations', icon: '🏗️', minutes: 25,
  title: 'Flink architecture & job lifecycle',
  summary: 'Client, JobManager (Dispatcher, ResourceManager, JobMaster), TaskManagers and slots, plus how your code becomes an ExecutionGraph.',
  objectives: ['Name the JobManager\'s three components and what each does', 'Explain TaskManagers, slots and subtasks', 'Trace StreamGraph → JobGraph → ExecutionGraph', 'Know the job status lifecycle you see in the UI'],
  sections: [
    { title: 'The cluster', html: `
${FA.D('cluster-arch')}
<ul>
<li><b>Client</b>: not part of the runtime. It builds the JobGraph from your <code>main()</code> and submits it (CLI <code>flink run</code>, REST, or the K8s operator). In <i>application mode</i>, <code>main()</code> runs on the JobManager instead.</li>
<li><b>Dispatcher</b>: REST endpoint for submitting jobs, starts a JobMaster per job, serves the <b>Web UI</b>.</li>
<li><b>ResourceManager</b>: manages <b>task slots</b>. In standalone mode it can only hand out slots of TMs that already exist. On Kubernetes/YARN (native) it can request new TaskManagers.</li>
<li><b>JobMaster</b>: one per job. It schedules tasks into slots, coordinates <b>checkpoints</b> and handles failures and restarts.</li>
<li><b>TaskManagers</b> (workers): JVM processes that run the tasks, buffer and exchange data streams, and hold local state. There must always be at least one.</li>
</ul>` },
    { title: 'Tasks, subtasks and slots', html: `
<p>Each <b>task slot</b> is a fixed slice of a TaskManager. Slots divide <b>managed memory</b> between them; <b>there is no CPU isolation</b> between slots.</p>
<p>Each operator runs as <b>N parallel subtasks</b> (N = its parallelism), and <b>each subtask is one thread</b>. Operators can be <b>chained</b> into one <b>task</b>, so one thread runs several operators back-to-back (Level 3).</p>
<ul>
<li>1 slot per TM → every subtask group runs in its own JVM (strong isolation, more overhead).</li>
<li>Many slots per TM → subtasks share the JVM, TCP connections (multiplexed), heartbeats and data structures.</li>
</ul>
${FA.C('', 'Rule of thumb', 'A common starting point is one slot per CPU core. Level 4 shows what happens when you oversubscribe CPU.')}` },
    { title: 'From code to running tasks', html: `
<ol>
<li><b>StreamGraph</b>: built from your DataStream API calls; one node per operator.</li>
<li><b>JobGraph</b>: the StreamGraph after <b>operator chaining</b>; one <b>JobVertex</b> per chain. This is exactly what the Web UI draws as boxes.</li>
<li><b>ExecutionGraph</b>: built by the JobMaster; the JobGraph expanded by parallelism (one <i>ExecutionVertex</i> per subtask) plus intermediate results. Each ExecutionVertex is deployed into a slot.</li>
</ol>
${FA.D('graphs-pipeline')}
${FA.W('jobgraph', { title: 'Five operators, but how many boxes? Toggle chaining.', model: { cluster: { tms: 1, slotsPerTm: 2, coresPerTm: 4, sourceRate: 10000, chaining: true }, ops: [{ kind: 'source', p: 2, name: 'Kafka' }, { kind: 'map', p: 2, name: 'Parse' }, { kind: 'filter', p: 2, name: 'Valid' }, { kind: 'map', p: 2, name: 'Enrich' }, { kind: 'sink', p: 2, name: 'Kafka out' }] }, controls: [{ op: 'cluster', key: 'chaining', type: 'check', label: 'Operator chaining' }] })}` },
    { title: 'Job lifecycle', html: `
<p>Statuses you will see in the UI and REST API:</p>
<p><code>CREATED</code> → <code>RUNNING</code> → <code>FINISHED</code> (bounded) | <code>CANCELLING</code> → <code>CANCELED</code> | <code>FAILING</code> → <code>RESTARTING</code> → <code>RUNNING</code> … → <code>FAILED</code> (when the restart strategy gives up).</p>
${FA.D('job-lifecycle')}
<p>Subtasks individually go through <code>SCHEDULED → DEPLOYING → INITIALIZING</code> (state restore) <code>→ RUNNING</code>. If a job is stuck in CREATED/SCHEDULED, it is usually waiting for <b>slots</b>. After <code>slot.request.timeout</code> (5 min default) you get <code>NoResourceAvailableException</code>.</p>` },
    { title: 'Deployment modes (preview)', html: `
<ul>
<li><b>Session mode</b>: one long-running cluster; many jobs share its TaskManagers. Fast submission, but jobs can affect each other and a TM crash hits all jobs on it.</li>
<li><b>Application mode</b>: one cluster per application; <code>main()</code> runs on the JobManager. Best isolation. This is the standard for Kubernetes and the Flink Kubernetes Operator.</li>
<li><b>Per-job mode</b> (YARN): deprecated in favour of application mode.</li>
</ul>` }
  ],
  refs: [['Flink architecture', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/concepts/flink-architecture/'], ['Deployment overview', 'https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/overview/']],
  quiz: [
    { q: 'Which JobManager component manages <b>task slots</b>?', options: ['Dispatcher', 'ResourceManager', 'JobMaster', 'Client'], answer: 1, explain: 'The ResourceManager is responsible for slot allocation/provisioning.' },
    { q: 'Which component coordinates <b>checkpoints</b> for a job?', options: ['TaskManager', 'Dispatcher', 'JobMaster', 'ResourceManager'], answer: 2, explain: 'Each job has a JobMaster that schedules and coordinates checkpoints and recovery.' },
    { q: 'What do task slots isolate?', options: ['CPU cores', 'Managed memory', 'Network bandwidth', 'Disk I/O'], answer: 1, explain: 'Slots split managed memory. There is no CPU isolation.' },
    { q: 'One subtask runs in…', options: ['One thread', 'One JVM', 'One container', 'One slot per operator'], answer: 0, explain: 'A subtask (one parallel instance of a task) is executed by one thread.' },
    { q: 'Which graph does the Web UI draw as boxes connected by arrows?', options: ['StreamGraph', 'JobGraph (after chaining)', 'Physical plan of the database', 'Kafka topology'], answer: 1, explain: 'Each box is a JobVertex = an operator chain.' },
    { q: 'In <b>application mode</b>, where does the program\'s <code>main()</code> run?', options: ['On the client machine', 'On the JobManager', 'On every TaskManager', 'In the Web UI'], answer: 1, explain: 'Application mode runs main() on the cluster (JobManager), giving one cluster per application.' },
    { q: 'A job has been in CREATED/SCHEDULED for minutes and then fails with <code>NoResourceAvailableException</code>. Most likely cause?', options: ['Wrong Kafka topic', 'Not enough free task slots for the requested parallelism', 'Watermarks too late', 'Checkpoint timeout'], answer: 1, explain: 'The scheduler could not get enough slots before slot.request.timeout.' },
    { q: 'In standalone mode, can the ResourceManager start new TaskManagers by itself?', options: ['Yes, always', 'No, it can only distribute slots of TaskManagers that are already running', 'Only on Windows', 'Only for batch jobs'], answer: 1, explain: 'Standalone RM cannot provision TMs. Native K8s/YARN integrations can.' },
    { q: 'What happens to subtasks in the <code>INITIALIZING</code> state?', options: ['They wait for slots', 'They restore state (e.g. from a checkpoint/savepoint) and open operators before RUNNING', 'They are being cancelled', 'They are writing to the sink'], answer: 1, explain: 'INITIALIZING includes state restore. Long INITIALIZING often means large state.' },
    { q: 'Many slots in one TaskManager means subtasks… (select all)', type: 'multi', options: ['share the same JVM', 'share TCP connections (multiplexed) and heartbeats', 'get separate CPU cores guaranteed', 'compete for CPU'], answer: [0, 1, 3], explain: 'Same JVM, shared connections/heartbeats, and since slots don\'t isolate CPU, they compete for cores.' }
  ]
});
