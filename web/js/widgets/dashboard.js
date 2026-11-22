/* Clickable tour of the Flink Web UI (mock). Every dashed red box explains itself. */
(function (FA) {
  'use strict';
  const h = FA.h;
  const X = {
    slots: '<b>Available / Total Task Slots</b>: total = Σ taskmanager.numberOfTaskSlots over all TaskManagers. A job needs (max parallelism per slot sharing group, summed). If available < needed, the job sits in CREATED/SCHEDULED and finally fails with NoResourceAvailableException.',
    tms: '<b>Task Managers</b>: the worker JVMs. Each registers its slots with the ResourceManager. In Kubernetes these are TM pods; if the count drops, check pod restarts/OOMKilled.',
    jobs: '<b>Running Job List</b>: click a job to open its graph. Status values: CREATED → RUNNING → (FAILING → RESTARTING → RUNNING) / CANCELED / FINISHED / FAILED.',
    version: '<b>Flink version / commit</b>: when running Beam, the cluster minor version must match the Beam FlinkRunner artifact (e.g. beam-runners-flink-1.19 ↔ Flink 1.19.x).',
    jobhdr: '<b>Job header</b>: job name, JobID (needed for REST/CLI e.g. <code>flink savepoint &lt;jobId&gt;</code>), status, start time and duration. <b>Cancel Job</b> stops without savepoint, so prefer <code>flink stop --savepointPath</code> for upgrades.',
    graph: '<b>Job graph</b>: each box is a TASK (JobVertex) = one or more operators CHAINED together, executed by "Parallelism" subtasks (threads). Arrows are data exchanges labeled with the ship strategy: FORWARD, HASH, REBALANCE, RESCALE, BROADCAST, GLOBAL. Colour: blue = idle, red = busy, black = backpressured.',
    edge: '<b>Edge label</b>: FORWARD (1:1, same parallelism, only this allows chaining), HASH (keyBy / Beam GroupByKey), REBALANCE (round-robin, default when parallelism changes), RESCALE (local round-robin), BROADCAST (to all), GLOBAL (all to subtask 0).',
    bpline: '<b>Backpressured (max)</b>: highest % of time any subtask of this task was blocked because it could not get an output buffer, i.e. downstream is too slow. <b>Busy (max)</b>: highest % of time any subtask was doing work. The bottleneck is usually the first RED task after a chain of BLACK tasks.',
    vtable: '<b>Vertex table</b>: Bytes/Records Received & Sent are counted on the NETWORK edges only, so a source shows 0 received and a sink shows 0 sent (chained operators inside a task are not counted). "Tasks" shows the status counts of the subtasks.',
    exceptions: '<b>Exceptions tab</b>: root cause + history of failures with the TaskManager/subtask where it happened. First stop when a job is RESTARTING. Look for the deepest "Caused by:".',
    timeline: '<b>TimeLine</b>: per subtask how long it spent in SCHEDULED/DEPLOYING/INITIALIZING/RUNNING. Long INITIALIZING usually means big state being restored.',
    ckOverview: '<b>Checkpoint counts</b>: Triggered / In progress / Completed / Failed / Restored. Failed counts increasing = investigate (timeouts are usually caused by backpressure or slow state uploads).',
    ckHistory: '<b>Checkpoint history</b>: End-to-End Duration (trigger → last ack), Checkpointed Data Size (incremental size for RocksDB incremental), Full Checkpoint Data Size, Processed / Persisted In-Flight Data (non-zero with unaligned checkpoints). Expand a row to see per-task sync/async duration and alignment time.',
    ckSummary: '<b>Summary</b>: min/avg/max of duration and size. Large alignment durations mean barriers are stuck behind backpressured buffers, so think about unaligned checkpoints or fixing the bottleneck.',
    ckConfig: '<b>Checkpoint configuration</b>: mode (EXACTLY_ONCE/AT_LEAST_ONCE), interval, timeout (default 10 min), min pause, max concurrent, unaligned on/off, externalized/retained checkpoint cleanup policy.',
    config: '<b>Configuration tab</b>: the effective job execution config (parallelism, max parallelism, object reuse, restart strategy…). With Beam you will also see the pipeline options here in the user config.',
    subtasks: '<b>SubTasks</b>: one row per parallel instance: records/bytes in & out, duration, attempt, host/TaskManager. Compare rows: one subtask with far more records = KEY SKEW.',
    bptab: '<b>BackPressure tab</b>: per-subtask ratio of backpressured / idle / busy time with status OK (≤10%), LOW (10–50%), HIGH (>50%).',
    wm: '<b>Watermarks tab</b>: current input watermark per subtask. "No Watermark" or a stuck value usually means an idle source partition. Fix with WatermarkStrategy.withIdleness() (or Beam\'s watermark settings in the IO).',
    metrics: '<b>Metrics tab</b>: add charts such as numRecordsInPerSecond, busyTimeMsPerSecond, backPressuredTimeMsPerSecond, currentInputWatermark, and Kafka source records-lag-max / pendingRecords.',
    flame: '<b>FlameGraph</b>: on-CPU / off-CPU / mixed stack samples for a task (needs rest.flamegraph.enabled: true). Use it on the RED task to see WHICH method burns CPU.',
    tmlist: '<b>Task Managers list</b>: per TM: data port, last heartbeat, all slots / free slots, CPU cores, physical memory, JVM heap, Flink managed memory. Click one to see the memory model and the logs, stdout and thread dump.',
    tmmem: '<b>TM memory panel</b>: shows the unified memory model: Framework/Task Heap, Managed (RocksDB), Network, Metaspace, Overhead with used vs max. Heap near max with long GC = state on heap too big or a leak. Container OOMKilled with heap fine = native memory (RocksDB) beyond managed budget.',
    jm: '<b>Job Manager</b>: effective cluster configuration (config.yaml + dynamic properties), logs, stdout. Great for checking what value is REALLY applied (e.g. taskmanager.numberOfTaskSlots, state.backend.type).'
  };

  FA.registerWidget('dashboard', function (el) {
    let screen = 'overview', jobTab = 'overview', vtab = 'subtasks', ckTab = 'history';
    el.appendChild(h('div', { class: 'wtitle' }, 'Flink Web UI tour: click every dashed red box'));
    const root = h('div', { class: 'fd' });
    const explain = h('div', { class: 'fd-explain', html: '👆 Click any element with a red dashed border to learn what it means and when to look at it.' });
    el.append(root, explain);
    root.addEventListener('click', e => {
      const t = e.target.closest('[data-x]');
      if (t) { explain.innerHTML = X[t.dataset.x] || ''; }
      const nav = e.target.closest('[data-nav]');
      if (nav) { const [k, v] = nav.dataset.nav.split(':'); if (k === 's') screen = v; if (k === 'j') { screen = 'job'; jobTab = v; } if (k === 'v') vtab = v; if (k === 'c') ckTab = v; draw(); }
    });
    const hot = (x, html, tag) => `<${tag || 'span'} class="hot" data-x="${x}">${html}</${tag || 'span'}>`;

    function graphModel() {
      return FA.Sim.compile({ cluster: { tms: 2, slotsPerTm: 4, coresPerTm: 8, sourceRate: 30000, chaining: true, checkpointing: true },
        ops: [{ kind: 'source', p: 4, name: 'KafkaIO.Read', cost: 2, sel: 1 }, { kind: 'map', p: 4, name: 'ParDo(Parse)', cost: 3, sel: 1 }, { kind: 'keyby', name: 'GroupByKey', skew: 0 },
          { kind: 'reduce', p: 4, name: 'Combine.perKey', cost: 4, sel: 1 }, { kind: 'sink', p: 4, name: 'JdbcIO.Write', cost: 160, sel: 0, part: 'rebalance' }] });
    }
    function draw() {
      const side = [['overview', 'Overview'], ['job', 'Running Jobs'], ['completed', 'Completed Jobs'], ['tms', 'Task Managers'], ['jm', 'Job Manager'], ['submit', 'Submit New Job']];
      root.innerHTML = `<div class="fd-top"><b>Apache Flink Dashboard</b><span style="opacity:.7">${hot('version', 'Version: 1.19.1 · Commit: abc1234')}</span></div>
        <div class="fd-body"><div class="fd-side">${side.map(([k, t]) => `<div class="${screen === k ? 'on' : ''}" data-nav="s:${k}">${t}</div>`).join('')}</div><div class="fd-main" id="fdm"></div></div>`;
      const m = root.querySelector('#fdm');
      if (screen === 'overview') {
        m.innerHTML = `<div style="display:flex;gap:12px;flex-wrap:wrap">
          <div style="border:1px solid #eee;padding:10px;border-radius:6px">${hot('slots', 'Available Task Slots: <b>0</b> &nbsp; Total Task Slots: <b>8</b>')}</div>
          <div style="border:1px solid #eee;padding:10px;border-radius:6px">${hot('tms', 'Task Managers: <b>2</b>')}</div>
          <div style="border:1px solid #eee;padding:10px;border-radius:6px">Running Jobs: <b>1</b> · Finished 3 · Cancelled 1 · Failed 0</div></div>
          <h4>Running Job List</h4><table>${hot('jobs', '<tr><th>Job Name</th><th>Start Time</th><th>Duration</th><th>Tasks</th><th>Status</th></tr><tr data-nav="j:overview" style="cursor:pointer"><td style="color:#1890ff">orders-aggregation (Beam)</td><td>2026-11-19 09:12</td><td>8h 23m</td><td>12 running</td><td><span style="color:#52c41a">RUNNING</span></td></tr>', 'tbody')}</table>`;
      } else if (screen === 'job') {
        const tabs = [['overview', 'Overview'], ['exceptions', 'Exceptions'], ['timeline', 'TimeLine'], ['checkpoints', 'Checkpoints'], ['config', 'Configuration']];
        m.innerHTML = `${hot('jobhdr', '<b style="font-size:15px">orders-aggregation (Beam)</b> &nbsp;<span style="background:#52c41a;color:#fff;padding:1px 6px;border-radius:3px">RUNNING</span> &nbsp;<small>JobID 7f3c…e21 · Start 09:12 · Duration 8h 23m</small> &nbsp;<button>Cancel Job</button>', 'div')}
          <div class="fd-tabs">${tabs.map(([k, t]) => `<span class="${jobTab === k ? 'on' : ''}" data-nav="j:${k}">${t}</span>`).join('')}</div><div id="jt"></div>`;
        const jt = m.querySelector('#jt');
        if (jobTab === 'overview') {
          const res = graphModel();
          const gw = h('div', { class: 'hot', 'data-x': 'graph', style: { overflow: 'auto', background: '#fafafa', borderRadius: '6px' } });
          jt.appendChild(gw);
          FA.SimView.renderGraph(gw, res, { animate: true });
          jt.insertAdjacentHTML('beforeend', `<div style="margin:6px 0;font-size:12px">${hot('edge', 'What do FORWARD / HASH / REBALANCE on the arrows mean?')} &nbsp; ${hot('bpline', 'What do “Backpressured (max)” and “Busy (max)” mean?')}</div>`);
          const rows = res.vertices.map(v => `<tr><td>${FA.esc(v.name)}</td><td style="color:#52c41a">RUNNING</td><td>${v.id === 0 ? '0 B' : FA.fmt(v.recIn * 120) + ' B/s'}</td><td>${v.id === 0 ? '0' : FA.fmt(v.recIn) + '/s'}</td><td>${v.id === res.vertices.length - 1 ? '0 B' : FA.fmt(v.recOut * 120) + ' B/s'}</td><td>${v.id === res.vertices.length - 1 ? '0' : FA.fmt(v.recOut) + '/s'}</td><td>${v.p}</td><td>${v.p} RUNNING</td></tr>`).join('');
          jt.insertAdjacentHTML('beforeend', `<table class="hot" data-x="vtable"><tr><th>Name</th><th>Status</th><th>Bytes Received</th><th>Records Received</th><th>Bytes Sent</th><th>Records Sent</th><th>Parallelism</th><th>Tasks</th></tr>${rows}</table>`);
          const vt = [['detail', 'Detail'], ['subtasks', 'SubTasks'], ['tmgr', 'TaskManagers'], ['wm', 'Watermarks'], ['acc', 'Accumulators'], ['bp', 'BackPressure'], ['metrics', 'Metrics'], ['flame', 'FlameGraph']];
          const sinkV = res.vertices[res.vertices.length - 1];
          let vb = '';
          if (vtab === 'subtasks') vb = `<table class="hot" data-x="subtasks"><tr><th>ID</th><th>Records Received</th><th>Records Sent</th><th>Attempt</th><th>Host</th><th>Status</th></tr>${sinkV.sub.map(s => `<tr><td>${s.s}</td><td>${FA.fmt(s.recIn)}/s</td><td>0</td><td>1</td><td>tm-${s.s % 2 + 1}:6121</td><td>RUNNING</td></tr>`).join('')}</table>`;
          else if (vtab === 'bp') vb = `<div class="hot" data-x="bptab">${res.vertices.map(v => `<div><b>${FA.esc(FA.SimView.shortName(v.name))}</b>: Backpressured ${Math.round(v.bpMax * 100)}% · Idle ${Math.round(v.idleMax * 100)}% · Busy ${Math.round(v.busyMax * 100)}% → <b style="color:${v.bpStatus === 'HIGH' ? '#ff4d4f' : v.bpStatus === 'LOW' ? '#fa8c16' : '#52c41a'}">${v.bpStatus}</b></div>`).join('')}</div>`;
          else if (vtab === 'wm') vb = `<table class="hot" data-x="wm"><tr><th>Subtask</th><th>Watermark</th></tr><tr><td>0</td><td>2026-11-19 17:34:58.000</td></tr><tr><td>1</td><td>2026-11-19 17:34:57.500</td></tr><tr><td>2</td><td style="color:#ff4d4f">No Watermark (idle partition?)</td></tr><tr><td>3</td><td>2026-11-19 17:34:58.100</td></tr></table>`;
          else if (vtab === 'metrics') vb = `<div class="hot" data-x="metrics">Add metric ▾ &nbsp; <code>0.numRecordsInPerSecond</code> · <code>busyTimeMsPerSecond</code> · <code>backPressuredTimeMsPerSecond</code> · <code>KafkaSourceReader.pendingRecords</code></div>`;
          else if (vtab === 'flame') vb = `<div class="hot" data-x="flame" style="font-family:monospace;font-size:11px"><div style="background:#ff7a45;width:100%">Task.run</div><div style="background:#ffa940;width:92%">StreamTask.processInput</div><div style="background:#ffc53d;width:85%">JdbcIO$WriteFn.processElement</div><div style="background:#ff4d4f;width:70%">PreparedStatement.executeBatch ← most CPU/off-CPU time here</div></div>`;
          else vb = `<div class="muted">(${vt.find(x => x[0] === vtab)[1]}: per-task details. Try SubTasks, BackPressure, Watermarks, Metrics and FlameGraph.)</div>`;
          jt.insertAdjacentHTML('beforeend', `<div style="margin-top:10px;border:1px solid #f0f0f0;border-radius:6px;padding:8px"><div style="font-size:12px;color:#8c8c8c">Selected task: <b>${FA.esc(sinkV.name)}</b></div><div class="fd-tabs">${vt.map(([k, t]) => `<span class="${vtab === k ? 'on' : ''}" data-nav="v:${k}">${t}</span>`).join('')}</div>${vb}</div>`);
        } else if (jobTab === 'exceptions') {
          jt.innerHTML = `<div class="hot" data-x="exceptions"><b>Root Exception</b><pre style="background:#fff1f0;color:#a8071a;white-space:pre-wrap;font-size:11px">org.apache.flink.runtime.JobException: Recovery is suppressed by FixedDelayRestartBackoffTimeStrategy(maxNumberRestartAttempts=3, backoffTimeMS=10000)
Caused by: java.sql.SQLTransientConnectionException: HikariPool-1 - Connection is not available, request timed out after 30000ms
    at JdbcIO$WriteFn.executeBatch(JdbcIO.java:1712)</pre><b>Exception History</b><table><tr><th>Time</th><th>Exception</th><th>Task</th><th>Location</th></tr><tr><td>17:01:22</td><td>SQLTransientConnectionException</td><td>JdbcIO.Write (2/4)</td><td>tm-2:6121</td></tr></table></div>`;
        } else if (jobTab === 'timeline') {
          jt.innerHTML = `<div class="hot" data-x="timeline">${['Source → ParDo', 'Combine.perKey', 'JdbcIO.Write'].map((n, i) => `<div style="margin:4px 0;font-size:12px">${n}<div style="display:flex;height:14px"><div style="flex:1;background:#d9d9d9"></div><div style="flex:${2 + i * 3};background:#69c0ff" title="INITIALIZING (state restore)"></div><div style="flex:20;background:#95de64"></div></div></div>`).join('')}<small>grey = SCHEDULED/DEPLOYING · blue = INITIALIZING · green = RUNNING</small></div>`;
        } else if (jobTab === 'checkpoints') {
          const ct = [['overview', 'Overview'], ['history', 'History'], ['summary', 'Summary'], ['cfg', 'Configuration']];
          let body = '';
          if (ckTab === 'overview') body = hot('ckOverview', 'Checkpoint Counts: Triggered <b>502</b> · In Progress <b>1</b> · Completed <b>497</b> · Failed <b style="color:#ff4d4f">4</b> · Restored <b>1</b><br>Latest Completed: ID 501, 18 s, 1.2 GB · Latest Restore: ID 312 (savepoint)', 'div');
          else if (ckTab === 'history') body = `<table class="hot" data-x="ckHistory"><tr><th>ID</th><th>Status</th><th>Acknowledged</th><th>End to End Duration</th><th>Checkpointed Data Size</th><th>Full Checkpoint Data Size</th><th>Processed (persisted) in-flight data</th></tr><tr><td>502</td><td>IN_PROGRESS</td><td>9/16</td><td>-</td><td>-</td><td>-</td><td>-</td></tr><tr><td>501</td><td style="color:#52c41a">COMPLETED</td><td>16/16</td><td>18s</td><td>64 MB</td><td>1.2 GB</td><td>0 B (0 B)</td></tr><tr><td>500</td><td style="color:#ff4d4f">FAILED</td><td>11/16</td><td>10m 0s</td><td>-</td><td>-</td><td>Checkpoint expired before completing</td></tr></table>`;
          else if (ckTab === 'summary') body = `<table class="hot" data-x="ckSummary"><tr><th></th><th>End to End Duration</th><th>Checkpointed Data Size</th><th>Alignment Duration</th></tr><tr><td>Minimum</td><td>2s</td><td>40 MB</td><td>0 ms</td></tr><tr><td>Average</td><td>21s</td><td>70 MB</td><td>12s</td></tr><tr><td>Maximum</td><td>10m</td><td>150 MB</td><td>9m 40s</td></tr></table>`;
          else body = hot('ckConfig', 'Checkpointing Mode: Exactly Once · Interval: 1m · Timeout: 10m · Minimum Pause: 0 ms · Maximum Concurrent: 1 · Unaligned Checkpoints: Disabled · Persist Checkpoints Externally: Enabled (retain on cancellation)', 'div');
          jt.innerHTML = `<div class="fd-tabs">${ct.map(([k, t]) => `<span class="${ckTab === k ? 'on' : ''}" data-nav="c:${k}">${t}</span>`).join('')}</div>${body}`;
        } else if (jobTab === 'config') {
          jt.innerHTML = `<table class="hot" data-x="config"><tr><th>Key</th><th>Value</th></tr><tr><td>Execution Mode</td><td>PIPELINED</td></tr><tr><td>Max. Number of Execution Retries</td><td>Restart with fixed delay (10 s), 3 attempts</td></tr><tr><td>Job Parallelism</td><td>4</td></tr><tr><td>Object Reuse Mode</td><td>false</td></tr><tr><td>User config: beam pipeline options</td><td>{"runner":"FlinkRunner","parallelism":4,"checkpointingInterval":60000,...}</td></tr></table>`;
        }
      } else if (screen === 'tms') {
        m.innerHTML = `<table class="hot" data-x="tmlist"><tr><th>Path, ID</th><th>Data Port</th><th>Last Heartbeat</th><th>All Slots</th><th>Free Slots</th><th>CPU Cores</th><th>Physical MEM</th><th>JVM Heap Size</th><th>Flink Managed MEM</th></tr><tr><td>tm-1:6122</td><td>6121</td><td>just now</td><td>4</td><td>0</td><td>4</td><td>16 GB</td><td>1.6 GB</td><td>1.4 GB</td></tr><tr><td>tm-2:6122</td><td>6121</td><td>just now</td><td>4</td><td>0</td><td>4</td><td>16 GB</td><td>1.6 GB</td><td>1.4 GB</td></tr></table>
          <div class="hot" data-x="tmmem" style="margin-top:10px;padding:8px;border:1px solid #f0f0f0;border-radius:6px"><b>tm-1 → Metrics: Memory</b><div style="display:flex;height:18px;margin-top:6px;font-size:10px;color:#fff"><div style="flex:128;background:#597ef7">FW heap</div><div style="flex:1459;background:#2f54eb">Task heap</div><div style="flex:1372;background:#13c2c2">Managed</div><div style="flex:343;background:#fa8c16">Network</div><div style="flex:128;background:#9254de">FW off</div><div style="flex:256;background:#8c8c8c">Meta</div><div style="flex:410;background:#595959">Overhead</div></div><small>Heap used 1.1 GB / 1.6 GB · Managed used 1.3 GB / 1.4 GB · Network segments 10,980 total / 9,500 available</small></div>`;
      } else if (screen === 'jm') {
        m.innerHTML = `<table class="hot" data-x="jm"><tr><th>Key</th><th>Value</th></tr><tr><td>jobmanager.rpc.address</td><td>flink-jobmanager</td></tr><tr><td>taskmanager.numberOfTaskSlots</td><td>4</td></tr><tr><td>parallelism.default</td><td>4</td></tr><tr><td>state.backend.type</td><td>rocksdb</td></tr><tr><td>execution.checkpointing.interval</td><td>60s</td></tr><tr><td>restart-strategy.type</td><td>fixed-delay</td></tr></table>`;
      } else {
        m.innerHTML = `<div class="muted" style="color:#8c8c8c">${screen === 'completed' ? 'Completed jobs: FINISHED / CANCELED / FAILED jobs (kept per jobstore settings). The history server keeps them after cluster shutdown.' : 'Submit New Job: upload a JAR, set entry class, parallelism, program args and savepoint path. Can be disabled with web.submit.enable: false (common in production).'}</div>`;
      }
    }
    draw();
  });
})(window.FA);
