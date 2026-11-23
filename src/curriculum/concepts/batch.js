export default [
  {
    term: 'MapReduce',
    aliases: ['map reduce'],
    summary: 'A way to run a big batch job on many machines: map tasks turn input into key-value pairs, reduce tasks combine all values per key.',
    detail: `**MapReduce** splits a job into two kinds of task. A **map task** reads one piece of the input and emits key-value pairs, for word count \`("fox", 1)\` for every word. A **reduce task** receives every value for a range of keys and combines them, here by adding them up. A **coordinator** (the paper calls it the master) hands tasks to **workers** and tracks which are idle, running or done.

The programmer writes only the map and reduce functions; the framework handles distribution, the shuffle between the phases and failures. Because tasks are small and deterministic, a failed one can simply be run again elsewhere.

Google described it in 2004; Hadoop MapReduce is the best-known open-source version, and its ideas live on in Spark and Flink's batch mode.`,
    related: ['Shuffle', 'Input split', 'Task re-execution'],
  },
  {
    term: 'Input split',
    aliases: ['split'],
    summary: 'One piece of a batch job\'s input, small enough for a single map task to process on its own.',
    detail: `The input of a MapReduce job is divided into **splits**, typically one file block of 64 to 128 MB, and each split becomes one map task. With 10 TB of input and 128 MB splits there are about 80,000 map tasks.

Many small tasks balance load well (a fast worker simply takes more of them) and make failures cheap: a crash loses only the splits that worker was processing. In this track the splits are the files \`in-0\`, \`in-1\`, ...; in a workflow, the outputs of one stage are the splits of the next.`,
    related: ['MapReduce'],
  },
  {
    term: 'Hash partitioning',
    aliases: ['hash partition', 'partitioning by hash'],
    summary: 'Choosing where a key goes by hashing it: partition = hash(key) mod N, so equal keys always meet.',
    detail: `With **hash partitioning** a key's partition is \`hash(key) % N\`. Every occurrence of a key, wherever it came from, lands in the same partition, and a good hash spreads distinct keys evenly.

In MapReduce each map task splits its output into R partitions this way, and reduce task r reads partition r from every map task. If \`hash("fox") % 3 = 1\`, every \`("fox", 1)\` from every split goes to reduce task 1, which can therefore produce the exact total. Every node must use the same hash function: the track fixes FNV-1a.

The weakness is skew: one very common key puts all of its values in one partition.`,
    related: ['Shuffle'],
  },
  {
    term: 'Shuffle',
    summary: 'Moving map output to the reduce tasks so that all values for each key end up in one place, grouped and sorted.',
    detail: `The **shuffle** sits between the map and reduce phases. Each map task writes its pairs into one partition per reduce task; each reduce task fetches its partition from every map task, then groups the pairs by key (Hadoop sorts them).

It is an all-to-all exchange: with M map tasks and R reduce tasks there are M × R pieces, here the files \`mr-<m>-<r>\`. That is why reduce cannot start until every map task is done: any of them might hold values for its keys. The shuffle is often the most expensive part of a job, which is why combiners pre-aggregate on the map side.`,
    related: ['Hash partitioning', 'MapReduce'],
  },
  {
    term: 'Task re-execution',
    aliases: ['re-execution', 'reassignment'],
    summary: 'When a worker fails, run its task again on another worker instead of trying to recover the lost work.',
    detail: `A MapReduce coordinator does not repair failed tasks; it runs them again. When a worker goes silent, every task it was running goes back to **idle** and is handed to the next worker that asks. In the original design even completed map tasks of a dead worker are redone, because their output lived on its local disk.

Re-execution is only correct because tasks are deterministic and commit atomically: the second attempt writes exactly what the first would have. The cost is wasted work and a longer job, which is cheap when tasks are small.`,
    related: ['Heartbeats', 'Deterministic tasks', 'Atomic commit'],
  },
  {
    term: 'Heartbeats',
    aliases: ['heartbeat'],
    summary: 'Small periodic "I am alive" messages; a node not heard from for a while is presumed dead.',
    detail: `A **heartbeat** is a message sent every few hundred milliseconds just to show the sender is running. The receiver records when it last heard from each peer and suspects any peer silent for longer than a timeout, for example 500 ms when heartbeats come every 100 ms.

A timeout cannot tell a dead node from a slow or cut-off one, so whatever the receiver does next must be safe even if the node is alive. In MapReduce that is re-running its task, which atomic commits make harmless. Note that heartbeats detect only silence: a straggler that heartbeats on time is never suspected.`,
    related: ['Task re-execution', 'Stragglers'],
  },
  {
    term: 'Atomic commit',
    aliases: ['atomic rename', 'temp file and rename'],
    summary: 'Make output visible all at once: write it under a temporary name, then rename it to the final name in one step.',
    detail: `A file written in pieces passes through incomplete states, and a writer that crashes leaves one behind. The fix is to write the whole file under a temporary name that no one else uses (\`tmp-n3-7-48213\`), then **rename** it to the final name. File-system rename is atomic: readers see the old content or the new, never a mixture.

In MapReduce this makes duplicate and retried attempts harmless. Two attempts of a reduce task each write their own temp file and each rename it to \`out-1\`; both files are complete and identical, so it does not matter which rename lands last. GFS, HDFS and the MIT 6.5840 lab all use this pattern.`,
    related: ['Deterministic tasks', 'Task re-execution'],
  },
  {
    term: 'Deterministic tasks',
    aliases: ['deterministic task', 'determinism'],
    summary: 'A task that produces exactly the same output every time it runs on the same input.',
    detail: `A task is **deterministic** when its output depends only on its input: no clock, no randomness, no calls to outside services. Run map task 3 twice and both runs write byte-for-byte the same files.

This is what lets a batch system treat failures casually: re-execution and backup copies cannot change the result, so the job's output is as if every task had run exactly once. A map function that, say, stamps each record with the current time breaks it: two attempts disagree, and which one wins depends on timing.`,
    related: ['Task re-execution', 'Atomic commit'],
  },
  {
    term: 'Stragglers',
    aliases: ['straggler'],
    summary: 'Machines that are alive but far slower than the rest, so the tasks they run hold up the whole job.',
    detail: `A **straggler** is a worker that keeps working, slowly: a failing disk retrying reads, a machine shared with another heavy job, a bad network card. Heartbeats arrive on time, so failure detection never fires, but every task it runs takes several times longer.

Because a phase ends only when its last task ends, one straggler can set the time of the whole job. In this track one worker's file requests are delayed by 150 to 300 ms each, enough to double or triple the job's time without backup tasks.`,
    related: ['Backup tasks', 'Heartbeats'],
  },
  {
    term: 'Backup tasks',
    aliases: ['backup task', 'speculative execution'],
    summary: 'Near the end of a phase, run spare copies of the tasks still in progress and keep whichever finishes first.',
    detail: `When a phase is almost done, most workers are idle and a few tasks are still running, some of them on stragglers. The coordinator starts a **backup** copy of each remaining task on an idle worker; the first copy to finish completes the task. Hadoop calls this **speculative execution**.

The extra work is small (at most one copy per remaining task) and the gain can be large: the MapReduce paper reports a sort taking 44% longer with backups disabled. Correctness rests on the same footing as re-execution: deterministic tasks and atomic commits, plus a coordinator that ignores the losing copy's completion.`,
    related: ['Stragglers', 'Duplicate completions'],
  },
  {
    term: 'Duplicate completions',
    aliases: ['duplicate completion'],
    summary: 'Two or more "task done" reports for the same task, from backups or retried attempts; only the first may count.',
    detail: `With re-execution and backup tasks, a task can be reported done more than once, and reports can arrive late, after the phase has moved on. A coordinator that counts reports ("six done messages, so the map phase is over") will move on too early when one task reports twice. Reducers then read files that do not exist yet, or \`job_ok\` goes out with outputs missing.

Track completion per task: mark task \`id\` of phase \`kind\` done the first time, and ignore any later report for it or for a phase that has ended.`,
    related: ['Backup tasks', 'Task re-execution'],
  },
  {
    term: 'Workflow',
    aliases: ['job DAG', 'pipeline'],
    summary: 'A chain or graph of batch jobs in which each job\'s output files are the next job\'s input.',
    detail: `Real batch processing is rarely one job. A **workflow** chains jobs: a word count feeds a job that groups words by count, which feeds a report. In general the jobs form a directed acyclic graph (a **job DAG**), and a scheduler such as Airflow, Oozie or Luigi starts each job when all its inputs are complete.

The boundary between jobs is a set of files. A downstream job must not start on half-written input, which is one more reason to commit outputs atomically. Dataflow engines such as Spark and Flink plan the whole DAG at once and can pipeline stages instead of materializing every boundary.`,
    related: ['Materialized intermediate state', 'Atomic commit'],
  },
  {
    term: 'Materialized intermediate state',
    aliases: ['materialization'],
    summary: 'Writing each stage\'s complete output to storage before the next stage starts reading it.',
    detail: `In a MapReduce workflow every job writes its full output to the distributed file system, and the next job reads it from there. This **materialization** has costs: the next stage cannot start until the slowest task of the previous one finishes, and the intermediate data is written (and replicated) even if nobody keeps it.

It also has benefits: each stage is independent, a failed stage can restart from the files of the stage before it, and you can inspect intermediate results. Dataflow engines (Spark, Flink, Tez) avoid much of the materialization by streaming data between operators and recomputing lost partitions from their lineage instead.`,
    related: ['Workflow'],
  },
];
