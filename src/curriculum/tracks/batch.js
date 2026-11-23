// Track: Batch processing. See docs/authoring.md for the shape of this file.
import { mrWordCount, mrCrashes, mrStragglers, mrPipeline } from '../../sim/workloads/batch.js';

export default {
  id: 'batch',
  code: 'BT',
  category: 'processing',
  title: 'Batch processing',
  level: 'Intermediate',
  blurb: 'MapReduce from scratch: split the input, map, shuffle by key, reduce, and finish the job even when workers crash or stall.',
  subtracks: [
    {
      title: 'MapReduce',
      tasks: [
        {
          slug: 'mr-wordcount',
          title: 'Map, shuffle, reduce',
          level: 'Medium',
          minutes: 30,
          summary: 'A coordinator hands out map tasks over input splits, then reduce tasks over hash partitions, and workers do the reading and writing.',
          concepts: ['MapReduce', 'input split', 'hash partitioning', 'shuffle'],
          workload: mrWordCount,
          tests: [
            { name: 'Three workers', blurb: 'n1 coordinates n2, n3 and n4. Six input splits, three reduce partitions.', nodeCount: 4, duration: 3000, params: { splits: 6, reduces: 3 } },
            { name: 'Two workers, more partitions', blurb: 'Eight splits and four reduce partitions shared by two workers.', nodeCount: 3, duration: 3500, params: { splits: 8, reduces: 4 } },
            { name: 'Five workers', blurb: 'Ten splits, three partitions, five workers asking for work at once.', nodeCount: 6, duration: 3000, params: { splits: 10, reduces: 3 } },
          ],
          hints: [
            'The starter is a correct MapReduce that runs entirely on n1. Keep `runMap` and `runReduce`; move the calls to them into the workers, and turn n1 into a coordinator that only hands out tasks.',
            'Let workers pull: every 100ms an idle worker sends `{ type: "get_task" }` to n1 and n1 answers with a task, or with `task: null` when it has nothing to give right now.',
            'n1 keeps one record per task: `{ kind, id, state }` with state `idle`, `running` or `done`. Hand out only map tasks until every map task is `done`, then reduce tasks: a reduce reads one partition of every map task\'s output.',
            'When a worker finishes, it sends `{ type: "task_done", kind, id }`. When the last reduce task is done, reply to the saved `job` message with `{ type: "job_ok", outputs: ["out-0", ...] }`.',
          ],
        },
        {
          slug: 'mr-crashes',
          title: 'Workers that die',
          level: 'Hard',
          minutes: 35,
          summary: 'Notice a worker that went silent, give its task to someone else, and commit every file atomically so no partial output survives.',
          concepts: ['task re-execution', 'heartbeats', 'atomic commit', 'deterministic tasks'],
          workload: mrCrashes,
          tests: [
            { name: 'A worker dies while mapping', blurb: 'A worker is killed halfway through writing a map task\'s output and stays down.', nodeCount: 4, duration: 4000, params: { splits: 6, reduces: 3, crashes: [{ phase: 'map' }] } },
            { name: 'A worker dies while reducing, and returns', blurb: 'A worker is killed halfway through writing a reduce output; it restarts 250ms later with no memory of its task.', nodeCount: 4, duration: 4000, params: { splits: 6, reduces: 3, crashes: [{ phase: 'reduce', restartAfter: 250 }] } },
            { name: 'Two workers die', blurb: 'One worker dies while mapping and comes back after 300ms; another dies while reducing and stays down.', nodeCount: 5, duration: 4500, params: { splits: 8, reduces: 3, crashes: [{ phase: 'map', restartAfter: 300 }, { phase: 'reduce' }] } },
          ],
          hints: [
            'Run the starter on "A worker dies while mapping": the crashed worker\'s task stays `running` for ever and the job never finishes.',
            'Have a busy worker send `{ type: "heartbeat" }` to n1 every 100ms. n1 remembers when it last heard from each worker, and every 100ms or so puts back to `idle` any running task whose worker has been silent for 500ms.',
            'A restarted worker has forgotten its task, but it keeps talking to you. Treat `get_task` from a worker as "I am free": any task you think it is running goes back to `idle`.',
            'Commit atomically: write the whole file under a temporary name that no other attempt can use (`"tmp-" + node.id + "-" + random`), then `{ type: "rename", from: tmp, to: final }`. Do this for intermediate files as well as for outputs.',
            'Two attempts of the same task produce identical files, because map and reduce are deterministic. That is why it is safe for a re-executed task to overwrite a file an earlier attempt committed.',
          ],
        },
        {
          slug: 'mr-stragglers',
          title: 'Stragglers and backup tasks',
          level: 'Hard',
          minutes: 30,
          summary: 'One worker is alive but very slow; near the end of each phase, run backup copies of the tasks still in progress and take whichever finishes first.',
          concepts: ['stragglers', 'backup tasks', 'speculative execution', 'duplicate completions'],
          workload: mrStragglers,
          tests: [
            { name: 'One slow worker', blurb: 'Each fs request from n2 takes 300ms longer than usual. Three workers.', nodeCount: 4, duration: 3000, params: { splits: 6, reduces: 3, slow: 'n2', slowDelay: 300 } },
            { name: 'A sluggish worker', blurb: 'Each fs request from n2 takes 150ms longer; six reduce partitions, so its late copies finish while the reduce phase is still running.', nodeCount: 4, duration: 3000, params: { splits: 6, reduces: 6, slow: 'n2', slowDelay: 150, deadline: 1300 } },
            { name: 'A slow worker among five', blurb: 'n4 is slow; ten splits; five workers.', nodeCount: 6, duration: 3000, params: { splits: 10, reduces: 3, slow: 'n4', slowDelay: 300 } },
            { name: 'Slow and crashed', blurb: 'n3 is slow, and another worker dies while reducing and stays down.', nodeCount: 5, duration: 3500, params: { splits: 8, reduces: 3, slow: 'n3', slowDelay: 300, crashes: [{ phase: 'reduce' }], deadline: 2000 } },
          ],
          hints: [
            'Run the starter on "One slow worker": the slow worker sends heartbeats, so nobody re-runs its task, and the whole job waits for it.',
            'When a worker asks for work and no task of the current phase is `idle`, give it a backup copy of a task that is still running on exactly one worker. Keep a list of workers per task instead of a single owner.',
            'The first `task_done` for a task marks it done. Every later one, for a task already done or from an earlier phase, must change nothing.',
            'A late copy still commits its files after the task is done. With an atomic rename of a complete, deterministic file that is harmless: it replaces the output with identical content.',
          ],
        },
        {
          slug: 'mr-pipeline',
          title: 'A two-stage workflow',
          level: 'Hard',
          minutes: 30,
          summary: 'Chain two MapReduce jobs: the outputs of the first stage are the input splits of the second, which starts only when the first is complete.',
          concepts: ['workflow', 'materialized intermediate state', 'job DAG'],
          workload: mrPipeline,
          tests: [
            { name: 'Two stages', blurb: 'Word count into three partitions, then words grouped by count into two. Three workers.', nodeCount: 4, duration: 3500, params: { splits: 6, stages: [{ fn: 'wordcount', reduces: 3, output: 'wc' }, { fn: 'bycount', reduces: 2, output: 'bc' }] } },
            { name: 'A worker dies in stage two', blurb: 'A worker is killed while mapping a stage-one output and restarts 200ms later.', nodeCount: 4, duration: 4000, params: { splits: 6, stages: [{ fn: 'wordcount', reduces: 3, output: 'wc' }, { fn: 'bycount', reduces: 2, output: 'bc' }], crashes: [{ phase: 'stage2', restartAfter: 200 }] } },
            { name: 'A slow worker', blurb: 'Each fs request from n3 takes 300ms longer; four workers.', nodeCount: 5, duration: 4000, params: { splits: 8, stages: [{ fn: 'wordcount', reduces: 4, output: 'wc' }, { fn: 'bycount', reduces: 2, output: 'bc' }], slow: 'n3', slowDelay: 300 } },
          ],
          hints: [
            'Keep one list of tasks per stage and a stage index. A stage has a map phase and a reduce phase, exactly as before; when its reduce phase is done, build the next stage\'s tasks.',
            'The input splits of stage `s + 1` are the outputs of stage `s`: `wc-0`, `wc-1`, ... Map task `m` of a stage reads its split `m`; the number of map tasks is the number of splits.',
            'Put the stage in the intermediate file names (`mr-<stage>-<m>-<r>`) so the two stages never touch each other\'s files, and send `fn` with each task so the worker knows which map and reduce to run.',
            'Reply `job_ok` with the last stage\'s outputs, and only when its last reduce task is done. A late completion from stage one must not count in stage two.',
          ],
        },
      ],
    },
  ],
  mistakes: [
    {
      title: 'Writing output in place',
      symptom: 'A worker that crashes, or a slow duplicate that is still writing, leaves a half-written file under the final name; readers see a partial result.',
      fix: 'Write the complete file under a temporary name unique to the attempt, then rename it onto the final name in one atomic step.',
    },
    {
      title: 'Waiting for a task that will never finish',
      symptom: 'After a worker crash the job hangs: the coordinator still believes the dead worker is running its task.',
      fix: 'Track when each worker was last heard from and put the tasks of a silent worker back in the queue. Treat a request for work as proof that the worker holds no task.',
    },
    {
      title: 'Counting completions instead of tasks',
      symptom: 'With backup tasks or re-executions, a second completion for the same task makes the coordinator move on early: reducers read missing files, or job_ok goes out before every output exists.',
      fix: 'Mark tasks done by id, ignore completions for tasks already done or from an earlier phase, and move on when every task is done.',
    },
    {
      title: 'Letting the coordinator do the work',
      symptom: 'Everything works on a laptop, but the job runs at the speed of one machine and the coordinator becomes the bottleneck.',
      fix: 'Keep the coordinator to bookkeeping. Workers read the inputs and write every file; the coordinator only hands out task descriptions.',
    },
  ],
  questions: [
    {
      q: 'Why does re-running a map task on another worker give the same result as running it once?',
      a: 'Because the map function is deterministic and its input split never changes: every attempt writes byte-for-byte the same intermediate files. Combined with an atomic commit, it does not matter which attempt commits last, or whether two of them commit. A map function that reads the clock, a random source or an external service would break this.',
    },
    {
      q: 'Why can reduce tasks not start until every map task has finished?',
      a: 'Each reduce task needs every value for its keys, and any map task might hold some of them in its partition. A reduce started early would miss values and produce wrong totals. Streaming systems relax this by producing partial results continuously, at the cost of a different model of completeness.',
    },
    {
      q: 'Why does a backup task speed the job up instead of just wasting work?',
      a: 'Job time is set by the last task to finish. Near the end of a phase most workers are idle anyway, so running a second copy of each remaining task costs little, and whichever copy finishes first ends the phase. The MapReduce paper reports that disabling backup tasks made a sort job take 44% longer.',
    },
  ],
  reading: [
    {
      kind: 'Paper',
      title: 'MapReduce: Simplified Data Processing on Large Clusters (Dean and Ghemawat, 2004)',
      url: 'https://research.google/pubs/mapreduce-simplified-data-processing-on-large-clusters/',
      note: 'The original paper: the master, task states, re-execution on worker failure, atomic renames and backup tasks.',
    },
    {
      kind: 'Docs',
      title: 'MIT 6.5840 Lab 1: MapReduce',
      url: 'https://pdos.csail.mit.edu/6.824/labs/lab-mr.html',
      note: 'The classic course lab this track follows: a coordinator, pulling workers, ten-second timeouts and temp-file renames.',
    },
    {
      kind: 'Book',
      title: 'Designing Data-Intensive Applications, chapter 10 (11 in the 2nd edition): Batch Processing',
      url: 'https://dataintensive.net/',
      note: 'MapReduce in context: Unix pipes, distributed file systems, joins, workflows and materialized intermediate state.',
    },
  ],
};
