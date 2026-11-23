// Track: Message queues. See docs/authoring.md for the shape of this file.
import { queue } from '../../sim/workloads/queues.js';

export default {
  id: 'queues',
  code: 'QU',
  category: 'scalability',
  title: 'Message queues',
  level: 'Intermediate',
  blurb: 'Hand work to consumers: acknowledgements, redelivery, deduplication and dead letters.',
  subtracks: [
    {
      title: 'Delivering work',
      tasks: [
        {
          slug: 'queue-fifo',
          title: 'A FIFO queue',
          level: 'Easy',
          minutes: 10,
          summary: 'Producers enqueue, consumers dequeue: each job goes out once, oldest first.',
          concepts: ['work queue', 'FIFO', 'competing consumers'],
          workload: queue,
          tests: [
            { name: 'Steady trickle', blurb: '40 jobs, two consumers. The queue is often empty.', nodeCount: 1, duration: 3000, params: { jobs: 40, consumers: 2 } },
            { name: 'Bursts', blurb: '120 jobs arrive 20 at a time; three consumers compete for them.', nodeCount: 1, duration: 4500, params: { jobs: 120, burst: 20, consumers: 3, work: [20, 60] } },
          ],
          hints: [
            'One array is enough. `push` adds at the back.',
            '`shift` removes and returns the front of an array, or `undefined` when it is empty.',
            'The reply must say `job: null` when nothing is waiting. `undefined` disappears from JSON.',
          ],
        },
        {
          slug: 'queue-ack',
          title: 'Acknowledge and redeliver',
          level: 'Medium',
          minutes: 25,
          summary: 'Keep a job until its consumer confirms it; hand it out again after a 500 ms timeout.',
          concepts: ['at-least-once', 'acknowledgement', 'visibility timeout'],
          workload: queue,
          tests: [
            { name: 'Reliable consumers', blurb: 'Every consumer acknowledges its job within 150 ms.', nodeCount: 1, duration: 4500, params: { jobs: 40, ack: true } },
            { name: 'Crashing consumers', blurb: 'A quarter of the jobs are taken by a consumer that dies before acknowledging.', nodeCount: 1, duration: 5500, params: { jobs: 60, ack: true, failRate: 0.25 } },
            { name: 'Bursts and crashes', blurb: '90 jobs in bursts of 15; some jobs lose their consumer twice.', nodeCount: 1, duration: 6500, params: { jobs: 90, burst: 15, ack: true, failRate: 0.3 } },
          ],
          hints: [
            'A job now has two places to be: waiting, or out with a consumer. Keep a second structure for the ones that are out, keyed by job id.',
            'When you hand a job out, start `node.after(500, ...)`. If it fires, the consumer is presumed dead: move the job back to the waiting list.',
            'On `ack`, cancel that timer (`node.after` returns `{ cancel() }`) and forget the job. Always reply `ack_ok`.',
            'Put a timed-out job back at the front of the queue, not the back: it is older than everything else waiting.',
          ],
        },
        {
          slug: 'queue-dedup',
          title: 'Effectively once',
          level: 'Medium',
          minutes: 25,
          summary: 'Producers retry enqueues and consumers ack late or twice. Neither may create or revive a job.',
          concepts: ['idempotence', 'deduplication', 'late acknowledgement'],
          workload: queue,
          tests: [
            { name: 'Retrying producers', blurb: 'Half of all enqueues arrive two or three times, some long after the job was finished.', nodeCount: 1, duration: 5500, params: { jobs: 60, ack: true, retries: true } },
            { name: 'A late acknowledgement', blurb: 'A consumer stalls for 680 ms, then acks a job that is already back in the queue.', nodeCount: 1, duration: 5000, params: { jobs: 20, ack: true, lateAck: true } },
            { name: 'Everything at once', blurb: 'Retries, crashes, slow consumers and double acks, in bursts.', nodeCount: 1, duration: 7500, params: { jobs: 90, burst: 10, consumers: 5, ack: true, retries: true, lateAck: true, failRate: 0.2, slowRate: 0.1 } },
          ],
          hints: [
            'A retry can arrive after the job was handed out and acknowledged. Checking the jobs currently in the queue is not enough: remember every id you ever accepted.',
            'Keep one record per id with a state: waiting, out with a consumer, or done. `enqueue` only creates the record if it is missing.',
            'An `ack` finishes the job whatever state it is in. If the job timed out and went back to the queue, it must not be handed out from there.',
            'Removing an id from the middle of an array is awkward. It is easier to leave it and have `dequeue` skip ids whose record is no longer waiting.',
            'A timer from an earlier hand-out may fire after the job is done. Let the timer check the state before it puts anything back.',
          ],
        },
      ],
    },
    {
      title: 'When things go wrong',
      tasks: [
        {
          slug: 'queue-dead-letter',
          title: 'Dead letters',
          level: 'Medium',
          minutes: 20,
          summary: 'After three failed attempts, park a job on a dead-letter list instead of retrying for ever.',
          concepts: ['poison message', 'dead-letter queue', 'retry limit'],
          workload: queue,
          tests: [
            { name: 'Poison jobs', blurb: 'Three of 40 jobs kill every consumer that touches them.', nodeCount: 1, duration: 6500, params: { jobs: 40, ack: true, poison: 3, maxDeliveries: 3 } },
            { name: 'Poison and flaky', blurb: 'Poison jobs among jobs that fail once or twice and then succeed, with retries and slow consumers.', nodeCount: 1, duration: 8500, params: { jobs: 80, burst: 8, consumers: 5, ack: true, poison: 4, maxDeliveries: 3, failRate: 0.25, retries: true, lateAck: true, slowRate: 0.05 } },
          ],
          hints: [
            'Add a counter to each job record and increase it in `dequeue`.',
            'The decision belongs in the timeout: if this was hand-out number 3, the job goes to a `dead` array; otherwise back to the queue.',
            'A job that fails twice and is acknowledged on the third attempt is a success. Only an expired third attempt is a dead letter.',
            '`dead_letters` replies `{ type: "dead_letters_ok", jobs: [...] }` with the same `{ id, payload }` objects you hand to consumers.',
          ],
        },
        {
          slug: 'queue-durable',
          title: 'A durable broker',
          level: 'Hard',
          minutes: 30,
          summary: 'Keep the queue on disk so a broker crash loses no job, no ack and no dead letter.',
          concepts: ['durability', 'crash recovery', 'persisted deadlines'],
          workload: queue,
          tests: [
            { name: 'One broker crash', blurb: 'The broker dies at 1.5 s with jobs waiting and jobs in flight, and restarts 150 ms later.', nodeCount: 1, duration: 7000, params: { jobs: 50, ack: true, maxDeliveries: 3, poison: 2, crashes: [1500] } },
            { name: 'Repeated crashes', blurb: 'Three crashes, on top of retries, flaky consumers and poison jobs.', nodeCount: 1, duration: 9000, params: { jobs: 90, burst: 6, consumers: 5, ack: true, maxDeliveries: 3, poison: 3, failRate: 0.2, retries: true, lateAck: true, crashes: [1800, 3200, 4600] } },
          ],
          hints: [
            'Start with `node.disk.jobs ??= {}` and friends, then use them instead of your variables. `??=` creates the value on first start and keeps the old one after a restart.',
            'The disk holds plain JSON. A `Map` becomes an object keyed by id; a timer handle cannot be stored at all.',
            'Jobs that were with a consumer when the broker died have lost their timers. At the top of the file, find them and deal with them.',
            'Either put those jobs straight back in the queue, or store each hand-out\'s deadline (`node.now() + 500`) on disk and start a new timer for whatever time is left.',
            'Without a stored timer handle you cannot cancel. Let the timer check, when it fires, that the job is still out on the same hand-out it was started for.',
          ],
        },
      ],
    },
  ],
  mistakes: [
    { title: 'Forgetting a job the moment it is handed out', symptom: 'Jobs vanish whenever a consumer crashes: "No job is lost" fails with the job last seen going to a consumer.', fix: 'Delete only on ack. Until then the job is in flight, with a timer that returns it to the queue.' },
    { title: 'Deduplicating only against jobs still in the queue', symptom: 'A retried enqueue that arrives after the job was processed makes the work run a second time.', fix: 'Remember every accepted id (or a bounded window of them) separately from the jobs themselves.' },
    { title: 'Letting an old timer or a late ack act on the wrong hand-out', symptom: 'An acknowledged job reappears, or a job is handed out again well before its timeout.', fix: 'Keep one record per job with its state, and have timers and acks check that state before changing anything.' },
    { title: 'Retrying a poison job for ever', symptom: 'One job is handed out every 500 ms until the test ends, and consumers keep dying on it.', fix: 'Count hand-outs per job and move it to a dead-letter list once the limit is reached.' },
  ],
  questions: [
    { q: 'Why can a queue not simply deliver every job exactly once?', a: 'When a consumer goes quiet the broker cannot tell whether it died before doing the work, or did the work and the ack was lost. Redelivering risks a duplicate; not redelivering risks a loss. Brokers choose at-least-once and leave it to consumers to make the work idempotent.' },
    { q: 'How do you choose a visibility timeout?', a: 'Longer than the slowest normal job, or healthy consumers will see their work handed to someone else. But not much longer, because it is also how long a job waits after its consumer really dies. Systems with long jobs let the consumer extend the timeout while it works.' },
    { q: 'What should happen to the jobs in a dead-letter queue?', a: 'They are for people, not for automatic retry: someone looks at why they failed, fixes the bug or the data, and re-enqueues or discards them. An alert on the list growing is usually the first sign of a bad deploy.' },
  ],
  reading: [
    { kind: 'Docs', title: 'Amazon SQS visibility timeout', url: 'https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html', note: 'The same hand-out, timeout and redelivery cycle in a production queue.' },
    { kind: 'Docs', title: 'RabbitMQ: consumer acknowledgements and publisher confirms', url: 'https://www.rabbitmq.com/docs/confirms', note: 'Acks in both directions, and what each one does and does not promise.' },
    { kind: 'Blog', title: 'You cannot have exactly-once delivery', url: 'https://bravenewgeek.com/you-cannot-have-exactly-once-delivery/', note: 'Why at-least-once plus idempotent processing is the honest version of exactly-once.' },
  ],
};
