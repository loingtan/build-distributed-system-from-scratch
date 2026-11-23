export default [
  {
    term: 'Work queue',
    aliases: ['job queue', 'task queue', 'message queue'],
    summary: 'A list of jobs held by a broker: producers add work, consumers take it and do it at their own pace.',
    detail: `A **work queue** decouples the code that asks for work from the code that does it. A producer, such as a web server, adds a job ("resize photo 17") and returns at once; a pool of consumers takes jobs off the queue whenever they have capacity.

The queue absorbs bursts: if a thousand uploads arrive in a minute and the workers can handle ten a second, the queue grows for a while and drains later, instead of the web servers falling over.

The process that holds the queue is the **broker**. It treats each job's payload as opaque and only stores and returns it. Examples are Amazon SQS, RabbitMQ, and job libraries such as Sidekiq or Celery.`,
    related: ['FIFO', 'Competing consumers', 'Acknowledgement'],
  },
  {
    term: 'FIFO',
    aliases: ['first in, first out', 'fifo queue'],
    summary: 'First in, first out: the job that has waited longest is handed out next.',
    detail: `A **FIFO** queue hands out jobs in the order they arrived. Enqueue A, B, C and the first three dequeues return A, B, C.

With several consumers and redelivery, FIFO describes the order of *first hand-outs*, not of completion: a job that times out and is requeued may finish after jobs enqueued behind it. Strict processing order needs one consumer per ordered stream, which is why systems like Kafka and SQS FIFO order messages only within a partition or message group.`,
    related: ['Work queue'],
  },
  {
    term: 'Competing consumers',
    summary: 'Several consumers take jobs from the same queue, and each job goes to exactly one of them.',
    detail: `With **competing consumers**, adding a worker adds capacity: the broker gives each job to whichever consumer asks next, and consumers never coordinate with each other.

The broker must make "take the first job and remove it" atomic, or two consumers asking at the same instant could receive the same job. A multithreaded broker uses a lock; a single-threaded event loop gets this for free because a handler runs to completion before the next one starts.

Because consumers compete, any one of them may disappear mid-job, which is what acknowledgements and visibility timeouts are for.`,
    related: ['Work queue', 'Acknowledgement'],
  },
  {
    term: 'At-least-once',
    aliases: ['at-least-once delivery'],
    summary: 'Every message is eventually processed, but some may be processed more than once.',
    detail: `**At-least-once delivery** means the broker keeps a job until a consumer confirms it, and redelivers it if no confirmation arrives in time. No accepted job is lost.

Duplicates are unavoidable: the broker cannot distinguish "the consumer died before the work" from "the consumer did the work and died before acknowledging". Both look like silence, and to be safe against the first the broker must redeliver in both cases.

The opposite, **at-most-once**, deletes a job when it is handed out, so a consumer crash loses it. Most systems choose at-least-once and make processing idempotent.`,
    related: ['Acknowledgement', 'Visibility timeout', 'Idempotence', 'At-most-once'],
  },
  {
    term: 'At-most-once',
    aliases: ['at-most-once delivery'],
    summary: 'Every message is processed zero or one times: nothing is repeated, but a crash can lose it.',
    detail: `With **at-most-once delivery**, the broker forgets a job the moment it hands it out (or a sender never retries). A job is never processed twice, but if the consumer crashes, the job is gone.

It suits data whose loss is cheap, such as metrics samples or live video frames, where a duplicate would be worse than a gap. For work that must be done, at-least-once plus idempotent handling is the usual choice.`,
    related: ['At-least-once'],
  },
  {
    term: 'Acknowledgement',
    aliases: ['ack', 'acknowledgment', 'consumer acknowledgement'],
    summary: 'A message from the consumer saying a job is finished, after which the broker may forget it.',
    detail: `An **acknowledgement** (ack) splits "handed out" from "finished". After a hand-out the job is *in flight*: not offered to others, but still held by the broker. Only the ack removes it.

For example: consumer c1 receives job-17 at t=10 and acks at t=90; the broker deletes job-17 at t=90. If c1 had crashed at t=40, no ack would come and the visibility timeout would return the job to the queue.

The consumer should ack only after the work's effects are safely stored. Acking first and then crashing turns at-least-once back into at-most-once.`,
    related: ['Visibility timeout', 'At-least-once', 'Late acknowledgement'],
  },
  {
    term: 'Visibility timeout',
    summary: 'How long a handed-out job stays hidden before the broker assumes the consumer died and offers it again.',
    detail: `When a job is handed out, the broker starts a clock. If no ack arrives within the **visibility timeout**, the job becomes visible again and the next dequeue can receive it.

With a 500 ms timeout, a job handed out at t=1000 and not acknowledged reappears at t=1500. It goes back to the front of the queue, because it is older than everything else waiting.

The value is a guess about how long a healthy consumer needs. Too short and slow but healthy consumers have their work redelivered; too long and a crashed consumer's job sits idle. Amazon SQS uses this exact name (30 seconds by default) and lets consumers extend it while they work.`,
    related: ['Acknowledgement', 'At-least-once', 'Late acknowledgement'],
  },
  {
    term: 'Idempotence',
    aliases: ['idempotent', 'idempotency', 'idempotent operation'],
    summary: 'An operation is idempotent if doing it twice has the same effect as doing it once.',
    detail: `An operation is **idempotent** when repeating it changes nothing further. "Set x to 5" is idempotent; "add 5 to x" is not.

Idempotence is what makes retries safe. A sender that hears no reply cannot tell whether its request was lost or only the reply was, so it resends. If the operation is idempotent, the duplicate is harmless. A broker makes \`enqueue\` idempotent by creating a job only if its id is new, and \`ack\` idempotent by marking the job done wherever it is.

At-least-once delivery combined with idempotent processing gives **effectively once**: the outcome is as if each message were processed exactly once.`,
    related: ['Deduplication', 'At-least-once'],
  },
  {
    term: 'Deduplication',
    aliases: ['dedup', 'dedupe', 'duplicate detection'],
    summary: 'Recognising a repeated request by its id and applying it only once.',
    detail: `**Deduplication** remembers which request ids have already been applied and ignores repeats. A producer that retries \`enqueue job-17\` gets \`enqueue_ok\` again, but no second job is created.

The memory must outlive the work it describes. A retry can arrive after job-17 was processed and removed, so checking only the jobs currently waiting is not enough: keep a record per id with its state (waiting, in flight, done).

In practice the record cannot be kept forever. Amazon SQS FIFO queues deduplicate within a five-minute window; a retry later than that is treated as new.`,
    related: ['Idempotence', 'At-least-once'],
  },
  {
    term: 'Late acknowledgement',
    aliases: ['late ack', 'duplicate ack'],
    summary: 'An ack that arrives after the visibility timeout, when the job may already be with another consumer.',
    detail: `A consumer that is slow rather than dead may ack after its visibility timeout has expired. By then the job is back in the queue or out with a second consumer. This is a **late acknowledgement**.

The safe response is to treat the ack as finishing the job wherever it is: the work was done. Later dequeues must skip it, the second consumer's ack becomes a harmless repeat, and any timer left from an earlier hand-out must check the job's state before requeuing it.

Ignoring late acks is also defensible (the job is simply done twice), but reviving or re-creating a job after an ack is never correct.`,
    related: ['Acknowledgement', 'Visibility timeout', 'Idempotence'],
  },
  {
    term: 'Poison message',
    aliases: ['poison pill', 'poison job'],
    summary: 'A job that fails every time it is processed, for example because its data is corrupt.',
    detail: `A **poison message** cannot succeed however often it is retried: the input is malformed, or it triggers a bug that crashes every consumer that touches it.

Under at-least-once delivery a poison message is redelivered forever, killing a consumer each time and wasting capacity healthy jobs need. Three poison jobs among forty can keep a small pool of consumers permanently busy crashing.

The defence is a retry limit followed by a dead-letter queue, which stops the loop without discarding the job.`,
    related: ['Retry limit', 'Dead-letter queue'],
  },
  {
    term: 'Dead-letter queue',
    aliases: ['DLQ', 'dead letter queue', 'dead-letter list', 'dead letters'],
    summary: 'A separate holding area for jobs that used up their attempts, kept for a person to inspect.',
    detail: `A **dead-letter queue** receives jobs the broker has given up on: typically those whose final allowed hand-out expired without an ack. Nothing consumes it automatically.

The job is stopped but not lost. An operator looks at the payload, fixes the cause (a bug, a corrupt file) and resubmits it. Amazon SQS moves a message to a configured DLQ after \`maxReceiveCount\` receives; RabbitMQ routes rejected or expired messages to a dead-letter exchange.

A DLQ needs monitoring. Each entry is accepted work that was never done; a DLQ nobody watches is data loss with extra steps.`,
    related: ['Poison message', 'Retry limit'],
  },
  {
    term: 'Retry limit',
    aliases: ['max deliveries', 'maximum delivery count', 'max receive count'],
    summary: 'The most times a job may be attempted before it is dead-lettered instead of retried again.',
    detail: `A **retry limit** caps how many times a job is handed out. With a limit of 3, a job whose third hand-out times out goes to the dead-letter queue instead of back to the queue. A job acknowledged on its first, second or third attempt is an ordinary success.

The limit is a judgement. Too low, and a couple of unlucky consumer crashes send a healthy job to the dead letters. Too high, and a poison job takes down many consumers before it is stopped.`,
    related: ['Poison message', 'Dead-letter queue'],
  },
  {
    term: 'Durability',
    aliases: ['durable'],
    summary: 'Once a system says it has stored something, a crash does not lose it.',
    detail: `A **durable** system writes state to storage that survives a crash (a disk, or other machines) before it acknowledges. The essential rule is order: write first, reply second. Then anything a client has been told is already safe.

For a broker, durability covers accepted jobs, acknowledgements, hand-out counts and the dead-letter list. Losing the ack record, for instance, would let a retried enqueue re-create a finished job.

Durability costs latency, since syncing to disk is slow. Real brokers batch writes or replicate to several machines instead of syncing every message.`,
    related: ['Crash recovery', 'Persisted deadlines'],
  },
  {
    term: 'Crash recovery',
    aliases: ['restart recovery'],
    summary: 'What a process does on restart to rebuild its working state from what it saved before crashing.',
    detail: `When a process crashes, everything in memory is lost: variables, open connections and timers. **Crash recovery** is the code that runs at start-up and rebuilds that state from durable storage.

For a broker, data on disk survives, but every timer for an in-flight job is gone. Recovery scans the job records, finds the ones that were in flight, and either returns them to the queue or starts new timers for whatever time they had left.

Recovery must be written for the state the crash can leave behind, which is any state between two handlers.`,
    related: ['Durability', 'Persisted deadlines'],
  },
  {
    term: 'Persisted deadlines',
    aliases: ['persisted deadline', 'stored deadline'],
    summary: 'Storing when a timeout expires, as data, so that the timeout survives a crash that kills the timer.',
    detail: `A running timer cannot be saved to disk, but the moment it is due can. Instead of "a timer is running for job-9", store "job-9's hand-out expires at t=1730". This is a **persisted deadline**.

After a restart at t=1600, the broker reads the deadline and starts a new timer for 130 ms. The timer becomes a reminder to go and look; the deadline is the truth. Because an old timer cannot be cancelled after a crash, the timer should check, when it fires, that the job is still on the hand-out with that deadline.

This only works if the clock is continuous across the restart; a wall clock that jumps can expire deadlines early or late.`,
    related: ['Crash recovery', 'Durability', 'Visibility timeout'],
  },
];
