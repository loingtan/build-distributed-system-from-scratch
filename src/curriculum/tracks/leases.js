// Track: Leases and fencing. See docs/authoring.md for the shape of this file.
import { leaseLock, leaseJobs, leaseClocks } from '../../sim/workloads/leases.js';

export default {
  id: 'leases',
  code: 'LS',
  category: 'consensus',
  title: 'Leases and fencing',
  level: 'Advanced',
  blurb: 'Locks in a distributed system expire: leases, fencing tokens that stop a stale holder, and what a paused process does to both.',
  subtracks: [
    {
      title: 'Leases and fencing tokens',
      tasks: [
        {
          slug: 'lease-lock-service',
          title: 'A lock with a lease',
          level: 'Medium',
          minutes: 25,
          summary: 'Build a lock server whose grants expire unless renewed, so a client that vanishes cannot hold the lock forever.',
          concepts: ['lease', 'distributed lock', 'lease renewal'],
          workload: leaseLock,
          tests: [
            { name: 'Taking turns', blurb: 'Three clients acquire, renew every 100ms, and release.', nodeCount: 1, duration: 4000, params: { scenario: 'turns' } },
            { name: 'Clients that vanish', blurb: 'Half the time a holder goes silent, then comes back late to renew and release.', nodeCount: 1, duration: 5000, params: { scenario: 'abandon' } },
            { name: 'Vanishing on a slow network', blurb: 'The same, with 10 to 40ms per message.', nodeCount: 1, duration: 5500, latency: [10, 40], params: { scenario: 'abandon' } },
          ],
          hints: [
            'Run the starter on "Clients that vanish": once a holder goes silent, every other acquire is refused until the end of the run.',
            'Store when the lease ends (`node.now() + 300`) next to the holder. The lock is free if there is no holder **or** that moment has passed.',
            'A renewal is only valid from the current holder while its lease is still running. After expiry, someone else may have held the lock in between, so the client must acquire again.',
            'A late `release` from a client whose lease expired must not free the lock that now belongs to someone else.',
          ],
        },
        {
          slug: 'lease-fencing',
          title: 'Fencing tokens',
          level: 'Hard',
          minutes: 35,
          summary: 'A worker paused past its lease writes stale data; make the storage refuse it with tokens that only go up.',
          concepts: ['fencing token', 'process pause', 'stale holder'],
          workload: leaseJobs,
          tests: [
            { name: 'Nothing goes wrong', blurb: 'Three workers take turns appending jobs to one list.', nodeCount: 5, duration: 3500, params: { scenario: 'healthy' } },
            { name: 'Paused holder', blurb: 'Twice, a worker freezes for 700ms right after reading the list, while holding a 300ms lease.', nodeCount: 5, duration: 4500, params: { scenario: 'pause' } },
            { name: 'Slow write', blurb: "Twice, a worker's write spends 700ms in the network.", nodeCount: 5, duration: 4500, params: { scenario: 'slow-write' } },
            { name: 'Waking up mid-job', blurb: "Twice, a worker is paused for 450ms after its read, and the next holder's write is held until the paused one has woken and written.", nodeCount: 5, duration: 4500, params: { scenario: 'race' } },
          ],
          hints: [
            'Open the Timeline on "Paused holder": the frozen worker wakes up, writes back the list it read before the pause, and erases a job another worker finished in between.',
            'The worker cannot detect this itself: from its point of view no time passed between the read and the write. The storage has to do the refusing.',
            'n1 keeps a counter and adds one for every **new** grant; the token goes into `acquire_ok`. Workers send it with every `read` and `write`.',
            'n2 remembers the highest token it has seen and refuses anything lower, on reads too: once the new holder has read, the old one must not write.',
          ],
        },
        {
          slug: 'lease-durable-tokens',
          title: 'Tokens that survive a crash',
          level: 'Medium',
          minutes: 20,
          summary: 'The lock server and the storage crash and restart; tokens must never go backwards and acknowledged jobs must stay.',
          concepts: ['monotonic counter', 'durability', 'fencing token'],
          workload: leaseJobs,
          tests: [
            { name: 'Lock server crashes', blurb: 'n1 crashes twice for 400ms while workers keep sending jobs.', nodeCount: 5, duration: 4500, params: { crashes: [{ node: 'n1', at: 1000, down: 400 }, { node: 'n1', at: 2600, down: 400 }] } },
            { name: 'Storage crashes', blurb: 'n2 crashes twice for 300ms.', nodeCount: 5, duration: 4500, params: { crashes: [{ node: 'n2', at: 1100, down: 300 }, { node: 'n2', at: 2700, down: 300 }] } },
            { name: 'Crashes and pauses', blurb: 'Workers are paused after reading, n1 crashes once and n2 crashes once.', nodeCount: 5, duration: 5000, params: { scenario: 'pause', crashes: [{ node: 'n1', at: 1500, down: 400 }, { node: 'n2', at: 3300, down: 300 }] } },
          ],
          hints: [
            'Run the starter on "Lock server crashes": after the restart n1 hands out token 1 again, and n2 refuses every write because it has seen higher tokens.',
            'Keep the counter in `node.disk` and increment it there before replying. A token that went out in a reply must never be handed out again.',
            "n2's list and its highest token belong on `node.disk` too, updated before it answers.",
            'n1 may forget who held the lock: it can grant again at once after a restart, because the old holder carries a lower token and n2 will refuse it.',
          ],
        },
      ],
    },
    {
      title: 'Timing without fencing',
      tasks: [
        {
          slug: 'lease-clock-margin',
          title: 'Stop before the lease ends',
          level: 'Hard',
          minutes: 30,
          summary: 'The printer cannot check tokens: measure the lease on your own clock, from when you asked, and stop with a margin to spare.',
          concepts: ['clock skew', 'safety margin', 'lease'],
          workload: leaseClocks,
          tests: [
            { name: 'Steady clocks', blurb: 'Three workers print jobs of 3 to 8 pages, one holder at a time.', nodeCount: 3, duration: 7000, params: { scenario: 'steady' } },
            { name: 'Skewed clocks', blurb: "Each node's clock is off by a fixed amount of up to 400ms either way.", nodeCount: 3, duration: 7000, clockSkew: 400, params: { scenario: 'skew' } },
            { name: 'Slow grants', blurb: 'Every second grant spends an extra 150ms in the network.', nodeCount: 3, duration: 8000, params: { scenario: 'slow-grants' } },
            { name: 'Paused after a grant', blurb: 'Three times, a worker freezes for 450ms just as a grant reaches it.', nodeCount: 3, duration: 7000, params: { scenario: 'pause' } },
          ],
          hints: [
            "Run the starter on \"Skewed clocks\": `expires_at` comes from the lock's clock, and comparing it with `node.now()` is off by the difference between the two clocks.",
            'Note `node.now()` just before you send `acquire`. The lock granted the lease after that moment, so `sentAt + lease_ms` is never later than the real end.',
            'Subtract a margin of at least 50ms, and check the time left before **every** page: a long job outlives one lease, and acquiring again as the holder extends it.',
            'A grant can arrive with no time left (it was slow, or you were paused as it arrived). Then do not print: ask again.',
          ],
        },
      ],
    },
  ],
  mistakes: [
    {
      title: 'Treating a lock as if it never expires',
      symptom: 'One client crashes while holding the lock, and every other client waits forever.',
      fix: 'Grant a lease, not a lock: it ends on its own unless the holder renews it in time.',
    },
    {
      title: 'Trusting the lease alone to keep a holder out',
      symptom: 'A worker paused by GC, or whose write was slow, finishes its write after its lease ended and erases newer data.',
      fix: 'Give every grant a fencing token that only goes up, send it with each request, and have the resource refuse tokens lower than the highest it has seen.',
    },
    {
      title: 'Keeping the token counter in memory',
      symptom: 'After the lock server restarts, the storage refuses every write, or a new holder reuses a token an old one had.',
      fix: 'Write the counter to disk before the reply that hands out a new token.',
    },
    {
      title: "Comparing the lock's timestamps with your own clock",
      symptom: 'Works on one machine; with skewed clocks a holder acts after its lease ended, or never thinks it holds the lock at all.',
      fix: 'Measure the lease as a duration on your own clock, starting from when you sent the request, and stop a margin before it runs out.',
    },
  ],
  questions: [
    {
      q: 'Why can a lease holder not protect itself against a process pause by checking the clock before every write?',
      a: 'The pause can fall between the check and the write: the check passes, the process stops for a second, and the write goes out long after the lease ended. The network can delay the write in the same way. Only the resource, which sees the requests in the order they arrive, can refuse the stale one, which is what a fencing token lets it do.',
    },
    {
      q: 'Why measure the lease from when the request was sent rather than when the grant arrived?',
      a: 'The lock server granted the lease somewhere between those two moments. Counting from the send gives an estimate that ends no later than the real lease, whatever the reply latency was. Counting from the arrival ends later than the real lease by the reply delay, which a slow network or a pause can make arbitrarily long.',
    },
    {
      q: 'Fencing tokens need the resource to check them. What can you do when it cannot, as with the printer in LS-4?',
      a: 'Fall back on timing: stop a safety margin before the lease ends by your own conservative estimate, and accept that a long enough pause or clock jump can still break mutual exclusion. Or put a service in front of the resource that checks tokens, or use a resource with conditional writes (compare-and-set on a version) that achieves the same thing.',
    },
  ],
  reading: [
    { kind: 'Blog', title: 'Martin Kleppmann: How to do distributed locking', url: 'https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html', note: 'The paused-client failure and fencing tokens, with the argument about why timing assumptions are not enough.' },
    { kind: 'Paper', title: 'Gray and Cheriton: Leases (SOSP 1989)', url: 'https://web.stanford.edu/class/cs240/readings/89-leases.pdf', note: 'Where leases come from: time-bounded promises that tolerate failed holders, and the clock assumptions they need.' },
    { kind: 'Book', title: 'Designing Data-Intensive Applications, chapters 8 and 9', url: 'https://dataintensive.net/', note: 'Process pauses, unreliable clocks, the truth defined by the majority, and fencing.' },
  ],
};
