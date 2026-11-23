export default [
  {
    term: 'Lease',
    aliases: ['leases', 'lock lease'],
    summary: 'A lock or right granted for a limited time; it ends on its own unless the holder renews it.',
    detail: `A **lease** is a promise with an expiry date: "you hold this lock for the next 300ms". The holder that wants to keep it renews before the time runs out; a holder that crashes or is cut off simply stops renewing, and the lease comes free without anyone having to detect the failure.

Example: n1 grants c1 the lock at 100ms with a 300ms lease. c1 renews at 200ms (now valid until 500ms), then crashes. Nobody tells n1, but at 500ms the lock is free and c2's next acquire is granted.

The price is a timing assumption: the holder must believe its lease has ended no later than the server does. Leases are used for distributed locks (Chubby, ZooKeeper sessions, etcd leases), leader leases in Raft-based systems, and cache invalidation.`,
    related: ['Lease renewal', 'Distributed lock', 'Fencing token'],
  },
  {
    term: 'Distributed lock',
    aliases: ['lock service'],
    summary: 'A lock handed out over the network, so that only one process across many machines does something at a time.',
    detail: `A **distributed lock** gives mutual exclusion between processes on different machines: one node (or a replicated group) grants the lock, and clients ask it before they act.

Unlike a mutex, the lock server cannot see whether its client is alive. So real lock services grant leases, and a careful design adds fencing tokens: a lease limits how long a dead holder blocks others, a token stops a holder that only seemed dead from doing damage.

Two kinds of use differ: locks for **efficiency** (avoid doing work twice; an occasional overlap is tolerable) and locks for **correctness** (an overlap corrupts data). Only the second needs fencing, and it needs it always.`,
    related: ['Lease', 'Fencing token'],
  },
  {
    term: 'Lease renewal',
    aliases: ['renewal', 'renew'],
    summary: 'The holder extends its lease before it runs out; after expiry it must acquire again instead.',
    detail: `To keep a lease, the holder **renews** it: the server extends the expiry to a full lease from now. A renewal is only valid while the lease is still running.

Once a lease has expired, renewing it must fail. Between expiry and the late renewal someone else may have held the lock and done work, so the old holder cannot continue as if nothing happened; it must acquire again and, if it gets the lock, re-read whatever state it depends on.

Holders usually renew at a fraction of the lease (every 100ms for a 300ms lease) so that one lost or slow renewal does not cost them the lock.`,
    related: ['Lease'],
  },
  {
    term: 'Fencing token',
    aliases: ['fencing tokens', 'fencing'],
    summary: 'A number that grows with every lock grant; the resource refuses requests carrying a lower number than it has seen.',
    detail: `A **fencing token** turns a timing problem into an ordering problem. The lock service attaches a number to every new grant: 33, then 34, then 35. The holder sends its token with every request to the protected resource, and the resource remembers the highest token it has seen and refuses anything lower.

Example: n3 gets token 33, reads, and is paused for 700ms. Its lease expires; n4 gets token 34, reads and writes. n3 wakes up and writes with 33. The storage has seen 34, so it refuses, and n4's work survives.

The resource must check tokens on every request that matters, reads included if a later write is built on them. Tokens must never go backwards, which means the counter has to survive crashes of the lock service.`,
    related: ['Stale holder', 'Process pause', 'Monotonic counter'],
  },
  {
    term: 'Process pause',
    aliases: ['GC pause', 'stop-the-world pause'],
    summary: 'A running program stops for a while (garbage collection, VM migration, swapping) and then continues as if no time passed.',
    detail: `A **process pause** freezes a program without killing it: a stop-the-world garbage collection, a VM being live-migrated, the OS swapping it out, or a \`SIGSTOP\`. Its memory is intact, its timers fire late, and messages wait in its buffers. When it resumes, the clock has jumped forward.

Pauses of hundreds of milliseconds or several seconds happen in practice. Code that checks "is my lease still valid?" and then acts can be paused between the check and the act, so no amount of checking by the paused process makes it safe; the resource has to refuse its late requests.

In this lab \`sim.pause(id, ms)\` models it: in the browser by holding the node's deliveries and timers, on the real runner with \`SIGSTOP\`/\`SIGCONT\`.`,
    related: ['Stale holder', 'Fencing token'],
  },
  {
    term: 'Stale holder',
    aliases: ['zombie holder', 'zombie leader'],
    summary: 'A process that still believes it holds a lock or leadership after its lease has ended and someone else took over.',
    detail: `A **stale holder** acts on a lease that is no longer valid. It got there by being paused, by its messages being delayed, or by being cut off from the lock service while it kept working.

It is dangerous because, from inside, it looks exactly like a legitimate holder. Its writes are well-formed, carry the right credentials, and arrive at a resource that cannot tell them apart from the new holder's, unless they carry a token.

The defence is to make authority checkable at the resource (a fencing token, or an epoch or term number as in Raft and ZooKeeper), not to rely on the stale process noticing its own staleness.`,
    related: ['Fencing token', 'Process pause'],
  },
  {
    term: 'Monotonic counter',
    aliases: ['monotonic counters'],
    summary: 'A number that only ever goes up, even across crashes and restarts.',
    detail: `A **monotonic counter** never returns a value it has returned before, nor a smaller one. Fencing tokens, Raft terms, Paxos ballots and ZooKeeper's zxid all rely on one.

Keeping it monotonic across a crash means making it durable: write the new value to disk before handing it out. A lock server that increments in memory and restarts at zero will reissue token 1, and every resource that has seen higher tokens will either refuse the new holders forever or, if it also forgot, accept stale ones.

A cheaper variant reserves ranges: persist "up to 1000", hand out 1 to 1000 from memory, and after a crash skip to 1001. Gaps are fine; going backwards is not.`,
    related: ['Fencing token', 'Durability'],
  },
  {
    term: 'Durability',
    summary: 'Once a system has acknowledged something, it is not lost, even if the machine crashes right after.',
    detail: `A system is **durable** if an acknowledged result survives a crash. In practice: write to stable storage (and flush it) before sending the reply that depends on it.

In the leases track, two things must be durable: the lock server's token counter, so tokens never repeat, and the storage's data and highest token, so acknowledged jobs stay and stale holders stay out after a restart.`,
    related: ['Monotonic counter'],
  },
  {
    term: 'Clock skew',
    summary: "The difference between two machines' clocks at the same instant.",
    detail: `**Clock skew** is how far apart two clocks read at the same moment. Even with NTP, machines differ by milliseconds and sometimes by much more, and a clock can jump when it is corrected.

So a timestamp from another machine cannot be compared with your own clock. A lease's \`expires_at\` from the lock server, checked against the holder's \`now()\`, is wrong by exactly the skew between them. Measure leases as **durations** on your own clock instead, starting from when you sent the request, and keep a safety margin for clocks that run at slightly different rates.`,
    related: ['Safety margin', 'Lease'],
  },
  {
    term: 'Safety margin',
    aliases: ['lease margin', 'clock drift margin'],
    summary: 'Stop acting some time before your own estimate of the lease end, to absorb message delay and clock drift.',
    detail: `A holder without fencing must stop acting before its lease ends, and its estimate of that end is uncertain. A **safety margin** subtracts a fixed amount from the estimate.

Start from a conservative estimate: \`sentAt + lease_ms\`, measured on your own clock from when you sent the acquire, because the server granted the lease after that moment. Then subtract enough for the request you are about to send to arrive (network delay) and for clocks running at slightly different speeds (drift, typically bounded in parts per million).

Example: lease 300ms, margin 50ms. Acquire sent at local time 1000 → stop sending at 1250. Even if the grant reply took 150ms to arrive, the estimate is still safe; only the usable time shrinks. A margin narrows the window for trouble but does not close it: a pause between the check and the send still breaks it, which is why fencing is preferred when the resource can support it.`,
    related: ['Clock skew', 'Lease', 'Fencing token'],
  },
];
