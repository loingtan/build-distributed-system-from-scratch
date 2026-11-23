// Acting only while the lease lasts. n1..n3 are workers; "lock" and "printer"
// are services. The printer cannot check fencing tokens, so the only thing
// keeping two workers from printing at once is timing.
//
// Two rules make that timing safe:
// 1. Measure the lease on your own clock, starting from the moment you SENT
//    acquire. The lock granted it later than that, so the real lease ends
//    later than your estimate, never earlier, however slow the reply was.
//    Never compare expires_at (the lock's clock) with node.now() (yours).
// 2. Stop a safety margin before your estimate runs out: a page still has to
//    travel to the printer, and clocks do not all run at exactly one speed.

const MARGIN_MS = 50;

let validUntil = -Infinity; // by this node's clock: last moment to send a page

const sleep = (ms) => new Promise((resolve) => node.after(ms, resolve));
const leaseLeft = () => validUntil - node.now();

// Get (or extend) the lease. As the holder, acquire extends it. Keeps trying.
async function acquire() {
  for (;;) {
    const sentAt = node.now();
    const res = await node.rpc('lock', { type: 'acquire' }, { timeout: 1000 });
    if (res && res.granted) {
      validUntil = sentAt + res.lease_ms - MARGIN_MS;
      // The reply may have been slow, or this process paused as it arrived:
      // the lease can be nearly or entirely gone already. Then ask again.
      if (leaseLeft() > 0) return;
    } else {
      await sleep(15 + node.random() * 20);
    }
  }
}

node.on('job', async (msg) => {
  const { job, pages } = msg.body;
  for (let page = 1; page <= pages; page++) {
    // Check right before every page: a page sent after this point could
    // reach the printer after the lease has ended.
    if (leaseLeft() <= 0) await acquire();
    await node.rpc('printer', { type: 'print', job, page }, { timeout: 1000 });
  }
  validUntil = -Infinity;
  node.send('lock', { type: 'release' });
  node.reply(msg, { type: 'job_ok', done: true });
});
