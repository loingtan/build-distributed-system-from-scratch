// Acting only while the lease lasts. n1..n3 are workers; "lock" and "printer"
// are services. The printer cannot check fencing tokens, so the only thing
// keeping two workers from printing at once is timing.
//
// This worker trusts the lock's expires_at and its own clock up to the last
// millisecond. That holds only if every clock agrees, every reply is fast and
// no process ever stops.
//
// TODO: measure the lease on this node's own clock, starting from the moment
// you sent acquire (not when the reply arrived), and treat it as over
// MARGIN_MS before lease_ms has passed. Check the time left before every page;
// if a grant arrives with no time left, ask again.

const MARGIN_MS = 50;

let expiresAt = -Infinity; // the lock's expires_at for our lease

const sleep = (ms) => new Promise((resolve) => node.after(ms, resolve));

async function acquire() {
  for (;;) {
    const res = await node.rpc('lock', { type: 'acquire' }, { timeout: 1000 });
    if (res && res.granted) {
      expiresAt = res.expires_at;
      return;
    }
    await sleep(15 + node.random() * 20);
  }
}

node.on('job', async (msg) => {
  const { job, pages } = msg.body;
  for (let page = 1; page <= pages; page++) {
    if (node.now() >= expiresAt) await acquire();
    await node.rpc('printer', { type: 'print', job, page }, { timeout: 1000 });
  }
  expiresAt = -Infinity;
  node.send('lock', { type: 'release' });
  node.reply(msg, { type: 'job_ok', done: true });
});
