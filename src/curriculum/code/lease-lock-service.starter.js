// A lock with a lease. n1 is the lock server; the clients are c1, c2, c3.
//
// This server hands out a lock and takes it back on release. It has no
// leases: a client that goes silent while holding the lock keeps it forever.
//
// TODO: give every grant a lease of 300ms (LEASE_MS). The lock is free again
// once the lease has run out. `renew` extends the lease to 300ms from now, but
// only for the current holder of a lease that has not expired; anyone else
// gets ok: false. `release` from anyone but the current holder is ignored.

const LEASE_MS = 300;

let holder = null; // client id, or null

node.on('acquire', (msg) => {
  if (holder === null || holder === msg.src) {
    holder = msg.src;
    node.reply(msg, { type: 'acquire_ok', granted: true, lease_ms: LEASE_MS });
  } else {
    node.reply(msg, { type: 'acquire_ok', granted: false, lease_ms: LEASE_MS });
  }
});

node.on('renew', (msg) => {
  node.reply(msg, { type: 'renew_ok', ok: holder === msg.src });
});

node.on('release', (msg) => {
  if (holder === msg.src) holder = null;
  node.reply(msg, { type: 'release_ok' });
});
