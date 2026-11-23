// A lock with a lease. n1 is the lock server; the clients are c1, c2, c3.
//
// The server remembers who holds the lock and until when. A lease that is not
// renewed simply runs out, so a client that crashes or goes silent while
// holding the lock cannot block everyone else forever.

const LEASE_MS = 300;

let holder = null;   // client id, or null
let expires = 0;     // node.now() at which the holder's lease ends

// The lock counts as held only while the lease is still running.
function heldBy(client) {
  return holder === client && node.now() < expires;
}
function isFree() {
  return holder === null || node.now() >= expires;
}

node.on('acquire', (msg) => {
  // A free lock, or one the requester already holds, is granted for a fresh lease.
  if (isFree() || heldBy(msg.src)) {
    holder = msg.src;
    expires = node.now() + LEASE_MS;
    node.reply(msg, { type: 'acquire_ok', granted: true, lease_ms: LEASE_MS });
  } else {
    node.reply(msg, { type: 'acquire_ok', granted: false, lease_ms: LEASE_MS });
  }
});

node.on('renew', (msg) => {
  // Only the holder of a lease that has not run out may extend it. After
  // expiry someone else may have held the lock in between: acquire again.
  if (heldBy(msg.src)) {
    expires = node.now() + LEASE_MS;
    node.reply(msg, { type: 'renew_ok', ok: true });
  } else {
    node.reply(msg, { type: 'renew_ok', ok: false });
  }
});

node.on('release', (msg) => {
  // A release from anyone but the current holder is stale; ignore it.
  if (heldBy(msg.src)) holder = null;
  node.reply(msg, { type: 'release_ok' });
});
