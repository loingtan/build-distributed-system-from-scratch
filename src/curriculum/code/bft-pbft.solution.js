// One PBFT agreement round (view 0, sequence number 1) among n = 4
// replicas, tolerating f = 1 Byzantine replica. The primary is n1.
const F = 1;
const QUORUM = 2 * F + 1;
const PRIMARY = node.nodeIds[0];
const VIEW = 0;
const SEQ = 1;

// ---- Given: toy signatures ---------------------------------------------
// A real system would use public-key signatures (for example Ed25519). Here
// every key sits in one table to keep the code short. The rule of the game:
// a node signs only with its own key. The Byzantine node (played by the
// tests) also knows only its own key.
const KEYS = { n1: 'key-n1-7f3a', n2: 'key-n2-19be', n3: 'key-n3-c05d', n4: 'key-n4-62f8', client: 'key-client-d4e1' };

function digest(text) {
  let h = 0x811c9dc5;
  for (let i = 0; i < text.length; i++) {
    h ^= text.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return ('0000000' + h.toString(16)).slice(-8);
}
// Did `signer` sign `text`?
function verify(signer, text, sig) {
  return KEYS[signer] !== undefined && digest(KEYS[signer] + '|' + text) === sig;
}
// ------------------------------------------------------------------------

let proposed = false;  // primary: sent its pre_prepare
let accepted = null;   // the value of the pre_prepare we accepted
let committed = false; // sent our commit
let decided = null;
const prepares = {};   // value -> { node: true }
const commits = {};    // value -> { node: true }

function toAll(body) {
  for (const id of node.nodeIds) node.send(id, body);
}
function add(table, value, from) {
  table[value] ??= {};
  table[value][from] = true;
  return Object.keys(table[value]).length;
}
const count = (table, value) => Object.keys(table[value] ?? {}).length;
const ours = (b) => b.view === VIEW && b.seq === SEQ && typeof b.value === 'string';
const signedByClient = (value, sig) => typeof value === 'string' && verify('client', 'request:' + value, sig);

// Prepared: we accepted the primary's value and 2f+1 replicas prepared that
// same value. No other value can ever be prepared by an honest replica now.
function maybeCommit() {
  if (committed || accepted === null || count(prepares, accepted) < QUORUM) return;
  committed = true;
  toAll({ type: 'commit', view: VIEW, seq: SEQ, value: accepted });
}

node.on('request', (msg) => {
  node.reply(msg, { type: 'request_ok' });
  const { value, sig } = msg.body;
  if (node.id !== PRIMARY || proposed || !signedByClient(value, sig)) return;
  proposed = true;
  toAll({ type: 'pre_prepare', view: VIEW, seq: SEQ, value, client_sig: sig });
});

// Only the primary may propose, only once, and only what the client signed.
node.on('pre_prepare', (msg) => {
  const b = msg.body;
  if (msg.src !== PRIMARY || accepted !== null || !ours(b) || !signedByClient(b.value, b.client_sig)) return;
  accepted = b.value;
  toAll({ type: 'prepare', view: VIEW, seq: SEQ, value: accepted });
  maybeCommit();
});

node.on('prepare', (msg) => {
  if (!ours(msg.body)) return;
  add(prepares, msg.body.value, msg.src);
  maybeCommit();
});

// 2f+1 matching commits include f+1 honest replicas that saw the value
// prepared, so the decision is final even if we never prepared it ourselves.
node.on('commit', (msg) => {
  if (!ours(msg.body)) return;
  if (add(commits, msg.body.value, msg.src) >= QUORUM && decided === null) decided = msg.body.value;
});

node.on('read', (msg) => {
  node.reply(msg, { type: 'read_ok', value: decided });
});
