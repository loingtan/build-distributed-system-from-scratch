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
const signedByClient = (value, sig) => typeof value === 'string' && verify('client', 'request:' + value, sig);

node.on('request', (msg) => {
  node.reply(msg, { type: 'request_ok' });
  const { value, sig } = msg.body;
  if (node.id !== PRIMARY || proposed || !signedByClient(value, sig)) return;
  proposed = true;
  toAll({ type: 'pre_prepare', view: VIEW, seq: SEQ, value, client_sig: sig });
});

node.on('pre_prepare', (msg) => {
  // TODO: accept it only if it comes from PRIMARY, is for VIEW and SEQ, is
  // the first one you accept, and signedByClient(value, client_sig) holds.
  // Then remember the value and send { type: 'prepare', view, seq, value }
  // to every replica.
});

node.on('prepare', (msg) => {
  // TODO: count distinct senders per value. Once you accepted a value and
  // QUORUM replicas prepared that same value, send (once)
  // { type: 'commit', view, seq, value } to every replica.
  // (Prepares can arrive before the pre_prepare: check again there.)
});

node.on('commit', (msg) => {
  // TODO: count distinct senders per value. On QUORUM commits for one
  // value, decide it.
});

node.on('read', (msg) => {
  node.reply(msg, { type: 'read_ok', value: decided });
});
