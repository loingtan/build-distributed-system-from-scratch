const chain = node.nodeIds;
const head = chain[0];
const tail = chain[chain.length - 1];
const next = chain[chain.indexOf(node.id) + 1] ?? null; // null at the tail

const log = [];          // writes in the head's order
const store = new Map(); // key -> value

let confirmed = 0;   // how many log entries next has confirmed
let sending = false;

function append(entry) {
  log.push(entry);
  store.set(entry.key, entry.value);
  if (node.id === tail) {
    // Every node before the tail already has this entry, so it is safe to
    // tell the client. This is node.reply done by hand.
    node.send(entry.client, { type: 'write_ok', in_reply_to: entry.msg_id });
  } else {
    passOn();
  }
}

// One loop per node pushes the log to its successor, in order, and repeats
// anything that was not confirmed.
async function passOn() {
  if (sending) return;
  sending = true;
  while (confirmed < log.length) {
    const res = await node.rpc(next, { type: 'chain', from: confirmed, entries: log.slice(confirmed) }, { timeout: 150 });
    if (res) confirmed = res.have;
  }
  sending = false;
}

node.on('write', (msg) => {
  if (node.id !== head) {
    node.reply(msg, { type: 'error', code: 'not_head', head });
    return;
  }
  const { key, value } = msg.body;
  // The entry carries who asked, so the tail can answer.
  append({ key, value, client: msg.src, msg_id: msg.body.msg_id });
});

node.on('chain', (msg) => {
  const { from, entries } = msg.body;
  // Take only entries that extend the log exactly; a repeat adds nothing.
  if (from <= log.length) {
    for (const entry of entries.slice(log.length - from)) append(entry);
  }
  node.reply(msg, { type: 'chain_ok', have: log.length });
});

node.on('read', (msg) => {
  if (node.id !== tail) {
    node.reply(msg, { type: 'error', code: 'not_tail', tail });
    return;
  }
  const key = msg.body.key;
  node.reply(msg, { type: 'read_ok', value: store.has(key) ? store.get(key) : null });
});
