// One integer, three fields, most significant first:
//
//   [ time in ms ][ node number: 6 bits ][ sequence: 12 bits ]
//
// Time leads, so ids sort by when they were made. The node number keeps two
// nodes apart within the same millisecond; the sequence keeps one node's ids
// apart within the same millisecond.
const NODE_BITS = 6;
const SEQ_BITS = 12;
const nodeNumber = Number(node.id.slice(1));
let lastMs = -1;
let seq = 0;

node.on('generate', (msg) => {
  let ms = Math.floor(node.now());
  // Never go backwards, even if the clock does: a reading at or before the
  // last millisecond we used continues that millisecond's sequence.
  if (ms <= lastMs) {
    ms = lastMs;
    seq += 1;
    if (seq >= 2 ** SEQ_BITS) {
      // Out of sequence numbers for this millisecond: borrow the next one.
      ms += 1;
      seq = 0;
    }
  } else {
    seq = 0;
  }
  lastMs = ms;

  // Multiplication rather than <<, which would truncate to 32 bits.
  const id = (ms * 2 ** NODE_BITS + nodeNumber) * 2 ** SEQ_BITS + seq;
  node.reply(msg, { type: 'generate_ok', id });
});
