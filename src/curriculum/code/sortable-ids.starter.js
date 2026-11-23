// Your unique ids from the last challenge. Unique, but they are strings and
// say nothing about when they were made.
let counter = 0;

node.on('generate', (msg) => {
  counter += 1;
  // TODO: build one integer out of the time, this node's number and a
  // per-millisecond sequence. node.now() is the current time in ms.
  node.reply(msg, { type: 'generate_ok', id: node.id + '-' + counter });
});
