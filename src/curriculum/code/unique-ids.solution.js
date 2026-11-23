// A node's id is unique in the cluster, and its own counter never repeats,
// so the pair is globally unique. No coordination, so no waiting on a partition.
let counter = 0;

node.on('generate', (msg) => {
  counter += 1;
  node.reply(msg, { type: 'generate_ok', id: node.id + '-' + counter });
});
