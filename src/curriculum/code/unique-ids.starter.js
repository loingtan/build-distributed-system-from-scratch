// This works on one node. Run the tests to see what goes wrong on three.
let counter = 0;

node.on('generate', (msg) => {
  counter += 1;
  node.reply(msg, { type: 'generate_ok', id: counter });
});
