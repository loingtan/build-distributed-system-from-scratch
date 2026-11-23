node.on('echo', (msg) => {
  node.reply(msg, { type: 'echo_ok', echo: msg.body.echo });
});
