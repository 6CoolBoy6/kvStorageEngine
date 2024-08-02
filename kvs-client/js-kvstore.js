

const net = require('net');

const client = net.createConnection({ port: 9096, host: '127.0.0.1' }, () => {
  console.log('connect kvstore');

  client.write('GET NameJS');
});

client.on('data', (data) => {
  console.log(`recv：${data.toString()}`);

  client.end();
});

client.on('error', (err) => {
  console.error('connect failed：', err);
});

client.on('close', () => {
  console.log('close connection');
});



