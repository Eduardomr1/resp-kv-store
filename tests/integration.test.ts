import test from 'node:test';
import assert from 'node:assert/strict';
import net from 'node:net';
import { TCPServer } from '../src/server.ts';

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function sendRaw(client: net.Socket, data: string | Buffer): Promise<Buffer> {
  return new Promise((resolve) => {
    client.once('data', (res) => {
      resolve(res);
    });
    client.write(data);
  });
}

test('Integration: TCP Socket full Redis lifecycle', async () => {
  const PORT = 16379;
  const server = new TCPServer({ port: PORT, host: '127.0.0.1' });
  await server.start();

  const client = net.createConnection({ port: PORT, host: '127.0.0.1' });
  await new Promise((resolve) => client.once('connect', resolve));

  try {
    // 1. PING -> +PONG\r\n
    let res = await sendRaw(client, '*1\r\n$4\r\nPING\r\n');
    assert.equal(res.toString(), '+PONG\r\n');

    // 2. SET key value
    res = await sendRaw(client, '*3\r\n$3\r\nSET\r\n$4\r\nuser\r\n$5\r\nalice\r\n');
    assert.equal(res.toString(), '+OK\r\n');

    // 3. GET key
    res = await sendRaw(client, '*2\r\n$3\r\nGET\r\n$4\r\nuser\r\n');
    assert.equal(res.toString(), '$5\r\nalice\r\n');

    // 4. SET with PX (TTL 80ms)
    res = await sendRaw(client, '*5\r\n$3\r\nSET\r\n$4\r\ntemp\r\n$4\r\n1234\r\n$2\r\nPX\r\n$2\r\n80\r\n');
    assert.equal(res.toString(), '+OK\r\n');

    // 5. GET before TTL
    res = await sendRaw(client, '*2\r\n$3\r\nGET\r\n$4\r\ntemp\r\n');
    assert.equal(res.toString(), '$4\r\n1234\r\n');

    // Sleep past expiration
    await sleep(100);

    // 6. GET after TTL -> $-1\r\n (Null bulk string)
    res = await sendRaw(client, '*2\r\n$3\r\nGET\r\n$4\r\ntemp\r\n');
    assert.equal(res.toString(), '$-1\r\n');

    // 7. Inline Command via Telnet emulation
    res = await sendRaw(client, 'PING "hello inline"\r\n');
    assert.equal(res.toString(), '$12\r\nhello inline\r\n');

    // 8. DBSIZE & INFO
    res = await sendRaw(client, '*1\r\n$6\r\nDBSIZE\r\n');
    assert.equal(res.toString(), ':1\r\n'); // only 'user' remains

    // 9. HSET & HGET
    res = await sendRaw(client, '*4\r\n$4\r\nHSET\r\n$4\r\nhash\r\n$3\r\ncol\r\n$3\r\nred\r\n');
    assert.equal(res.toString(), ':1\r\n');
    res = await sendRaw(client, '*3\r\n$4\r\nHGET\r\n$4\r\nhash\r\n$3\r\ncol\r\n');
    assert.equal(res.toString(), '$3\r\nred\r\n');

    // 10. TYPE & WRONGTYPE
    res = await sendRaw(client, '*2\r\n$4\r\nTYPE\r\n$4\r\nhash\r\n');
    assert.equal(res.toString(), '+hash\r\n');
    res = await sendRaw(client, '*2\r\n$3\r\nGET\r\n$4\r\nhash\r\n');
    assert.ok(res.toString().startsWith('-WRONGTYPE'));

    // 11. ZADD & ZRANGE (SkipList)
    res = await sendRaw(client, '*4\r\n$4\r\nZADD\r\n$2\r\nzs\r\n$2\r\n50\r\n$5\r\nalice\r\n');
    assert.equal(res.toString(), ':1\r\n');
    res = await sendRaw(client, '*4\r\n$4\r\nZADD\r\n$2\r\nzs\r\n$3\r\n100\r\n$3\r\nbob\r\n');
    assert.equal(res.toString(), ':1\r\n');
    res = await sendRaw(client, '*4\r\n$6\r\nZRANGE\r\n$2\r\nzs\r\n$1\r\n0\r\n$2\r\n-1\r\n');
    assert.equal(res.toString(), '*2\r\n$5\r\nalice\r\n$3\r\nbob\r\n');

    // 12. DEL
    res = await sendRaw(client, '*4\r\n$3\r\nDEL\r\n$4\r\nuser\r\n$4\r\nhash\r\n$2\r\nzs\r\n');
    assert.equal(res.toString(), ':3\r\n');

    // 13. QUIT
    res = await sendRaw(client, '*1\r\n$4\r\nQUIT\r\n');
    assert.equal(res.toString(), '+OK\r\n');
  } finally {
    client.destroy();
    await server.stop();
  }
});

test('Integration: Live TCP Transactions & PubSub', async () => {
  const PORT = 16380;
  const server = new TCPServer({ port: PORT, host: '127.0.0.1' });
  await server.start();

  const client1 = net.createConnection({ port: PORT, host: '127.0.0.1' });
  const client2 = net.createConnection({ port: PORT, host: '127.0.0.1' });
  await Promise.all([
    new Promise((resolve) => client1.once('connect', resolve)),
    new Promise((resolve) => client2.once('connect', resolve)),
  ]);

  try {
    // 1. Transactions: MULTI -> QUEUED -> EXEC
    let res = await sendRaw(client1, '*1\r\n$5\r\nMULTI\r\n');
    assert.equal(res.toString(), '+OK\r\n');

    res = await sendRaw(client1, '*3\r\n$3\r\nSET\r\n$5\r\ntxkey\r\n$5\r\nhello\r\n');
    assert.equal(res.toString(), '+QUEUED\r\n');

    res = await sendRaw(client1, '*2\r\n$3\r\nGET\r\n$5\r\ntxkey\r\n');
    assert.equal(res.toString(), '+QUEUED\r\n');

    res = await sendRaw(client1, '*1\r\n$4\r\nEXEC\r\n');
    assert.equal(res.toString(), '*2\r\n+OK\r\n$5\r\nhello\r\n');

    // 2. PubSub: client2 subscribes, client1 publishes
    const subPromise = new Promise<Buffer>((resolve) => {
      client2.once('data', resolve);
    });
    client2.write('*2\r\n$9\r\nSUBSCRIBE\r\n$7\r\nupdates\r\n');
    const subAck = await subPromise;
    assert.equal(subAck.toString(), '*3\r\n$9\r\nsubscribe\r\n$7\r\nupdates\r\n:1\r\n');

    const msgPromise = new Promise<Buffer>((resolve) => {
      client2.once('data', resolve);
    });
    // Publish from client1
    res = await sendRaw(client1, '*3\r\n$7\r\nPUBLISH\r\n$7\r\nupdates\r\n$6\r\nv1.0.0\r\n');
    assert.equal(res.toString(), ':1\r\n');

    const receivedPush = await msgPromise;
    assert.equal(receivedPush.toString(), '*3\r\n$7\r\nmessage\r\n$7\r\nupdates\r\n$6\r\nv1.0.0\r\n');
  } finally {
    client1.destroy();
    client2.destroy();
    await server.stop();
  }
});

test('Integration: Live TCP BLPOP/BRPOP Asynchronous Producer-Consumer & Timeout', async () => {
  const PORT = 16382;
  const server = new TCPServer({ port: PORT, host: '127.0.0.1' });
  await server.start();

  const consumer = net.createConnection({ port: PORT, host: '127.0.0.1' });
  const producer = net.createConnection({ port: PORT, host: '127.0.0.1' });
  await Promise.all([
    new Promise((resolve) => consumer.once('connect', resolve)),
    new Promise((resolve) => producer.once('connect', resolve)),
  ]);

  try {
    // 1. Consumer blocks on empty 'jobs' queue for up to 3 seconds
    const blpopPromise = new Promise<Buffer>((resolve) => {
      consumer.once('data', resolve);
    });
    consumer.write('*3\r\n$5\r\nBLPOP\r\n$4\r\njobs\r\n$1\r\n3\r\n');

    await sleep(50); // Give time to register in server

    // 2. Producer pushes an element 'urgent-job'
    const pushRes = await sendRaw(producer, '*3\r\n$5\r\nRPUSH\r\n$4\r\njobs\r\n$10\r\nurgent-job\r\n');
    assert.equal(pushRes.toString(), ':0\r\n'); // Delivered directly to waiting consumer, list remains 0 length

    // 3. Consumer receives unblocking response immediately
    const consumerRes = await blpopPromise;
    assert.equal(consumerRes.toString(), '*2\r\n$4\r\njobs\r\n$10\r\nurgent-job\r\n');

    // 4. Test BLPOP timeout expiration
    const timeoutPromise = new Promise<Buffer>((resolve) => {
      consumer.once('data', resolve);
    });
    // Block on 'nonexistent' with 1 second timeout
    consumer.write('*3\r\n$5\r\nBLPOP\r\n$11\r\nnonexistent\r\n$1\r\n1\r\n');

    const timeoutRes = await timeoutPromise;
    assert.equal(timeoutRes.toString(), '*-1\r\n');
  } finally {
    consumer.destroy();
    producer.destroy();
    await server.stop();
  }
});


