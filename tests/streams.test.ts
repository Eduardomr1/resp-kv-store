import test from 'node:test';
import assert from 'node:assert/strict';
import net from 'node:net';
import { StorageEngine } from '../src/storage/engine.ts';
import { RedisStream, StreamError } from '../src/storage/stream.ts';
import { TCPServer } from '../src/server.ts';
import { RESPSerializer } from '../src/resp/serializer.ts';

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function sendRaw(client: net.Socket, data: string | Buffer): Promise<Buffer> {
  return new Promise((resolve) => {
    client.once('data', resolve);
    client.write(data);
  });
}

test('RedisStream: ID monotonicity and validation', () => {
  const stream = new RedisStream();

  // 1. Explicit monotonic IDs
  const id1 = stream.add('1000-0', [['sensor', Buffer.from('temp')]]);
  assert.equal(id1, '1000-0');

  const id2 = stream.add('1000-1', [['sensor', Buffer.from('humidity')]]);
  assert.equal(id2, '1000-1');

  const id3 = stream.add('1001-0', [['sensor', Buffer.from('pressure')]]);
  assert.equal(id3, '1001-0');

  assert.equal(stream.len(), 3);

  // 2. Reject 0-0
  assert.throws(
    () => stream.add('0-0', [['a', Buffer.from('b')]]),
    (err: any) => err instanceof StreamError && err.message.includes('greater than 0-0')
  );

  // 3. Reject equal or smaller ID
  assert.throws(
    () => stream.add('1000-1', [['a', Buffer.from('b')]]),
    (err: any) => err instanceof StreamError && err.message.includes('equal or smaller')
  );
  assert.throws(
    () => stream.add('999-0', [['a', Buffer.from('b')]]),
    (err: any) => err instanceof StreamError && err.message.includes('equal or smaller')
  );
});

test('RedisStream: Auto ID generation with *', () => {
  const stream = new RedisStream();

  const id1 = stream.add('*', [['metric', Buffer.from('cpu')]]);
  const id2 = stream.add('*', [['metric', Buffer.from('ram')]]);

  assert.ok(id1.includes('-'));
  assert.ok(id2.includes('-'));

  const parsed1 = RedisStream.parseId(id1);
  const parsed2 = RedisStream.parseId(id2);

  assert.ok(RedisStream.compareIds(parsed2, parsed1) > 0);
});

test('RedisStream: XRANGE and XREVRANGE range slicing', () => {
  const stream = new RedisStream();
  stream.add('100-0', [['val', Buffer.from('a')]]);
  stream.add('100-1', [['val', Buffer.from('b')]]);
  stream.add('200-0', [['val', Buffer.from('c')]]);
  stream.add('300-0', [['val', Buffer.from('d')]]);

  // XRANGE - +
  const all = stream.range('-', '+');
  assert.equal(all.length, 4);
  assert.equal(all[0].id, '100-0');
  assert.equal(all[3].id, '300-0');

  // XRANGE with explicit bounds
  const slice = stream.range('100-1', '200-0');
  assert.equal(slice.length, 2);
  assert.equal(slice[0].id, '100-1');
  assert.equal(slice[1].id, '200-0');

  // XRANGE with COUNT
  const countSlice = stream.range('-', '+', 2);
  assert.equal(countSlice.length, 2);
  assert.equal(countSlice[1].id, '100-1');

  // XREVRANGE
  const rev = stream.revrange('+', '-');
  assert.equal(rev.length, 4);
  assert.equal(rev[0].id, '300-0');
  assert.equal(rev[3].id, '100-0');
});

test('RedisStream: MAXLEN trimming', () => {
  const stream = new RedisStream();
  for (let i = 0; i < 10; i++) {
    stream.add(`100-${i}`, [['i', Buffer.from(i.toString())]], 5);
  }

  assert.equal(stream.len(), 5);
  assert.equal(stream.getEntries()[0].id, '100-5');
  assert.equal(stream.getEntries()[4].id, '100-9');
});

test('StorageEngine: Stream operations integration (xadd, xrange, xlen, xread)', () => {
  const engine = new StorageEngine();

  try {
    const id1 = engine.xadd('events', '*', [['user', Buffer.from('alice')], ['action', Buffer.from('login')]]);
    const id2 = engine.xadd('events', '*', [['user', Buffer.from('bob')], ['action', Buffer.from('logout')]]);

    assert.equal(engine.xlen('events'), 2);

    const range = engine.xrange('events', '-', '+');
    assert.equal(range.length, 2);
    assert.equal(range[0].fields[0][0], 'user');
    assert.equal(range[0].fields[0][1].toString(), 'alice');

    // XREAD after id1
    const read = engine.xread([{ key: 'events', lastId: id1 }]);
    assert.equal(read.length, 1);
    assert.equal(read[0].entries.length, 1);
    assert.equal(read[0].entries[0].id, id2);
  } finally {
    engine.close();
  }
});

test('Integration: Live TCP XADD, XRANGE, XLEN, and blocking XREAD', async () => {
  const PORT = 16385;
  const server = new TCPServer({ port: PORT, host: '127.0.0.1' });
  await server.start();

  const consumer = net.createConnection({ port: PORT, host: '127.0.0.1' });
  const producer = net.createConnection({ port: PORT, host: '127.0.0.1' });
  await Promise.all([
    new Promise((resolve) => consumer.once('connect', resolve)),
    new Promise((resolve) => producer.once('connect', resolve)),
  ]);

  try {
    // 1. Producer XADD to 'mystream'
    const xaddCmd = RESPSerializer.encodeArray([
      Buffer.from('XADD'),
      Buffer.from('mystream'),
      Buffer.from('1500-0'),
      Buffer.from('sensor'),
      Buffer.from('thermostat'),
      Buffer.from('temp'),
      Buffer.from('21.5'),
    ]);
    const addRes = await sendRaw(producer, xaddCmd);
    assert.equal(addRes.toString(), '$6\r\n1500-0\r\n');

    // 2. XLEN mystream
    const lenRes = await sendRaw(producer, RESPSerializer.encodeArray([Buffer.from('XLEN'), Buffer.from('mystream')]));
    assert.equal(lenRes.toString(), ':1\r\n');

    // 3. XRANGE mystream - +
    const rangeRes = await sendRaw(producer, RESPSerializer.encodeArray([Buffer.from('XRANGE'), Buffer.from('mystream'), Buffer.from('-'), Buffer.from('+')]));
    assert.ok(rangeRes.toString().includes('1500-0'));
    assert.ok(rangeRes.toString().includes('thermostat'));

    // 4. Consumer blocking XREAD BLOCK 2000 STREAMS mystream 1500-0
    const xreadBlockPromise = new Promise<Buffer>((resolve) => {
      consumer.once('data', resolve);
    });
    const blockCmd = RESPSerializer.encodeArray([
      Buffer.from('XREAD'),
      Buffer.from('BLOCK'),
      Buffer.from('2000'),
      Buffer.from('STREAMS'),
      Buffer.from('mystream'),
      Buffer.from('1500-0'),
    ]);
    consumer.write(blockCmd);

    await sleep(50); // Give consumer time to suspend

    // Producer pushes next event: 1500-1
    const pushCmd = RESPSerializer.encodeArray([
      Buffer.from('XADD'),
      Buffer.from('mystream'),
      Buffer.from('1500-1'),
      Buffer.from('sensor'),
      Buffer.from('thermostat'),
      Buffer.from('temp'),
      Buffer.from('22.0'),
    ]);
    await sendRaw(producer, pushCmd);

    // Consumer is unblocked immediately!
    const unblockedRes = await xreadBlockPromise;
    assert.ok(unblockedRes.toString().includes('mystream'));
    assert.ok(unblockedRes.toString().includes('1500-1'));
    assert.ok(unblockedRes.toString().includes('22.0'));

    // 5. Consumer blocking XREAD timeout on empty stream
    const timeoutPromise = new Promise<Buffer>((resolve) => {
      consumer.once('data', resolve);
    });
    const timeoutCmd = RESPSerializer.encodeArray([
      Buffer.from('XREAD'),
      Buffer.from('BLOCK'),
      Buffer.from('500'),
      Buffer.from('STREAMS'),
      Buffer.from('emptystream'),
      Buffer.from('0-0'),
    ]);
    consumer.write(timeoutCmd);

    const timeoutRes = await timeoutPromise;
    assert.equal(timeoutRes.toString(), '*-1\r\n');
  } finally {
    consumer.destroy();
    producer.destroy();
    await server.stop();
  }
});
