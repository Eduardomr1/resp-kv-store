import test from 'node:test';
import assert from 'node:assert/strict';
import net from 'node:net';
import { SlowlogManager } from '../src/telemetry/slowlog.ts';
import { TCPServer } from '../src/server.ts';
import { RESPSerializer } from '../src/resp/serializer.ts';

function sendRaw(client: net.Socket, data: string | Buffer): Promise<Buffer> {
  return new Promise((resolve) => {
    client.once('data', resolve);
    client.write(data);
  });
}

test('SlowlogManager: Ring buffer recording and threshold filtering', () => {
  const slowlog = new SlowlogManager({ logSlowerThanUs: 1000, maxLen: 5 });

  // 1. Below threshold: not recorded
  slowlog.record(500, ['GET', 'fastkey']);
  assert.equal(slowlog.len(), 0);

  // 2. Above threshold: recorded
  slowlog.record(1500, ['SET', 'slowkey', 'val']);
  assert.equal(slowlog.len(), 1);

  const entries = slowlog.get();
  assert.equal(entries.length, 1);
  assert.equal(entries[0].durationMicroseconds, 1500);
  assert.deepEqual(entries[0].args, ['SET', 'slowkey', 'val']);

  // 3. Ring buffer eviction at maxLen = 5
  for (let i = 0; i < 10; i++) {
    slowlog.record(2000 + i, ['CMD', `arg${i}`]);
  }
  assert.equal(slowlog.len(), 5);

  // Reset
  slowlog.reset();
  assert.equal(slowlog.len(), 0);
});

test('Integration: Live TCP SLOWLOG commands', async () => {
  const PORT = 16388;
  // Configure slowlog with threshold 0us so every command is recorded
  const slowlog = new SlowlogManager({ logSlowerThanUs: 0, maxLen: 10 });
  const server = new TCPServer({ port: PORT, host: '127.0.0.1', slowlog });
  await server.start();

  const client = net.createConnection({ port: PORT, host: '127.0.0.1' });
  await new Promise((resolve) => client.once('connect', resolve));

  try {
    // Execute a couple commands
    await sendRaw(client, RESPSerializer.encodeArray([Buffer.from('PING')]));
    await sendRaw(client, RESPSerializer.encodeArray([Buffer.from('SET'), Buffer.from('benchkey'), Buffer.from('hello')]));

    // SLOWLOG LEN
    const lenRes = await sendRaw(client, RESPSerializer.encodeArray([Buffer.from('SLOWLOG'), Buffer.from('LEN')]));
    const len = Number(lenRes.toString().replace(/[:\r\n]/g, ''));
    assert.ok(len >= 2);

    // SLOWLOG GET 2
    const getRes = await sendRaw(client, RESPSerializer.encodeArray([Buffer.from('SLOWLOG'), Buffer.from('GET'), Buffer.from('2')]));
    assert.ok(getRes.toString().includes('SET') || getRes.toString().includes('PING'));

    // SLOWLOG RESET
    const resetRes = await sendRaw(client, RESPSerializer.encodeArray([Buffer.from('SLOWLOG'), Buffer.from('RESET')]));
    assert.equal(resetRes.toString(), '+OK\r\n');

    const lenAfter = await sendRaw(client, RESPSerializer.encodeArray([Buffer.from('SLOWLOG'), Buffer.from('LEN')]));
    assert.equal(lenAfter.toString(), ':0\r\n');
  } finally {
    client.destroy();
    await server.stop();
  }
});
