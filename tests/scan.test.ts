import test from 'node:test';
import assert from 'node:assert/strict';
import net from 'node:net';
import { StorageEngine } from '../src/storage/engine.ts';
import { TCPServer } from '../src/server.ts';
import { RESPSerializer } from '../src/resp/serializer.ts';

function sendRaw(client: net.Socket, data: string | Buffer): Promise<Buffer> {
  return new Promise((resolve) => {
    client.once('data', resolve);
    client.write(data);
  });
}

test('StorageEngine: SCAN cursor iteration with MATCH and COUNT', () => {
  const engine = new StorageEngine();

  try {
    // Populate 25 keys: user:0 .. user:24 and 5 misc keys
    for (let i = 0; i < 25; i++) {
      engine.set(`user:${i}`, `data_${i}`);
    }
    for (let i = 0; i < 5; i++) {
      engine.set(`misc:${i}`, `val_${i}`);
    }

    // 1. Full scan with default count
    let cursor = 0;
    const collectedKeys = new Set<string>();
    do {
      const res = engine.scan(cursor, undefined, 10);
      cursor = res.cursor;
      for (const k of res.keys) {
        collectedKeys.add(k);
      }
    } while (cursor !== 0);

    assert.equal(collectedKeys.size, 30);

    // 2. Scan with MATCH 'user:*'
    cursor = 0;
    const userKeys = new Set<string>();
    do {
      const res = engine.scan(cursor, 'user:*', 10);
      cursor = res.cursor;
      for (const k of res.keys) {
        userKeys.add(k);
      }
    } while (cursor !== 0);

    assert.equal(userKeys.size, 25);
    for (const k of userKeys) {
      assert.ok(k.startsWith('user:'));
    }
  } finally {
    engine.close();
  }
});

test('StorageEngine: HSCAN, SSCAN, ZSCAN cursor iteration', () => {
  const engine = new StorageEngine();

  try {
    // 1. HSCAN
    for (let i = 0; i < 20; i++) {
      engine.hset('myhash', `field_${i}`, `val_${i}`);
    }
    let hCursor = 0;
    const collectedFields = new Map<string, string>();
    do {
      const res = engine.hscan('myhash', hCursor, undefined, 5);
      hCursor = res.cursor;
      for (let i = 0; i < res.entries.length; i += 2) {
        collectedFields.set(res.entries[i].toString(), res.entries[i + 1].toString());
      }
    } while (hCursor !== 0);

    assert.equal(collectedFields.size, 20);

    // 2. SSCAN
    for (let i = 0; i < 15; i++) {
      engine.sadd('myset', `member_${i}`);
    }
    let sCursor = 0;
    const collectedMembers = new Set<string>();
    do {
      const res = engine.sscan('myset', sCursor, 'member_1*', 5);
      sCursor = res.cursor;
      for (const m of res.members) {
        collectedMembers.add(m);
      }
    } while (sCursor !== 0);

    // member_1, member_10..member_14 -> 7 members
    assert.equal(collectedMembers.size, 6); // member_1, 10, 11, 12, 13, 14
    for (const m of collectedMembers) {
      assert.ok(m.startsWith('member_1'));
    }

    // 3. ZSCAN
    for (let i = 0; i < 10; i++) {
      engine.zadd('myzset', i * 10, `zmem_${i}`);
    }
    let zCursor = 0;
    const collectedZMembers = new Map<string, number>();
    do {
      const res = engine.zscan('myzset', zCursor, undefined, 4);
      zCursor = res.cursor;
      for (let i = 0; i < res.entries.length; i += 2) {
        collectedZMembers.set(res.entries[i], Number(res.entries[i + 1]));
      }
    } while (zCursor !== 0);

    assert.equal(collectedZMembers.size, 10);
    assert.equal(collectedZMembers.get('zmem_5'), 50);
  } finally {
    engine.close();
  }
});

test('Integration: Live TCP SCAN, HSCAN, SSCAN, ZSCAN commands', async () => {
  const PORT = 16387;
  const server = new TCPServer({ port: PORT, host: '127.0.0.1' });
  await server.start();

  const client = net.createConnection({ port: PORT, host: '127.0.0.1' });
  await new Promise((resolve) => client.once('connect', resolve));

  try {
    // Populate keys
    await sendRaw(client, RESPSerializer.encodeArray([Buffer.from('SET'), Buffer.from('k:1'), Buffer.from('v1')]));
    await sendRaw(client, RESPSerializer.encodeArray([Buffer.from('SET'), Buffer.from('k:2'), Buffer.from('v2')]));
    await sendRaw(client, RESPSerializer.encodeArray([Buffer.from('SET'), Buffer.from('other:1'), Buffer.from('vo')]));

    // SCAN 0 MATCH k:* COUNT 10
    const scanCmd = RESPSerializer.encodeArray([
      Buffer.from('SCAN'),
      Buffer.from('0'),
      Buffer.from('MATCH'),
      Buffer.from('k:*'),
      Buffer.from('COUNT'),
      Buffer.from('10'),
    ]);
    const scanRes = await sendRaw(client, scanCmd);
    assert.ok(scanRes.toString().includes('k:1'));
    assert.ok(scanRes.toString().includes('k:2'));
    assert.ok(!scanRes.toString().includes('other:1'));

    // HSCAN
    await sendRaw(client, RESPSerializer.encodeArray([Buffer.from('HSET'), Buffer.from('hkey'), Buffer.from('f1'), Buffer.from('v1'), Buffer.from('f2'), Buffer.from('v2')]));
    const hscanRes = await sendRaw(client, RESPSerializer.encodeArray([Buffer.from('HSCAN'), Buffer.from('hkey'), Buffer.from('0')]));
    assert.ok(hscanRes.toString().includes('f1'));
    assert.ok(hscanRes.toString().includes('v2'));
  } finally {
    client.destroy();
    await server.stop();
  }
});
