import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { Buffer } from 'node:buffer';
import { StorageEngine } from '../src/storage/engine.ts';
import {
  RDBEncoder,
  RDBDecoder,
  encodeLength,
  decodeLength,
  encodeString,
  decodeString,
  encodeInteger,
  decodeInteger,
  encodeScore,
  decodeScore,
  crc64,
} from '../src/storage/rdb.ts';
import {
  RDB_MAGIC,
  OPCODE_SELECTDB,
  OPCODE_EXPIRETIME_MS,
  OPCODE_EXPIRETIME_SEC,
  OPCODE_EOF,
  TYPE_STRING,
  TYPE_LIST,
  TYPE_SET,
  TYPE_ZSET,
  TYPE_HASH,
  CorruptRDBError,
} from '../src/storage/rdb_types.ts';
import { TCPServer } from '../src/server.ts';
import net from 'node:net';

const TEST_DUMP_FILE = path.resolve('./test_dump.rdb');
const TEST_BG_DUMP_FILE = path.resolve('./test_bg_dump.rdb');

function cleanup() {
  for (const file of [TEST_DUMP_FILE, TEST_BG_DUMP_FILE]) {
    if (fs.existsSync(file)) {
      try {
        fs.unlinkSync(file);
      } catch {}
    }
  }
}

test('RDB Constants and Magic Header', () => {
  assert.equal(RDB_MAGIC, 'REDIS0011');
  assert.equal(OPCODE_SELECTDB, 0xfe);
  assert.equal(OPCODE_EXPIRETIME_MS, 0xfc);
  assert.equal(OPCODE_EXPIRETIME_SEC, 0xfd);
  assert.equal(OPCODE_EOF, 0xff);
  assert.equal(TYPE_STRING, 0x00);
  assert.equal(TYPE_LIST, 0x01);
  assert.equal(TYPE_SET, 0x02);
  assert.equal(TYPE_ZSET, 0x03);
  assert.equal(TYPE_HASH, 0x04);
});

test('RDB Binary Helper: Length prefix encoding and decoding', () => {
  const testLengths = [0, 1, 63, 64, 127, 255, 16383, 16384, 65535, 1000000];

  for (const len of testLengths) {
    const enc = encodeLength(len);
    const offsetRef = { offset: 0 };
    const dec = decodeLength(enc, offsetRef);

    assert.equal(dec, len, `Length mismatch for ${len}`);
    assert.equal(offsetRef.offset, enc.length, `Offset mismatch for ${len}`);
  }

  // 6-bit length representation check (len <= 63 must be 1 byte)
  assert.equal(encodeLength(10).length, 1);
  assert.equal(encodeLength(63).length, 1);

  // 14-bit length representation check (64 <= len <= 16383 must be 2 bytes)
  assert.equal(encodeLength(64).length, 2);
  assert.equal(encodeLength(16383).length, 2);

  // 32-bit length representation check (len >= 16384 must be 5 bytes)
  assert.equal(encodeLength(16384).length, 5);
  assert.equal(encodeLength(100000).length, 5);
});

test('RDB Binary Helper: String and binary Buffer encoding/decoding', () => {
  const testStrings = [
    '',
    'hello',
    'redis-kv-store',
    'UTF-8 🚀 測試 данные ✨',
    Buffer.from([0x00, 0xff, 0xfe, 0x00, 0x42, 0x13]), // binary with null bytes
  ];

  for (const original of testStrings) {
    const enc = encodeString(original);
    const offsetRef = { offset: 0 };
    const dec = decodeString(enc, offsetRef);

    const expectedBuf = Buffer.isBuffer(original) ? original : Buffer.from(original, 'utf8');
    assert.deepEqual(dec, expectedBuf);
    assert.equal(offsetRef.offset, enc.length);
  }
});

test('RDB Binary Helper: Integer special encoding and decoding', () => {
  const integers = [-128, -50, 0, 42, 127, -32000, 32000, -100000, 2000000];

  for (const val of integers) {
    const enc = encodeInteger(val);
    const offsetRef = { offset: 0 };
    const dec = decodeInteger(enc, offsetRef);
    assert.equal(dec, val);
  }
});

test('RDB Binary Helper: Floating-point scores encoding/decoding for ZSET', () => {
  const scores = [0, 1, -1, 3.14159, -42.88, 1000000.5, Infinity, -Infinity, NaN];

  for (const score of scores) {
    const enc = encodeScore(score);
    const offsetRef = { offset: 0 };
    const dec = decodeScore(enc, offsetRef);

    if (Number.isNaN(score)) {
      assert.ok(Number.isNaN(dec));
    } else {
      assert.equal(dec, score);
    }
    assert.equal(offsetRef.offset, enc.length);
  }
});

test('RDB Magic Header validation and corruption detection', () => {
  const engine = new StorageEngine();
  try {
    // 1. Buffer too short
    assert.throws(
      () => RDBDecoder.decode(Buffer.from('REDIS'), engine),
      (err: any) => err instanceof CorruptRDBError && err.message.includes('too short')
    );

    // 2. Invalid magic header prefix
    assert.throws(
      () => RDBDecoder.decode(Buffer.from('FOOB0011\xff0000000000000000'), engine),
      (err: any) => err instanceof CorruptRDBError && err.message.includes("expected 'REDIS'")
    );

    // 3. Invalid version string
    assert.throws(
      () => RDBDecoder.decode(Buffer.from('REDISabcd\xff0000000000000000'), engine),
      (err: any) => err instanceof CorruptRDBError && err.message.includes('Invalid RDB version')
    );

    // 4. Missing EOF opcode
    const noEofBuf = Buffer.concat([
      Buffer.from(RDB_MAGIC, 'ascii'),
      Buffer.from([OPCODE_SELECTDB, 0x00]),
      Buffer.alloc(8, 0), // 8 dummy bytes for checksum
    ]);
    assert.throws(
      () => RDBDecoder.decode(noEofBuf, engine),
      (err: any) => err instanceof CorruptRDBError
    );

    // 5. Checksum mismatch detection (tampered payload)
    engine.set('key1', 'val1');
    const validDump = RDBEncoder.encode(engine);
    assert.ok(validDump.length >= 18);

    // Corrupt one byte inside the payload
    const corruptDump = Buffer.from(validDump);
    corruptDump[12] ^= 0xff; // flip bits

    assert.throws(
      () => RDBDecoder.decode(corruptDump, engine),
      (err: any) => err instanceof CorruptRDBError && err.message.includes('checksum mismatch')
    );
  } finally {
    engine.close();
  }
});

test('RDB Serialization and Deserialization: Strings', () => {
  const engine1 = new StorageEngine();
  const engine2 = new StorageEngine();

  try {
    engine1.set('str:empty', '');
    engine1.set('str:simple', 'Redis In-Memory Engine');
    engine1.set('str:unicode', '🚀 Node.js TypeScript RESP Store 🌟');
    engine1.set('str:binary', Buffer.from([0x00, 0x01, 0x02, 0xfe, 0xff]));

    const dump = RDBEncoder.encode(engine1);
    assert.ok(dump.subarray(0, 9).toString('ascii') === RDB_MAGIC);

    RDBDecoder.decode(dump, engine2);

    assert.equal(engine2.get('str:empty')?.toString('utf8'), '');
    assert.equal(engine2.get('str:simple')?.toString('utf8'), 'Redis In-Memory Engine');
    assert.equal(engine2.get('str:unicode')?.toString('utf8'), '🚀 Node.js TypeScript RESP Store 🌟');
    assert.deepEqual(engine2.get('str:binary'), Buffer.from([0x00, 0x01, 0x02, 0xfe, 0xff]));
  } finally {
    engine1.close();
    engine2.close();
  }
});

test('RDB Serialization and Deserialization: Hashes', () => {
  const engine1 = new StorageEngine();
  const engine2 = new StorageEngine();

  try {
    engine1.hset('user:1001', 'name', 'Alice');
    engine1.hset('user:1001', 'role', 'Admin');
    engine1.hset('user:1001', 'avatar', Buffer.from([0x89, 0x50, 0x4e, 0x47])); // PNG header

    const dump = RDBEncoder.encode(engine1);
    RDBDecoder.decode(dump, engine2);

    assert.equal(engine2.type('user:1001'), 'hash');
    assert.equal(engine2.hlen('user:1001'), 3);
    assert.equal(engine2.hget('user:1001', 'name')?.toString('utf8'), 'Alice');
    assert.equal(engine2.hget('user:1001', 'role')?.toString('utf8'), 'Admin');
    assert.deepEqual(engine2.hget('user:1001', 'avatar'), Buffer.from([0x89, 0x50, 0x4e, 0x47]));
  } finally {
    engine1.close();
    engine2.close();
  }
});

test('RDB Serialization and Deserialization: Lists', () => {
  const engine1 = new StorageEngine();
  const engine2 = new StorageEngine();

  try {
    engine1.rpush('tasks', 'task1', 'task2', 'task3');
    engine1.lpush('tasks', 'urgent0');

    const dump = RDBEncoder.encode(engine1);
    RDBDecoder.decode(dump, engine2);

    assert.equal(engine2.type('tasks'), 'list');
    assert.equal(engine2.llen('tasks'), 4);
    const items = engine2.lrange('tasks', 0, -1).map((b) => b.toString('utf8'));
    assert.deepEqual(items, ['urgent0', 'task1', 'task2', 'task3']);
  } finally {
    engine1.close();
    engine2.close();
  }
});

test('RDB Serialization and Deserialization: Sets', () => {
  const engine1 = new StorageEngine();
  const engine2 = new StorageEngine();

  try {
    engine1.sadd('languages', 'TypeScript', 'Rust', 'Go', 'Python');

    const dump = RDBEncoder.encode(engine1);
    RDBDecoder.decode(dump, engine2);

    assert.equal(engine2.type('languages'), 'set');
    assert.equal(engine2.scard('languages'), 4);
    assert.equal(engine2.sismember('languages', 'TypeScript'), true);
    assert.equal(engine2.sismember('languages', 'Rust'), true);
    assert.equal(engine2.sismember('languages', 'Java'), false);
  } finally {
    engine1.close();
    engine2.close();
  }
});

test('RDB Serialization and Deserialization: Sorted Sets (ZSets)', () => {
  const engine1 = new StorageEngine();
  const engine2 = new StorageEngine();

  try {
    engine1.zadd('leaderboard', 100, 'player1');
    engine1.zadd('leaderboard', 250.75, 'player2');
    engine1.zadd('leaderboard', -15.5, 'player_penalty');
    engine1.zadd('leaderboard', 500, 'boss');

    const dump = RDBEncoder.encode(engine1);
    RDBDecoder.decode(dump, engine2);

    assert.equal(engine2.type('leaderboard'), 'zset');
    assert.equal(engine2.zcard('leaderboard'), 4);
    assert.equal(engine2.zscore('leaderboard', 'player1'), 100);
    assert.equal(engine2.zscore('leaderboard', 'player2'), 250.75);
    assert.equal(engine2.zscore('leaderboard', 'player_penalty'), -15.5);
    assert.equal(engine2.zscore('leaderboard', 'boss'), 500);

    // Verify ordering by score ascending
    const range = engine2.zrange('leaderboard', 0, -1);
    assert.deepEqual(range, ['player_penalty', 'player1', 'player2', 'boss']);
  } finally {
    engine1.close();
    engine2.close();
  }
});

test('RDB Expiration TTL restoration with exact timestamps', () => {
  const engine1 = new StorageEngine();
  const engine2 = new StorageEngine();

  try {
    const now = Date.now();
    const futureMs = now + 120000; // 2 minutes in future
    const shortMs = now + 60000;  // 1 minute in future

    engine1.set('persisted_key', 'never_expires');
    engine1.set('future_key_1', 'val_1', { pxat: futureMs });
    engine1.set('future_key_2', 'val_2', { pxat: shortMs });

    const dump = RDBEncoder.encode(engine1);
    RDBDecoder.decode(dump, engine2);

    // Persistent key has no expiration
    assert.equal(engine2.get('persisted_key')?.toString(), 'never_expires');
    assert.equal(engine2.ttl('persisted_key'), -1);
    assert.equal(engine2.getEntry('persisted_key')?.expiresAt, null);

    // Expiring keys restore exact millisecond timestamps
    assert.equal(engine2.get('future_key_1')?.toString(), 'val_1');
    assert.equal(engine2.getEntry('future_key_1')?.expiresAt, futureMs);
    assert.ok(engine2.pttl('future_key_1') > 50000);

    assert.equal(engine2.get('future_key_2')?.toString(), 'val_2');
    assert.equal(engine2.getEntry('future_key_2')?.expiresAt, shortMs);
    assert.ok(engine2.pttl('future_key_2') > 30000);
  } finally {
    engine1.close();
    engine2.close();
  }
});

test('RDB Expiration: Expired keys are discarded during encode and decode', async () => {
  const engine1 = new StorageEngine();
  const engine2 = new StorageEngine();

  try {
    engine1.set('active_key', 'still_alive');
    engine1.set('expired_key', 'about_to_die', { px: 20 });

    // Wait for expired_key to expire
    await new Promise((r) => setTimeout(r, 40));

    const dump = RDBEncoder.encode(engine1);
    RDBDecoder.decode(dump, engine2);

    assert.equal(engine2.get('active_key')?.toString(), 'still_alive');
    assert.equal(engine2.get('expired_key'), null);
    assert.equal(engine2.exists('expired_key'), 0);
  } finally {
    engine1.close();
    engine2.close();
  }
});

test('RDB File Persistence: Atomic save, bgsave, and load', async () => {
  cleanup();

  const engine1 = new StorageEngine();
  const engine2 = new StorageEngine();
  const engine3 = new StorageEngine();

  try {
    engine1.set('service', 'auth');
    engine1.hset('config', 'env', 'production');
    engine1.rpush('queue', 'jobA', 'jobB');
    engine1.sadd('clusters', 'us-east', 'eu-west');
    engine1.zadd('metrics', 99.9, 'uptime');

    // 1. Atomic synchronous save
    RDBEncoder.save(TEST_DUMP_FILE, engine1);
    assert.ok(fs.existsSync(TEST_DUMP_FILE));
    assert.ok(fs.statSync(TEST_DUMP_FILE).size > 0);

    // 2. Load into clean engine2
    const loadedSync = RDBDecoder.load(TEST_DUMP_FILE, engine2);
    assert.equal(loadedSync, true);

    assert.equal(engine2.get('service')?.toString(), 'auth');
    assert.equal(engine2.hget('config', 'env')?.toString(), 'production');
    assert.deepEqual(
      engine2.lrange('queue', 0, -1).map((b) => b.toString()),
      ['jobA', 'jobB']
    );
    assert.equal(engine2.sismember('clusters', 'us-east'), true);
    assert.equal(engine2.zscore('metrics', 'uptime'), 99.9);

    // 3. Load from non-existent file returns false
    const missingLoad = RDBDecoder.load('./non_existent_file.rdb', engine3);
    assert.equal(missingLoad, false);

    // 4. Asynchronous background save (bgsave)
    await RDBEncoder.bgsave(TEST_BG_DUMP_FILE, engine1);
    assert.ok(fs.existsSync(TEST_BG_DUMP_FILE));

    const loadedBg = RDBDecoder.load(TEST_BG_DUMP_FILE, engine3);
    assert.equal(loadedBg, true);
    assert.equal(engine3.get('service')?.toString(), 'auth');
  } finally {
    engine1.close();
    engine2.close();
    engine3.close();
    cleanup();
  }
});

test('RDB End-to-End Roundtrip: Full Polymorphic Dataset with Expirations', () => {
  cleanup();

  const sourceEngine = new StorageEngine();
  const targetEngine = new StorageEngine();

  try {
    // 1. Strings
    sourceEngine.set('str:1', 'simple_value');
    sourceEngine.set('str:2', Buffer.from([0x01, 0x02, 0x03, 0x04]));
    sourceEngine.set('str:3', 'special_utf8_🔥_test');

    // 2. Hashes
    sourceEngine.hset('hash:1', 'fieldA', 'valA');
    sourceEngine.hset('hash:1', 'fieldB', 'valB');
    sourceEngine.hset('hash:2', 'binaryField', Buffer.from([0xde, 0xad, 0xbe, 0xef]));

    // 3. Lists
    sourceEngine.rpush('list:1', 'v1', 'v2', 'v3', 'v4');

    // 4. Sets
    sourceEngine.sadd('set:1', 'memA', 'memB', 'memC');

    // 5. Sorted Sets (ZSets)
    sourceEngine.zadd('zset:1', 1.5, 'item1');
    sourceEngine.zadd('zset:1', 10.0, 'item2');
    sourceEngine.zadd('zset:1', -5.25, 'item3');

    // 6. TTL Expiring Keys
    const futureTime = Date.now() + 180000;
    sourceEngine.set('ttl:key', 'will_expire_later', { pxat: futureTime });

    // Save and restore
    RDBEncoder.save(TEST_DUMP_FILE, sourceEngine);
    const success = RDBDecoder.load(TEST_DUMP_FILE, targetEngine);
    assert.equal(success, true);

    // Verify all keys
    assert.equal(targetEngine.dbsize(), 9);

    // Verify Strings
    assert.equal(targetEngine.get('str:1')?.toString(), 'simple_value');
    assert.deepEqual(targetEngine.get('str:2'), Buffer.from([0x01, 0x02, 0x03, 0x04]));
    assert.equal(targetEngine.get('str:3')?.toString(), 'special_utf8_🔥_test');

    // Verify Hashes
    assert.equal(targetEngine.hget('hash:1', 'fieldA')?.toString(), 'valA');
    assert.equal(targetEngine.hget('hash:1', 'fieldB')?.toString(), 'valB');
    assert.deepEqual(
      targetEngine.hget('hash:2', 'binaryField'),
      Buffer.from([0xde, 0xad, 0xbe, 0xef])
    );

    // Verify Lists
    assert.deepEqual(
      targetEngine.lrange('list:1', 0, -1).map((b) => b.toString()),
      ['v1', 'v2', 'v3', 'v4']
    );

    // Verify Sets
    assert.equal(targetEngine.scard('set:1'), 3);
    assert.equal(targetEngine.sismember('set:1', 'memA'), true);
    assert.equal(targetEngine.sismember('set:1', 'memB'), true);
    assert.equal(targetEngine.sismember('set:1', 'memC'), true);

    // Verify ZSets
    assert.equal(targetEngine.zcard('zset:1'), 3);
    assert.equal(targetEngine.zscore('zset:1', 'item1'), 1.5);
    assert.equal(targetEngine.zscore('zset:1', 'item2'), 10.0);
    assert.equal(targetEngine.zscore('zset:1', 'item3'), -5.25);
    assert.deepEqual(targetEngine.zrange('zset:1', 0, -1), ['item3', 'item1', 'item2']);

    // Verify Expiration
    assert.equal(targetEngine.get('ttl:key')?.toString(), 'will_expire_later');
    assert.equal(targetEngine.getEntry('ttl:key')?.expiresAt, futureTime);
  } finally {
    sourceEngine.close();
    targetEngine.close();
    cleanup();
  }
});

test('RDB TCP Server: LIVE SAVE and BGSAVE commands via RESP protocol', async () => {
  cleanup();

  const server = new TCPServer({
    port: 16399,
    host: '127.0.0.1',
    rdbFilePath: TEST_DUMP_FILE,
  });

  await server.start();

  try {
    // Populate keys via storage engine
    const engine = server.getStorage();
    engine.set('server_key', 'server_val');
    engine.rpush('server_list', 'elem1', 'elem2');

    // Connect client and send SAVE
    const client = net.createConnection({ port: 16399, host: '127.0.0.1' });
    await new Promise((resolve) => client.once('connect', resolve));

    const response = await new Promise<string>((resolve) => {
      client.once('data', (data) => resolve(data.toString('utf8')));
      client.write('*1\r\n$4\r\nSAVE\r\n');
    });

    assert.equal(response, '+OK\r\n');
    assert.ok(fs.existsSync(TEST_DUMP_FILE));

    // Send BGSAVE
    const bgResponse = await new Promise<string>((resolve) => {
      client.once('data', (data) => resolve(data.toString('utf8')));
      client.write('*1\r\n$6\r\nBGSAVE\r\n');
    });

    assert.ok(bgResponse.includes('Background saving started'));

    client.destroy();

    // Verify file content restores into fresh engine
    const restoredEngine = new StorageEngine();
    try {
      const ok = RDBDecoder.load(TEST_DUMP_FILE, restoredEngine);
      assert.equal(ok, true);
      assert.equal(restoredEngine.get('server_key')?.toString(), 'server_val');
      assert.deepEqual(
        restoredEngine.lrange('server_list', 0, -1).map((b) => b.toString()),
        ['elem1', 'elem2']
      );
    } finally {
      restoredEngine.close();
    }
  } finally {
    await server.stop();
    cleanup();
  }
});
