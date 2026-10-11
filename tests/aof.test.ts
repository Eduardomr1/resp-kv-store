import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { StorageEngine } from '../src/storage/engine.ts';

const TEST_AOF_PATH = path.resolve('./test_appendonly.aof');

function cleanup() {
  if (fs.existsSync(TEST_AOF_PATH)) {
    try { fs.unlinkSync(TEST_AOF_PATH); } catch {}
  }
}

test('AOF Persistence: Crash-recovery and Replay', () => {
  cleanup();

  // 1. Initial engine session
  const engine1 = new StorageEngine({
    aofEnabled: true,
    aofFilePath: TEST_AOF_PATH,
    aofFsyncPolicy: 'always',
  });

  engine1.set('hero', 'Batman');
  engine1.hset('profile', 'level', '99');
  engine1.rpush('logs', 'entry1', 'entry2');
  engine1.sadd('skills', 'stealth', 'gadgets');
  engine1.zadd('scores', 500, 'boss');

  assert.ok(fs.existsSync(TEST_AOF_PATH));
  const fileSizeBefore = fs.statSync(TEST_AOF_PATH).size;
  assert.ok(fileSizeBefore > 0);

  engine1.close();

  // 2. Recovery engine session (simulating restart after crash)
  const engine2 = new StorageEngine({
    aofEnabled: true,
    aofFilePath: TEST_AOF_PATH,
    aofFsyncPolicy: 'always',
  });

  try {
    assert.equal(engine2.get('hero')?.toString(), 'Batman');
    assert.equal(engine2.hget('profile', 'level')?.toString(), '99');
    assert.deepEqual(engine2.lrange('logs', 0, -1).map((b) => b.toString()), ['entry1', 'entry2']);
    assert.equal(engine2.sismember('skills', 'stealth'), true);
    assert.equal(engine2.zscore('scores', 'boss'), 500);
  } finally {
    engine2.close();
    cleanup();
  }
});

test('AOF Persistence: Atomic Rewrite Compaction', async () => {
  cleanup();

  const engine = new StorageEngine({
    aofEnabled: true,
    aofFilePath: TEST_AOF_PATH,
    aofFsyncPolicy: 'always',
  });

  try {
    // Generate redundant mutations
    for (let i = 0; i < 50; i++) {
      engine.set('counter', `val_${i}`);
    }

    const bloatedSize = fs.statSync(TEST_AOF_PATH).size;

    // Trigger compaction rewrite
    const aof = engine.getAOF()!;
    await aof.rewrite(engine);

    const compactedSize = fs.statSync(TEST_AOF_PATH).size;
    assert.ok(compactedSize < bloatedSize, `Expected ${compactedSize} < ${bloatedSize}`);

    // Verify state after rewrite
    assert.equal(engine.get('counter')?.toString(), 'val_49');
  } finally {
    engine.close();
    cleanup();
  }
});
