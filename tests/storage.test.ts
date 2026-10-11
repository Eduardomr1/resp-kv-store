import test from 'node:test';
import assert from 'node:assert/strict';
import { StorageEngine } from '../src/storage/engine.ts';

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

test('StorageEngine: Basic CRUD operations', () => {
  const engine = new StorageEngine();
  try {
    assert.equal(engine.set('key1', 'value1'), true);
    assert.equal(engine.get('key1')?.toString(), 'value1');
    assert.equal(engine.exists('key1', 'nonexistent'), 1);

    assert.equal(engine.set('key2', Buffer.from([0x01, 0x02, 0x03])), true);
    assert.deepEqual(engine.get('key2'), Buffer.from([0x01, 0x02, 0x03]));

    assert.equal(engine.del('key1', 'key2', 'key3'), 2);
    assert.equal(engine.get('key1'), null);
    assert.equal(engine.dbsize(), 0);
  } finally {
    engine.close();
  }
});

test('StorageEngine: NX and XX conditional flags', () => {
  const engine = new StorageEngine();
  try {
    // NX: only if not exists
    assert.equal(engine.set('a', 'valA', { nx: true }), true);
    assert.equal(engine.set('a', 'newVal', { nx: true }), false);
    assert.equal(engine.get('a')?.toString(), 'valA');

    // XX: only if exists
    assert.equal(engine.set('b', 'valB', { xx: true }), false);
    assert.equal(engine.get('b'), null);

    assert.equal(engine.set('a', 'updatedA', { xx: true }), true);
    assert.equal(engine.get('a')?.toString(), 'updatedA');
  } finally {
    engine.close();
  }
});

test('StorageEngine: Passive (Lazy) Expiration', async () => {
  const engine = new StorageEngine({ activeExpireIntervalMs: 10000 }); // disable rapid active sweep for this test
  try {
    engine.set('temp', 'temporary', { px: 50 }); // 50ms TTL

    assert.equal(engine.get('temp')?.toString(), 'temporary');
    assert.ok(engine.pttl('temp') > 0);

    // Sleep past expiration
    await sleep(70);

    // Passive check occurs upon access
    assert.equal(engine.get('temp'), null);
    assert.equal(engine.exists('temp'), 0);
    assert.equal(engine.ttl('temp'), -2);

    const stats = engine.stats();
    assert.equal(stats.expiredPassive, 1);
  } finally {
    engine.close();
  }
});

test('StorageEngine: Active (Probabilistic) Expiration Sweep', async () => {
  const engine = new StorageEngine({
    activeExpireIntervalMs: 10,
    activeExpireSampleSize: 50,
  });

  try {
    // Populate 100 keys with 30ms TTL
    for (let i = 0; i < 100; i++) {
      engine.set(`batch:${i}`, `val:${i}`, { px: 30 });
    }

    assert.equal(engine.dbsize(), 100);
    const statsInitial = engine.stats();
    assert.equal(statsInitial.expiringKeys, 100);

    // Wait for expiration + active sweeper cycles
    await sleep(100);

    // Explicitly run an active cycle or verify automatic sweep cleared them
    engine.activeExpireCycle();

    const statsAfter = engine.stats();
    assert.ok(statsAfter.expiredActive > 80, `Expected >80 active evictions, got ${statsAfter.expiredActive}`);
    assert.ok(engine.dbsize() < 20);
  } finally {
    engine.close();
  }
});

test('StorageEngine: Dense Swap-and-Pop and Persist integrity', () => {
  const engine = new StorageEngine();
  try {
    for (let i = 0; i < 50; i++) {
      engine.set(`k:${i}`, `v:${i}`, { ex: 60 });
    }

    assert.equal(engine.stats().expiringKeys, 50);

    // Remove TTL from middle keys
    assert.equal(engine.persist('k:25'), true);
    assert.equal(engine.persist('k:0'), true);
    assert.equal(engine.persist('k:49'), true);
    assert.equal(engine.persist('k:999'), false); // non-existent

    assert.equal(engine.ttl('k:25'), -1);
    assert.equal(engine.stats().expiringKeys, 47);

    // Delete intermediate keys
    engine.del('k:10', 'k:20', 'k:30');
    assert.equal(engine.stats().expiringKeys, 44);
  } finally {
    engine.close();
  }
});

test('StorageEngine: Keys glob matching', () => {
  const engine = new StorageEngine();
  try {
    engine.set('user:1:profile', 'u1');
    engine.set('user:2:profile', 'u2');
    engine.set('user:10:profile', 'u10');
    engine.set('product:1', 'p1');

    const userKeys = engine.keys('user:?:profile');
    assert.equal(userKeys.length, 2);
    assert.ok(userKeys.includes('user:1:profile'));
    assert.ok(userKeys.includes('user:2:profile'));

    const allUsers = engine.keys('user:*');
    assert.equal(allUsers.length, 3);

    const allKeys = engine.keys('*');
    assert.equal(allKeys.length, 4);
  } finally {
    engine.close();
  }
});
