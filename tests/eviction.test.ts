import test from 'node:test';
import assert from 'node:assert/strict';
import {
  OOMError,
  type EvictionPolicy,
} from '../src/eviction/types.ts';
import {
  lfuLogIncr,
  lfuDecay,
  lfuDecayCounter,
  LFU_INIT_VAL,
  LFU_MAX_VALUE,
} from '../src/eviction/lfu.ts';
import { EvictionPool } from '../src/eviction/pool.ts';
import { MemoryGovernor } from '../src/eviction/governor.ts';
import { StorageEngine } from '../src/storage/engine.ts';

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// ==========================================
// 1. Eviction Pool Mechanics
// ==========================================

test('Eviction Pool: sorting and replacement mechanics', () => {
  // Create a pool with maxSize = 3, using allkeys-lru policy (higher metric = higher priority)
  const pool = new EvictionPool(3, 'allkeys-lru');

  // Insert k1 (idle=10), k2 (idle=50), k3 (idle=30)
  assert.equal(pool.insert('k1', 10), true);
  assert.equal(pool.insert('k2', 50), true);
  assert.equal(pool.insert('k3', 30), true);
  assert.equal(pool.size, 3);

  // Pool should be sorted from best (highest idle) to worst: k2 (50), k3 (30), k1 (10)
  assert.equal(pool.getBestCandidate()?.key, 'k2');
  assert.equal(pool.getBestCandidate()?.metric, 50);
  assert.equal(pool.getWorstCandidate()?.key, 'k1');
  assert.equal(pool.getWorstCandidate()?.metric, 10);
  assert.deepEqual(
    pool.entries.map((e) => e.key),
    ['k2', 'k3', 'k1']
  );

  // Pool is now full (size=3).
  // Inserting candidate worse than the worst (k1 with metric 10) should be rejected
  assert.equal(pool.insert('k_worse', 5), false);
  assert.equal(pool.size, 3);
  assert.equal(pool.has('k_worse'), false);

  // Inserting candidate better than the worst (metric 40 > 10) should displace k1
  assert.equal(pool.insert('k_better', 40), true);
  assert.equal(pool.size, 3);
  assert.equal(pool.has('k1'), false); // k1 displaced
  assert.equal(pool.has('k_better'), true);

  // New order: k2 (50), k_better (40), k3 (30)
  assert.deepEqual(
    pool.entries.map((e) => e.key),
    ['k2', 'k_better', 'k3']
  );

  // Updating existing candidate k3 with metric 60 should move it to the top
  assert.equal(pool.insert('k3', 60), true);
  assert.equal(pool.size, 3);
  assert.deepEqual(
    pool.entries.map((e) => e.key),
    ['k3', 'k2', 'k_better']
  );

  // Popping best candidate removes k3 (60)
  const popped = pool.popBestCandidate();
  assert.equal(popped?.key, 'k3');
  assert.equal(popped?.metric, 60);
  assert.equal(pool.size, 2);
  assert.deepEqual(
    pool.entries.map((e) => e.key),
    ['k2', 'k_better']
  );
});

test('Eviction Pool: volatile-ttl sorting (lower remaining time evicted first)', () => {
  // For volatile-ttl: lower remaining ms is a better candidate
  const pool = new EvictionPool(3, 'volatile-ttl');

  // Insert candidates with remaining ms: 500ms, 100ms, 300ms
  pool.insert('k1', 500);
  pool.insert('k2', 100);
  pool.insert('k3', 300);

  // Best candidate is k2 (100ms remaining), worst is k1 (500ms remaining)
  assert.equal(pool.getBestCandidate()?.key, 'k2');
  assert.equal(pool.getWorstCandidate()?.key, 'k1');
  assert.deepEqual(
    pool.entries.map((e) => e.key),
    ['k2', 'k3', 'k1']
  );

  // Candidate with 800ms remaining is worse than 500ms -> rejected
  assert.equal(pool.insert('k_worse', 800), false);
  assert.equal(pool.size, 3);

  // Candidate with 50ms remaining is better than 500ms -> replaces k1
  assert.equal(pool.insert('k_better', 50), true);
  assert.equal(pool.size, 3);
  assert.equal(pool.has('k1'), false);
  assert.equal(pool.getBestCandidate()?.key, 'k_better');
  assert.deepEqual(
    pool.entries.map((e) => e.key),
    ['k_better', 'k2', 'k3']
  );
});

// ==========================================
// 2. LFU Counter & Temporal Decay
// ==========================================

test('LFU: Morris logarithmic increment and capping', () => {
  // When counter <= LFU_INIT_VAL (5), increment probability is 1.0 (always increments)
  assert.equal(lfuLogIncr(0), 1);
  assert.equal(lfuLogIncr(5), 6);

  // Deterministic testing with mock randomFn:
  // When randomFn returns 0 (r < p is always true), increment occurs
  assert.equal(lfuLogIncr(10, 10, () => 0), 11);
  assert.equal(lfuLogIncr(100, 10, () => 0), 101);

  // When randomFn returns 0.999 (r >= p), counter does not increment
  assert.equal(lfuLogIncr(10, 10, () => 0.999), 10);
  assert.equal(lfuLogIncr(200, 10, () => 0.999), 200);

  // Capped at 255
  assert.equal(lfuLogIncr(255, 10, () => 0), 255);
});

test('LFU: Temporal decay calculation', () => {
  // Counter 10 decayed by 3 minutes with decayInterval = 1 -> 7
  assert.equal(lfuDecayCounter(10, 3, 1), 7);

  // Counter 10 decayed by 5 minutes with decayInterval = 2 -> 10 - floor(5/2) = 8
  assert.equal(lfuDecayCounter(10, 5, 2), 8);

  // Counter cannot decay below 0
  assert.equal(lfuDecayCounter(4, 10, 1), 0);

  // 0 elapsed minutes or disabled decay produces no change
  assert.equal(lfuDecayCounter(10, 0, 1), 10);
  assert.equal(lfuDecayCounter(10, 5, 0), 10);

  // Timestamp-based decay advancing decayTime
  const t0 = 1000000;
  const tNow = t0 + 120000; // 2 minutes later
  const res = lfuDecay(20, t0, tNow, 1);
  assert.equal(res.counter, 18);
  assert.equal(res.lastDecayTime, t0 + 120000);
});

// ==========================================
// 3. noeviction Policy
// ==========================================

test('MemoryGovernor: noeviction policy throws OOMError when limit exceeded', () => {
  const governor = new MemoryGovernor({
    maxmemory: 100,
    policy: 'noeviction',
  });

  governor.registerEntry('k1', {}, 50);
  assert.equal(governor.getMemoryUsage(), 50);
  assert.doesNotThrow(() => governor.evictIfNeeded());

  // Add entry that breaches maxmemory
  governor.registerEntry('k2', {}, 60);
  assert.equal(governor.getMemoryUsage(), 110);

  assert.throws(
    () => governor.evictIfNeeded(),
    (err: any) => {
      assert.ok(err instanceof OOMError);
      assert.equal(err.name, 'OOMError');
      assert.equal(err.message, "OOM command not allowed when used memory > 'maxmemory'");
      return true;
    }
  );

  const stats = governor.getStats();
  assert.equal(stats.evictionErrors, 1);
});

// ==========================================
// 4. allkeys-lru Policy
// ==========================================

test('MemoryGovernor: allkeys-lru evicts least recently accessed keys', () => {
  const governor = new MemoryGovernor({
    maxmemory: 100,
    policy: 'allkeys-lru',
    sampleSize: 10, // Sample all keys
  });

  const now = Date.now();
  // k1 was accessed 5000ms ago (oldest / highest idle)
  governor.registerEntry('k1', { lastAccessTime: now - 5000 }, 40);
  // k2 was accessed 2000ms ago
  governor.registerEntry('k2', { lastAccessTime: now - 2000 }, 40);
  // k3 was accessed 100ms ago (newest)
  governor.registerEntry('k3', { lastAccessTime: now - 100 }, 40);

  // Total memory = 120 > maxmemory (100)
  assert.equal(governor.getMemoryUsage(), 120);

  const evicted = governor.evictIfNeeded();
  assert.equal(evicted, 1);
  assert.equal(governor.getMemoryUsage(), 80);

  // k1 should be evicted because it had the largest idle time
  assert.equal(governor.getEntry('k1'), undefined);
  assert.ok(governor.getEntry('k2') !== undefined);
  assert.ok(governor.getEntry('k3') !== undefined);
});

// ==========================================
// 5. volatile-lru Policy
// ==========================================

test('MemoryGovernor: volatile-lru only evicts keys with expiration set', () => {
  const governor = new MemoryGovernor({
    maxmemory: 100,
    policy: 'volatile-lru',
    sampleSize: 10,
  });

  const now = Date.now();
  // Persistent key (no expiration), accessed 10000ms ago (very old)
  governor.registerEntry('k_persist', { lastAccessTime: now - 10000, expiresAt: null }, 40);
  // Volatile key 1, accessed 5000ms ago
  governor.registerEntry('k_vol1', { lastAccessTime: now - 5000, expiresAt: now + 60000 }, 40);
  // Volatile key 2, accessed 1000ms ago
  governor.registerEntry('k_vol2', { lastAccessTime: now - 1000, expiresAt: now + 60000 }, 40);

  // Total memory = 120 > 100
  assert.equal(governor.getMemoryUsage(), 120);

  const evicted = governor.evictIfNeeded();
  assert.equal(evicted, 1);

  // k_vol1 should be evicted, k_persist must be preserved despite being older
  assert.ok(governor.getEntry('k_persist') !== undefined);
  assert.equal(governor.getEntry('k_vol1'), undefined);
  assert.ok(governor.getEntry('k_vol2') !== undefined);

  // If no volatile keys exist and memory is exceeded under volatile-lru, it must throw OOMError
  governor.registerEntry('k_persist2', { expiresAt: null }, 80);
  assert.throws(() => governor.evictIfNeeded(), OOMError);
});

// ==========================================
// 6. allkeys-lfu Policy
// ==========================================

test('MemoryGovernor: allkeys-lfu evicts least frequently used keys despite recency', () => {
  const governor = new MemoryGovernor({
    maxmemory: 100,
    policy: 'allkeys-lfu',
    sampleSize: 10,
  });

  const now = Date.now();
  // k_frequent has high frequency (counter = 150), accessed 5000ms ago
  governor.registerEntry(
    'k_frequent',
    { lfuCounter: 150, lfuDecayTime: now, lastAccessTime: now - 5000 },
    40
  );
  // k_infrequent has low frequency (counter = 5), accessed 10ms ago (very recent!)
  governor.registerEntry(
    'k_infrequent',
    { lfuCounter: 5, lfuDecayTime: now, lastAccessTime: now - 10 },
    40
  );
  // k_medium has counter = 50
  governor.registerEntry(
    'k_medium',
    { lfuCounter: 50, lfuDecayTime: now, lastAccessTime: now - 2000 },
    40
  );

  // Total memory = 120 > 100
  const evicted = governor.evictIfNeeded();
  assert.equal(evicted, 1);

  // k_infrequent has the lowest counter (5) -> highest 255 - counter -> evicted first,
  // even though it was accessed much more recently than k_frequent!
  assert.equal(governor.getEntry('k_infrequent'), undefined);
  assert.ok(governor.getEntry('k_frequent') !== undefined);
  assert.ok(governor.getEntry('k_medium') !== undefined);
});

// ==========================================
// 7. volatile-ttl Policy
// ==========================================

test('MemoryGovernor: volatile-ttl evicts keys closest to expiration', () => {
  const governor = new MemoryGovernor({
    maxmemory: 100,
    policy: 'volatile-ttl',
    sampleSize: 10,
  });

  const now = Date.now();
  // Persistent key: should not be evicted
  governor.registerEntry('k_persist', { expiresAt: null }, 40);
  // Key expiring in 2000ms
  governor.registerEntry('k_soon', { expiresAt: now + 2000 }, 40);
  // Key expiring in 100000ms
  governor.registerEntry('k_later', { expiresAt: now + 100000 }, 40);

  // Total memory = 120 > 100
  const evicted = governor.evictIfNeeded();
  assert.equal(evicted, 1);

  // k_soon has the smallest remaining TTL -> evicted first
  assert.equal(governor.getEntry('k_soon'), undefined);
  assert.ok(governor.getEntry('k_later') !== undefined);
  assert.ok(governor.getEntry('k_persist') !== undefined);
});

// ==========================================
// 8. Random Policies
// ==========================================

test('MemoryGovernor: allkeys-random and volatile-random policies', () => {
  const governor = new MemoryGovernor({
    maxmemory: 50,
    policy: 'allkeys-random',
  });

  governor.registerEntry('k1', {}, 30);
  governor.registerEntry('k2', {}, 30);
  assert.equal(governor.getMemoryUsage(), 60);

  governor.evictIfNeeded();
  assert.equal(governor.getMemoryUsage(), 30);
  assert.equal(governor.getStats().evictedKeys, 1);

  // volatile-random skips persistent keys
  const volGovernor = new MemoryGovernor({
    maxmemory: 30,
    policy: 'volatile-random',
  });
  volGovernor.registerEntry('k_persist', { expiresAt: null }, 20);
  volGovernor.registerEntry('k_vol', { expiresAt: Date.now() + 10000 }, 20);
  // Memory is 20 + 20 = 40 > 30

  volGovernor.evictIfNeeded();
  // k_vol must be the one evicted
  assert.equal(volGovernor.getEntry('k_vol'), undefined);
  assert.ok(volGovernor.getEntry('k_persist') !== undefined);
  assert.equal(volGovernor.getMemoryUsage(), 20);
});

// ==========================================
// 9. StorageEngine End-to-End Integration
// ==========================================

test('StorageEngine integration: noeviction throws OOMError when limit exceeded', () => {
  // Key overhead is ~Buffer.byteLength(k) + 64 + valLength.
  // Setting key "k1" with "val1" is ~ (2 + 64 + 4) = 70 bytes.
  const engine = new StorageEngine({
    maxmemory: 100,
    maxmemoryPolicy: 'noeviction',
  });

  try {
    assert.equal(engine.set('k1', 'val1'), true);
    assert.equal(engine.get('k1')?.toString(), 'val1');

    // Setting second key breaches 100 byte limit
    assert.throws(
      () => engine.set('k2', 'val2'),
      (err: any) => {
        assert.ok(err instanceof OOMError);
        assert.equal(err.message, "OOM command not allowed when used memory > 'maxmemory'");
        return true;
      }
    );

    // k1 still exists and k2 was not stored
    assert.equal(engine.get('k1')?.toString(), 'val1');
    assert.equal(engine.get('k2'), null);
  } finally {
    engine.close();
  }
});

test('StorageEngine integration: allkeys-lru evicts least recently accessed key', async () => {
  const engine = new StorageEngine({
    maxmemory: 200,
    maxmemoryPolicy: 'allkeys-lru',
    evictionSampleSize: 10,
  });

  try {
    engine.set('k1', 'val1'); // ~70 bytes
    await sleep(20);
    engine.set('k2', 'val2'); // ~70 bytes
    await sleep(20);

    // Access k1 so k2 becomes the least recently used
    assert.equal(engine.get('k1')?.toString(), 'val1');
    await sleep(20);

    // Add k3 which breaches 200 bytes limit (70 * 3 = 210 > 200)
    engine.set('k3', 'val3');

    // k2 should have been evicted, while k1 and k3 remain
    assert.equal(engine.get('k2'), null);
    assert.equal(engine.get('k1')?.toString(), 'val1');
    assert.equal(engine.get('k3')?.toString(), 'val3');
  } finally {
    engine.close();
  }
});

test('StorageEngine integration: volatile-ttl evicts key closest to expiry', () => {
  const engine = new StorageEngine({
    maxmemory: 200,
    maxmemoryPolicy: 'volatile-ttl',
    evictionSampleSize: 10,
  });

  try {
    engine.set('k_soon', 'val1', { px: 2000 });     // ~70 bytes, expires in 2s
    engine.set('k_later', 'val2', { px: 100000 });   // ~70 bytes, expires in 100s

    // Add third key to trigger eviction
    engine.set('k_new', 'val3', { px: 50000 });

    // k_soon was closest to expiration, so it should be evicted
    assert.equal(engine.get('k_soon'), null);
    assert.equal(engine.get('k_later')?.toString(), 'val2');
    assert.equal(engine.get('k_new')?.toString(), 'val3');
  } finally {
    engine.close();
  }
});

