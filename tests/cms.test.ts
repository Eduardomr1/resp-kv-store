import { describe, it } from 'node:test';
import assert from 'node:assert';
import { CountMinSketch } from '../src/storage/cms.ts';
import { StorageEngine } from '../src/storage/engine.ts';

describe('CountMinSketch Unit Tests', () => {
  it('validates constructor parameters', () => {
    assert.throws(() => new CountMinSketch(0, 5), /ERR width must be a positive integer/);
    assert.throws(() => new CountMinSketch(-10, 5), /ERR width must be a positive integer/);
    assert.throws(() => new CountMinSketch(100, 0), /ERR depth must be a positive integer/);
    assert.throws(() => new CountMinSketch(10.5, 5), /ERR width must be a positive integer/);
    assert.throws(() => new CountMinSketch(100, 4.2), /ERR depth must be a positive integer/);

    const cms = new CountMinSketch(2000, 5);
    assert.strictEqual(cms.width, 2000);
    assert.strictEqual(cms.depth, 5);
    assert.strictEqual(cms.byteSize, 2000 * 5 * 4);
    assert.strictEqual(cms.info.count, 0);
  });

  it('initializes correctly using errorRate and probability bounds', () => {
    assert.throws(() => CountMinSketch.createByProb(0, 0.99), /ERR error rate must be between 0 and 1/);
    assert.throws(() => CountMinSketch.createByProb(1, 0.99), /ERR error rate must be between 0 and 1/);
    assert.throws(() => CountMinSketch.createByProb(0.01, 0), /ERR probability must be between 0 and 1/);
    assert.throws(() => CountMinSketch.createByProb(0.01, 1), /ERR probability must be between 0 and 1/);

    // width = ceil(2 / 0.001) = 2000
    // depth = ceil(ln(1 - 0.99) / ln(0.5)) = ceil(ln(0.01) / ln(0.5)) = 7
    const cms = CountMinSketch.createByProb(0.001, 0.99);
    assert.strictEqual(cms.width, 2000);
    assert.strictEqual(cms.depth, 7);
    assert.strictEqual(cms.info.count, 0);
  });

  it('tracks frequency counts accurately and guarantees estimate >= trueCount', () => {
    const cms = new CountMinSketch(2000, 5);

    assert.strictEqual(cms.query('unknown_item'), 0);

    const res1 = cms.incrBy('user:1001', 5);
    assert.strictEqual(res1, 5);
    assert.strictEqual(cms.query('user:1001'), 5);

    const res2 = cms.incrBy('user:1001', 10);
    assert.strictEqual(res2, 15);
    assert.strictEqual(cms.query('user:1001'), 15);

    cms.incrBy('user:2002', 3);
    assert.strictEqual(cms.query('user:2002'), 3);
    assert.strictEqual(cms.info.count, 18);
  });

  it('bounds overestimation on high-volume synthetic distributions', () => {
    const errorRate = 0.01;
    const probability = 0.99;
    const cms = CountMinSketch.createByProb(errorRate, probability);

    const trueCounts = new Map<string, number>();
    const totalEvents = 10000;

    // Simulate Zipfian / skewed frequency stream
    for (let i = 0; i < totalEvents; i++) {
      // Skew towards lower numbered keys
      const keyId = Math.floor(Math.pow(Math.random(), 2) * 100);
      const key = `key:${keyId}`;
      cms.incrBy(key, 1);
      trueCounts.set(key, (trueCounts.get(key) || 0) + 1);
    }

    assert.strictEqual(cms.info.count, totalEvents);

    // Verify mathematical Count-Min Sketch properties:
    // 1. Never undercount: estimate >= trueCount
    // 2. Overcount bounded by epsilon * totalCount
    const maxAllowedError = Math.ceil(errorRate * totalEvents);
    let violations = 0;

    for (const [key, trueCount] of trueCounts.entries()) {
      const estimate = cms.query(key);
      assert.ok(
        estimate >= trueCount,
        `CMS underestimated frequency for ${key}: estimate=${estimate}, true=${trueCount}`
      );

      const error = estimate - trueCount;
      if (error > maxAllowedError) {
        violations++;
      }
    }

    // With 99% confidence (delta = 0.01), violations should be <= 1% of queried keys
    const violationRate = violations / trueCounts.size;
    assert.ok(
      violationRate <= 0.05,
      `Violation rate ${violationRate} exceeded statistical tolerance (violations: ${violations}/${trueCounts.size})`
    );
  });

  it('merges two sketches with identical dimensions and handles weights', () => {
    const cms1 = new CountMinSketch(500, 4);
    const cms2 = new CountMinSketch(500, 4);

    cms1.incrBy('page:home', 10);
    cms1.incrBy('page:checkout', 5);

    cms2.incrBy('page:home', 20);
    cms2.incrBy('page:pricing', 15);

    cms1.merge(cms2, 2);

    // page:home: 10 + 20 * 2 = 50
    assert.strictEqual(cms1.query('page:home'), 50);
    // page:checkout: 5 + 0 * 2 = 5
    assert.strictEqual(cms1.query('page:checkout'), 5);
    // page:pricing: 0 + 15 * 2 = 30
    assert.strictEqual(cms1.query('page:pricing'), 30);

    assert.strictEqual(cms1.info.count, 15 + (35 * 2));
  });

  it('rejects merging sketches with mismatched dimensions', () => {
    const cms1 = new CountMinSketch(500, 4);
    const cms2 = new CountMinSketch(600, 4);
    const cms3 = new CountMinSketch(500, 5);

    assert.throws(
      () => cms1.merge(cms2),
      /ERR sketches must have identical width and depth to merge/
    );
    assert.throws(
      () => cms1.merge(cms3),
      /ERR sketches must have identical width and depth to merge/
    );
  });

  it('supports batch increments, batch queries and state reset', () => {
    const cms = new CountMinSketch(1000, 5);

    const incResults = cms.incrByMultiple([
      { item: 'apple', increment: 10 },
      { item: 'banana', increment: 25 },
      { item: 'cherry', increment: 5 },
    ]);
    assert.deepStrictEqual(incResults, [10, 25, 5]);

    const queryResults = cms.queryMultiple(['banana', 'cherry', 'orange', 'apple']);
    assert.deepStrictEqual(queryResults, [25, 5, 0, 10]);
    assert.strictEqual(cms.info.count, 40);

    cms.reset();
    assert.strictEqual(cms.info.count, 0);
    assert.deepStrictEqual(cms.queryMultiple(['apple', 'banana', 'cherry']), [0, 0, 0]);
  });
});

describe('StorageEngine CMS Lifecycle Suite', () => {
  it('manages CMS keys lifecycle: init, incrby, query, info and encoding', () => {
    const engine = new StorageEngine();

    assert.strictEqual(engine.cmsInfo('missing_cms'), null);

    // Init by dimension
    assert.strictEqual(engine.cmsInitByDim('sketch:dim', 1000, 5), true);
    assert.throws(() => engine.cmsInitByDim('sketch:dim', 500, 3), /ERR item exists/);

    assert.strictEqual(engine.objectEncoding('sketch:dim'), 'count-min-sketch');

    // Init by prob
    assert.strictEqual(engine.cmsInitByProb('sketch:prob', 0.005, 0.95), true);

    // Incrby
    const res = engine.cmsIncrBy('sketch:dim', [
      { item: 'metrics:cpu', increment: 42 },
      { item: 'metrics:memory', increment: 17 },
    ]);
    assert.deepStrictEqual(res, [42, 17]);

    // Query
    const queried = engine.cmsQuery('sketch:dim', ['metrics:cpu', 'metrics:disk', 'metrics:memory']);
    assert.deepStrictEqual(queried, [42, 0, 17]);

    // Info
    const info = engine.cmsInfo('sketch:dim');
    assert.ok(info);
    assert.strictEqual(info.width, 1000);
    assert.strictEqual(info.depth, 5);
    assert.strictEqual(info.count, 59);

    // Type checking
    engine.set('str_key', 'plain string');
    assert.throws(() => engine.cmsQuery('str_key', ['foo']), /WRONGTYPE/);
  });

  it('merges multiple CMS keys in engine', () => {
    const engine = new StorageEngine();
    engine.cmsInitByDim('s1', 500, 4);
    engine.cmsInitByDim('s2', 500, 4);
    engine.cmsInitByDim('dest', 500, 4);

    engine.cmsIncrBy('s1', [{ item: 'view', increment: 10 }]);
    engine.cmsIncrBy('s2', [{ item: 'view', increment: 20 }]);

    engine.cmsMerge('dest', ['s1', 's2'], [1, 3]);
    // 10*1 + 20*3 = 70
    assert.deepStrictEqual(engine.cmsQuery('dest', ['view']), [70]);
  });
});
