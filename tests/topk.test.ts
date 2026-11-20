import { describe, it } from 'node:test';
import assert from 'node:assert';
import { TopK } from '../src/storage/topk.ts';

describe('TopK HeavyKeeper Algorithm Suite', () => {
  it('validates configuration parameters and exposes accurate info/byteSize', () => {
    assert.throws(() => new TopK(0), /ERR k must be a positive integer/);
    assert.throws(() => new TopK(-5), /ERR k must be a positive integer/);
    assert.throws(() => new TopK(5, 0), /ERR width must be a positive integer/);
    assert.throws(() => new TopK(5, 8, 0), /ERR depth must be a positive integer/);
    assert.throws(() => new TopK(5, 8, 7, 0), /ERR decay must be in \(0, 1\]/);
    assert.throws(() => new TopK(5, 8, 7, 1.2), /ERR decay must be in \(0, 1\]/);

    const topk = new TopK(10, 64, 5, 0.92);
    assert.deepStrictEqual(topk.info(), {
      k: 10,
      width: 64,
      depth: 5,
      decay: 0.92,
    });
    assert.ok(topk.byteSize >= 64 * 5 * 8);
  });

  it('tracks exact heavy hitters and counts when distinct items <= k', () => {
    const topk = new TopK(5, 32, 5, 0.9);

    const evicted1 = topk.add(['alpha', 'beta', 'alpha', 'gamma', 'alpha', 'beta']);
    assert.deepStrictEqual(evicted1, [null, null, null, null, null, null]);

    assert.deepStrictEqual(topk.query(['alpha', 'beta', 'gamma', 'delta']), [true, true, true, false]);
    assert.deepStrictEqual(topk.count(['alpha', 'beta', 'gamma', 'delta']), [3, 2, 1, 0]);
    assert.deepStrictEqual(topk.list(false), ['alpha', 'beta', 'gamma']);
    assert.deepStrictEqual(topk.list(true), [
      { item: 'alpha', count: 3 },
      { item: 'beta', count: 2 },
      { item: 'gamma', count: 1 },
    ]);
  });

  it('evicts lowest-count item from Min-Heap when a heavier item arrives', () => {
    const topk = new TopK(2, 64, 5, 0.9);

    assert.deepStrictEqual(topk.incrBy([
      { item: 'cold_item', increment: 3 },
      { item: 'warm_item', increment: 10 },
    ]), [null, null]);

    // Adding a heavy hitter must evict 'cold_item'
    const evicted = topk.incrBy([{ item: 'hot_item', increment: 25 }]);
    assert.deepStrictEqual(evicted, ['cold_item']);

    assert.deepStrictEqual(topk.query(['hot_item', 'warm_item', 'cold_item']), [true, true, false]);
    assert.deepStrictEqual(topk.list(true), [
      { item: 'hot_item', count: 25 },
      { item: 'warm_item', count: 10 },
    ]);
  });

  it('converges on true top-k elephants under Zipf-like skewed stream with noise', () => {
    const topk = new TopK(3, 128, 7, 0.9);

    const elephants = new Map<string, number>([
      ['endpoint:/api/auth', 450],
      ['endpoint:/api/orders', 320],
      ['endpoint:/api/search', 240],
    ]);

    for (const [item, freq] of elephants.entries()) {
      for (let i = 0; i < freq; i++) {
        topk.add([item]);
      }
    }

    // Inject 1,200 distinct low-frequency noise ("mice") items
    for (let i = 0; i < 1200; i++) {
      topk.add([`noise:req:${i}`]);
    }

    assert.deepStrictEqual(
      topk.query(['endpoint:/api/auth', 'endpoint:/api/orders', 'endpoint:/api/search']),
      [true, true, true]
    );

    const ranked = topk.list(false) as string[];
    assert.strictEqual(ranked.length, 3);
    assert.strictEqual(ranked[0], 'endpoint:/api/auth');
    assert.strictEqual(ranked[1], 'endpoint:/api/orders');
    assert.strictEqual(ranked[2], 'endpoint:/api/search');
  });

  it('validates incrBy positive integers and handles large increments safely', () => {
    const topk = new TopK(2, 16, 4, 0.9);

    assert.throws(
      () => topk.incrBy([{ item: 'bad', increment: 0 }]),
      /ERR increment must be a positive integer/
    );
    assert.throws(
      () => topk.incrBy([{ item: 'bad', increment: -3 }]),
      /ERR increment must be a positive integer/
    );

    topk.incrBy([
      { item: 'whale_a', increment: 500_000 },
      { item: 'whale_b', increment: 250_000 },
      { item: 'tiny_c', increment: 1 },
    ]);

    assert.deepStrictEqual(topk.query(['whale_a', 'whale_b', 'tiny_c']), [true, true, false]);
    assert.deepStrictEqual(topk.list(true), [
      { item: 'whale_a', count: 500_000 },
      { item: 'whale_b', count: 250_000 },
    ]);
  });
});
