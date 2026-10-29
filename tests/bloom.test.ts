import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { BloomFilter } from '../src/storage/bloom.ts';
import { StorageEngine } from '../src/storage/engine.ts';

describe('Bloom Filter Probabilistic Data Structure', () => {
  it('guarantees zero false negatives for inserted elements', () => {
    const bf = new BloomFilter(500, 0.01);
    const words = ['apple', 'banana', 'orange', 'strawberry', 'pineapple'];

    for (const w of words) {
      bf.add(w);
    }

    for (const w of words) {
      assert.equal(bf.exists(w), true, `Expected ${w} to exist in bloom filter`);
    }
  });

  it('maintains expected false positive rate under capacity', () => {
    const capacity = 1000;
    const targetErrorRate = 0.05;
    const bf = new BloomFilter(capacity, targetErrorRate);

    // Insert 500 items
    for (let i = 0; i < 500; i++) {
      bf.add(`item:${i}`);
    }

    // Test 1000 items that were never inserted
    let falsePositives = 0;
    const testCount = 1000;
    for (let i = 1000; i < 1000 + testCount; i++) {
      if (bf.exists(`item:${i}`)) {
        falsePositives++;
      }
    }

    const actualFpRate = falsePositives / testCount;
    // Should be reasonably close to target error rate (<= 0.08)
    assert.ok(actualFpRate <= 0.08, `FP rate was ${actualFpRate}`);
  });

  it('supports BF.RESERVE, BF.ADD, BF.EXISTS, and BF.INFO in StorageEngine', () => {
    const engine = new StorageEngine();
    assert.equal(engine.bfReserve('bf:test', 0.01, 1000), true);

    assert.equal(engine.bfAdd('bf:test', 'hello'), true);
    assert.equal(engine.bfAdd('bf:test', 'hello'), false); // already present
    assert.equal(engine.bfExists('bf:test', 'hello'), true);
    assert.equal(engine.bfExists('bf:test', 'world'), false);

    const info = engine.bfInfo('bf:test');
    assert.ok(info !== null);
    assert.equal(info.capacity, 1000);
    assert.equal(info.numberOfItemsInserted, 1);
  });
});
