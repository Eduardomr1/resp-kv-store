import test from 'node:test';
import assert from 'node:assert/strict';
import { SkipList } from '../src/storage/skiplist.ts';

test('SkipList: insertion and sorted ordering', () => {
  const sl = new SkipList();

  sl.insert('charlie', 30);
  sl.insert('alice', 10);
  sl.insert('bob', 20);

  assert.equal(sl.size, 3);

  const range = sl.getRange(0, -1);
  assert.deepEqual(range, [
    { member: 'alice', score: 10 },
    { member: 'bob', score: 20 },
    { member: 'charlie', score: 30 },
  ]);
});

test('SkipList: tie-breaking by lexicographical member order', () => {
  const sl = new SkipList();

  sl.insert('zebra', 100);
  sl.insert('apple', 100);
  sl.insert('mango', 100);

  const range = sl.getRange(0, -1);
  assert.deepEqual(range, [
    { member: 'apple', score: 100 },
    { member: 'mango', score: 100 },
    { member: 'zebra', score: 100 },
  ]);
});

test('SkipList: deletion and rank range slicing', () => {
  const sl = new SkipList();

  for (let i = 0; i < 20; i++) {
    sl.insert(`item:${i}`, i * 10);
  }

  assert.equal(sl.size, 20);

  // Sub-ranges
  const firstThree = sl.getRange(0, 2);
  assert.equal(firstThree.length, 3);
  assert.equal(firstThree[0].member, 'item:0');
  assert.equal(firstThree[2].member, 'item:2');

  const lastTwo = sl.getRange(-2, -1);
  assert.equal(lastTwo.length, 2);
  assert.equal(lastTwo[0].member, 'item:18');
  assert.equal(lastTwo[1].member, 'item:19');

  // Deletions
  assert.equal(sl.delete('item:5', 50), true);
  assert.equal(sl.delete('nonexistent', 999), false);
  assert.equal(sl.size, 19);

  // Verify item:5 no longer in range
  const all = sl.getRange(0, -1);
  assert.equal(all.some((x) => x.member === 'item:5'), false);
});
