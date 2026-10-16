import test from 'node:test';
import assert from 'node:assert/strict';
import { SkipList, parseScoreBound } from '../src/storage/skiplist.ts';

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

test('SkipList: O(log N) order-statistic getRank consistency across insertions and deletions', () => {
  const sl = new SkipList();
  const entries: Array<{ member: string; score: number }> = [];

  for (let i = 0; i < 200; i++) {
    const score = (i * 37) % 50; // Deliberate score ties
    const member = `member_${String(i).padStart(3, '0')}`;
    sl.insert(member, score);
    entries.push({ member, score });
  }

  // Delete every 4th entry
  for (let i = 0; i < entries.length; i += 4) {
    sl.delete(entries[i].member, entries[i].score);
  }

  const remaining = entries
    .filter((_, idx) => idx % 4 !== 0)
    .sort((a, b) => (a.score !== b.score ? a.score - b.score : a.member < b.member ? -1 : 1));

  assert.equal(sl.size, remaining.length);
  for (let expectedRank = 0; expectedRank < remaining.length; expectedRank++) {
    const item = remaining[expectedRank];
    assert.equal(sl.getRank(item.member, item.score), expectedRank);
  }
  assert.equal(sl.getRank('missing_member', 10), null);
});

test('SkipList: getRangeByScore and countByScore with inclusive/exclusive bounds and LIMIT', () => {
  const sl = new SkipList();
  for (let i = 1; i <= 10; i++) {
    sl.insert(`m${i}`, i * 10); // 10..100
  }

  // Inclusive [20, 50] -> m2, m3, m4, m5
  const inc = sl.getRangeByScore(parseScoreBound('20'), parseScoreBound('50'));
  assert.deepEqual(
    inc.map((x) => x.member),
    ['m2', 'm3', 'm4', 'm5']
  );

  // Exclusive (20, (50 -> m3, m4
  const exc = sl.getRangeByScore(parseScoreBound('(20'), parseScoreBound('(50'));
  assert.deepEqual(
    exc.map((x) => x.member),
    ['m3', 'm4']
  );
  assert.equal(sl.countByScore(parseScoreBound('(20'), parseScoreBound('80')), 6);

  // Infinite bounds + reverse + pagination LIMIT
  const pagedRev = sl.getRangeByScore(parseScoreBound('-inf'), parseScoreBound('+inf'), {
    reverse: true,
    offset: 2,
    count: 3,
  });
  assert.deepEqual(
    pagedRev.map((x) => x.member),
    ['m8', 'm7', 'm6']
  );
});
