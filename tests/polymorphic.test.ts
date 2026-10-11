import test from 'node:test';
import assert from 'node:assert/strict';
import { StorageEngine } from '../src/storage/engine.ts';
import { WrongTypeError } from '../src/storage/types.ts';

test('Polymorphic: Hashes operations', () => {
  const engine = new StorageEngine();
  try {
    assert.equal(engine.hset('user:1', 'name', 'Alice'), 1);
    assert.equal(engine.hset('user:1', 'age', '30'), 1);
    assert.equal(engine.hset('user:1', 'name', 'Alicia'), 0); // update

    assert.equal(engine.type('user:1'), 'hash');
    assert.equal(engine.hget('user:1', 'name')?.toString(), 'Alicia');
    assert.equal(engine.hget('user:1', 'age')?.toString(), '30');
    assert.equal(engine.hlen('user:1'), 2);

    assert.deepEqual(engine.hmget('user:1', ['name', 'missing']), [
      Buffer.from('Alicia'),
      null,
    ]);

    assert.equal(engine.hexists('user:1', 'name'), true);
    assert.equal(engine.hexists('user:1', 'gender'), false);

    assert.equal(engine.hdel('user:1', 'age', 'nonexistent'), 1);
    assert.equal(engine.hlen('user:1'), 1);
  } finally {
    engine.close();
  }
});

test('Polymorphic: Lists operations', () => {
  const engine = new StorageEngine();
  try {
    assert.equal(engine.rpush('queue', 'job1', 'job2'), 2);
    assert.equal(engine.lpush('queue', 'job0'), 3);

    assert.equal(engine.type('queue'), 'list');
    assert.equal(engine.llen('queue'), 3);

    const range = engine.lrange('queue', 0, -1).map((b) => b.toString());
    assert.deepEqual(range, ['job0', 'job1', 'job2']);

    assert.equal(engine.lpop('queue')?.toString(), 'job0');
    assert.equal(engine.rpop('queue')?.toString(), 'job2');
    assert.equal(engine.llen('queue'), 1);
  } finally {
    engine.close();
  }
});

test('Polymorphic: Sets operations', () => {
  const engine = new StorageEngine();
  try {
    assert.equal(engine.sadd('tags', 'redis', 'nosql', 'fast', 'redis'), 3);
    assert.equal(engine.type('tags'), 'set');
    assert.equal(engine.scard('tags'), 3);

    assert.equal(engine.sismember('tags', 'redis'), true);
    assert.equal(engine.sismember('tags', 'sql'), false);

    assert.equal(engine.srem('tags', 'fast'), 1);
    assert.equal(engine.scard('tags'), 2);

    const members = engine.smembers('tags');
    assert.ok(members.includes('redis'));
    assert.ok(members.includes('nosql'));
  } finally {
    engine.close();
  }
});

test('Polymorphic: Sorted Sets (ZSET) operations', () => {
  const engine = new StorageEngine();
  try {
    assert.equal(engine.zadd('leaderboard', 100, 'alice'), 1);
    assert.equal(engine.zadd('leaderboard', 250, 'charlie'), 1);
    assert.equal(engine.zadd('leaderboard', 180, 'bob'), 1);
    assert.equal(engine.zadd('leaderboard', 300, 'alice'), 0); // score update

    assert.equal(engine.type('leaderboard'), 'zset');
    assert.equal(engine.zcard('leaderboard'), 3);

    assert.equal(engine.zscore('leaderboard', 'bob'), 180);
    assert.equal(engine.zscore('leaderboard', 'alice'), 300);

    const rankAsc = engine.zrange('leaderboard', 0, -1);
    assert.deepEqual(rankAsc, ['bob', 'charlie', 'alice']);

    const withScores = engine.zrangeWithScores('leaderboard', 0, 1);
    assert.deepEqual(withScores, [
      { member: 'bob', score: 180 },
      { member: 'charlie', score: 250 },
    ]);

    assert.equal(engine.zrem('leaderboard', 'charlie'), 1);
    assert.equal(engine.zcard('leaderboard'), 2);
  } finally {
    engine.close();
  }
});

test('Polymorphic: WRONGTYPE enforcement', () => {
  const engine = new StorageEngine();
  try {
    engine.set('str_key', 'plain string');

    assert.throws(() => engine.hset('str_key', 'field', 'val'), WrongTypeError);
    assert.throws(() => engine.lpush('str_key', 'val'), WrongTypeError);
    assert.throws(() => engine.sadd('str_key', 'val'), WrongTypeError);
    assert.throws(() => engine.zadd('str_key', 10, 'val'), WrongTypeError);

    engine.hset('hash_key', 'f', 'v');
    assert.throws(() => engine.get('hash_key'), WrongTypeError);
  } finally {
    engine.close();
  }
});
