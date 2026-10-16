import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import net from 'node:net';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { StorageEngine } from '../src/storage/engine.ts';
import { TCPServer } from '../src/server.ts';
import { RESPParser } from '../src/resp/parser.ts';
import { RESPSerializer } from '../src/resp/serializer.ts';

describe('Extended Hash, List, Set, and Sorted Set Commands', () => {
  describe('StorageEngine Unit Suite', () => {
    test('Hash: HSETNX, HINCRBY, HSTRLEN, and HRANDFIELD', () => {
      const engine = new StorageEngine();
      try {
        assert.equal(engine.hsetnx('profile', 'handle', 'eduardo'), true);
        assert.equal(engine.hsetnx('profile', 'handle', 'other'), false);
        assert.equal(engine.hget('profile', 'handle')?.toString(), 'eduardo');
        assert.equal(engine.hstrlen('profile', 'handle'), 7);
        assert.equal(engine.hstrlen('profile', 'missing'), 0);

        assert.equal(engine.hincrby('profile', 'karma', 10), 10);
        assert.equal(engine.hincrby('profile', 'karma', -3), 7);
        assert.throws(() => engine.hincrby('profile', 'handle', 1), /not an integer/);

        // HRANDFIELD single, distinct positive count, and negative count with replacement
        engine.hset('profile', 'role', 'engineer');
        const single = engine.hrandfield('profile') as string;
        assert.ok(['handle', 'karma', 'role'].includes(single));

        const distinctTwo = engine.hrandfield('profile', 2) as string[];
        assert.equal(distinctTwo.length, 2);
        assert.notEqual(distinctTwo[0], distinctTwo[1]);

        const withVals = engine.hrandfield('profile', 10, true) as (string | Buffer)[];
        assert.equal(withVals.length, 6); // 3 fields * 2 (field + value)

        const repeated = engine.hrandfield('profile', -5, false) as string[];
        assert.equal(repeated.length, 5);
      } finally {
        engine.close();
      }
    });

    test('List: LINDEX, LSET, LTRIM, LMOVE, and AOF replay durability', () => {
      const tmpAof = path.join(os.tmpdir(), `resp_kv_list_ext_${Date.now()}_${process.pid}.aof`);
      const engine = new StorageEngine({ aofEnabled: true, aofFilePath: tmpAof, aofFsyncPolicy: 'always' });
      try {
        engine.rpush('items', 'a', 'b', 'c', 'd', 'e');
        assert.equal(engine.lindex('items', 0)?.toString(), 'a');
        assert.equal(engine.lindex('items', -1)?.toString(), 'e');
        assert.equal(engine.lindex('items', 99), null);

        engine.lset('items', 1, 'B');
        engine.lset('items', -1, 'E');
        assert.throws(() => engine.lset('items', 50, 'x'), /index out of range/);
        assert.throws(() => engine.lset('no_list', 0, 'x'), /no such key/);

        engine.ltrim('items', 1, 3);
        assert.deepEqual(
          engine.lrange('items', 0, -1).map((b) => b.toString()),
          ['B', 'c', 'd']
        );

        // Circular rotation with LMOVE on same list (RIGHT -> LEFT)
        assert.equal(engine.lmove('items', 'items', 'RIGHT', 'LEFT')?.toString(), 'd');
        assert.deepEqual(
          engine.lrange('items', 0, -1).map((b) => b.toString()),
          ['d', 'B', 'c']
        );

        // Cross-list LMOVE
        assert.equal(engine.lmove('items', 'archive', 'LEFT', 'RIGHT')?.toString(), 'd');
        assert.deepEqual(
          engine.lrange('archive', 0, -1).map((b) => b.toString()),
          ['d']
        );

        engine.close();

        // Recover from AOF and confirm exact list states
        const recovered = new StorageEngine({ aofEnabled: true, aofFilePath: tmpAof, aofFsyncPolicy: 'no' });
        try {
          assert.deepEqual(
            recovered.lrange('items', 0, -1).map((b) => b.toString()),
            ['B', 'c']
          );
          assert.deepEqual(
            recovered.lrange('archive', 0, -1).map((b) => b.toString()),
            ['d']
          );
        } finally {
          recovered.close();
        }
      } finally {
        engine.close();
        if (fs.existsSync(tmpAof)) fs.unlinkSync(tmpAof);
      }
    });

    test('Set: algebra (SUNION, SINTER, SDIFF + *STORE), SMISMEMBER, SRANDMEMBER, SPOP, SMOVE', () => {
      const engine = new StorageEngine();
      try {
        engine.sadd('s1', 'a', 'b', 'c', 'd');
        engine.sadd('s2', 'c', 'd', 'e');
        engine.sadd('s3', 'a', 'c', 'f');

        assert.deepEqual(engine.smismember('s1', ['a', 'z', 'c']), [1, 0, 1]);
        assert.deepEqual(engine.sunion('s1', 's2').sort(), ['a', 'b', 'c', 'd', 'e']);
        assert.deepEqual(engine.sinter('s1', 's2', 's3').sort(), ['c']);
        assert.deepEqual(engine.sdiff('s1', 's2', 's3').sort(), ['b']);

        assert.equal(engine.sunionstore('dst_u', 's2', 's3'), 5);
        assert.equal(engine.sinterstore('dst_i', 's1', 's2'), 2);
        assert.equal(engine.sdiffstore('dst_d', 's1', 's2'), 2);
        assert.deepEqual(engine.smembers('dst_i').sort(), ['c', 'd']);

        assert.equal(engine.smove('s1', 's2', 'a'), true);
        assert.equal(engine.sismember('s1', 'a'), false);
        assert.equal(engine.sismember('s2', 'a'), true);

        const sampled = engine.srandmember('s2', 2) as string[];
        assert.equal(sampled.length, 2);

        const popped = engine.spop('dst_d', 2) as string[];
        assert.equal(popped.length, 2);
        assert.equal(engine.exists('dst_d'), 0);
      } finally {
        engine.close();
      }
    });

    test('ZSet: ZINCRBY, ZRANK/ZREVRANK, ZRANGEBYSCORE, ZCOUNT, ZPOPMIN/MAX, and ZMPOP', () => {
      const engine = new StorageEngine();
      try {
        engine.zadd('scores', 10, 'alpha');
        engine.zadd('scores', 20, 'beta');
        engine.zadd('scores', 30, 'gamma');
        engine.zadd('scores', 40, 'delta');

        assert.equal(engine.zincrby('scores', 5, 'beta'), 25);
        assert.equal(engine.zrank('scores', 'beta'), 1);
        assert.equal(engine.zrevrank('scores', 'beta'), 2);
        assert.equal(engine.zrank('scores', 'unknown'), null);

        assert.equal(engine.zcount('scores', '(10', '30'), 2);
        assert.deepEqual(
          engine.zrangebyscore('scores', '10', '(40', { offset: 1, count: 2 }).map((x) => x.member),
          ['beta', 'gamma']
        );
        assert.deepEqual(
          engine.zrangebyscore('scores', '10', '40', { reverse: true, offset: 0, count: 2 }).map((x) => x.member),
          ['delta', 'gamma']
        );

        assert.deepEqual(engine.zpopmin('scores', 1), [{ member: 'alpha', score: 10 }]);
        assert.deepEqual(engine.zpopmax('scores', 1), [{ member: 'delta', score: 40 }]);

        const mpop = engine.zmpop(['missing_zset', 'scores'], 'MAX', 2);
        assert.deepEqual(mpop, {
          key: 'scores',
          elements: [
            { member: 'gamma', score: 30 },
            { member: 'beta', score: 25 },
          ],
        });
        assert.equal(engine.exists('scores'), 0);
      } finally {
        engine.close();
      }
    });
  });

  describe('TCP Server Integration Suite', () => {
    test('Dispatches extended Hash, List, Set, and ZSet commands with WATCH & BLPOP coordination', async () => {
      const PORT = 16420;
      const server = new TCPServer({ port: PORT, host: '127.0.0.1' });
      await server.start();

      const c1 = net.createConnection({ port: PORT, host: '127.0.0.1' });
      const c2 = net.createConnection({ port: PORT, host: '127.0.0.1' });
      await Promise.all([
        new Promise((r) => c1.once('connect', r)),
        new Promise((r) => c2.once('connect', r)),
      ]);

      const execCmd = (sock: net.Socket, parts: string[]): Promise<any> => {
        const parser = new RESPParser();
        return new Promise((resolve) => {
          const onData = (chunk: Buffer) => {
            parser.feed(chunk);
            const parsed = parser.parse();
            if (parsed.length > 0) {
              sock.off('data', onData);
              resolve(parsed[0]);
            }
          };
          sock.on('data', onData);
          sock.write(RESPSerializer.encodeArray(parts));
        });
      };

      try {
        // Hash extensions
        let res = await execCmd(c1, ['HSETNX', 'h:1', 'title', 'architect']);
        assert.equal(res.value, 1);
        res = await execCmd(c1, ['HSETNX', 'h:1', 'title', 'override']);
        assert.equal(res.value, 0);
        res = await execCmd(c1, ['HSTRLEN', 'h:1', 'title']);
        assert.equal(res.value, 9);
        res = await execCmd(c1, ['HINCRBY', 'h:1', 'views', '42']);
        assert.equal(res.value, 42);
        res = await execCmd(c1, ['HRANDFIELD', 'h:1', '2', 'WITHVALUES']);
        assert.equal(res.type, 'array');
        assert.equal(res.value.length, 4);

        // List extensions + LMOVE waking blocked BLPOP client
        await execCmd(c1, ['RPUSH', 'list:src', 'one', 'two', 'three']);
        res = await execCmd(c1, ['LINDEX', 'list:src', '-1']);
        assert.equal(res.value.toString(), 'three');
        res = await execCmd(c1, ['LSET', 'list:src', '1', 'TWO']);
        assert.equal(res.value, 'OK');
        res = await execCmd(c1, ['LTRIM', 'list:src', '0', '1']);
        assert.equal(res.value, 'OK');

        const blockedPopPromise = execCmd(c2, ['BLPOP', 'list:dst', '2']);
        await new Promise((r) => setTimeout(r, 25));
        res = await execCmd(c1, ['LMOVE', 'list:src', 'list:dst', 'RIGHT', 'LEFT']);
        assert.equal(res.value.toString(), 'TWO');

        const woken = await blockedPopPromise;
        assert.equal(woken.type, 'array');
        assert.equal(woken.value[0].value.toString(), 'list:dst');
        assert.equal(woken.value[1].value.toString(), 'TWO');

        // Set extensions
        await execCmd(c1, ['SADD', 'set:a', 'm1', 'm2', 'm3']);
        await execCmd(c1, ['SADD', 'set:b', 'm2', 'm3', 'm4']);
        res = await execCmd(c1, ['SMISMEMBER', 'set:a', 'm1', 'm4']);
        assert.deepEqual(
          res.value.map((x: any) => x.value),
          [1, 0]
        );
        res = await execCmd(c1, ['SINTERSTORE', 'set:inter', 'set:a', 'set:b']);
        assert.equal(res.value, 2);
        res = await execCmd(c1, ['SMOVE', 'set:a', 'set:b', 'm1']);
        assert.equal(res.value, 1);
        res = await execCmd(c1, ['SPOP', 'set:inter', '1']);
        assert.equal(res.type, 'array');
        assert.equal(res.value.length, 1);

        // ZSet extensions + WATCH isolation on ZMPOP
        await execCmd(c1, ['ZADD', 'z:board', '100', 'p1', '200', 'p2', '300', 'p3']);
        res = await execCmd(c1, ['ZINCRBY', 'z:board', '25.5', 'p1']);
        assert.equal(res.value.toString(), '125.5');
        res = await execCmd(c1, ['ZRANK', 'z:board', 'p1']);
        assert.equal(res.value, 0);
        res = await execCmd(c1, ['ZREVRANK', 'z:board', 'p1']);
        assert.equal(res.value, 2);
        res = await execCmd(c1, ['ZCOUNT', 'z:board', '120', '250']);
        assert.equal(res.value, 2);

        res = await execCmd(c1, ['ZREVRANGEBYSCORE', 'z:board', '+inf', '150', 'WITHSCORES', 'LIMIT', '0', '2']);
        assert.deepEqual(
          res.value.map((x: any) => x.value.toString()),
          ['p3', '300', 'p2', '200']
        );

        // Client 1 watches z:untouched while Client 2 runs ZMPOP across z:untouched (empty) and z:board
        await execCmd(c1, ['WATCH', 'z:untouched']);
        res = await execCmd(c2, ['ZMPOP', '2', 'z:untouched', 'z:board', 'MIN', 'COUNT', '2']);
        assert.equal(res.type, 'array');
        assert.equal(res.value[0].value.toString(), 'z:board');
        assert.equal(res.value[1].value.length, 2);

        await execCmd(c1, ['MULTI']);
        await execCmd(c1, ['SET', 'z:untouched', 'safe']);
        res = await execCmd(c1, ['EXEC']);
        assert.equal(res.type, 'array');
        assert.equal(res.value.length, 1);
      } finally {
        c1.destroy();
        c2.destroy();
        await server.stop();
      }
    });
  });
});
