import { describe, it } from 'node:test';
import assert from 'node:assert';
import fs from 'node:fs';
import path from 'node:path';
import net from 'node:net';
import { TopK } from '../src/storage/topk.ts';
import { StorageEngine } from '../src/storage/engine.ts';
import { WrongTypeError } from '../src/storage/types.ts';
import { TCPServer } from '../src/server.ts';
import { RESPParser } from '../src/resp/parser.ts';

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

describe('StorageEngine Top-K Lifecycle Suite', () => {
  it('manages TOPK.RESERVE, ADD, INCRBY, QUERY, COUNT, LIST, INFO and memory tracking', () => {
    const engine = new StorageEngine();
    try {
      assert.strictEqual(engine.topkReserve('hits:2026', 3, 16, 5, 0.9), true);
      assert.throws(() => engine.topkReserve('hits:2026', 3), /ERR item exists/);
      assert.strictEqual(engine.type('hits:2026'), 'TopK-');
      assert.strictEqual(engine.objectEncoding('hits:2026'), 'topk');

      const initialMem = engine.memoryUsage('hits:2026');
      assert.ok(initialMem !== null && initialMem > 0);

      assert.deepStrictEqual(engine.topkAdd('hits:2026', ['a', 'b', 'c', 'a']), [null, null, null, null]);
      assert.deepStrictEqual(engine.topkIncrBy('hits:2026', [{ item: 'd', increment: 5 }]), ['b']);
      assert.deepStrictEqual(engine.topkQuery('hits:2026', ['d', 'a', 'c', 'b']), [true, true, true, false]);
      assert.deepStrictEqual(engine.topkCount('hits:2026', ['d', 'a', 'c']), [5, 2, 1]);
      assert.deepStrictEqual(engine.topkList('hits:2026', false), ['d', 'a', 'c']);
      assert.deepStrictEqual(engine.topkInfo('hits:2026'), { k: 3, width: 16, depth: 5, decay: 0.9 });

      const expandedMem = engine.memoryUsage('hits:2026');
      assert.ok(expandedMem !== null && expandedMem >= initialMem);

      assert.strictEqual(engine.del('hits:2026'), 1);
      assert.strictEqual(engine.stats().memoryEstimateBytes, 0);
    } finally {
      engine.close();
    }
  });

  it('enforces WRONGTYPE, missing key errors, and TTL expiration', async () => {
    const engine = new StorageEngine();
    try {
      assert.throws(() => engine.topkAdd('missing', ['x']), /ERR key does not exist/);
      assert.throws(() => engine.topkQuery('missing', ['x']), /ERR key does not exist/);
      assert.throws(() => engine.topkList('missing'), /ERR key does not exist/);
      assert.throws(() => engine.topkInfo('missing'), /ERR key does not exist/);

      engine.set('str_key', 'hello');
      assert.throws(() => engine.topkAdd('str_key', ['x']), WrongTypeError);
      assert.throws(() => engine.topkIncrBy('str_key', [{ item: 'x', increment: 1 }]), WrongTypeError);
      assert.throws(() => engine.topkQuery('str_key', ['x']), WrongTypeError);
      assert.throws(() => engine.topkCount('str_key', ['x']), WrongTypeError);
      assert.throws(() => engine.topkList('str_key'), WrongTypeError);
      assert.throws(() => engine.topkInfo('str_key'), WrongTypeError);

      engine.topkReserve('ttl_topk', 2, 8, 5, 0.9);
      engine.topkAdd('ttl_topk', ['k1']);
      assert.strictEqual(engine.pexpire('ttl_topk', 30), true);

      await new Promise((r) => setTimeout(r, 60));
      assert.strictEqual(engine.type('ttl_topk'), 'none');
      assert.throws(() => engine.topkQuery('ttl_topk', ['k1']), /ERR key does not exist/);
    } finally {
      engine.close();
    }
  });
});

describe('StorageEngine Top-K AOF Persistence & Compaction Suite', () => {
  const aofPath = path.resolve('./test_topk_appendonly.aof');
  const cleanup = () => {
    if (fs.existsSync(aofPath)) {
      try { fs.unlinkSync(aofPath); } catch {}
    }
  };

  it('recovers Top-K state after restart and compacts via AOF rewrite', async () => {
    cleanup();
    const engine1 = new StorageEngine({
      aofEnabled: true,
      aofFilePath: aofPath,
      aofFsyncPolicy: 'always',
    });

    try {
      engine1.topkReserve('trending', 2, 16, 5, 0.9);
      for (let i = 0; i < 20; i++) {
        engine1.topkAdd('trending', ['video_a']);
      }
      engine1.topkIncrBy('trending', [
        { item: 'video_b', increment: 12 },
        { item: 'video_c', increment: 3 },
      ]);
      const bloatedSize = fs.statSync(aofPath).size;

      await engine1.getAOF()!.rewrite(engine1);
      const compactedSize = fs.statSync(aofPath).size;
      assert.ok(compactedSize < bloatedSize, `Expected ${compactedSize} < ${bloatedSize}`);
    } finally {
      engine1.close();
    }

    const engine2 = new StorageEngine({
      aofEnabled: true,
      aofFilePath: aofPath,
      aofFsyncPolicy: 'always',
    });

    try {
      assert.strictEqual(engine2.type('trending'), 'TopK-');
      assert.deepStrictEqual(engine2.topkInfo('trending'), {
        k: 2,
        width: 16,
        depth: 5,
        decay: 0.9,
      });
      assert.deepStrictEqual(engine2.topkQuery('trending', ['video_a', 'video_b', 'video_c']), [
        true,
        true,
        false,
      ]);
      assert.deepStrictEqual(engine2.topkList('trending', true), [
        { item: 'video_a', count: 20 },
        { item: 'video_b', count: 12 },
      ]);
    } finally {
      engine2.close();
      cleanup();
    }
  });
});

describe('Server TCP Top-K Integration Suite', () => {
  it('executes full suite of TOPK commands, introspection, and transactions over live TCP socket', async () => {
    const port = 16435;
    const server = new TCPServer({ port });
    await server.start();

    const createClient = () => {
      const socket = net.createConnection({ port, host: '127.0.0.1' });
      const parser = new RESPParser();
      const responses: any[] = [];
      socket.on('data', (chunk) => {
        parser.feed(chunk);
        for (const item of parser.parse()) {
          responses.push(item);
        }
      });
      const send = async (cmds: string[]) => {
        const target = responses.length + 1;
        let payload = `*${cmds.length}\r\n`;
        for (const c of cmds) {
          payload += `$${Buffer.byteLength(c)}\r\n${c}\r\n`;
        }
        socket.write(payload);
        while (responses.length < target) {
          await new Promise((r) => setTimeout(r, 5));
        }
        return responses[target - 1];
      };
      return { socket, send };
    };

    const client1 = createClient();
    const client2 = createClient();

    try {
      // 1. TOPK.RESERVE
      const r1 = await client1.send(['TOPK.RESERVE', 'api_hits', '3', '32', '5', '0.9']);
      assert.strictEqual(r1.type, 'simple_string');
      assert.strictEqual(r1.value, 'OK');

      // Duplicate reserve error
      const rDup = await client1.send(['TOPK.RESERVE', 'api_hits', '3']);
      assert.strictEqual(rDup.type, 'error');
      assert.match(rDup.value, /ERR item exists/);

      // Invalid reserve parameters
      const rBadDecay = await client1.send(['TOPK.RESERVE', 'bad_tk', '3', '8', '7', '1.5']);
      assert.strictEqual(rBadDecay.type, 'error');

      // 2. TOPK.ADD (null bulk strings when heap not full)
      const r2 = await client1.send(['TOPK.ADD', 'api_hits', '/home', '/login', '/cart', '/home']);
      assert.strictEqual(r2.type, 'array');
      assert.deepStrictEqual(
        r2.value.map((v: any) => (v.type === 'null' ? null : v.value.toString('utf8'))),
        [null, null, null, null]
      );

      // 3. TOPK.INCRBY (evicts '/login' or '/cart' with count 1)
      const r3 = await client1.send(['TOPK.INCRBY', 'api_hits', '/checkout', '10', '/home', '5']);
      assert.strictEqual(r3.type, 'array');
      const evictedItem = r3.value[0].type === 'null' ? null : r3.value[0].value.toString('utf8');
      assert.ok(evictedItem === '/login' || evictedItem === '/cart');
      assert.strictEqual(r3.value[1].type, 'null');

      // 4. TOPK.QUERY & TOPK.COUNT
      const rQuery = await client1.send(['TOPK.QUERY', 'api_hits', '/checkout', '/home', '/missing']);
      assert.deepStrictEqual(
        rQuery.value.map((v: any) => v.value),
        [1, 1, 0]
      );

      const rCount = await client1.send(['TOPK.COUNT', 'api_hits', '/checkout', '/home']);
      assert.deepStrictEqual(
        rCount.value.map((v: any) => v.value),
        [10, 7]
      );

      // 5. TOPK.LIST & TOPK.LIST WITHCOUNT
      const rList = await client1.send(['TOPK.LIST', 'api_hits']);
      assert.strictEqual(rList.value[0].value.toString('utf8'), '/checkout');
      assert.strictEqual(rList.value[1].value.toString('utf8'), '/home');

      const rListWithCount = await client1.send(['TOPK.LIST', 'api_hits', 'WITHCOUNT']);
      assert.strictEqual(rListWithCount.value.length, 6);
      assert.strictEqual(rListWithCount.value[0].value.toString('utf8'), '/checkout');
      assert.strictEqual(rListWithCount.value[1].value, 10);
      assert.strictEqual(rListWithCount.value[2].value.toString('utf8'), '/home');
      assert.strictEqual(rListWithCount.value[3].value, 7);

      const rListSyntaxErr = await client1.send(['TOPK.LIST', 'api_hits', 'BADOPT']);
      assert.strictEqual(rListSyntaxErr.type, 'error');

      // 6. TOPK.INFO
      const rInfo = await client1.send(['TOPK.INFO', 'api_hits']);
      const infoMap = rInfo.value.map((v: any) =>
        Buffer.isBuffer(v.value) ? v.value.toString('utf8') : String(v.value)
      );
      assert.deepStrictEqual(infoMap, ['k', '3', 'width', '32', 'depth', '5', 'decay', '0.9']);

      // 7. Introspection: TYPE, OBJECT ENCODING, MEMORY USAGE
      const rType = await client1.send(['TYPE', 'api_hits']);
      assert.strictEqual(rType.value, 'TopK-');

      const rEnc = await client1.send(['OBJECT', 'ENCODING', 'api_hits']);
      assert.strictEqual(rEnc.value.toString('utf8'), 'topk');

      const rMem = await client1.send(['MEMORY', 'USAGE', 'api_hits']);
      assert.strictEqual(rMem.type, 'integer');
      assert.ok(rMem.value > 0);

      // 8. Transactions & WATCH OCC integration
      assert.strictEqual((await client1.send(['WATCH', 'api_hits'])).value, 'OK');
      assert.strictEqual((await client1.send(['MULTI'])).value, 'OK');
      assert.strictEqual((await client1.send(['TOPK.ADD', 'api_hits', '/profile'])).value, 'QUEUED');

      // Concurrent mutation by client2 invalidates watched key
      await client2.send(['TOPK.INCRBY', 'api_hits', '/checkout', '2']);

      const rExecAborted = await client1.send(['EXEC']);
      assert.strictEqual(rExecAborted.type, 'null'); // Null array *-1\r\n on OCC conflict
    } finally {
      client1.socket.destroy();
      client2.socket.destroy();
      await server.stop();
    }
  });
});
