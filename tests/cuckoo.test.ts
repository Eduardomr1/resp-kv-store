import { describe, it } from 'node:test';
import assert from 'node:assert';
import net from 'node:net';
import { CuckooFilter } from '../src/storage/cuckoo.ts';
import { StorageEngine } from '../src/storage/engine.ts';
import { TCPServer } from '../src/server.ts';
import { RESPParser } from '../src/resp/parser.ts';

describe('CuckooFilter Engine & Algorithm Suite', () => {
  it('validates capacity and bucket configuration', () => {
    assert.throws(() => new CuckooFilter(0, 2), /ERR capacity must be > 0/);
    assert.throws(() => new CuckooFilter(-50, 2), /ERR capacity must be > 0/);
    assert.throws(() => new CuckooFilter(100, 0), /ERR bucket size must be > 0/);

    const cf = new CuckooFilter(2000, 4, 500, 1);
    assert.strictEqual(cf.capacity, 2000);
    assert.strictEqual(cf.bucketSize, 4);
    assert.strictEqual(cf.numBuckets, 512); // ceil(2000 / 4) = 500 -> 512
    assert.strictEqual(cf.count, 0);

    const info = cf.info();
    assert.strictEqual(info.numBuckets, 512);
    assert.strictEqual(info.bucketSize, 4);
    assert.strictEqual(info.numItemsInserted, 0);
  });

  it('performs insertion, lookup, and dynamic deletion preserving set invariants', () => {
    const cf = new CuckooFilter(500, 2);
    const keys = ['user:1', 'user:2', 'user:3', 'user:4'];

    for (const k of keys) {
      assert.strictEqual(cf.exists(k), false);
      assert.strictEqual(cf.add(k), true);
      assert.strictEqual(cf.exists(k), true);
    }
    assert.strictEqual(cf.count, 4);

    // Dynamic deletion
    assert.strictEqual(cf.delete('user:2'), true);
    assert.strictEqual(cf.exists('user:2'), false);
    assert.strictEqual(cf.count, 3);

    // Deleting non-existent returns false
    assert.strictEqual(cf.delete('user:2'), false);
    assert.strictEqual(cf.delete('nonexistent'), false);
    assert.strictEqual(cf.count, 3);
  });

  it('handles duplicate counting and CF.ADDNX idempotency', () => {
    const cf = new CuckooFilter(200, 4);

    assert.strictEqual(cf.addNx('token_abc'), true);
    assert.strictEqual(cf.addNx('token_abc'), false);
    assert.strictEqual(cf.countOccurrences('token_abc'), 1);

    cf.add('token_abc');
    assert.strictEqual(cf.countOccurrences('token_abc'), 2);

    cf.delete('token_abc');
    assert.strictEqual(cf.countOccurrences('token_abc'), 1);
  });
});

describe('StorageEngine Cuckoo Lifecycle Suite', () => {
  it('manages CF.RESERVE, CF.ADD, CF.EXISTS, CF.DEL in engine', () => {
    const engine = new StorageEngine();

    assert.strictEqual(engine.cfReserve('cf_key', 1000, 2, 500, 1), true);
    assert.throws(() => engine.cfReserve('cf_key', 1000), /ERR item exists/);

    assert.strictEqual(engine.cfAdd('cf_key', 'alpha'), true);
    assert.strictEqual(engine.cfAddNx('cf_key', 'alpha'), false);
    assert.strictEqual(engine.cfAddNx('cf_key', 'beta'), true);

    assert.strictEqual(engine.cfExists('cf_key', 'alpha'), true);
    assert.strictEqual(engine.cfExists('cf_key', 'beta'), true);
    assert.strictEqual(engine.cfExists('cf_key', 'gamma'), false);

    assert.deepStrictEqual(engine.cfMExists('cf_key', ['alpha', 'beta', 'gamma']), [true, true, false]);

    // Deletion
    assert.strictEqual(engine.cfDel('cf_key', 'alpha'), true);
    assert.strictEqual(engine.cfExists('cf_key', 'alpha'), false);
    assert.strictEqual(engine.cfDel('cf_key', 'alpha'), false);

    // Count
    assert.strictEqual(engine.cfCount('cf_key', 'beta'), 1);
    assert.strictEqual(engine.cfCount('cf_key', 'gamma'), 0);

    // Info
    const info = engine.cfInfo('cf_key');
    assert.strictEqual(info.numItemsInserted, 2);
    assert.strictEqual(info.numItemsDeleted, 1);
  });

  it('supports CF.INSERT with automatic creation and NOCREATE flag', () => {
    const engine = new StorageEngine();

    // Auto-create
    const res1 = engine.cfInsert('auto_cf', ['item1', 'item2']);
    assert.deepStrictEqual(res1, [true, true]);
    assert.strictEqual(engine.cfExists('auto_cf', 'item1'), true);

    // NOCREATE on non-existent key throws
    assert.throws(() => engine.cfInsert('missing_cf', ['x'], 1000, true), /ERR not found/);
  });
});

describe('Server TCP Cuckoo Filter Integration Suite', () => {
  it('executes full suite of CF commands over live RESP TCP socket', async () => {
    const port = 16430;
    const server = new TCPServer({ port });
    await server.start();

    const client = net.createConnection({ port, host: '127.0.0.1' });
    const parser = new RESPParser();
    const responses: any[] = [];

    client.on('data', (chunk) => {
      parser.feed(chunk);
      const items = parser.parse();
      for (const item of items) {
        responses.push(item);
      }
    });

    const send = (cmds: string[]) => {
      let payload = `*${cmds.length}\r\n`;
      for (const c of cmds) {
        payload += `$${Buffer.byteLength(c)}\r\n${c}\r\n`;
      }
      client.write(payload);
    };

    const waitForResponse = async (targetCount: number): Promise<void> => {
      while (responses.length < targetCount) {
        await new Promise((r) => setTimeout(r, 10));
      }
    };

    try {
      // 1. CF.RESERVE
      send(['CF.RESERVE', 'tcp_cf', '500', 'BUCKETSIZE', '2', 'MAXITERATIONS', '500']);
      await waitForResponse(1);
      assert.strictEqual(responses[0].type, 'simple_string');
      assert.strictEqual(responses[0].value, 'OK');

      // 2. CF.ADD
      send(['CF.ADD', 'tcp_cf', 'apple']);
      await waitForResponse(2);
      assert.strictEqual(responses[1].type, 'integer');
      assert.strictEqual(responses[1].value, 1);

      // 3. CF.EXISTS
      send(['CF.EXISTS', 'tcp_cf', 'apple']);
      await waitForResponse(3);
      assert.strictEqual(responses[2].type, 'integer');
      assert.strictEqual(responses[2].value, 1);

      send(['CF.EXISTS', 'tcp_cf', 'orange']);
      await waitForResponse(4);
      assert.strictEqual(responses[3].type, 'integer');
      assert.strictEqual(responses[3].value, 0);

      // 4. CF.ADDNX
      send(['CF.ADDNX', 'tcp_cf', 'apple']);
      await waitForResponse(5);
      assert.strictEqual(responses[4].type, 'integer');
      assert.strictEqual(responses[4].value, 0); // already exists

      send(['CF.ADDNX', 'tcp_cf', 'banana']);
      await waitForResponse(6);
      assert.strictEqual(responses[5].type, 'integer');
      assert.strictEqual(responses[5].value, 1); // newly added

      // 5. CF.MEXISTS
      send(['CF.MEXISTS', 'tcp_cf', 'apple', 'banana', 'cherry']);
      await waitForResponse(7);
      assert.strictEqual(responses[6].type, 'array');
      assert.deepStrictEqual(
        responses[6].value.map((v: any) => v.value),
        [1, 1, 0],
      );

      // 6. CF.INSERT
      send(['CF.INSERT', 'inserted_cf', 'CAPACITY', '1000', 'ITEMS', 'itemA', 'itemB']);
      await waitForResponse(8);
      assert.strictEqual(responses[7].type, 'array');
      assert.deepStrictEqual(
        responses[7].value.map((v: any) => v.value),
        [1, 1],
      );

      // 7. CF.COUNT
      send(['CF.COUNT', 'tcp_cf', 'banana']);
      await waitForResponse(9);
      assert.strictEqual(responses[8].type, 'integer');
      assert.strictEqual(responses[8].value, 1);

      // 8. CF.DEL
      send(['CF.DEL', 'tcp_cf', 'banana']);
      await waitForResponse(10);
      assert.strictEqual(responses[9].type, 'integer');
      assert.strictEqual(responses[9].value, 1);

      send(['CF.EXISTS', 'tcp_cf', 'banana']);
      await waitForResponse(11);
      assert.strictEqual(responses[10].type, 'integer');
      assert.strictEqual(responses[10].value, 0);

      // 9. CF.INFO
      send(['CF.INFO', 'tcp_cf']);
      await waitForResponse(12);
      assert.strictEqual(responses[11].type, 'array');
      const infoArr = responses[11].value.map((v: any) =>
        Buffer.isBuffer(v.value) ? v.value.toString('utf8') : String(v.value),
      );
      assert.strictEqual(infoArr.includes('Size'), true);
      assert.strictEqual(infoArr.includes('Number of buckets'), true);
      assert.strictEqual(infoArr.includes('Number of items inserted'), true);
    } finally {
      client.destroy();
      await server.stop();
    }
  });
});
