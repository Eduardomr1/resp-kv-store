import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { HyperLogLog } from '../src/storage/hll.ts';
import { StorageEngine } from '../src/storage/engine.ts';
import { TCPServer } from '../src/server.ts';
import { RESPParser } from '../src/resp/parser.ts';
import net from 'node:net';

describe('HyperLogLog Operations & Engine Suite', () => {
  describe('HyperLogLog Unit Tests', () => {
    test('initial count is 0 and toBuffer/fromBuffer roundtrips', () => {
      const hll = new HyperLogLog();
      assert.strictEqual(hll.count(), 0);

      hll.add('test-item');
      assert.strictEqual(hll.count(), 1);

      const buf = hll.toBuffer();
      assert.strictEqual(buf.length, 16384);

      const restored = HyperLogLog.fromBuffer(buf);
      assert.strictEqual(restored.count(), 1);
    });

    test('add returns true on register change, false on duplicate', () => {
      const hll = new HyperLogLog();
      const updated = hll.add('metric-1');
      assert.strictEqual(updated, true);

      // Adding the exact same item does not alter register maximum
      const duplicate = hll.add('metric-1');
      assert.strictEqual(duplicate, false);
      assert.strictEqual(hll.count(), 1);
    });

    test('cardinality estimation within theoretical error margin (< 2%)', () => {
      const hll = new HyperLogLog();
      const TOTAL_ITEMS = 2000;

      for (let i = 0; i < TOTAL_ITEMS; i++) {
        hll.add(`session-user-${i}`);
      }

      const estimated = hll.count();
      const errorRate = Math.abs(estimated - TOTAL_ITEMS) / TOTAL_ITEMS;

      // Redis HLL typical standard error for m=16384 is ~0.81%
      assert.ok(
        errorRate < 0.03,
        `Expected error < 3%, got ${(errorRate * 100).toFixed(2)}% (est: ${estimated}, true: ${TOTAL_ITEMS})`
      );
    });

    test('merge calculates union cardinality accurately', () => {
      const hll1 = new HyperLogLog();
      const hll2 = new HyperLogLog();

      for (let i = 0; i < 500; i++) {
        hll1.add(`item-${i}`);
      }
      for (let i = 250; i < 750; i++) {
        hll2.add(`item-${i}`);
      }

      // Total unique items: 750
      hll1.merge(hll2);
      const est = hll1.count();
      const errorRate = Math.abs(est - 750) / 750;
      assert.ok(
        errorRate < 0.04,
        `Expected error < 4%, got ${(errorRate * 100).toFixed(2)}% (est: ${est}, true: 750)`
      );
    });
  });

  describe('StorageEngine HLL Methods', () => {
    test('pfadd, pfcount, and pfmerge lifecycle', () => {
      const engine = new StorageEngine();

      assert.strictEqual(engine.pfcount('hll_none'), 0);

      // PFADD returns 1 when registers modified
      const res1 = engine.pfadd('h1', 'apple', 'banana', 'cherry');
      assert.strictEqual(res1, 1);
      assert.strictEqual(engine.pfcount('h1'), 3);

      // PFADD returns 0 if nothing modified
      const res2 = engine.pfadd('h1', 'apple');
      assert.strictEqual(res2, 0);

      // Second HLL
      engine.pfadd('h2', 'banana', 'date', 'elderberry');
      assert.strictEqual(engine.pfcount('h2'), 3);

      // Multi-key PFCOUNT merges on the fly without altering keys
      const multiCount = engine.pfcount('h1', 'h2');
      assert.strictEqual(multiCount, 5); // apple, banana, cherry, date, elderberry

      // PFMERGE into new key
      const merged = engine.pfmerge('h_union', 'h1', 'h2');
      assert.strictEqual(merged, true);
      assert.strictEqual(engine.pfcount('h_union'), 5);
    });
  });

  describe('Server TCP HyperLogLog Integration', () => {
    test('PFADD, PFCOUNT, PFMERGE over TCP', async () => {
      const server = new TCPServer({ port: 16400 });
      await server.start();

      const client = net.createConnection({ port: 16400, host: '127.0.0.1' });
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
        // PFADD
        send(['PFADD', 'hll_users', 'alice', 'bob', 'charlie']);
        await waitForResponse(1);
        assert.strictEqual(responses[0].type, 'integer');
        assert.strictEqual(responses[0].value, 1);

        // PFCOUNT
        send(['PFCOUNT', 'hll_users']);
        await waitForResponse(2);
        assert.strictEqual(responses[1].type, 'integer');
        assert.strictEqual(responses[1].value, 3);

        // PFADD with duplicate
        send(['PFADD', 'hll_users', 'alice']);
        await waitForResponse(3);
        assert.strictEqual(responses[2].type, 'integer');
        assert.strictEqual(responses[2].value, 0);

        // Second key
        send(['PFADD', 'hll_users2', 'david', 'alice']);
        await waitForResponse(4);

        // Multi-key PFCOUNT
        send(['PFCOUNT', 'hll_users', 'hll_users2']);
        await waitForResponse(5);
        assert.strictEqual(responses[4].type, 'integer');
        assert.strictEqual(responses[4].value, 4);

        // PFMERGE
        send(['PFMERGE', 'hll_merged', 'hll_users', 'hll_users2']);
        await waitForResponse(6);
        assert.strictEqual(responses[5].type, 'simple_string');
        assert.strictEqual(responses[5].value, 'OK');

        // Verify count of merged
        send(['PFCOUNT', 'hll_merged']);
        await waitForResponse(7);
        assert.strictEqual(responses[6].type, 'integer');
        assert.strictEqual(responses[6].value, 4);
      } finally {
        client.end();
        await server.stop();
      }
    });
  });
});
