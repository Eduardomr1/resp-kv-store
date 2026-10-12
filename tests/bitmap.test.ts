import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { BitmapHelper } from '../src/storage/bitmap.ts';
import { StorageEngine } from '../src/storage/engine.ts';
import { TCPServer } from '../src/server.ts';
import { RESPParser } from '../src/resp/parser.ts';
import net from 'node:net';

describe('Bitmap Operations & Engine Suite', () => {
  describe('BitmapHelper Unit Tests', () => {
    test('setBit expands buffer and tracks previous bit', () => {
      let buf = Buffer.alloc(0);

      // Bit 0 is highest bit of byte 0 (0x80)
      const res1 = BitmapHelper.setBit(buf, 0, 1);
      assert.strictEqual(res1.oldBit, 0);
      assert.strictEqual(res1.buffer.length, 1);
      assert.strictEqual(res1.buffer[0], 0x80);

      // Set bit 0 again, should return 1
      const res2 = BitmapHelper.setBit(res1.buffer, 0, 1);
      assert.strictEqual(res2.oldBit, 1);

      // Clear bit 0
      const res3 = BitmapHelper.setBit(res2.buffer, 0, 0);
      assert.strictEqual(res3.oldBit, 1);
      assert.strictEqual(res3.buffer[0], 0x00);

      // Set bit 15 (last bit of byte 1)
      const res4 = BitmapHelper.setBit(res3.buffer, 15, 1);
      assert.strictEqual(res4.buffer.length, 2);
      assert.strictEqual(res4.buffer[0], 0x00);
      assert.strictEqual(res4.buffer[1], 0x01);
    });

    test('getBit retrieves correct bit with big-endian indexing', () => {
      // Byte 0: 0b10100000 (0xA0) -> bit 0=1, bit 1=0, bit 2=1, rest=0
      const buf = Buffer.from([0xa0]);
      assert.strictEqual(BitmapHelper.getBit(buf, 0), 1);
      assert.strictEqual(BitmapHelper.getBit(buf, 1), 0);
      assert.strictEqual(BitmapHelper.getBit(buf, 2), 1);
      assert.strictEqual(BitmapHelper.getBit(buf, 3), 0);
      assert.strictEqual(BitmapHelper.getBit(buf, 7), 0);
      assert.strictEqual(BitmapHelper.getBit(buf, 100), 0); // Out of bounds returns 0
    });

    test('bitCount calculates popcount with byte slicing', () => {
      // 0xFF (8 bits set), 0x00 (0 bits), 0x0F (4 bits set)
      const buf = Buffer.from([0xff, 0x00, 0x0f]);
      assert.strictEqual(BitmapHelper.bitCount(buf), 12);
      assert.strictEqual(BitmapHelper.bitCount(buf, 0, 0), 8);
      assert.strictEqual(BitmapHelper.bitCount(buf, 1, 1), 0);
      assert.strictEqual(BitmapHelper.bitCount(buf, 2, 2), 4);
      assert.strictEqual(BitmapHelper.bitCount(buf, -1, -1), 4);
      assert.strictEqual(BitmapHelper.bitCount(buf, 1, -1), 4);
    });

    test('bitPos finds first occurrence of bit', () => {
      // 0x00 (all 0), 0x04 (0b00000100 -> bit 13 set)
      const buf = Buffer.from([0x00, 0x04]);
      assert.strictEqual(BitmapHelper.bitPos(buf, 1), 13);
      assert.strictEqual(BitmapHelper.bitPos(buf, 0), 0);
      assert.strictEqual(BitmapHelper.bitPos(buf, 1, 1), 13);

      // Key with all 1s
      const allOnes = Buffer.from([0xff, 0xff]);
      assert.strictEqual(BitmapHelper.bitPos(allOnes, 1), 0);
      // Looking for 0 in allOnes within range returns -1
      assert.strictEqual(BitmapHelper.bitPos(allOnes, 0, 0, 1), -1);
    });

    test('bitOp handles AND, OR, XOR, NOT with unequal length padding', () => {
      const b1 = Buffer.from([0x0f]); // 0000 1111
      const b2 = Buffer.from([0x33, 0xff]); // 0011 0011, 1111 1111

      const andRes = BitmapHelper.bitOp('AND', [b1, b2]);
      assert.strictEqual(andRes.length, 2);
      assert.strictEqual(andRes[0], 0x03); // 0000 0011
      assert.strictEqual(andRes[1], 0x00); // padded b1 is 0x00

      const orRes = BitmapHelper.bitOp('OR', [b1, b2]);
      assert.strictEqual(orRes.length, 2);
      assert.strictEqual(orRes[0], 0x3f); // 0011 1111
      assert.strictEqual(orRes[1], 0xff);

      const xorRes = BitmapHelper.bitOp('XOR', [b1, b2]);
      assert.strictEqual(xorRes.length, 2);
      assert.strictEqual(xorRes[0], 0x3c); // 0011 1100
      assert.strictEqual(xorRes[1], 0xff);

      const notRes = BitmapHelper.bitOp('NOT', [b1]);
      assert.strictEqual(notRes.length, 1);
      assert.strictEqual(notRes[0], 0xf0);
    });
  });

  describe('StorageEngine Bitmap Methods', () => {
    test('engine setbit, getbit, bitcount, bitpos lifecycle', () => {
      const engine = new StorageEngine();
      assert.strictEqual(engine.getbit('mykey', 10), 0);

      // Set bit 10 to 1
      assert.strictEqual(engine.setbit('mykey', 10, 1), 0);
      assert.strictEqual(engine.getbit('mykey', 10), 1);
      assert.strictEqual(engine.getbit('mykey', 9), 0);

      // Set bit 10 to 0
      assert.strictEqual(engine.setbit('mykey', 10, 0), 1);
      assert.strictEqual(engine.getbit('mykey', 10), 0);

      // Populate multiple bits
      engine.setbit('counter', 0, 1);
      engine.setbit('counter', 1, 1);
      engine.setbit('counter', 7, 1);
      engine.setbit('counter', 15, 1);

      assert.strictEqual(engine.bitcount('counter'), 4);
      assert.strictEqual(engine.bitcount('counter', 0, 0), 3);
      assert.strictEqual(engine.bitcount('counter', 1, 1), 1);

      assert.strictEqual(engine.bitpos('counter', 1), 0);
      assert.strictEqual(engine.bitpos('counter', 0), 2);
    });

    test('engine bitop combines multiple keys', () => {
      const engine = new StorageEngine();
      engine.set('k1', Buffer.from([0xaa])); // 1010 1010
      engine.set('k2', Buffer.from([0x55])); // 0101 0101

      const len = engine.bitop('OR', 'k_dest', 'k1', 'k2');
      assert.strictEqual(len, 1);
      const dest = engine.get('k_dest');
      assert.ok(dest);
      assert.strictEqual(dest[0], 0xff);
    });
  });

  describe('Server TCP Bitmap Integration', () => {
    test('SETBIT, GETBIT, BITCOUNT, BITPOS, BITOP over TCP', async () => {
      const server = new TCPServer({ port: 16398 });
      await server.start();

      const client = net.createConnection({ port: 16398, host: '127.0.0.1' });
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
        // SETBIT
        send(['SETBIT', 'bm', '7', '1']);
        await waitForResponse(1);
        assert.strictEqual(responses[0].type, 'integer');
        assert.strictEqual(responses[0].value, 0);

        // GETBIT
        send(['GETBIT', 'bm', '7']);
        await waitForResponse(2);
        assert.strictEqual(responses[1].type, 'integer');
        assert.strictEqual(responses[1].value, 1);

        send(['GETBIT', 'bm', '6']);
        await waitForResponse(3);
        assert.strictEqual(responses[2].type, 'integer');
        assert.strictEqual(responses[2].value, 0);

        // BITCOUNT
        send(['BITCOUNT', 'bm']);
        await waitForResponse(4);
        assert.strictEqual(responses[3].type, 'integer');
        assert.strictEqual(responses[3].value, 1);

        // BITPOS
        send(['BITPOS', 'bm', '1']);
        await waitForResponse(5);
        assert.strictEqual(responses[4].type, 'integer');
        assert.strictEqual(responses[4].value, 7);

        // BITOP
        send(['SET', 'bm2', '\x01']);
        await waitForResponse(6);

        send(['BITOP', 'XOR', 'bm_xor', 'bm', 'bm2']);
        await waitForResponse(7);
        assert.strictEqual(responses[6].type, 'integer');
        assert.strictEqual(responses[6].value, 1);

        // BITCOUNT on XOR result: (0x01 ^ 0x01) = 0x00 -> 0 bits
        send(['BITCOUNT', 'bm_xor']);
        await waitForResponse(8);
        assert.strictEqual(responses[7].type, 'integer');
        assert.strictEqual(responses[7].value, 0);
      } finally {
        client.end();
        await server.stop();
      }
    });
  });
});
