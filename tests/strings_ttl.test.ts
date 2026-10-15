import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import net from 'node:net';
import { StorageEngine } from '../src/storage/engine.ts';
import { TCPServer } from '../src/server.ts';
import { RESPParser } from '../src/resp/parser.ts';
import { RESPSerializer } from '../src/resp/serializer.ts';

describe('Extended String, Atomic Counter, and TTL Primitives', () => {
  describe('StorageEngine Unit Suite', () => {
    test('INCR, INCRBY, DECR, DECRBY handle 64-bit bounds and preserve TTL', () => {
      const engine = new StorageEngine();
      try {
        assert.equal(engine.incr('counter'), 1);
        assert.equal(engine.incrby('counter', 9), 10);
        assert.equal(engine.decr('counter'), 9);
        assert.equal(engine.decrby('counter', 4), 5);

        // Preserve TTL across counter increments
        engine.pexpire('counter', 60000);
        const pttlBefore = engine.pttl('counter');
        assert.ok(pttlBefore > 50000);
        assert.equal(engine.incr('counter'), 6);
        assert.ok(engine.pttl('counter') > 50000);

        // 64-bit signed integer boundary
        engine.set('big', '9223372036854775806');
        assert.equal(engine.incr('big'), 9223372036854775807n);
        assert.throws(() => engine.incr('big'), /overflow/);

        // Reject non-integer string values
        engine.set('notint', '123abc');
        assert.throws(() => engine.incr('notint'), /not an integer/);
      } finally {
        engine.close();
      }
    });

    test('MSET, MSETNX, APPEND, STRLEN, GETDEL, GETEX, and EXPIRETIME', () => {
      const engine = new StorageEngine();
      try {
        engine.mset([
          ['k1', 'hello'],
          ['k2', 'world'],
        ]);
        assert.equal(engine.get('k1')?.toString(), 'hello');
        assert.equal(engine.get('k2')?.toString(), 'world');

        // MSETNX fails atomically if any key exists
        assert.equal(
          engine.msetnx([
            ['k2', 'override'],
            ['k3', 'new'],
          ]),
          false
        );
        assert.equal(engine.get('k2')?.toString(), 'world');
        assert.equal(engine.get('k3'), null);

        // MSETNX succeeds when all keys are new
        assert.equal(
          engine.msetnx([
            ['k3', 'alpha'],
            ['k4', 'beta'],
          ]),
          true
        );
        assert.equal(engine.get('k3')?.toString(), 'alpha');
        assert.equal(engine.get('k4')?.toString(), 'beta');

        // APPEND & STRLEN
        assert.equal(engine.append('k1', ' there'), 11);
        assert.equal(engine.strlen('k1'), 11);
        assert.equal(engine.strlen('missing'), 0);

        // GETDEL
        assert.equal(engine.getdel('k4')?.toString(), 'beta');
        assert.equal(engine.get('k4'), null);
        assert.equal(engine.getdel('k4'), null);

        // GETEX & EXPIRETIME / PEXPIRETIME
        assert.equal(engine.expiretime('k1'), -1);
        assert.equal(engine.expiretime('missing'), -2);

        const futureMs = Date.now() + 120000;
        assert.equal(engine.getex('k1', { pxat: futureMs })?.toString(), 'hello there');
        assert.equal(engine.pexpiretime('k1'), futureMs);
        assert.equal(engine.expiretime('k1'), Math.floor(futureMs / 1000));

        assert.equal(engine.getex('k1', { persist: true })?.toString(), 'hello there');
        assert.equal(engine.pexpiretime('k1'), -1);
      } finally {
        engine.close();
      }
    });
  });

  describe('TCP Server Integration Suite', () => {
    test('SET GET/EXAT/PXAT, counters, MSETNX WATCH isolation, GETEX, GETDEL over TCP', async () => {
      const PORT = 16410;
      const server = new TCPServer({ port: PORT, host: '127.0.0.1' });
      await server.start();

      const client1 = net.createConnection({ port: PORT, host: '127.0.0.1' });
      const client2 = net.createConnection({ port: PORT, host: '127.0.0.1' });
      await Promise.all([
        new Promise((r) => client1.once('connect', r)),
        new Promise((r) => client2.once('connect', r)),
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
        // SET with GET on new key returns null bulk string AND still mutates
        let res = await execCmd(client1, ['SET', 'token', 'v1', 'GET']);
        assert.equal(res.type, 'null');

        // SET with GET on existing key returns old value
        res = await execCmd(client1, ['SET', 'token', 'v2', 'GET', 'EX', '60']);
        assert.equal(res.type, 'bulk_string');
        assert.equal(res.value.toString(), 'v1');

        // Conflicting TTL flags return syntax error
        res = await execCmd(client1, ['SET', 'token', 'v3', 'EX', '10', 'PX', '1000']);
        assert.equal(res.type, 'error');

        // INCRBY / DECRBY over TCP
        res = await execCmd(client1, ['INCRBY', 'visits', '25']);
        assert.equal(res.value, 25);
        res = await execCmd(client1, ['DECRBY', 'visits', '10']);
        assert.equal(res.value, 15);

        // WATCH + failed MSETNX should NOT abort client1's transaction
        await execCmd(client1, ['WATCH', 'new_watched']);
        res = await execCmd(client2, ['MSETNX', 'visits', '999', 'new_watched', 'abc']);
        assert.equal(res.value, 0);

        await execCmd(client1, ['MULTI']);
        await execCmd(client1, ['SET', 'new_watched', 'committed']);
        res = await execCmd(client1, ['EXEC']);
        assert.equal(res.type, 'array');
        assert.equal(res.value.length, 1);

        // GETEX & GETDEL
        const targetSec = Math.floor(Date.now() / 1000) + 300;
        res = await execCmd(client1, ['GETEX', 'new_watched', 'EXAT', targetSec.toString()]);
        assert.equal(res.value.toString(), 'committed');

        res = await execCmd(client1, ['EXPIRETIME', 'new_watched']);
        assert.equal(res.value, targetSec);

        res = await execCmd(client1, ['GETDEL', 'new_watched']);
        assert.equal(res.value.toString(), 'committed');
        res = await execCmd(client1, ['GET', 'new_watched']);
        assert.equal(res.type, 'null');
      } finally {
        client1.destroy();
        client2.destroy();
        await server.stop();
      }
    });
  });
});
