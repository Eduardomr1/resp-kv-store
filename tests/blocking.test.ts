import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { BlockingManager } from '../src/blocking/manager.ts';
import { RESP_CONSTANTS } from '../src/resp/serializer.ts';
import { StorageEngine } from '../src/storage/engine.ts';
import type { ClientSink } from '../src/blocking/types.ts';

class MockClientSink extends EventEmitter implements ClientSink {
  public id: string;
  public messages: Buffer[] = [];

  constructor(id: string) {
    super();
    this.id = id;
  }

  public write(data: Buffer | string): boolean {
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data, 'utf8');
    this.messages.push(buf);
    return true;
  }

  public clear(): void {
    this.messages = [];
  }
}

test('BlockingManager: Immediate pop when list already contains elements', async () => {
  const engine = new StorageEngine();
  const manager = new BlockingManager({ storage: engine });
  const client = new MockClientSink('client-1');

  try {
    // Populate list: ['elem1', 'elem2']
    engine.rpush('mylist', 'elem1', 'elem2');

    // 1. BLPOP: pops from left ('elem1')
    const resLeft = await manager.block(client, ['mylist'], 0, 'left');
    assert.ok(resLeft);
    assert.equal(
      resLeft.toString('utf8'),
      '*2\r\n$6\r\nmylist\r\n$5\r\nelem1\r\n'
    );
    assert.equal(engine.llen('mylist'), 1);

    // 2. BRPOP: pops from right ('elem2')
    const resRight = await manager.block(client, ['mylist'], 0, 'right');
    assert.ok(resRight);
    assert.equal(
      resRight.toString('utf8'),
      '*2\r\n$6\r\nmylist\r\n$5\r\nelem2\r\n'
    );
    assert.equal(engine.llen('mylist'), 0);

    // 3. Multi-key immediate pop: checks keys in order and pops from first non-empty list
    engine.rpush('thirdKey', 'targetValue');
    const multiRes = await manager.block(
      client,
      ['emptyKey1', 'emptyKey2', 'thirdKey', 'fourthKey'],
      0,
      'left'
    );
    assert.ok(multiRes);
    assert.equal(
      multiRes.toString('utf8'),
      '*2\r\n$8\r\nthirdKey\r\n$11\r\ntargetValue\r\n'
    );
    assert.equal(engine.llen('thirdKey'), 0);
  } finally {
    manager.clear();
    engine.close();
  }
});

test('BlockingManager: Asynchronous unblocking when a push arrives on a blocked key (FIFO order)', async () => {
  const manager = new BlockingManager();
  const client1 = new MockClientSink('c1');
  const client2 = new MockClientSink('c2');
  const client3 = new MockClientSink('c3');

  try {
    // Clients block in order: c1, then c2, then c3 on 'tasks'
    const p1 = manager.block(client1, ['tasks'], 0, 'left');
    const p2 = manager.block(client2, ['tasks'], 0, 'left');
    const p3 = manager.block(client3, ['tasks'], 0, 'left');

    assert.equal(manager.getBlockedCount('tasks'), 3);
    assert.equal(manager.hasBlockedClients('tasks'), true);

    // Push 1: Should wake up c1 (oldest)
    const consumed1 = manager.signalPush('tasks', Buffer.from('job-1'), 'left');
    assert.equal(consumed1, true);

    const res1 = await p1;
    assert.ok(res1);
    assert.equal(
      res1.toString('utf8'),
      '*2\r\n$5\r\ntasks\r\n$5\r\njob-1\r\n'
    );
    assert.equal(manager.getBlockedCount('tasks'), 2);

    // Push 2: Should wake up c2
    const consumed2 = manager.signalPush('tasks', Buffer.from('job-2'), 'left');
    assert.equal(consumed2, true);

    const res2 = await p2;
    assert.ok(res2);
    assert.equal(
      res2.toString('utf8'),
      '*2\r\n$5\r\ntasks\r\n$5\r\njob-2\r\n'
    );
    assert.equal(manager.getBlockedCount('tasks'), 1);

    // Push 3: Should wake up c3
    const consumed3 = manager.signalPush('tasks', Buffer.from('job-3'), 'left');
    assert.equal(consumed3, true);

    const res3 = await p3;
    assert.ok(res3);
    assert.equal(
      res3.toString('utf8'),
      '*2\r\n$5\r\ntasks\r\n$5\r\njob-3\r\n'
    );
    assert.equal(manager.getBlockedCount('tasks'), 0);
    assert.equal(manager.hasBlockedClients('tasks'), false);

    // Push 4: No clients waiting on 'tasks' -> returns false
    const consumed4 = manager.signalPush('tasks', Buffer.from('job-4'), 'left');
    assert.equal(consumed4, false);
  } finally {
    manager.clear();
  }
});

test('BlockingManager: Timeout expiration returning null array *-1\\r\\n', async () => {
  const manager = new BlockingManager();
  const client = new MockClientSink('client-timeout');

  try {
    const startTime = Date.now();
    // Block with 0.05s (50ms) timeout
    const p = manager.block(client, ['emptyList'], 0.05, 'left');

    assert.equal(manager.getBlockedCount('emptyList'), 1);
    assert.equal(manager.getPendingTimerCount(), 1);

    const res = await p;
    const elapsedMs = Date.now() - startTime;

    assert.ok(elapsedMs >= 40, `Elapsed time was ${elapsedMs}ms, expected >= 40ms`);
    assert.ok(res);
    assert.deepEqual(res, RESP_CONSTANTS.NULL_ARRAY);
    assert.equal(res.toString('utf8'), '*-1\r\n');

    // Unregistered after timeout
    assert.equal(manager.getBlockedCount('emptyList'), 0);
    assert.equal(manager.getPendingTimerCount(), 0);

    // Subsequent push does not find the timed-out client
    const consumed = manager.signalPush('emptyList', Buffer.from('tooLate'), 'left');
    assert.equal(consumed, false);
  } finally {
    manager.clear();
  }
});

test('BlockingManager: Multi-key blocking (waking up on the first key that receives an item)', async () => {
  const manager = new BlockingManager();
  const client1 = new MockClientSink('client-multi');

  try {
    // Client waits on three keys: ['alpha', 'beta', 'gamma']
    const p1 = manager.block(client1, ['alpha', 'beta', 'gamma'], 0, 'left');

    assert.equal(manager.getBlockedCount('alpha'), 1);
    assert.equal(manager.getBlockedCount('beta'), 1);
    assert.equal(manager.getBlockedCount('gamma'), 1);

    // Push arrives on the second key 'beta'
    const consumed = manager.signalPush('beta', Buffer.from('hello-beta'), 'left');
    assert.equal(consumed, true);

    const res = await p1;
    assert.ok(res);
    assert.equal(
      res.toString('utf8'),
      '*2\r\n$4\r\nbeta\r\n$10\r\nhello-beta\r\n'
    );

    // Client must be unregistered from ALL keys
    assert.equal(manager.hasBlockedClients('alpha'), false);
    assert.equal(manager.hasBlockedClients('beta'), false);
    assert.equal(manager.hasBlockedClients('gamma'), false);
    assert.equal(manager.getBlockedCount(), 0);

    // Later pushes to alpha or gamma must return false
    assert.equal(manager.signalPush('alpha', Buffer.from('x'), 'left'), false);
    assert.equal(manager.signalPush('gamma', Buffer.from('y'), 'left'), false);
  } finally {
    manager.clear();
  }
});

test('BlockingManager: Overlapping multi-key clients preserve FIFO per key', async () => {
  const manager = new BlockingManager();
  const clientA = new MockClientSink('clientA');
  const clientB = new MockClientSink('clientB');

  try {
    // Client A blocks on [K1, K2]
    const pA = manager.block(clientA, ['K1', 'K2'], 0, 'left');
    // Client B blocks on [K2, K3]
    const pB = manager.block(clientB, ['K2', 'K3'], 0, 'left');

    // On K2, Client A is older than Client B
    const consumedA = manager.signalPush('K2', Buffer.from('val-for-A'), 'left');
    assert.equal(consumedA, true);

    const resA = await pA;
    assert.ok(resA);
    assert.equal(resA.toString('utf8'), '*2\r\n$2\r\nK2\r\n$9\r\nval-for-A\r\n');

    // Client A should be cleared from K1 as well
    assert.equal(manager.hasBlockedClients('K1'), false);

    // Client B is still waiting on K2 and K3
    assert.equal(manager.hasBlockedClients('K2'), true);
    assert.equal(manager.hasBlockedClients('K3'), true);

    // Push to K3 wakes Client B
    const consumedB = manager.signalPush('K3', Buffer.from('val-for-B'), 'left');
    assert.equal(consumedB, true);

    const resB = await pB;
    assert.ok(resB);
    assert.equal(resB.toString('utf8'), '*2\r\n$2\r\nK3\r\n$9\r\nval-for-B\r\n');

    // All cleared
    assert.equal(manager.getBlockedCount(), 0);
  } finally {
    manager.clear();
  }
});

test('BlockingManager: Client disconnect cleanup without dangling timers (manual removeClient)', async () => {
  const manager = new BlockingManager();
  const client = new MockClientSink('client-disconnect');

  try {
    // Block with 10s timeout
    const p = manager.block(client, ['k1', 'k2'], 10, 'left');

    assert.equal(manager.getPendingTimerCount(), 1);
    assert.equal(manager.getBlockedCount('k1'), 1);
    assert.equal(manager.getBlockedCount('k2'), 1);

    // Simulate socket disconnect / error cleanup
    manager.removeClient(client);

    const res = await p;
    // Disconnected client request resolves to null
    assert.equal(res, null);

    // Verifies all timers are cleared and registrations purged
    assert.equal(manager.getPendingTimerCount(), 0);
    assert.equal(manager.hasBlockedClients('k1'), false);
    assert.equal(manager.hasBlockedClients('k2'), false);
    assert.equal(manager.getBlockedCount(), 0);

    // Pushes do not consume anything
    assert.equal(manager.signalPush('k1', Buffer.from('data'), 'left'), false);
    assert.equal(manager.signalPush('k2', Buffer.from('data'), 'left'), false);
  } finally {
    manager.clear();
  }
});

test('BlockingManager: Client disconnect cleanup on EventEmitter socket close event', async () => {
  const manager = new BlockingManager();
  const socket = new MockClientSink('socket-1');

  try {
    const p = manager.block(socket, ['listA'], 15, 'left');

    assert.equal(manager.getPendingTimerCount(), 1);
    assert.equal(manager.hasBlockedClients('listA'), true);

    // Socket emits close event
    socket.emit('close');

    const res = await p;
    assert.equal(res, null);

    assert.equal(manager.getPendingTimerCount(), 0);
    assert.equal(manager.hasBlockedClients('listA'), false);
  } finally {
    manager.clear();
  }
});

test('BlockingManager: Binary data handling in signalPush', async () => {
  const manager = new BlockingManager();
  const client = new MockClientSink('client-bin');

  try {
    const p = manager.block(client, ['binList'], 0, 'left');

    // Binary payload with null bytes and high bytes
    const binaryData = Buffer.from([0x00, 0x01, 0xfe, 0xff, 0x00, 0x42]);
    const consumed = manager.signalPush('binList', binaryData, 'left');
    assert.equal(consumed, true);

    const res = await p;
    assert.ok(res);

    // Verify RESP prefix: *2\r\n$7\r\nbinList\r\n$6\r\n<6 bytes>\r\n
    const expectedHeader = Buffer.from('*2\r\n$7\r\nbinList\r\n$6\r\n', 'ascii');
    assert.deepEqual(res.subarray(0, expectedHeader.length), expectedHeader);
    const payload = res.subarray(expectedHeader.length, expectedHeader.length + 6);
    assert.deepEqual(payload, binaryData);
  } finally {
    manager.clear();
  }
});

test('BlockingManager: High concurrency FIFO order across multiple keys', async () => {
  const manager = new BlockingManager();
  const CLIENT_COUNT = 30;
  const clients: MockClientSink[] = [];
  const promises: Promise<Buffer | null>[] = [];

  try {
    for (let i = 0; i < CLIENT_COUNT; i++) {
      const client = new MockClientSink(`client-${i}`);
      clients.push(client);
      promises.push(manager.block(client, ['concurrentQueue'], 0, 'left'));
    }

    assert.equal(manager.getBlockedCount('concurrentQueue'), CLIENT_COUNT);

    // Push in order 0..29
    for (let i = 0; i < CLIENT_COUNT; i++) {
      const consumed = manager.signalPush(
        'concurrentQueue',
        Buffer.from(`item-${i}`),
        'left'
      );
      assert.equal(consumed, true);
    }

    const results = await Promise.all(promises);
    for (let i = 0; i < CLIENT_COUNT; i++) {
      const expected = `*2\r\n$15\r\nconcurrentQueue\r\n$${`item-${i}`.length}\r\nitem-${i}\r\n`;
      assert.equal(results[i]?.toString('utf8'), expected);
    }

    assert.equal(manager.getBlockedCount('concurrentQueue'), 0);
  } finally {
    manager.clear();
  }
});

test('BlockingManager: timeoutSeconds === 0 blocks indefinitely until pushed', async () => {
  const manager = new BlockingManager();
  const client = new MockClientSink('c-indefinite');

  try {
    const p = manager.block(client, ['waitForever'], 0, 'left');
    assert.equal(manager.getPendingTimerCount(), 0); // No timer started
    assert.equal(manager.getBlockedCount('waitForever'), 1);

    // Wait a brief delay to verify it doesn't expire
    await new Promise((r) => setTimeout(r, 60));
    assert.equal(manager.getBlockedCount('waitForever'), 1);

    // Signal push unblocks it
    manager.signalPush('waitForever', Buffer.from('finallyPushed'), 'left');
    const res = await p;
    assert.ok(res);
    assert.equal(res.toString('utf8'), '*2\r\n$11\r\nwaitForever\r\n$13\r\nfinallyPushed\r\n');
  } finally {
    manager.clear();
  }
});

test('BlockingManager: End-to-end Producer-Consumer integration with StorageEngine', async () => {
  const engine = new StorageEngine();
  const manager = new BlockingManager({ storage: engine });
  const client = new MockClientSink('consumer-1');

  try {
    // Helper function mimicking server push logic:
    // If a blocked client consumes it via signalPush, done; otherwise store in engine.
    const push = (key: string, val: string) => {
      const buf = Buffer.from(val, 'utf8');
      const consumed = manager.signalPush(key, buf, 'left');
      if (!consumed) {
        engine.lpush(key, buf);
      }
    };

    // 1. Push when NO consumer is blocked -> item saved in engine
    push('queue', 'item-stored');
    assert.equal(engine.llen('queue'), 1);

    // 2. Consumer calls block on queue -> immediately pops 'item-stored' from engine
    const res1 = await manager.block(client, ['queue'], 0, 'left');
    assert.ok(res1);
    assert.equal(res1.toString('utf8'), '*2\r\n$5\r\nqueue\r\n$11\r\nitem-stored\r\n');
    assert.equal(engine.llen('queue'), 0);

    // 3. Queue is now empty. Consumer calls block on queue -> blocks asynchronously
    const p2 = manager.block(client, ['queue'], 0, 'left');
    assert.equal(manager.hasBlockedClients('queue'), true);

    // 4. Producer pushes item-live -> directly consumed by blocked consumer, NOT stored in engine
    push('queue', 'item-live');
    const res2 = await p2;
    assert.ok(res2);
    assert.equal(res2.toString('utf8'), '*2\r\n$5\r\nqueue\r\n$9\r\nitem-live\r\n');
    assert.equal(engine.llen('queue'), 0); // Engine still has 0 items!
  } finally {
    manager.clear();
    engine.close();
  }
});

