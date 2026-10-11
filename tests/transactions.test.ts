import test from 'node:test';
import assert from 'node:assert/strict';
import { TransactionManager, TransactionSession } from '../src/transactions/manager.ts';
import { StorageEngine } from '../src/storage/engine.ts';
import { RESPSerializer, RESP_CONSTANTS } from '../src/resp/serializer.ts';

test('Transactions: Basic MULTI / QUEUED / EXEC execution', () => {
  const manager = new TransactionManager();
  const session = manager.createSession();
  const engine = new StorageEngine();

  try {
    assert.equal(session.isMulti, false);
    assert.equal(session.queue.length, 0);

    // 1. MULTI
    const multiRes = manager.multi(session);
    assert.equal(multiRes.toString(), '+OK\r\n');
    assert.equal(session.isMulti, true);

    // 2. Queue commands
    const q1 = manager.queueCommand(session, {
      commandName: 'SET',
      rawArgs: ['key1', 'val1'],
      execute: () => {
        engine.set('key1', 'val1');
        return RESP_CONSTANTS.OK;
      },
    });
    assert.equal(q1.toString(), '+QUEUED\r\n');

    const q2 = manager.queueCommand(session, {
      commandName: 'GET',
      rawArgs: ['key1'],
      execute: () => {
        const val = engine.get('key1');
        return RESPSerializer.encodeBulkString(val);
      },
    });
    assert.equal(q2.toString(), '+QUEUED\r\n');

    assert.equal(session.queue.length, 2);
    // Ensure commands have not executed yet
    assert.equal(engine.get('key1'), null);

    // 3. EXEC
    const execRes = manager.exec(session);
    assert.equal(session.isMulti, false);
    assert.equal(session.queue.length, 0);

    // Result should be RESP Array of 2 items: +OK\r\n and $4\r\nval1\r\n
    const expected = Buffer.concat([
      Buffer.from('*2\r\n', 'ascii'),
      RESP_CONSTANTS.OK,
      RESPSerializer.encodeBulkString('val1'),
    ]);
    assert.deepEqual(execRes, expected);

    // Verify engine state after exec
    assert.equal(engine.get('key1')?.toString(), 'val1');
  } finally {
    engine.close();
  }
});

test('Transactions: Empty MULTI / EXEC returns *0\r\n', () => {
  const manager = new TransactionManager();
  const session = manager.createSession();

  const multiRes = manager.multi(session);
  assert.equal(multiRes.toString(), '+OK\r\n');

  const execRes = manager.exec(session);
  assert.equal(execRes.toString(), '*0\r\n');
  assert.equal(session.isMulti, false);
});

test('Transactions: DISCARD behavior and state reset', () => {
  const manager = new TransactionManager();
  const session = manager.createSession();
  const engine = new StorageEngine();

  try {
    let executed = false;
    manager.watch(session, 'mykey');
    assert.equal(session.watchedKeys.size, 1);

    manager.multi(session);
    assert.equal(session.isMulti, true);

    manager.queueCommand(session, {
      commandName: 'SET',
      rawArgs: ['mykey', 'discardedVal'],
      execute: () => {
        executed = true;
        engine.set('mykey', 'discardedVal');
        return RESP_CONSTANTS.OK;
      },
    });

    assert.equal(session.queue.length, 1);

    // DISCARD
    const discardRes = manager.discard(session);
    assert.equal(discardRes.toString(), '+OK\r\n');

    // Verify state was reset
    assert.equal(session.isMulti, false);
    assert.equal(session.queue.length, 0);
    assert.equal(session.watchedKeys.size, 0);
    assert.equal(session.isDirty, false);

    // Verify queued command never executed
    assert.equal(executed, false);
    assert.equal(engine.get('mykey'), null);
  } finally {
    engine.close();
  }
});

test('Transactions: WATCH optimistic locking success (when watched key is not modified)', () => {
  const manager = new TransactionManager();
  const session = manager.createSession();
  const engine = new StorageEngine();

  try {
    // Watch key before multi
    const watchRes = manager.watch(session, 'account:a', 'account:b');
    assert.equal(watchRes.toString(), '+OK\r\n');
    assert.equal(session.watchedKeys.has('account:a'), true);
    assert.equal(session.watchedKeys.has('account:b'), true);
    assert.equal(session.isDirty, false);

    manager.multi(session);

    manager.queueCommand(session, {
      commandName: 'SET',
      rawArgs: ['account:a', '100'],
      execute: () => {
        engine.set('account:a', '100');
        return RESP_CONSTANTS.OK;
      },
    });

    manager.queueCommand(session, {
      commandName: 'SET',
      rawArgs: ['account:b', '200'],
      execute: () => {
        engine.set('account:b', '200');
        return RESP_CONSTANTS.OK;
      },
    });

    const execRes = manager.exec(session);
    const expected = Buffer.concat([
      Buffer.from('*2\r\n', 'ascii'),
      RESP_CONSTANTS.OK,
      RESP_CONSTANTS.OK,
    ]);
    assert.deepEqual(execRes, expected);
    assert.equal(engine.get('account:a')?.toString(), '100');
    assert.equal(engine.get('account:b')?.toString(), '200');
    assert.equal(session.watchedKeys.size, 0);
  } finally {
    engine.close();
  }
});

test('Transactions: WATCH optimistic locking abort (when watched key IS modified before EXEC, returning *-1\r\n)', () => {
  const manager = new TransactionManager();
  const sessionA = manager.createSession();
  const engine = new StorageEngine();

  try {
    let executed = false;
    // Session A watches inventory
    manager.watch(sessionA, 'inventory');
    assert.equal(sessionA.isDirty, false);

    // Session A begins MULTI
    manager.multi(sessionA);
    manager.queueCommand(sessionA, {
      commandName: 'SET',
      rawArgs: ['inventory', 'purchased'],
      execute: () => {
        executed = true;
        engine.set('inventory', 'purchased');
        return RESP_CONSTANTS.OK;
      },
    });

    // Another client or operation modifies 'inventory'
    engine.set('inventory', 'out-of-stock');
    manager.touchKey('inventory');

    // sessionA should now be flagged dirty
    assert.equal(sessionA.isDirty, true);

    // EXEC should abort and return RESP Null Array *-1\r\n
    const execRes = manager.exec(sessionA);
    assert.equal(execRes.toString(), '*-1\r\n');

    // Ensure queued command did NOT execute
    assert.equal(executed, false);
    assert.equal(engine.get('inventory')?.toString(), 'out-of-stock');

    // Ensure session state was reset
    assert.equal(sessionA.isMulti, false);
    assert.equal(sessionA.queue.length, 0);
    assert.equal(sessionA.watchedKeys.size, 0);
    assert.equal(sessionA.isDirty, false);
  } finally {
    engine.close();
  }
});

test('Transactions: UNWATCH clearing watches', () => {
  const manager = new TransactionManager();
  const session = manager.createSession();
  const engine = new StorageEngine();

  try {
    // 1. Watch keys
    manager.watch(session, 'k1', 'k2');
    assert.equal(session.watchedKeys.size, 2);

    // 2. UNWATCH
    const unwatchRes = manager.unwatch(session);
    assert.equal(unwatchRes.toString(), '+OK\r\n');
    assert.equal(session.watchedKeys.size, 0);
    assert.equal(session.isDirty, false);

    // 3. Touch a previously watched key
    manager.touchKey('k1');
    assert.equal(session.isDirty, false); // Still not dirty because unwatched

    // 4. MULTI and EXEC succeeds
    manager.multi(session);
    manager.queueCommand(session, {
      commandName: 'SET',
      rawArgs: ['k1', 'val'],
      execute: () => {
        engine.set('k1', 'val');
        return RESP_CONSTANTS.OK;
      },
    });

    const execRes = manager.exec(session);
    assert.equal(execRes.toString(), '*1\r\n+OK\r\n');
    assert.equal(engine.get('k1')?.toString(), 'val');
  } finally {
    engine.close();
  }
});

test('Transactions: UNWATCH resets dirty state after a key modification', () => {
  const manager = new TransactionManager();
  const session = manager.createSession();

  manager.watch(session, 'stock');
  assert.equal(session.isDirty, false);

  // Key is touched while watched -> session becomes dirty
  manager.touchKey('stock');
  assert.equal(session.isDirty, true);

  // UNWATCH flushes watches and resets dirty flag
  manager.unwatch(session);
  assert.equal(session.isDirty, false);
  assert.equal(session.watchedKeys.size, 0);

  // Subsequent transaction should proceed without aborting
  manager.multi(session);
  manager.queueCommand(session, {
    commandName: 'PING',
    rawArgs: [],
    execute: () => RESP_CONSTANTS.PONG,
  });
  const execRes = manager.exec(session);
  assert.equal(execRes.toString(), '*1\r\n+PONG\r\n');
});

test('Transactions: Error handling (EXEC without MULTI, DISCARD without MULTI, nested MULTI, WATCH in MULTI)', () => {
  const manager = new TransactionManager();
  const session = manager.createSession();

  // 1. EXEC without MULTI
  const execErr = manager.exec(session);
  assert.equal(execErr.toString(), '-ERR EXEC without MULTI\r\n');

  // 2. DISCARD without MULTI
  const discardErr = manager.discard(session);
  assert.equal(discardErr.toString(), '-ERR DISCARD without MULTI\r\n');

  // 3. Begin MULTI
  const multiRes = manager.multi(session);
  assert.equal(multiRes.toString(), '+OK\r\n');

  // 4. Nested MULTI
  const nestedMultiErr = manager.multi(session);
  assert.equal(nestedMultiErr.toString(), '-ERR MULTI calls can not be nested\r\n');

  // 5. WATCH inside MULTI
  const watchInsideMultiErr = manager.watch(session, 'somekey');
  assert.equal(watchInsideMultiErr.toString(), '-ERR WATCH inside MULTI is not allowed\r\n');

  // Cleanup
  manager.discard(session);
  assert.equal(session.isMulti, false);
});

test('Transactions: Multiple concurrent sessions watching different and shared keys', () => {
  const manager = new TransactionManager();
  const session1 = manager.createSession();
  const session2 = manager.createSession();
  const session3 = manager.createSession();

  // Session 1 watches 'keyA'
  manager.watch(session1, 'keyA');
  // Session 2 watches 'keyB'
  manager.watch(session2, 'keyB');
  // Session 3 watches 'keyA' and 'keyB'
  manager.watch(session3, 'keyA', 'keyB');

  assert.equal(manager.getWatchersCount('keyA'), 2);
  assert.equal(manager.getWatchersCount('keyB'), 2);

  // Begin MULTI on all three
  manager.multi(session1);
  manager.multi(session2);
  manager.multi(session3);

  manager.queueCommand(session1, {
    commandName: 'PING',
    rawArgs: [],
    execute: () => RESP_CONSTANTS.PONG,
  });
  manager.queueCommand(session2, {
    commandName: 'PING',
    rawArgs: [],
    execute: () => RESP_CONSTANTS.PONG,
  });
  manager.queueCommand(session3, {
    commandName: 'PING',
    rawArgs: [],
    execute: () => RESP_CONSTANTS.PONG,
  });

  // Touch only 'keyA'
  manager.touchKey('keyA');

  assert.equal(session1.isDirty, true);
  assert.equal(session2.isDirty, false); // keyB untouched
  assert.equal(session3.isDirty, true);

  // Session 1 should abort
  assert.equal(manager.exec(session1).toString(), '*-1\r\n');

  // Session 2 should succeed!
  assert.equal(manager.exec(session2).toString(), '*1\r\n+PONG\r\n');

  // Session 3 should abort
  assert.equal(manager.exec(session3).toString(), '*-1\r\n');
});

test('Transactions: Command runtime error inside MULTI block does not abort other commands', () => {
  const manager = new TransactionManager();
  const session = manager.createSession();

  manager.multi(session);

  // 1. Success command
  manager.queueCommand(session, {
    commandName: 'SET',
    rawArgs: ['k', 'v'],
    execute: () => RESP_CONSTANTS.OK,
  });

  // 2. Erroneous command
  manager.queueCommand(session, {
    commandName: 'LPUSH',
    rawArgs: ['k', 'item'],
    execute: () => RESPSerializer.encodeError('Operation against a key holding the wrong kind of value', 'WRONGTYPE'),
  });

  // 3. Another success command
  manager.queueCommand(session, {
    commandName: 'PING',
    rawArgs: [],
    execute: () => RESP_CONSTANTS.PONG,
  });

  const execRes = manager.exec(session);
  const expected = Buffer.concat([
    Buffer.from('*3\r\n', 'ascii'),
    RESP_CONSTANTS.OK,
    Buffer.from('-WRONGTYPE Operation against a key holding the wrong kind of value\r\n'),
    RESP_CONSTANTS.PONG,
  ]);
  assert.deepEqual(execRes, expected);
});

test('Transactions: TransactionStatus snapshot reporting', () => {
  const manager = new TransactionManager();
  const session = manager.createSession();

  assert.deepEqual(session.getStatus(), {
    state: 'IDLE',
    isMulti: false,
    isDirty: false,
    queueLength: 0,
    watchedKeysCount: 0,
  });

  manager.watch(session, 'k1', 'k2');
  assert.equal(session.getStatus().watchedKeysCount, 2);

  manager.multi(session);
  assert.equal(session.getStatus().state, 'MULTI');
  assert.equal(session.getStatus().isMulti, true);

  manager.queueCommand(session, {
    commandName: 'PING',
    rawArgs: [],
    execute: () => RESP_CONSTANTS.PONG,
  });
  assert.equal(session.getStatus().queueLength, 1);

  manager.touchKey('k1');
  assert.equal(session.getStatus().state, 'DIRTY');
  assert.equal(session.getStatus().isDirty, true);

  manager.discard(session);
  assert.equal(session.getStatus().state, 'IDLE');
  assert.equal(session.getStatus().isMulti, false);
  assert.equal(session.getStatus().isDirty, false);
});
