import test from 'node:test';
import assert from 'node:assert/strict';
import net from 'node:net';
import { EventEmitter } from 'node:events';
import { ReplicationBacklog } from '../src/replication/backlog.ts';
import { ReplicationManager } from '../src/replication/manager.ts';
import { TCPServer } from '../src/server.ts';
import { StorageEngine } from '../src/storage/engine.ts';
import { RESPSerializer } from '../src/resp/serializer.ts';

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Creates a mock TCP Socket for fast, isolated unit testing of protocol handlers.
 */
class MockSocket extends EventEmitter {
  public written: Buffer[] = [];
  public writable: boolean = true;
  public destroyed: boolean = false;

  public write(data: Buffer | string): boolean {
    if (!this.writable || this.destroyed) return false;
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    this.written.push(buf);
    return true;
  }

  public setNoDelay(): void {}

  public end(): void {
    this.writable = false;
    this.emit('end');
    this.destroy();
  }

  public destroy(): void {
    if (this.destroyed) return;
    this.destroyed = true;
    this.writable = false;
    this.emit('close');
  }

  public getLastWrittenString(): string {
    if (this.written.length === 0) return '';
    return this.written[this.written.length - 1].toString('utf8');
  }

  public getAllWrittenBuffer(): Buffer {
    return Buffer.concat(this.written);
  }

  public clearWritten(): void {
    this.written = [];
  }
}

// =========================================================================
// 1. REPLICATION BACKLOG TESTS: Circular Ring Buffer & Offset Tracking
// =========================================================================

test('ReplicationBacklog: initial state and small writes', () => {
  const backlog = new ReplicationBacklog(1024);

  assert.equal(backlog.size, 1024);
  assert.equal(backlog.master_repl_offset, 0n);
  assert.equal(backlog.offset, 0n);
  assert.equal(backlog.bufferedBytes, 0);
  assert.equal(backlog.oldestOffset, 0n);

  // Offset 0n when empty returns empty buffer
  assert.deepEqual(backlog.getStreamFrom(0n), Buffer.alloc(0));
  // Negative offset returns null
  assert.equal(backlog.getStreamFrom(-1n), null);
  // Future offset returns null
  assert.equal(backlog.getStreamFrom(1n), null);

  // Write 15 bytes
  const chunk1 = Buffer.from('SET key1 val1\r\n');
  backlog.write(chunk1);

  assert.equal(backlog.master_repl_offset, 15n);
  assert.equal(backlog.bufferedBytes, 15);
  assert.equal(backlog.oldestOffset, 0n);

  // Stream from 0n returns full chunk
  assert.deepEqual(backlog.getStreamFrom(0n), chunk1);
  // Stream from 4n returns slice
  assert.deepEqual(backlog.getStreamFrom(4n), Buffer.from('key1 val1\r\n'));
  // Stream from current offset (15n) returns empty buffer
  assert.deepEqual(backlog.getStreamFrom(15n), Buffer.alloc(0));
  // Stream ahead returns null
  assert.equal(backlog.getStreamFrom(16n), null);
});

test('ReplicationBacklog: sequential writes and multi-chunk retrieval', () => {
  const backlog = new ReplicationBacklog(100);

  const chunkA = Buffer.from('COMMAND_A;'); // 10 bytes
  const chunkB = Buffer.from('COMMAND_B;'); // 10 bytes
  const chunkC = Buffer.from('COMMAND_C;'); // 10 bytes

  backlog.write(chunkA);
  assert.equal(backlog.master_repl_offset, 10n);

  backlog.write(chunkB);
  assert.equal(backlog.master_repl_offset, 20n);

  backlog.write(chunkC);
  assert.equal(backlog.master_repl_offset, 30n);

  // From beginning (0n)
  const full = backlog.getStreamFrom(0n);
  assert.equal(full?.toString('utf8'), 'COMMAND_A;COMMAND_B;COMMAND_C;');

  // From middle (10n)
  const partial = backlog.getStreamFrom(10n);
  assert.equal(partial?.toString('utf8'), 'COMMAND_B;COMMAND_C;');

  // From 20n
  const tail = backlog.getStreamFrom(20n);
  assert.equal(tail?.toString('utf8'), 'COMMAND_C;');
});

test('ReplicationBacklog: circular ring buffer wrap-around and expiration', () => {
  const CAPACITY = 50;
  const backlog = new ReplicationBacklog(CAPACITY);

  // Write 30 bytes
  const part1 = Buffer.alloc(30, 'A');
  backlog.write(part1);
  assert.equal(backlog.master_repl_offset, 30n);
  assert.equal(backlog.bufferedBytes, 30);
  assert.equal(backlog.oldestOffset, 0n);
  assert.ok(backlog.getStreamFrom(0n) !== null);

  // Write 40 bytes: total written = 70 bytes.
  // Capacity is 50. Oldest available offset is 70 - 50 = 20n.
  const part2 = Buffer.alloc(40, 'B');
  backlog.write(part2);
  assert.equal(backlog.master_repl_offset, 70n);
  assert.equal(backlog.bufferedBytes, 50);
  assert.equal(backlog.oldestOffset, 20n);

  // Offsets 0n..19n must return null (expired window)
  assert.equal(backlog.getStreamFrom(0n), null);
  assert.equal(backlog.getStreamFrom(10n), null);
  assert.equal(backlog.getStreamFrom(19n), null);

  // Offset 20n is the exact boundary of the window
  const streamFrom20 = backlog.getStreamFrom(20n);
  assert.ok(streamFrom20 !== null);
  assert.equal(streamFrom20.length, 50);

  // The stream from 20 to 70:
  // Offset 20..29 was 'A' (10 bytes), Offset 30..69 was 'B' (40 bytes)
  const expectedFrom20 = Buffer.concat([Buffer.alloc(10, 'A'), Buffer.alloc(40, 'B')]);
  assert.deepEqual(streamFrom20, expectedFrom20);

  // Offset 30n..70n (40 bytes of B)
  const streamFrom30 = backlog.getStreamFrom(30n);
  assert.equal(streamFrom30?.length, 40);
  assert.deepEqual(streamFrom30, Buffer.alloc(40, 'B'));
});

test('ReplicationBacklog: large write exceeding capacity in a single call', () => {
  const CAPACITY = 32;
  const backlog = new ReplicationBacklog(CAPACITY);

  // Write 100 bytes at once
  const hugeData = Buffer.alloc(100);
  for (let i = 0; i < 100; i++) hugeData[i] = i;

  backlog.write(hugeData);

  assert.equal(backlog.master_repl_offset, 100n);
  assert.equal(backlog.bufferedBytes, 32);
  assert.equal(backlog.oldestOffset, 68n); // 100 - 32 = 68

  // Expired offsets
  assert.equal(backlog.getStreamFrom(0n), null);
  assert.equal(backlog.getStreamFrom(67n), null);

  // Valid oldest boundary: 68n to 100n (32 bytes)
  const stream = backlog.getStreamFrom(68n);
  assert.ok(stream !== null);
  assert.equal(stream.length, 32);
  assert.deepEqual(stream, hugeData.subarray(68, 100));
});

test('ReplicationBacklog: reset behavior', () => {
  const backlog = new ReplicationBacklog(128);
  backlog.write(Buffer.from('SOME_COMMAND_STREAM'));
  assert.ok(backlog.master_repl_offset > 0n);

  backlog.reset();
  assert.equal(backlog.master_repl_offset, 0n);
  assert.equal(backlog.bufferedBytes, 0);
  assert.equal(backlog.oldestOffset, 0n);
});

// =========================================================================
// 2. PSYNC PROTOCOL LOGIC: Full Resync vs Partial Resync
// =========================================================================

test('PSYNC: Master ReplId is a valid 40-character hexadecimal string', () => {
  const manager = new ReplicationManager();
  assert.equal(typeof manager.master_replid, 'string');
  assert.equal(manager.master_replid.length, 40);
  assert.match(manager.master_replid, /^[0-9a-f]{40}$/);
});

test('PSYNC: Initial synchronization (PSYNC ? -1) triggers FULLRESYNC', () => {
  const manager = new ReplicationManager({ masterReplId: 'a'.repeat(40) });
  const mockSocket = new MockSocket() as unknown as net.Socket;

  // Master has offset 0
  const reply = manager.handleMasterCommand(['PSYNC', '?', '-1'], mockSocket);
  assert.ok(reply !== null);
  assert.equal(reply.toString('utf8'), `+FULLRESYNC ${'a'.repeat(40)} 0\r\n`);

  // Master tracks the replica connection in 'online' state
  assert.equal(manager.replicas.size, 1);
  const replica = Array.from(manager.replicas)[0];
  assert.equal(replica.state, 'online');
  assert.equal(replica.offset, 0n);
});

test('PSYNC: replid mismatch falls back to FULLRESYNC', () => {
  const manager = new ReplicationManager({ masterReplId: '1'.repeat(40) });
  const mockSocket = new MockSocket() as unknown as net.Socket;

  // Replica presents mismatched replid
  const reply = manager.handleMasterCommand(
    ['PSYNC', '2'.repeat(40), '0'],
    mockSocket
  );
  assert.ok(reply !== null);
  assert.equal(reply.toString('utf8'), `+FULLRESYNC ${'1'.repeat(40)} 0\r\n`);
});

test('PSYNC: Partial resynchronization succeeds with +CONTINUE and backlog stream', () => {
  const manager = new ReplicationManager({ masterReplId: 'f'.repeat(40) });
  const mockSocket = new MockSocket() as unknown as net.Socket;

  // Master performs writes before replica connects
  const cmd1 = RESPSerializer.encodeArray(['SET', 'key1', 'val1']);
  const cmd2 = RESPSerializer.encodeArray(['SET', 'key2', 'val2']);
  manager.propagate(cmd1);
  manager.propagate(cmd2);

  const totalOffset = BigInt(cmd1.length + cmd2.length);
  assert.equal(manager.offset, totalOffset);

  // Replica requests partial resync from offset 0
  const reply = manager.handleMasterCommand(
    ['PSYNC', 'f'.repeat(40), '0'],
    mockSocket
  );
  assert.ok(reply !== null);

  // The reply must start with +CONTINUE\r\n
  const continueHeader = '+CONTINUE\r\n';
  const replyStr = reply.toString('utf8');
  assert.ok(replyStr.startsWith(continueHeader));

  // The remainder of the response is the stream of cmd1 + cmd2
  const streamBytes = reply.subarray(Buffer.byteLength(continueHeader));
  assert.deepEqual(streamBytes, Buffer.concat([cmd1, cmd2]));
});

test('PSYNC: Partial resync from mid-stream offset', () => {
  const manager = new ReplicationManager({ masterReplId: 'e'.repeat(40) });
  const mockSocket = new MockSocket() as unknown as net.Socket;

  const cmd1 = RESPSerializer.encodeArray(['SET', 'alpha', '1']);
  const cmd2 = RESPSerializer.encodeArray(['SET', 'beta', '2']);
  manager.propagate(cmd1);
  manager.propagate(cmd2);

  // Replica already processed cmd1, so it requests stream starting from cmd1.length
  const offset = BigInt(cmd1.length);
  const reply = manager.handleMasterCommand(
    ['PSYNC', 'e'.repeat(40), offset.toString()],
    mockSocket
  );
  assert.ok(reply !== null);

  const continueHeader = '+CONTINUE\r\n';
  assert.ok(reply.toString('utf8').startsWith(continueHeader));

  const streamBytes = reply.subarray(Buffer.byteLength(continueHeader));
  // Must only contain cmd2
  assert.deepEqual(streamBytes, cmd2);
});

test('PSYNC: Offset expired from backlog window falls back to FULLRESYNC', () => {
  const BACKLOG_CAPACITY = 40;
  const manager = new ReplicationManager({
    masterReplId: 'd'.repeat(40),
    backlogSize: BACKLOG_CAPACITY,
  });
  const mockSocket = new MockSocket() as unknown as net.Socket;

  // Write 100 bytes
  const largeData = Buffer.alloc(100, 'X');
  manager.propagate(largeData);

  assert.equal(manager.offset, 100n);
  // Oldest available offset is 60n (100 - 40)
  assert.equal(manager.backlog.oldestOffset, 60n);

  // Replica asks for offset 50 (which fell off the window)
  const reply = manager.handleMasterCommand(
    ['PSYNC', 'd'.repeat(40), '50'],
    mockSocket
  );
  assert.ok(reply !== null);

  // Must fall back to FULLRESYNC
  assert.equal(reply.toString('utf8'), `+FULLRESYNC ${'d'.repeat(40)} 100\r\n`);
});

// =========================================================================
// 3. COMMAND PROPAGATION TO MULTIPLE REPLICAS
// =========================================================================

test('Handshake sequence: PING, REPLCONF listening-port, REPLCONF capa', () => {
  const manager = new ReplicationManager();
  const mockSocket = new MockSocket() as unknown as net.Socket;

  // 1. PING
  const pingResp = manager.handleMasterCommand(['PING'], mockSocket);
  assert.equal(pingResp?.toString('utf8'), '+PONG\r\n');

  // 2. REPLCONF listening-port
  const portResp = manager.handleMasterCommand(
    ['REPLCONF', 'listening-port', '6381'],
    mockSocket
  );
  assert.equal(portResp?.toString('utf8'), '+OK\r\n');

  // 3. REPLCONF capa
  const capaResp = manager.handleMasterCommand(
    ['REPLCONF', 'capa', 'psync2'],
    mockSocket
  );
  assert.equal(capaResp?.toString('utf8'), '+OK\r\n');

  // 4. PSYNC
  const psyncResp = manager.handleMasterCommand(['PSYNC', '?', '-1'], mockSocket);
  assert.ok(psyncResp?.toString('utf8').startsWith('+FULLRESYNC'));

  // Replica connection stored with port and capability
  assert.equal(manager.replicas.size, 1);
  const conn = Array.from(manager.replicas)[0];
  assert.equal(conn.listeningPort, 6381);
  assert.ok(conn.capabilities.has('psync2'));
});

test('Command propagation forwards raw bytes to all connected replicas', () => {
  const manager = new ReplicationManager();

  const sock1 = new MockSocket();
  const sock2 = new MockSocket();
  const sock3 = new MockSocket();

  // Connect and handshake all 3 replicas
  manager.handleMasterCommand(['PSYNC', '?', '-1'], sock1 as unknown as net.Socket);
  manager.handleMasterCommand(['PSYNC', '?', '-1'], sock2 as unknown as net.Socket);
  manager.handleMasterCommand(['PSYNC', '?', '-1'], sock3 as unknown as net.Socket);

  assert.equal(manager.replicas.size, 3);

  // Clear handshake responses from mocks
  sock1.clearWritten();
  sock2.clearWritten();
  sock3.clearWritten();

  // Master propagates command
  const setCmd = RESPSerializer.encodeArray(['SET', 'counter', '42']);
  manager.propagate(setCmd);

  // All 3 sockets received the exact bytes
  assert.deepEqual(sock1.getAllWrittenBuffer(), setCmd);
  assert.deepEqual(sock2.getAllWrittenBuffer(), setCmd);
  assert.deepEqual(sock3.getAllWrittenBuffer(), setCmd);

  // Propagate another command
  sock1.clearWritten();
  sock2.clearWritten();
  sock3.clearWritten();

  const delCmd = RESPSerializer.encodeArray(['DEL', 'counter']);
  manager.propagate(delCmd);

  assert.deepEqual(sock1.getAllWrittenBuffer(), delCmd);
  assert.deepEqual(sock2.getAllWrittenBuffer(), delCmd);
  assert.deepEqual(sock3.getAllWrittenBuffer(), delCmd);
});

test('Command propagation cleanly handles replica socket disconnections', () => {
  const manager = new ReplicationManager();

  const sock1 = new MockSocket();
  const sock2 = new MockSocket();

  manager.handleMasterCommand(['PSYNC', '?', '-1'], sock1 as unknown as net.Socket);
  manager.handleMasterCommand(['PSYNC', '?', '-1'], sock2 as unknown as net.Socket);

  assert.equal(manager.replicas.size, 2);

  // Simulate socket 1 disconnect
  sock1.destroy();

  assert.equal(manager.replicas.size, 1);

  sock2.clearWritten();
  const cmd = RESPSerializer.encodeArray(['SET', 'status', 'active']);
  manager.propagate(cmd);

  // Remaining replica received the command
  assert.deepEqual(sock2.getAllWrittenBuffer(), cmd);
});

// =========================================================================
// 4. OFFSET SYNCHRONIZATION AND EXPIRATION DETECTION
// =========================================================================

test('Offset synchronization tracks exact byte counts on master and replica', () => {
  const manager = new ReplicationManager();

  const cmdA = RESPSerializer.encodeArray(['SET', 'k1', 'v1']);
  const cmdB = RESPSerializer.encodeArray(['HSET', 'hash', 'f', 'v']);
  const cmdC = RESPSerializer.encodeArray(['DEL', 'k1']);

  manager.propagate(cmdA);
  assert.equal(manager.offset, BigInt(cmdA.length));

  manager.propagate(cmdB);
  assert.equal(manager.offset, BigInt(cmdA.length + cmdB.length));

  manager.propagate(cmdC);
  assert.equal(manager.offset, BigInt(cmdA.length + cmdB.length + cmdC.length));

  // Replica synchronization
  const repManager = new ReplicationManager({ role: 'replica' });
  repManager.replica_offset = 0n;

  // Simulate receiving commands from master
  repManager.applyCommand({
    type: 'array',
    value: [
      { type: 'bulk_string', value: Buffer.from('SET') },
      { type: 'bulk_string', value: Buffer.from('k1') },
      { type: 'bulk_string', value: Buffer.from('v1') },
    ],
  });
  repManager.replica_offset += BigInt(cmdA.length);

  assert.equal(repManager.offset, BigInt(cmdA.length));
});

test('Window expiration detection differentiates valid vs expired offsets', () => {
  const WINDOW_SIZE = 60;
  const manager = new ReplicationManager({
    masterReplId: 'c'.repeat(40),
    backlogSize: WINDOW_SIZE,
  });

  const writeData = Buffer.alloc(150, 'Z');
  manager.propagate(writeData);

  assert.equal(manager.offset, 150n);
  // Valid window is [90n, 150n]
  assert.equal(manager.backlog.oldestOffset, 90n);

  const mockSocket = new MockSocket() as unknown as net.Socket;

  // 1. Expired offset (89n) -> FULLRESYNC
  const resExpired = manager.handleMasterCommand(
    ['PSYNC', 'c'.repeat(40), '89'],
    mockSocket
  );
  assert.ok(resExpired?.toString('utf8').startsWith('+FULLRESYNC'));

  // 2. Exact window edge (90n) -> +CONTINUE (60 bytes)
  const resEdge = manager.handleMasterCommand(
    ['PSYNC', 'c'.repeat(40), '90'],
    mockSocket
  );
  assert.ok(resEdge?.toString('utf8').startsWith('+CONTINUE\r\n'));
  assert.equal(resEdge!.length, 11 + 60); // '+CONTINUE\r\n' (11) + 60

  // 3. Inside window (120n) -> +CONTINUE (30 bytes)
  const resInside = manager.handleMasterCommand(
    ['PSYNC', 'c'.repeat(40), '120'],
    mockSocket
  );
  assert.ok(resInside?.toString('utf8').startsWith('+CONTINUE\r\n'));
  assert.equal(resInside!.length, 11 + 30);
});

// =========================================================================
// 5. END-TO-END MASTER-REPLICA LIVE REPLICATION OVER TCP SOCKETS
// =========================================================================

test('End-to-End: Master and Replica handshake, propagate write stream and sync state', async () => {
  const MASTER_PORT = 17379;
  const REPLICA_PORT = 17380;

  const masterEngine = new StorageEngine();
  const masterServer = new TCPServer({
    port: MASTER_PORT,
    host: '127.0.0.1',
    engine: masterEngine,
  });
  await masterServer.start();

  const replicaEngine = new StorageEngine();
  const replicaServer = new TCPServer({
    port: REPLICA_PORT,
    host: '127.0.0.1',
    engine: replicaEngine,
  });
  await replicaServer.start();

  try {
    // Replica connects and initiates replication from Master
    const repManager = replicaServer.getReplication();
    await repManager.replicateFrom('127.0.0.1', MASTER_PORT);

    assert.equal(repManager.role, 'replica');
    assert.equal(repManager.replicationState, 'online');
    assert.equal(repManager.master_replid, masterServer.getReplication().master_replid);

    // Client connects to Master and executes write commands
    const masterClient = net.createConnection({ port: MASTER_PORT, host: '127.0.0.1' });
    await new Promise((r) => masterClient.once('connect', r));

    const sendCmd = (client: net.Socket, cmd: string): Promise<string> =>
      new Promise((resolve) => {
        client.once('data', (buf) => resolve(buf.toString('utf8')));
        client.write(cmd);
      });

    // 1. SET on Master
    let res = await sendCmd(masterClient, '*3\r\n$3\r\nSET\r\n$4\r\nuser\r\n$5\r\nalice\r\n');
    assert.equal(res, '+OK\r\n');

    // 2. HSET on Master
    res = await sendCmd(
      masterClient,
      '*4\r\n$4\r\nHSET\r\n$7\r\nprofile\r\n$4\r\nrole\r\n$5\r\nadmin\r\n'
    );
    assert.equal(res, ':1\r\n');

    // 3. LPUSH on Master
    res = await sendCmd(
      masterClient,
      '*3\r\n$5\r\nLPUSH\r\n$5\r\nqueue\r\n$5\r\ntask1\r\n'
    );
    assert.equal(res, ':1\r\n');

    masterClient.destroy();

    // Allow a brief moment for replication stream to arrive and be processed
    await sleep(80);

    // Verify storage on Replica reflects replicated writes!
    assert.equal(replicaEngine.get('user')?.toString('utf8'), 'alice');
    assert.equal(replicaEngine.hget('profile', 'role')?.toString('utf8'), 'admin');
    const queueItems = replicaEngine.lrange('queue', 0, -1);
    assert.equal(queueItems.length, 1);
    assert.equal(queueItems[0].toString('utf8'), 'task1');

    // Verify replication offsets match
    assert.ok(masterServer.getReplication().offset > 0n);
    assert.equal(repManager.offset, masterServer.getReplication().offset);

    // 4. Test client querying Replica over TCP
    const replicaClient = net.createConnection({ port: REPLICA_PORT, host: '127.0.0.1' });
    await new Promise((r) => replicaClient.once('connect', r));

    res = await sendCmd(replicaClient, '*2\r\n$3\r\nGET\r\n$4\r\nuser\r\n');
    assert.equal(res, '$5\r\nalice\r\n');

    res = await sendCmd(
      replicaClient,
      '*3\r\n$4\r\nHGET\r\n$7\r\nprofile\r\n$4\r\nrole\r\n'
    );
    assert.equal(res, '$5\r\nadmin\r\n');

    replicaClient.destroy();
  } finally {
    await replicaServer.stop();
    await masterServer.stop();
  }
});

test('End-to-End: Reconnection and Partial Resynchronization (PSYNC continue)', async () => {
  const MASTER_PORT = 17381;

  const masterEngine = new StorageEngine();
  const masterServer = new TCPServer({
    port: MASTER_PORT,
    host: '127.0.0.1',
    engine: masterEngine,
  });
  await masterServer.start();

  try {
    const replicaEngine = new StorageEngine();
    const repManager = new ReplicationManager({
      engine: replicaEngine,
      listeningPort: 17382,
    });

    // 1. Initial replication handshake
    await repManager.replicateFrom('127.0.0.1', MASTER_PORT);
    assert.equal(repManager.replicationState, 'online');

    // 2. Master writes first batch
    const masterClient = net.createConnection({ port: MASTER_PORT, host: '127.0.0.1' });
    await new Promise((r) => masterClient.once('connect', r));

    const sendCmd = (client: net.Socket, cmd: string): Promise<string> =>
      new Promise((resolve) => {
        client.once('data', (buf) => resolve(buf.toString('utf8')));
        client.write(cmd);
      });

    await sendCmd(masterClient, '*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n');
    await sleep(50);

    const savedOffset = repManager.offset;
    assert.ok(savedOffset > 0n);
    assert.equal(replicaEngine.get('k1')?.toString('utf8'), 'v1');

    // 3. Replica temporarily disconnects
    await repManager.disconnect();
    assert.equal(repManager.replicationState, 'disconnected');

    // 4. Master performs additional writes while replica is offline
    await sendCmd(masterClient, '*3\r\n$3\r\nSET\r\n$2\r\nk2\r\n$2\r\nv2\r\n');
    await sendCmd(masterClient, '*3\r\n$3\r\nSET\r\n$2\r\nk3\r\n$2\r\nv3\r\n');
    masterClient.destroy();

    // 5. Replica reconnects requesting partial resync from savedOffset
    await repManager.replicateFrom('127.0.0.1', MASTER_PORT, {
      replid: repManager.master_replid,
      offset: savedOffset,
    });

    assert.equal(repManager.replicationState, 'online');
    await sleep(50);

    // Verify all keys from offline period are synchronized!
    assert.equal(replicaEngine.get('k1')?.toString('utf8'), 'v1');
    assert.equal(replicaEngine.get('k2')?.toString('utf8'), 'v2');
    assert.equal(replicaEngine.get('k3')?.toString('utf8'), 'v3');
    assert.equal(repManager.offset, masterServer.getReplication().offset);

    await repManager.disconnect();
  } finally {
    await masterServer.stop();
  }
});
