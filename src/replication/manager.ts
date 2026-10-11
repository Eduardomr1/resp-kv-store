import net from 'node:net';
import crypto from 'node:crypto';
import { ReplicationBacklog } from './backlog.ts';
import {
  type ReplicationRole,
  type ReplicationState,
  type ReplicaConnection,
  type ReplicationInfo,
  type ReplicationManagerOptions,
} from './types.ts';
import { RESPParser } from '../resp/parser.ts';
import { RESPSerializer, RESP_CONSTANTS } from '../resp/serializer.ts';
import type { RESPValue } from '../resp/types.ts';
import type { StorageEngine } from '../storage/engine.ts';
import type { SetOptions } from '../storage/types.ts';

/**
 * Helper class to coordinate asynchronous RESP frame decoding during handshake
 * and transition smoothly to continuous streaming without losing intermediate frames.
 */
class FrameReceiver {
  private parser = new RESPParser();
  private queue: { value: RESPValue; byteLength: number }[] = [];
  private waiter: {
    resolve: (frame: { value: RESPValue; byteLength: number }) => void;
    reject: (err: Error) => void;
  } | null = null;
  private streamingCallback: ((frame: { value: RESPValue; byteLength: number }) => void) | null = null;

  public feed(chunk: Buffer): void {
    this.parser.feed(chunk);
    const frames = this.parser.parseWithByteCount();
    for (const frame of frames) {
      if (this.streamingCallback) {
        this.streamingCallback(frame);
      } else if (this.waiter) {
        const w = this.waiter;
        this.waiter = null;
        w.resolve(frame);
      } else {
        this.queue.push(frame);
      }
    }
  }

  public nextFrame(timeoutMs: number = 5000): Promise<{ value: RESPValue; byteLength: number }> {
    if (this.queue.length > 0) {
      return Promise.resolve(this.queue.shift()!);
    }
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        if (this.waiter?.resolve === resolve) {
          this.waiter = null;
          reject(new Error(`Timeout waiting for master response (${timeoutMs}ms)`));
        }
      }, timeoutMs);

      this.waiter = {
        resolve: (frame) => {
          clearTimeout(timer);
          resolve(frame);
        },
        reject: (err) => {
          clearTimeout(timer);
          reject(err);
        },
      };
    });
  }

  public abort(err: Error): void {
    if (this.waiter) {
      const w = this.waiter;
      this.waiter = null;
      w.reject(err);
    }
  }

  public setStreamingMode(callback: (frame: { value: RESPValue; byteLength: number }) => void): void {
    this.streamingCallback = callback;
    while (this.queue.length > 0) {
      const frame = this.queue.shift()!;
      callback(frame);
    }
  }
}

/**
 * ReplicationManager: Master-Replica Replication Engine.
 *
 * Implements Redis-compatible replication:
 * - Master replication ID (40-char hex string)
 * - Circular ring backlog for partial resynchronization (PSYNC / CONTINUE)
 * - Multi-replica tracking and real-time command propagation
 * - Replication handshake protocol (PING -> REPLCONF -> PSYNC)
 * - Master-to-Replica continuous write stream execution
 */
export class ReplicationManager {
  public role: ReplicationRole;
  public master_replid: string;
  public replicas: Set<ReplicaConnection> = new Set();
  public backlog: ReplicationBacklog;
  public replica_offset: bigint = 0n;
  public listeningPort: number;
  public replicationState: ReplicationState = 'none';

  public master_host?: string;
  public master_port?: number;
  public masterSocket: net.Socket | null = null;

  private engine: StorageEngine | null = null;
  private onCommand?: (commandArgs: (string | Buffer)[]) => void;
  private socketToReplica: Map<net.Socket, ReplicaConnection> = new Map();

  constructor(options: ReplicationManagerOptions = {}) {
    this.role = options.role ?? 'master';
    this.master_replid = options.masterReplId ?? crypto.randomBytes(20).toString('hex');
    this.listeningPort = options.listeningPort ?? 6379;
    this.backlog = new ReplicationBacklog(options.backlogSize ?? 1024 * 1024);
    this.engine = options.engine ?? null;
    this.onCommand = options.onCommand;
  }

  /**
   * 40-character hexadecimal replication ID.
   */
  public get replid(): string {
    return this.master_replid;
  }

  /**
   * Current replication offset.
   * On Master: returns master_repl_offset from backlog.
   * On Replica: returns accumulated processed bytes offset.
   */
  public get offset(): bigint {
    return this.role === 'master' ? this.backlog.master_repl_offset : this.replica_offset;
  }

  public get master_repl_offset(): bigint {
    return this.backlog.master_repl_offset;
  }

  /**
   * Sets the storage engine for applying replicated commands.
   */
  public setStorageEngine(engine: StorageEngine): void {
    this.engine = engine;
  }

  /**
   * Handles incoming Master replication handshake commands (PING, REPLCONF, PSYNC).
   * Returns RESP serialized buffer response, or null if command is unhandled or requires no reply.
   */
  public handleCommand(value: RESPValue, socket: net.Socket): Buffer | null {
    if (value.type !== 'array') return null;
    const args = value.value;
    if (args.length === 0) return null;

    const first = args[0];
    if (first.type !== 'bulk_string' && first.type !== 'simple_string') return null;
    const cmd = first.value.toString('utf8').toUpperCase();

    const strArgs: string[] = [];
    for (const a of args) {
      if (a.type === 'bulk_string') strArgs.push(a.value.toString('utf8'));
      else if (a.type === 'simple_string') strArgs.push(a.value);
      else if (a.type === 'integer') strArgs.push(a.value.toString());
      else strArgs.push('');
    }

    return this.handleHandshake(cmd, strArgs, socket);
  }

  /**
   * Overloaded master command handler supporting raw argument lists or RESPValue arrays.
   */
  public handleMasterCommand(
    args: (string | Buffer)[] | RESPValue[],
    socket: net.Socket
  ): Buffer | null {
    if (args.length === 0) return null;

    if (typeof args[0] === 'object' && args[0] !== null && 'type' in (args[0] as any)) {
      return this.handleCommand({ type: 'array', value: args as RESPValue[] }, socket);
    }

    const strArgs = (args as (string | Buffer)[]).map((a) =>
      Buffer.isBuffer(a) ? a.toString('utf8') : String(a)
    );
    const cmd = strArgs[0].toUpperCase();
    return this.handleHandshake(cmd, strArgs, socket);
  }

  /**
   * Core replication handshake logic on Master:
   * - PING -> +PONG\r\n
   * - REPLCONF listening-port <port> -> +OK\r\n
   * - REPLCONF capa psync2 -> +OK\r\n
   * - PSYNC <replid> <offset>:
   *   - Initial sync (PSYNC ? -1): +FULLRESYNC <master_replid> <current_offset>\r\n
   *   - Partial resync (PSYNC <master_replid> <offset> within backlog): +CONTINUE\r\n + backlogStream
   *   - Otherwise: full resync
   */
  public handleHandshake(cmd: string, args: string[], socket: net.Socket): Buffer | null {
    const replica = this.getOrCreateReplica(socket);

    if (cmd === 'PING') {
      return RESP_CONSTANTS.PONG;
    }

    if (cmd === 'REPLCONF') {
      if (args.length >= 3) {
        const sub = args[1].toLowerCase();
        if (sub === 'listening-port') {
          const port = parseInt(args[2], 10);
          if (!isNaN(port)) {
            replica.listeningPort = port;
          }
          return RESP_CONSTANTS.OK;
        } else if (sub === 'capa') {
          replica.capabilities.add(args[2].toLowerCase());
          return RESP_CONSTANTS.OK;
        } else if (sub === 'ack') {
          try {
            replica.ackOffset = BigInt(args[2]);
            replica.lastAckTime = Date.now();
          } catch {}
          return null; // Silent ACK
        }
      }
      return RESP_CONSTANTS.OK;
    }

    if (cmd === 'PSYNC') {
      const reqReplId = args[1] ?? '?';
      const reqOffsetStr = args[2] ?? '-1';

      // 1. Initial sync: PSYNC ? -1
      if (reqReplId === '?' && reqOffsetStr === '-1') {
        replica.state = 'online';
        replica.offset = this.backlog.master_repl_offset;
        this.replicas.add(replica);
        const header = `+FULLRESYNC ${this.master_replid} ${this.backlog.master_repl_offset}\r\n`;
        return Buffer.from(header, 'utf8');
      }

      // 2. Partial resync: PSYNC <master_replid> <offset>
      if (reqReplId === this.master_replid) {
        try {
          const reqOffset = BigInt(reqOffsetStr);
          const backlogStream = this.backlog.getStreamFrom(reqOffset);
          if (backlogStream !== null) {
            replica.state = 'online';
            replica.offset = this.backlog.master_repl_offset;
            this.replicas.add(replica);
            const continueHeader = Buffer.from('+CONTINUE\r\n', 'utf8');
            return Buffer.concat([continueHeader, backlogStream]);
          }
        } catch {
          // Fall through to full resync on parse error
        }
      }

      // 3. Otherwise: Full resynchronization
      replica.state = 'online';
      replica.offset = this.backlog.master_repl_offset;
      this.replicas.add(replica);
      const header = `+FULLRESYNC ${this.master_replid} ${this.backlog.master_repl_offset}\r\n`;
      return Buffer.from(header, 'utf8');
    }

    return null;
  }

  /**
   * Command propagation: writes to replication backlog and forwards the raw bytes
   * to all connected replica sockets.
   */
  public propagate(commandBytes: Buffer): void {
    this.backlog.write(commandBytes);

    for (const replica of this.replicas) {
      if (replica.state === 'online' && replica.socket.writable) {
        try {
          replica.socket.write(commandBytes);
          replica.offset = this.backlog.master_repl_offset;
        } catch {
          replica.state = 'disconnected';
          this.replicas.delete(replica);
        }
      }
    }
  }

  /**
   * Helper to propagate high-level command token arrays.
   */
  public propagateCommand(args: (string | Buffer)[]): void {
    const bytes = RESPSerializer.encodeArray(args);
    this.propagate(bytes);
  }

  /**
   * Replica client connection handler:
   * - Connects TCP socket to master.
   * - Executes replication handshake: PING -> REPLCONF listening-port -> REPLCONF capa psync2 -> PSYNC ? -1.
   * - Consumes continuous command stream from master and applies write commands.
   */
  public async replicateFrom(
    masterHost: string,
    masterPort: number,
    options?: { replid?: string; offset?: bigint }
  ): Promise<void> {
    this.role = 'replica';
    this.master_host = masterHost;
    this.master_port = masterPort;
    this.replicationState = 'connecting';

    const socket = net.createConnection({ host: masterHost, port: masterPort });
    socket.setNoDelay(true);
    this.masterSocket = socket;

    await new Promise<void>((resolve, reject) => {
      socket.once('connect', resolve);
      socket.once('error', (err) => {
        this.replicationState = 'disconnected';
        reject(err);
      });
    });

    const receiver = new FrameReceiver();

    socket.on('data', (chunk) => {
      receiver.feed(chunk);
    });

    socket.on('close', () => {
      this.replicationState = 'disconnected';
      this.masterSocket = null;
    });

    socket.on('error', (err) => {
      receiver.abort(err);
      this.replicationState = 'disconnected';
    });

    try {
      // Step 1: PING -> +PONG\r\n
      this.replicationState = 'handshake_ping';
      socket.write(RESPSerializer.encodeArray(['PING']));
      const pingFrame = await receiver.nextFrame();
      if (pingFrame.value.type !== 'simple_string' || pingFrame.value.value !== 'PONG') {
        throw new Error(`Expected +PONG from master, got: ${JSON.stringify(pingFrame.value)}`);
      }

      // Step 2: REPLCONF listening-port <port> -> +OK\r\n
      this.replicationState = 'handshake_replconf_port';
      socket.write(
        RESPSerializer.encodeArray(['REPLCONF', 'listening-port', this.listeningPort.toString()])
      );
      const portFrame = await receiver.nextFrame();
      if (portFrame.value.type !== 'simple_string' || portFrame.value.value !== 'OK') {
        throw new Error(
          `Expected +OK from master for listening-port, got: ${JSON.stringify(portFrame.value)}`
        );
      }

      // Step 3: REPLCONF capa psync2 -> +OK\r\n
      this.replicationState = 'handshake_replconf_capa';
      socket.write(RESPSerializer.encodeArray(['REPLCONF', 'capa', 'psync2']));
      const capaFrame = await receiver.nextFrame();
      if (capaFrame.value.type !== 'simple_string' || capaFrame.value.value !== 'OK') {
        throw new Error(
          `Expected +OK from master for capa psync2, got: ${JSON.stringify(capaFrame.value)}`
        );
      }

      // Step 4: PSYNC <replid> <offset> -> +FULLRESYNC or +CONTINUE
      this.replicationState = 'handshake_psync';
      const psyncReplId =
        options?.replid ??
        (this.master_replid && this.master_replid !== '?' ? this.master_replid : '?');
      const psyncOffset =
        options?.offset !== undefined ? options.offset.toString() : '-1';

      socket.write(RESPSerializer.encodeArray(['PSYNC', psyncReplId, psyncOffset]));

      const psyncFrame = await receiver.nextFrame();
      if (psyncFrame.value.type !== 'simple_string') {
        throw new Error(
          `Expected simple string response for PSYNC, got: ${JSON.stringify(psyncFrame.value)}`
        );
      }

      const replyStr = psyncFrame.value.value;
      if (replyStr.startsWith('FULLRESYNC')) {
        const parts = replyStr.split(' ');
        this.master_replid = parts[1];
        this.replica_offset = BigInt(parts[2]);
        this.replicationState = 'online';
      } else if (replyStr.startsWith('CONTINUE')) {
        this.replicationState = 'online';
      } else {
        throw new Error(`Unexpected PSYNC response from master: ${replyStr}`);
      }

      // Step 5: Streaming mode — consumes continuous command stream from master and applies writes
      receiver.setStreamingMode((frame) => {
        this.replica_offset += BigInt(frame.byteLength);
        this.applyCommand(frame.value);
      });
    } catch (err) {
      this.replicationState = 'disconnected';
      socket.destroy();
      throw err;
    }
  }

  /**
   * Applies replicated commands to the storage engine and invokes callbacks.
   */
  public applyCommand(value: RESPValue): void {
    if (value.type !== 'array') return;
    const args = value.value;
    if (args.length === 0) return;

    const first = args[0];
    if (first.type !== 'bulk_string' && first.type !== 'simple_string') return;
    const cmd = first.value.toString('utf8').toUpperCase();

    // Check for internal replication protocol commands
    if (cmd === 'PING') {
      return; // Heartbeat from master
    }

    if (cmd === 'REPLCONF') {
      if (args.length >= 2) {
        const sub =
          args[1].type === 'bulk_string'
            ? args[1].value.toString('utf8').toUpperCase()
            : (args[1] as any).value?.toString().toUpperCase();
        if (sub === 'GETACK') {
          if (this.masterSocket && this.masterSocket.writable) {
            const ackMsg = RESPSerializer.encodeArray([
              'REPLCONF',
              'ACK',
              this.replica_offset.toString(),
            ]);
            this.masterSocket.write(ackMsg);
          }
        }
      }
      return;
    }

    // Convert arguments to standard formats
    const rawBuffers: Buffer[] = args.map((a: any) =>
      a.type === 'bulk_string' ? a.value : Buffer.from(a.value?.toString() ?? '')
    );

    if (this.onCommand) {
      this.onCommand(rawBuffers);
    }

    if (this.engine) {
      this.executeOnEngine(cmd, args);
    }
  }

  /**
   * Executes mutating command directly against StorageEngine.
   */
  private executeOnEngine(cmd: string, args: RESPValue[]): void {
    if (!this.engine) return;

    try {
      switch (cmd) {
        case 'SET': {
          if (args.length < 3 || args[1].type !== 'bulk_string' || args[2].type !== 'bulk_string')
            return;
          const key = args[1].value.toString('utf8');
          const val = args[2].value;
          const options: SetOptions = {};

          for (let i = 3; i < args.length; i++) {
            const opt = (args[i] as any).value?.toString('utf8').toUpperCase();
            if (opt === 'NX') options.nx = true;
            else if (opt === 'XX') options.xx = true;
            else if (opt === 'KEEPTTL') options.keepttl = true;
            else if (opt === 'EX' && i + 1 < args.length) {
              const sec = Number((args[++i] as any).value?.toString('utf8'));
              if (!isNaN(sec)) options.ex = sec;
            } else if (opt === 'PX' && i + 1 < args.length) {
              const ms = Number((args[++i] as any).value?.toString('utf8'));
              if (!isNaN(ms)) options.px = ms;
            }
          }

          this.engine.set(key, val, options);
          break;
        }

        case 'DEL': {
          if (args.length < 2) return;
          const keys = args.slice(1).map((a: any) => a.value.toString('utf8'));
          this.engine.del(...keys);
          break;
        }

        case 'HSET': {
          if (args.length < 4) return;
          const key = (args[1] as any).value.toString('utf8');
          for (let i = 2; i < args.length; i += 2) {
            const field = (args[i] as any).value.toString('utf8');
            const val = (args[i + 1] as any).value;
            this.engine.hset(key, field, val);
          }
          break;
        }

        case 'HDEL': {
          if (args.length < 3) return;
          const key = (args[1] as any).value.toString('utf8');
          const fields = args.slice(2).map((a: any) => a.value.toString('utf8'));
          this.engine.hdel(key, ...fields);
          break;
        }

        case 'LPUSH': {
          if (args.length < 3) return;
          const key = (args[1] as any).value.toString('utf8');
          const vals = args.slice(2).map((a: any) => a.value);
          this.engine.lpush(key, ...vals);
          break;
        }

        case 'RPUSH': {
          if (args.length < 3) return;
          const key = (args[1] as any).value.toString('utf8');
          const vals = args.slice(2).map((a: any) => a.value);
          this.engine.rpush(key, ...vals);
          break;
        }

        case 'LPOP': {
          if (args.length < 2) return;
          const key = (args[1] as any).value.toString('utf8');
          const count = args.length > 2 ? parseInt((args[2] as any).value.toString('utf8'), 10) : 1;
          this.engine.lpop(key, count);
          break;
        }

        case 'RPOP': {
          if (args.length < 2) return;
          const key = (args[1] as any).value.toString('utf8');
          const count = args.length > 2 ? parseInt((args[2] as any).value.toString('utf8'), 10) : 1;
          this.engine.rpop(key, count);
          break;
        }

        case 'SADD': {
          if (args.length < 3) return;
          const key = (args[1] as any).value.toString('utf8');
          const members = args.slice(2).map((a: any) => a.value.toString('utf8'));
          this.engine.sadd(key, ...members);
          break;
        }

        case 'SREM': {
          if (args.length < 3) return;
          const key = (args[1] as any).value.toString('utf8');
          const members = args.slice(2).map((a: any) => a.value.toString('utf8'));
          this.engine.srem(key, ...members);
          break;
        }

        case 'ZADD': {
          if (args.length < 4) return;
          const key = (args[1] as any).value.toString('utf8');
          for (let i = 2; i < args.length; i += 2) {
            const score = Number((args[i] as any).value.toString('utf8'));
            const member = (args[i + 1] as any).value.toString('utf8');
            this.engine.zadd(key, score, member);
          }
          break;
        }

        case 'ZREM': {
          if (args.length < 3) return;
          const key = (args[1] as any).value.toString('utf8');
          const members = args.slice(2).map((a: any) => a.value.toString('utf8'));
          this.engine.zrem(key, ...members);
          break;
        }

        case 'EXPIRE': {
          if (args.length < 3) return;
          const key = (args[1] as any).value.toString('utf8');
          const sec = parseInt((args[2] as any).value.toString('utf8'), 10);
          this.engine.expire(key, sec);
          break;
        }

        case 'PEXPIRE': {
          if (args.length < 3) return;
          const key = (args[1] as any).value.toString('utf8');
          const ms = parseInt((args[2] as any).value.toString('utf8'), 10);
          this.engine.pexpire(key, ms);
          break;
        }

        case 'PERSIST': {
          if (args.length < 2) return;
          const key = (args[1] as any).value.toString('utf8');
          this.engine.persist(key);
          break;
        }

        case 'FLUSHALL':
        case 'FLUSHDB': {
          this.engine.flushall();
          break;
        }
      }
    } catch (err) {
      console.error('[Replication Apply Error]', err);
    }
  }

  /**
   * Retrieves or creates a ReplicaConnection for a socket.
   */
  private getOrCreateReplica(socket: net.Socket): ReplicaConnection {
    let conn = this.socketToReplica.get(socket);
    if (!conn) {
      conn = {
        socket,
        state: 'handshake',
        capabilities: new Set(),
        offset: 0n,
      };
      this.socketToReplica.set(socket, conn);

      const cleanup = () => {
        conn!.state = 'disconnected';
        this.replicas.delete(conn!);
        this.socketToReplica.delete(socket);
      };

      socket.once('close', cleanup);
      socket.once('error', cleanup);
    }
    return conn;
  }

  /**
   * Returns current replication telemetry details.
   */
  public getInfo(): ReplicationInfo {
    return {
      role: this.role,
      master_replid: this.master_replid,
      master_repl_offset: this.offset,
      second_repl_offset: -1n,
      connected_slaves: this.replicas.size,
      master_host: this.master_host,
      master_port: this.master_port,
      master_link_status: this.replicationState === 'online' ? 'up' : 'down',
    };
  }

  /**
   * Gracefully closes all replication sockets.
   */
  public async disconnect(): Promise<void> {
    if (this.masterSocket) {
      this.masterSocket.destroy();
      this.masterSocket = null;
    }

    for (const replica of this.replicas) {
      replica.state = 'disconnected';
      replica.socket.destroy();
    }

    this.replicas.clear();
    this.socketToReplica.clear();
    this.replicationState = 'disconnected';
  }
}
