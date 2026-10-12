import net from 'node:net';
import { performance } from 'node:perf_hooks';
import { RESPParser, ProtocolError } from './resp/parser.ts';
import { RESPSerializer, RESP_CONSTANTS } from './resp/serializer.ts';
import type { RESPValue } from './resp/types.ts';
import { StorageEngine } from './storage/engine.ts';
import { WrongTypeError, type SetOptions } from './storage/types.ts';
import type { StreamEntry } from './storage/stream.ts';
import { OOMError, type EvictionPolicy } from './eviction/types.ts';
import { RDBEncoder } from './storage/rdb.ts';
import { ReplicationManager } from './replication/manager.ts';
import { PubSubHub } from './pubsub/hub.ts';
import { TransactionManager, TransactionSession } from './transactions/manager.ts';
import { BlockingManager } from './blocking/manager.ts';
import { SlowlogManager } from './telemetry/slowlog.ts';

export interface ServerOptions {
  port?: number;
  host?: string;
  engine?: StorageEngine;
  replication?: ReplicationManager;
  pubsub?: PubSubHub;
  transactions?: TransactionManager;
  blocking?: BlockingManager;
  slowlog?: SlowlogManager;
  rdbFilePath?: string;
}

export class TCPServer {
  private server: net.Server;
  private engine: StorageEngine;
  private replication: ReplicationManager;
  private pubsub: PubSubHub;
  private transactions: TransactionManager;
  private blocking: BlockingManager;
  private slowlog: SlowlogManager;
  private streamListeners: Map<string, Set<() => void>> = new Map();
  private socketSessions: Map<net.Socket, TransactionSession> = new Map();
  private port: number;
  private host: string;
  private rdbFilePath: string;
  private isRunning: boolean = false;

  constructor(options: ServerOptions = {}) {
    this.port = options.port ?? 6379;
    this.host = options.host ?? '127.0.0.1';
    this.rdbFilePath = options.rdbFilePath ?? './dump.rdb';
    this.engine = options.engine ?? new StorageEngine();
    this.pubsub = options.pubsub ?? new PubSubHub();
    this.transactions = options.transactions ?? new TransactionManager();
    this.blocking = options.blocking ?? new BlockingManager({ storage: this.engine });
    this.slowlog = options.slowlog ?? new SlowlogManager();
    this.replication =
      options.replication ??
      new ReplicationManager({
        engine: this.engine,
        listeningPort: this.port,
      });

    this.server = net.createServer((socket) => this.handleConnection(socket));
    this.server.on('error', (err) => {
      console.error('[Server Error]', err);
    });
  }

  public getStorage(): StorageEngine {
    return this.engine;
  }

  public getReplication(): ReplicationManager {
    return this.replication;
  }

  public getPubSub(): PubSubHub {
    return this.pubsub;
  }

  public getTransactions(): TransactionManager {
    return this.transactions;
  }

  public getBlocking(): BlockingManager {
    return this.blocking;
  }

  public getSlowlog(): SlowlogManager {
    return this.slowlog;
  }

  public start(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.server.listen(this.port, this.host, () => {
        this.isRunning = true;
        console.log(`[RESP Server] Listening on ${this.host}:${this.port} (Process PID: ${process.pid})`);
        resolve();
      });

      this.server.once('error', (err) => {
        if (!this.isRunning) reject(err);
      });
    });
  }

  public stop(): Promise<void> {
    return new Promise((resolve) => {
      this.replication.disconnect().catch(() => {});
      this.engine.close();
      this.server.close(() => {
        this.isRunning = false;
        resolve();
      });
    });
  }

  private handleConnection(socket: net.Socket): void {
    socket.setNoDelay(true);
    const parser = new RESPParser();
    const session = this.transactions.createSession();
    this.socketSessions.set(socket, session);

    socket.on('data', (chunk) => {
      try {
        parser.feed(chunk);
        const commands = parser.parse();

        for (const cmd of commands) {
          const startUs = performance.now() * 1000;
          const response = this.executeCommand(cmd, socket);
          const durationUs = performance.now() * 1000 - startUs;

          if (cmd.type === 'array' && cmd.value.length > 0) {
            const firstCmd = (cmd.value[0] as any).value?.toString('utf8').toUpperCase();
            if (firstCmd !== 'SLOWLOG') {
              this.slowlog.record(
                durationUs,
                cmd.value.map((v: any) => v.value),
                socket.remoteAddress ? `${socket.remoteAddress}:${socket.remotePort}` : ''
              );
            }
          }

          if (response && socket.writable) {
            socket.write(response);
          }
        }
      } catch (err) {
        if (err instanceof ProtocolError) {
          socket.write(RESPSerializer.encodeError(err.message, 'ERR'));
          socket.end();
        } else if (err instanceof WrongTypeError) {
          socket.write(RESPSerializer.encodeError(err.message, 'WRONGTYPE'));
        } else if (err instanceof OOMError) {
          socket.write(RESPSerializer.encodeError(err.message, 'OOM'));
        } else {
          console.error('[Socket Execution Error]', err);
          socket.write(RESPSerializer.encodeError('Internal server error'));
        }
      }
    });

    socket.on('close', () => {
      this.pubsub.removeSubscriber(socket);
      this.blocking.removeClient(socket);
      this.transactions.unwatch(session);
      this.socketSessions.delete(socket);
    });

    socket.on('error', (err) => {
      if ((err as any).code !== 'ECONNRESET') {
        console.error('[Client Socket Error]', err.message);
      }
    });
  }

  private notifyStreamKey(key: string): void {
    const listeners = this.streamListeners.get(key);
    if (listeners) {
      for (const cb of Array.from(listeners)) {
        cb();
      }
    }
  }

  private formatStreamEntriesArray(entries: StreamEntry[]): Buffer {
    const list: any[] = [];
    for (const e of entries) {
      const flat: (string | Buffer)[] = [];
      for (const [k, v] of e.fields) {
        flat.push(k, v);
      }
      list.push([e.id, flat]);
    }
    return RESPSerializer.encodeArray(list);
  }

  private static readonly WRITE_COMMANDS = new Set([
    'SET', 'DEL', 'HSET', 'HDEL', 'LPUSH', 'RPUSH', 'LPOP', 'RPOP',
    'SADD', 'SREM', 'ZADD', 'ZREM', 'EXPIRE', 'PEXPIRE', 'PERSIST',
    'FLUSHALL', 'FLUSHDB', 'XADD', 'SETBIT', 'BITOP', 'PFADD', 'PFMERGE'
  ]);

  private touchKeysForCommand(commandName: string, args: RESPValue[]): void {
    if (args.length < 2) return;
    if (commandName === 'FLUSHALL' || commandName === 'FLUSHDB') {
      for (const key of this.transactions.keyVersions.keys()) {
        this.transactions.touchKey(key);
      }
      return;
    }
    if (commandName === 'DEL' || commandName === 'EXISTS') {
      for (let i = 1; i < args.length; i++) {
        this.transactions.touchKey((args[i] as any).value.toString('utf8'));
      }
      return;
    }
    if (commandName === 'BITOP' && args.length >= 3) {
      const destKey = (args[2] as any).value.toString('utf8');
      this.transactions.touchKey(destKey);
      return;
    }
    const key = (args[1] as any).value.toString('utf8');
    this.transactions.touchKey(key);
  }

  private executeCommand(value: RESPValue, socket: net.Socket): Buffer | null {
    if (value.type !== 'array') {
      return RESPSerializer.encodeError('Expected array command');
    }

    const args = value.value;
    if (args.length === 0) return null;

    const first = args[0];
    if (first.type !== 'bulk_string') {
      return RESPSerializer.encodeError('Invalid command token format');
    }

    const commandName = first.value.toString('utf8').toUpperCase();
    const session = this.socketSessions.get(socket) ?? this.transactions.createSession();

    if (commandName === 'REPLCONF' || commandName === 'PSYNC') {
      return this.replication.handleCommand(value, socket);
    }

    // 1. PubSub context mode filter
    if (this.pubsub.isPubSubMode(socket)) {
      const allowed = new Set([
        'SUBSCRIBE',
        'PSUBSCRIBE',
        'UNSUBSCRIBE',
        'PUNSUBSCRIBE',
        'PING',
        'QUIT',
      ]);
      if (!allowed.has(commandName)) {
        return RESPSerializer.encodeError('only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context');
      }
    }

    // 2. Transaction MULTI queueing check
    if (session.isMulti) {
      if (commandName === 'EXEC') {
        return this.transactions.exec(session);
      }
      if (commandName === 'DISCARD') {
        return this.transactions.discard(session);
      }
      if (commandName === 'MULTI') {
        return this.transactions.multi(session);
      }
      if (commandName === 'WATCH') {
        const keys = args.slice(1).map((a: any) => a.value.toString('utf8'));
        return this.transactions.watch(session, ...keys);
      }

      session.enqueue(
        commandName,
        args.map((a: any) => a.value),
        () => {
          const res = this.dispatchCommand(commandName, args, socket, session) ?? RESP_CONSTANTS.OK;
          if (res && TCPServer.WRITE_COMMANDS.has(commandName) && res[0] !== 0x2d) {
            this.replication.propagate(RESPSerializer.encodeArray(args));
            this.touchKeysForCommand(commandName, args);
          }
          return res;
        }
      );
      return RESP_CONSTANTS.QUEUED;
    }

    const result = this.dispatchCommand(commandName, args, socket, session);
    if (result && TCPServer.WRITE_COMMANDS.has(commandName)) {
      if (result.length > 0 && result[0] !== 0x2d) {
        const isNullBulk = result.length === 5 && result.toString() === '$-1\r\n';
        if (!isNullBulk) {
          this.replication.propagate(RESPSerializer.encodeArray(args));
          this.touchKeysForCommand(commandName, args);
        }
      }
    }
    return result;
  }

  private dispatchCommand(
    commandName: string,
    args: RESPValue[],
    socket: net.Socket,
    session: TransactionSession
  ): Buffer | null {
    try {
      switch (commandName) {
        // =======================
        // CONNECTION & BASE
        // =======================
        case 'PING': {
          if (args.length === 1) return RESP_CONSTANTS.PONG;
          if (args.length === 2 && args[1].type === 'bulk_string') {
            return RESPSerializer.encodeBulkString(args[1].value);
          }
          return RESPSerializer.encodeError("wrong number of arguments for 'ping' command");
        }

        case 'ECHO': {
          if (args.length !== 2 || args[1].type !== 'bulk_string') {
            return RESPSerializer.encodeError("wrong number of arguments for 'echo' command");
          }
          return RESPSerializer.encodeBulkString(args[1].value);
        }

        case 'TYPE': {
          if (args.length !== 2 || args[1].type !== 'bulk_string') {
            return RESPSerializer.encodeError("wrong number of arguments for 'type' command");
          }
          const key = args[1].value.toString('utf8');
          return RESPSerializer.encodeSimpleString(this.engine.type(key));
        }

        // =======================
        // TRANSACTIONS
        // =======================
        case 'MULTI': {
          return this.transactions.multi(session);
        }

        case 'EXEC': {
          return this.transactions.exec(session);
        }

        case 'DISCARD': {
          return this.transactions.discard(session);
        }

        case 'WATCH': {
          if (args.length < 2) {
            return RESPSerializer.encodeError("wrong number of arguments for 'watch' command");
          }
          const keys = args.slice(1).map((a: any) => a.value.toString('utf8'));
          return this.transactions.watch(session, ...keys);
        }

        case 'UNWATCH': {
          return this.transactions.unwatch(session);
        }

        // =======================
        // PUB / SUB
        // =======================
        case 'SUBSCRIBE': {
          if (args.length < 2) {
            return RESPSerializer.encodeError("wrong number of arguments for 'subscribe' command");
          }
          const channels = args.slice(1).map((a: any) => a.value.toString('utf8'));
          const frames = this.pubsub.subscribe(socket, ...channels);
          return Buffer.concat(frames);
        }

        case 'UNSUBSCRIBE': {
          const channels = args.slice(1).map((a: any) => a.value.toString('utf8'));
          const frames = this.pubsub.unsubscribe(socket, ...channels);
          return Buffer.concat(frames);
        }

        case 'PSUBSCRIBE': {
          if (args.length < 2) {
            return RESPSerializer.encodeError("wrong number of arguments for 'psubscribe' command");
          }
          const patterns = args.slice(1).map((a: any) => a.value.toString('utf8'));
          const frames = this.pubsub.psubscribe(socket, ...patterns);
          return Buffer.concat(frames);
        }

        case 'PUNSUBSCRIBE': {
          const patterns = args.slice(1).map((a: any) => a.value.toString('utf8'));
          const frames = this.pubsub.punsubscribe(socket, ...patterns);
          return Buffer.concat(frames);
        }

        case 'PUBLISH': {
          if (args.length !== 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'publish' command");
          }
          const channel = (args[1] as any).value.toString('utf8');
          const message = (args[2] as any).value;
          const count = this.pubsub.publish(channel, message);
          return RESPSerializer.encodeInteger(count);
        }

        // =======================
        // STRING OPERATIONS
        // =======================
        case 'SET': {
          if (args.length < 3 || args[1].type !== 'bulk_string' || args[2].type !== 'bulk_string') {
            return RESPSerializer.encodeError("wrong number of arguments for 'set' command");
          }

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
              if (isNaN(sec)) return RESPSerializer.encodeError('value is not an integer or out of range');
              options.ex = sec;
            } else if (opt === 'PX' && i + 1 < args.length) {
              const ms = Number((args[++i] as any).value?.toString('utf8'));
              if (isNaN(ms)) return RESPSerializer.encodeError('value is not an integer or out of range');
              options.px = ms;
            } else {
              return RESPSerializer.encodeError(`syntax error near '${opt}'`);
            }
          }

          const success = this.engine.set(key, val, options);
          if (options.nx || options.xx) {
            return success ? RESP_CONSTANTS.OK : RESP_CONSTANTS.NULL_BULK;
          }
          return RESP_CONSTANTS.OK;
        }

        case 'GET': {
          if (args.length !== 2 || args[1].type !== 'bulk_string') {
            return RESPSerializer.encodeError("wrong number of arguments for 'get' command");
          }
          const key = args[1].value.toString('utf8');
          return RESPSerializer.encodeBulkString(this.engine.get(key));
        }

        case 'MGET': {
          if (args.length < 2) {
            return RESPSerializer.encodeError("wrong number of arguments for 'mget' command");
          }
          const keys = args.slice(1).map((a: any) => a.value.toString('utf8'));
          return RESPSerializer.encodeArray(this.engine.mget(keys));
        }

        // =======================
        // HASH OPERATIONS
        // =======================
        case 'HSET': {
          if (args.length < 4 || (args.length - 2) % 2 !== 0) {
            return RESPSerializer.encodeError("wrong number of arguments for 'hset' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          let addedCount = 0;
          for (let i = 2; i < args.length; i += 2) {
            const field = (args[i] as any).value.toString('utf8');
            const val = (args[i + 1] as any).value;
            addedCount += this.engine.hset(key, field, val);
          }
          return RESPSerializer.encodeInteger(addedCount);
        }

        case 'HGET': {
          if (args.length !== 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'hget' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const field = (args[2] as any).value.toString('utf8');
          return RESPSerializer.encodeBulkString(this.engine.hget(key, field));
        }

        case 'HMGET': {
          if (args.length < 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'hmget' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const fields = args.slice(2).map((a: any) => a.value.toString('utf8'));
          return RESPSerializer.encodeArray(this.engine.hmget(key, fields));
        }

        case 'HDEL': {
          if (args.length < 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'hdel' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const fields = args.slice(2).map((a: any) => a.value.toString('utf8'));
          return RESPSerializer.encodeInteger(this.engine.hdel(key, ...fields));
        }

        case 'HEXISTS': {
          if (args.length !== 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'hexists' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const field = (args[2] as any).value.toString('utf8');
          return RESPSerializer.encodeInteger(this.engine.hexists(key, field) ? 1 : 0);
        }

        case 'HGETALL': {
          if (args.length !== 2) {
            return RESPSerializer.encodeError("wrong number of arguments for 'hgetall' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const pairs = this.engine.hgetall(key);
          const flatList: (string | Buffer)[] = [];
          for (const [f, v] of pairs) {
            flatList.push(f, v);
          }
          return RESPSerializer.encodeArray(flatList);
        }

        case 'HKEYS': {
          if (args.length !== 2) {
            return RESPSerializer.encodeError("wrong number of arguments for 'hkeys' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          return RESPSerializer.encodeArray(this.engine.hkeys(key));
        }

        case 'HVALS': {
          if (args.length !== 2) {
            return RESPSerializer.encodeError("wrong number of arguments for 'hvals' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          return RESPSerializer.encodeArray(this.engine.hvals(key));
        }

        case 'HLEN': {
          if (args.length !== 2) {
            return RESPSerializer.encodeError("wrong number of arguments for 'hlen' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          return RESPSerializer.encodeInteger(this.engine.hlen(key));
        }

        // =======================
        // LIST OPERATIONS
        // =======================
        case 'LPUSH': {
          if (args.length < 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'lpush' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const values = args.slice(2).map((a: any) => a.value);
          if (this.blocking.hasBlockedClients(key)) {
            for (const val of values) {
              if (!this.blocking.signalPush(key, val, 'left')) {
                this.engine.lpush(key, val);
              }
            }
            return RESPSerializer.encodeInteger(this.engine.llen(key));
          }
          return RESPSerializer.encodeInteger(this.engine.lpush(key, ...values));
        }

        case 'RPUSH': {
          if (args.length < 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'rpush' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const values = args.slice(2).map((a: any) => a.value);
          if (this.blocking.hasBlockedClients(key)) {
            for (const val of values) {
              if (!this.blocking.signalPush(key, val, 'right')) {
                this.engine.rpush(key, val);
              }
            }
            return RESPSerializer.encodeInteger(this.engine.llen(key));
          }
          return RESPSerializer.encodeInteger(this.engine.rpush(key, ...values));
        }

        case 'BLPOP': {
          if (args.length < 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'blpop' command");
          }
          const timeoutStr = (args[args.length - 1] as any).value.toString('utf8');
          const timeoutSec = Number(timeoutStr);
          if (isNaN(timeoutSec) || timeoutSec < 0) {
            return RESPSerializer.encodeError('timeout is not an integer or out of range');
          }
          const keys = args.slice(1, -1).map((a: any) => a.value.toString('utf8'));

          if (session.isMulti) {
            for (const key of keys) {
              const popped = this.engine.lpop(key);
              if (popped !== null && popped !== undefined) {
                const val = Array.isArray(popped) ? popped[0] : popped;
                if (val !== null && val !== undefined) {
                  return BlockingManager.formatPopResponse(key, val);
                }
              }
            }
            return RESP_CONSTANTS.NULL_ARRAY;
          }

          this.blocking
            .block(socket, keys, timeoutSec, 'left')
            .then((res) => {
              if (res && socket.writable) {
                socket.write(res);
              }
            })
            .catch((err) => {
              if (socket.writable) {
                socket.write(RESPSerializer.encodeError(err.message));
              }
            });
          return null;
        }

        case 'BRPOP': {
          if (args.length < 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'brpop' command");
          }
          const timeoutStr = (args[args.length - 1] as any).value.toString('utf8');
          const timeoutSec = Number(timeoutStr);
          if (isNaN(timeoutSec) || timeoutSec < 0) {
            return RESPSerializer.encodeError('timeout is not an integer or out of range');
          }
          const keys = args.slice(1, -1).map((a: any) => a.value.toString('utf8'));

          if (session.isMulti) {
            for (const key of keys) {
              const popped = this.engine.rpop(key);
              if (popped !== null && popped !== undefined) {
                const val = Array.isArray(popped) ? popped[0] : popped;
                if (val !== null && val !== undefined) {
                  return BlockingManager.formatPopResponse(key, val);
                }
              }
            }
            return RESP_CONSTANTS.NULL_ARRAY;
          }

          this.blocking
            .block(socket, keys, timeoutSec, 'right')
            .then((res) => {
              if (res && socket.writable) {
                socket.write(res);
              }
            })
            .catch((err) => {
              if (socket.writable) {
                socket.write(RESPSerializer.encodeError(err.message));
              }
            });
          return null;
        }

        case 'LPOP': {
          if (args.length < 2) {
            return RESPSerializer.encodeError("wrong number of arguments for 'lpop' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const count = args.length >= 3 ? parseInt((args[2] as any).value.toString('utf8'), 10) : 1;
          const res = this.engine.lpop(key, count);
          if (Array.isArray(res)) return RESPSerializer.encodeArray(res);
          return RESPSerializer.encodeBulkString(res);
        }

        case 'RPOP': {
          if (args.length < 2) {
            return RESPSerializer.encodeError("wrong number of arguments for 'rpop' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const count = args.length >= 3 ? parseInt((args[2] as any).value.toString('utf8'), 10) : 1;
          const res = this.engine.rpop(key, count);
          if (Array.isArray(res)) return RESPSerializer.encodeArray(res);
          return RESPSerializer.encodeBulkString(res);
        }

        case 'LRANGE': {
          if (args.length !== 4) {
            return RESPSerializer.encodeError("wrong number of arguments for 'lrange' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const start = parseInt((args[2] as any).value.toString('utf8'), 10);
          const stop = parseInt((args[3] as any).value.toString('utf8'), 10);
          return RESPSerializer.encodeArray(this.engine.lrange(key, start, stop));
        }

        case 'LLEN': {
          if (args.length !== 2) {
            return RESPSerializer.encodeError("wrong number of arguments for 'llen' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          return RESPSerializer.encodeInteger(this.engine.llen(key));
        }

        // =======================
        // SET OPERATIONS
        // =======================
        case 'SADD': {
          if (args.length < 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'sadd' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const members = args.slice(2).map((a: any) => a.value.toString('utf8'));
          return RESPSerializer.encodeInteger(this.engine.sadd(key, ...members));
        }

        case 'SREM': {
          if (args.length < 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'srem' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const members = args.slice(2).map((a: any) => a.value.toString('utf8'));
          return RESPSerializer.encodeInteger(this.engine.srem(key, ...members));
        }

        case 'SMEMBERS': {
          if (args.length !== 2) {
            return RESPSerializer.encodeError("wrong number of arguments for 'smembers' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          return RESPSerializer.encodeArray(this.engine.smembers(key));
        }

        case 'SISMEMBER': {
          if (args.length !== 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'sismember' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const member = (args[2] as any).value.toString('utf8');
          return RESPSerializer.encodeInteger(this.engine.sismember(key, member) ? 1 : 0);
        }

        case 'SCARD': {
          if (args.length !== 2) {
            return RESPSerializer.encodeError("wrong number of arguments for 'scard' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          return RESPSerializer.encodeInteger(this.engine.scard(key));
        }

        // =======================
        // SORTED SET (ZSET)
        // =======================
        case 'ZADD': {
          if (args.length < 4 || (args.length - 2) % 2 !== 0) {
            return RESPSerializer.encodeError("wrong number of arguments for 'zadd' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          let added = 0;
          for (let i = 2; i < args.length; i += 2) {
            const score = Number((args[i] as any).value.toString('utf8'));
            if (isNaN(score)) return RESPSerializer.encodeError('value is not a valid float');
            const member = (args[i + 1] as any).value.toString('utf8');
            added += this.engine.zadd(key, score, member);
          }
          return RESPSerializer.encodeInteger(added);
        }

        case 'ZSCORE': {
          if (args.length !== 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'zscore' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const member = (args[2] as any).value.toString('utf8');
          const score = this.engine.zscore(key, member);
          return score === null ? RESP_CONSTANTS.NULL_BULK : RESPSerializer.encodeBulkString(score.toString());
        }

        case 'ZRANGE': {
          if (args.length < 4) {
            return RESPSerializer.encodeError("wrong number of arguments for 'zrange' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const start = parseInt((args[2] as any).value.toString('utf8'), 10);
          const stop = parseInt((args[3] as any).value.toString('utf8'), 10);
          const withScores = args.length >= 5 && (args[4] as any).value.toString('utf8').toUpperCase() === 'WITHSCORES';

          if (withScores) {
            const list = this.engine.zrangeWithScores(key, start, stop);
            const flat: (string | number)[] = [];
            for (const item of list) {
              flat.push(item.member, item.score.toString());
            }
            return RESPSerializer.encodeArray(flat);
          } else {
            return RESPSerializer.encodeArray(this.engine.zrange(key, start, stop));
          }
        }

        case 'ZREM': {
          if (args.length < 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'zrem' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const members = args.slice(2).map((a: any) => a.value.toString('utf8'));
          return RESPSerializer.encodeInteger(this.engine.zrem(key, ...members));
        }

        case 'ZCARD': {
          if (args.length !== 2) {
            return RESPSerializer.encodeError("wrong number of arguments for 'zcard' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          return RESPSerializer.encodeInteger(this.engine.zcard(key));
        }

        // =======================
        // KEY & DB MANAGEMENT
        // =======================
        case 'DEL': {
          if (args.length < 2) return RESPSerializer.encodeError("wrong number of arguments for 'del' command");
          const keys = args.slice(1).map((a: any) => a.value.toString('utf8'));
          return RESPSerializer.encodeInteger(this.engine.del(...keys));
        }

        case 'EXISTS': {
          if (args.length < 2) return RESPSerializer.encodeError("wrong number of arguments for 'exists' command");
          const keys = args.slice(1).map((a: any) => a.value.toString('utf8'));
          return RESPSerializer.encodeInteger(this.engine.exists(...keys));
        }

        case 'TTL': {
          if (args.length !== 2) return RESPSerializer.encodeError("wrong number of arguments for 'ttl' command");
          return RESPSerializer.encodeInteger(this.engine.ttl((args[1] as any).value.toString('utf8')));
        }

        case 'PTTL': {
          if (args.length !== 2) return RESPSerializer.encodeError("wrong number of arguments for 'pttl' command");
          return RESPSerializer.encodeInteger(this.engine.pttl((args[1] as any).value.toString('utf8')));
        }

        case 'EXPIRE': {
          if (args.length !== 3) return RESPSerializer.encodeError("wrong number of arguments for 'expire' command");
          const key = (args[1] as any).value.toString('utf8');
          const sec = parseInt((args[2] as any).value.toString('utf8'), 10);
          if (isNaN(sec)) return RESPSerializer.encodeError('value is not an integer or out of range');
          return RESPSerializer.encodeInteger(this.engine.expire(key, sec) ? 1 : 0);
        }

        case 'PEXPIRE': {
          if (args.length !== 3) return RESPSerializer.encodeError("wrong number of arguments for 'pexpire' command");
          const key = (args[1] as any).value.toString('utf8');
          const ms = parseInt((args[2] as any).value.toString('utf8'), 10);
          if (isNaN(ms)) return RESPSerializer.encodeError('value is not an integer or out of range');
          return RESPSerializer.encodeInteger(this.engine.pexpire(key, ms) ? 1 : 0);
        }

        case 'PERSIST': {
          if (args.length !== 2) return RESPSerializer.encodeError("wrong number of arguments for 'persist' command");
          return RESPSerializer.encodeInteger(this.engine.persist((args[1] as any).value.toString('utf8')) ? 1 : 0);
        }

        case 'KEYS': {
          if (args.length !== 2) return RESPSerializer.encodeError("wrong number of arguments for 'keys' command");
          return RESPSerializer.encodeArray(this.engine.keys((args[1] as any).value.toString('utf8')));
        }

        case 'DBSIZE': {
          return RESPSerializer.encodeInteger(this.engine.dbsize());
        }

        case 'FLUSHALL':
        case 'FLUSHDB': {
          this.engine.flushall();
          return RESP_CONSTANTS.OK;
        }

        case 'BGREWRITEAOF': {
          const aof = this.engine.getAOF();
          if (!aof) return RESPSerializer.encodeError('AOF persistence is not enabled');
          aof.rewrite(this.engine).catch((err) => console.error('[AOF Rewrite Error]', err));
          return RESPSerializer.encodeSimpleString('Background append only file rewriting started');
        }

        case 'SAVE': {
          try {
            RDBEncoder.save(this.rdbFilePath, this.engine);
            return RESP_CONSTANTS.OK;
          } catch (err: any) {
            return RESPSerializer.encodeError(`ERR SAVE failed: ${err.message}`);
          }
        }

        case 'BGSAVE': {
          RDBEncoder.bgsave(this.rdbFilePath, this.engine).catch((err) =>
            console.error('[BGSAVE Error]', err)
          );
          return RESPSerializer.encodeSimpleString('Background saving started');
        }

        case 'INFO': {
          const stats = this.engine.stats();
          const mem = process.memoryUsage();
          const lines = [
            '# Server',
            'redis_version:7.2.0-compat',
            `process_id:${process.pid}`,
            '# Memory',
            `used_memory:${stats.memoryEstimateBytes}`,
            `used_memory_rss:${mem.rss}`,
            `used_memory_heap:${mem.heapUsed}`,
            `maxmemory:${this.engine.getMemoryGovernor().getMaxMemory()}`,
            `maxmemory_policy:${this.engine.getMemoryGovernor().getPolicy()}`,
            '# Persistence',
            `aof_enabled:${stats.aofEnabled ? 1 : 0}`,
            `aof_current_size:${stats.aofFileSize}`,
            `aof_rewrites:${stats.aofRewritesCount}`,
            '# Stats',
            `keyspace_hits:${stats.hits}`,
            `keyspace_misses:${stats.misses}`,
            `expired_keys_passive:${stats.expiredPassive}`,
            `expired_keys_active:${stats.expiredActive}`,
            `active_sweeps_count:${stats.activeCyclesCount}`,
            '# Keyspace',
            `db0:keys=${stats.totalKeys},expires=${stats.expiringKeys}`,
            '# Replication',
            `role:${this.replication.role}`,
            `connected_slaves:${this.replication.replicas.size}`,
            `master_replid:${this.replication.master_replid}`,
            `master_repl_offset:${this.replication.offset}`,
            'second_repl_offset:-1',
            'repl_backlog_active:1',
            `repl_backlog_size:${this.replication.backlog.size}`,
            `repl_backlog_first_byte_offset:${this.replication.backlog.oldestOffset}`,
            `repl_backlog_histlen:${this.replication.backlog.bufferedBytes}`,
          ];
          return RESPSerializer.encodeBulkString(lines.join('\r\n'));
        }

        case 'CONFIG': {
          if (args.length < 2) {
            return RESPSerializer.encodeError("wrong number of arguments for 'config' command");
          }
          const subCmd = (args[1] as any).value?.toString('utf8').toUpperCase() ?? '';
          if (subCmd === 'GET' && args.length >= 3) {
            const param = (args[2] as any).value?.toString('utf8').toLowerCase() ?? '';
            if (param === 'maxmemory') {
              return RESPSerializer.encodeArray([
                Buffer.from('maxmemory'),
                Buffer.from(this.engine.getMemoryGovernor().getMaxMemory().toString()),
              ]);
            } else if (param === 'maxmemory-policy') {
              return RESPSerializer.encodeArray([
                Buffer.from('maxmemory-policy'),
                Buffer.from(this.engine.getMemoryGovernor().getPolicy()),
              ]);
            }
            return RESPSerializer.encodeArray([]);
          } else if (subCmd === 'SET' && args.length >= 4) {
            const param = (args[2] as any).value?.toString('utf8').toLowerCase() ?? '';
            const val = (args[3] as any).value?.toString('utf8') ?? '';
            if (param === 'maxmemory') {
              this.engine.getMemoryGovernor().setMaxMemory(Number(val));
              return RESP_CONSTANTS.OK;
            } else if (param === 'maxmemory-policy') {
              this.engine.getMemoryGovernor().setPolicy(val as EvictionPolicy);
              return RESP_CONSTANTS.OK;
            }
            return RESPSerializer.encodeError(`Unsupported CONFIG parameter ${param}`);
          }
          return RESPSerializer.encodeError(`Unknown CONFIG subcommand ${subCmd}`);
        }

        case 'REPLICAOF': {
          if (args.length !== 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'replicaof' command");
          }
          const targetHost = (args[1] as any).value.toString('utf8');
          const targetPortStr = (args[2] as any).value.toString('utf8');
          if (targetHost.toUpperCase() === 'NO' && targetPortStr.toUpperCase() === 'ONE') {
            this.replication.role = 'master';
            return RESP_CONSTANTS.OK;
          }
          const targetPort = parseInt(targetPortStr, 10);
          this.replication.replicateFrom(targetHost, targetPort).catch((err) => {
            console.error('[Replication Error]', err);
          });
          return RESP_CONSTANTS.OK;
        }

        case 'XADD': {
          if (args.length < 5) {
            return RESPSerializer.encodeError("wrong number of arguments for 'xadd' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          let argIdx = 2;
          let maxlen: number | undefined;

          const token2 = (args[argIdx] as any).value.toString('utf8').toUpperCase();
          if (token2 === 'MAXLEN') {
            argIdx++;
            if (argIdx < args.length && (args[argIdx] as any).value.toString('utf8') === '~') {
              argIdx++;
            }
            if (argIdx >= args.length) return RESPSerializer.encodeError('syntax error');
            maxlen = parseInt((args[argIdx++] as any).value.toString('utf8'), 10);
            if (isNaN(maxlen) || maxlen < 0) return RESPSerializer.encodeError('value is not an integer or out of range');
          }

          if (argIdx >= args.length) {
            return RESPSerializer.encodeError("wrong number of arguments for 'xadd' command");
          }

          const idSpec = (args[argIdx++] as any).value.toString('utf8');
          const remaining = args.length - argIdx;
          if (remaining < 2 || remaining % 2 !== 0) {
            return RESPSerializer.encodeError("wrong number of arguments for 'xadd' command");
          }

          const fields: [string, Buffer][] = [];
          for (let i = argIdx; i < args.length; i += 2) {
            const f = (args[i] as any).value.toString('utf8');
            const v = (args[i + 1] as any).value;
            fields.push([f, v]);
          }

          try {
            const addedId = this.engine.xadd(key, idSpec, fields, maxlen);
            this.notifyStreamKey(key);
            return RESPSerializer.encodeBulkString(addedId);
          } catch (err: any) {
            return RESPSerializer.encodeError(err.message);
          }
        }

        case 'XRANGE': {
          if (args.length < 4) {
            return RESPSerializer.encodeError("wrong number of arguments for 'xrange' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const start = (args[2] as any).value.toString('utf8');
          const end = (args[3] as any).value.toString('utf8');
          let count: number | undefined;
          if (args.length >= 6 && (args[4] as any).value.toString('utf8').toUpperCase() === 'COUNT') {
            count = parseInt((args[5] as any).value.toString('utf8'), 10);
            if (isNaN(count)) return RESPSerializer.encodeError('value is not an integer or out of range');
          }

          const entries = this.engine.xrange(key, start, end, count);
          return this.formatStreamEntriesArray(entries);
        }

        case 'XREVRANGE': {
          if (args.length < 4) {
            return RESPSerializer.encodeError("wrong number of arguments for 'xrevrange' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const end = (args[2] as any).value.toString('utf8');
          const start = (args[3] as any).value.toString('utf8');
          let count: number | undefined;
          if (args.length >= 6 && (args[4] as any).value.toString('utf8').toUpperCase() === 'COUNT') {
            count = parseInt((args[5] as any).value.toString('utf8'), 10);
            if (isNaN(count)) return RESPSerializer.encodeError('value is not an integer or out of range');
          }

          const entries = this.engine.xrevrange(key, end, start, count);
          return this.formatStreamEntriesArray(entries);
        }

        case 'XLEN': {
          if (args.length !== 2) {
            return RESPSerializer.encodeError("wrong number of arguments for 'xlen' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          return RESPSerializer.encodeInteger(this.engine.xlen(key));
        }

        case 'XREAD': {
          if (args.length < 4) {
            return RESPSerializer.encodeError("wrong number of arguments for 'xread' command");
          }
          let count: number | undefined;
          let blockMs: number | undefined;
          let streamsIdx = -1;

          for (let i = 1; i < args.length; i++) {
            const token = (args[i] as any).value.toString('utf8').toUpperCase();
            if (token === 'COUNT' && i + 1 < args.length) {
              count = parseInt((args[++i] as any).value.toString('utf8'), 10);
              if (isNaN(count) || count < 0) return RESPSerializer.encodeError('value is not an integer or out of range');
            } else if (token === 'BLOCK' && i + 1 < args.length) {
              blockMs = parseInt((args[++i] as any).value.toString('utf8'), 10);
              if (isNaN(blockMs) || blockMs < 0) return RESPSerializer.encodeError('value is not an integer or out of range');
            } else if (token === 'STREAMS') {
              streamsIdx = i;
              break;
            }
          }

          if (streamsIdx === -1) {
            return RESPSerializer.encodeError("syntax error, STREAMS keyword missing");
          }

          const streamTokens = args.slice(streamsIdx + 1);
          if (streamTokens.length < 2 || streamTokens.length % 2 !== 0) {
            return RESPSerializer.encodeError("wrong number of arguments for 'xread' command");
          }

          const half = streamTokens.length / 2;
          const streamKeys = streamTokens.slice(0, half).map((a: any) => a.value.toString('utf8'));
          const streamIds = streamTokens.slice(half).map((a: any) => a.value.toString('utf8'));
          const streamSpecs = streamKeys.map((k, idx) => ({ key: k, lastId: streamIds[idx] }));

          const formatXreadResult = (res: { key: string; entries: StreamEntry[] }[]) => {
            if (res.length === 0) return RESP_CONSTANTS.NULL_ARRAY;
            const outerList: any[] = [];
            for (const item of res) {
              const entriesFormatted: any[] = [];
              for (const e of item.entries) {
                const flat: (string | Buffer)[] = [];
                for (const [k, v] of e.fields) {
                  flat.push(k, v);
                }
                entriesFormatted.push([e.id, flat]);
              }
              outerList.push([item.key, entriesFormatted]);
            }
            return RESPSerializer.encodeArray(outerList);
          };

          const initial = this.engine.xread(streamSpecs, count);
          if (initial.length > 0 || blockMs === undefined) {
            return formatXreadResult(initial);
          }

          // Asynchronous blocking XREAD
          let timer: NodeJS.Timeout | null = null;
          let isResolved = false;

          const cleanupListeners = () => {
            for (const k of streamKeys) {
              const set = this.streamListeners.get(k);
              if (set) {
                set.delete(onNotify);
                if (set.size === 0) this.streamListeners.delete(k);
              }
            }
            if (timer) clearTimeout(timer);
          };

          const onNotify = () => {
            if (isResolved) return;
            const updated = this.engine.xread(streamSpecs, count);
            if (updated.length > 0) {
              isResolved = true;
              cleanupListeners();
              if (socket.writable) {
                socket.write(formatXreadResult(updated));
              }
            }
          };

          for (const k of streamKeys) {
            let set = this.streamListeners.get(k);
            if (!set) {
              set = new Set();
              this.streamListeners.set(k, set);
            }
            set.add(onNotify);
          }

          if (blockMs > 0) {
            timer = setTimeout(() => {
              if (isResolved) return;
              isResolved = true;
              cleanupListeners();
              if (socket.writable) {
                socket.write(RESP_CONSTANTS.NULL_ARRAY);
              }
            }, blockMs);
          }

          socket.once('close', cleanupListeners);
          return null;
        }

        case 'SCAN': {
          if (args.length < 2) {
            return RESPSerializer.encodeError("wrong number of arguments for 'scan' command");
          }
          const cursor = parseInt((args[1] as any).value.toString('utf8'), 10);
          if (isNaN(cursor) || cursor < 0) return RESPSerializer.encodeError('value is not an integer or out of range');

          let match: string | undefined;
          let count: number = 10;

          for (let i = 2; i < args.length; i++) {
            const opt = (args[i] as any).value?.toString('utf8').toUpperCase();
            if (opt === 'MATCH' && i + 1 < args.length) {
              match = (args[++i] as any).value?.toString('utf8');
            } else if (opt === 'COUNT' && i + 1 < args.length) {
              count = parseInt((args[++i] as any).value?.toString('utf8'), 10);
              if (isNaN(count) || count < 0) return RESPSerializer.encodeError('value is not an integer or out of range');
            }
          }

          const res = this.engine.scan(cursor, match, count);
          return RESPSerializer.encodeArray([res.cursor.toString(), res.keys]);
        }

        case 'HSCAN': {
          if (args.length < 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'hscan' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const cursor = parseInt((args[2] as any).value.toString('utf8'), 10);
          if (isNaN(cursor) || cursor < 0) return RESPSerializer.encodeError('value is not an integer or out of range');

          let match: string | undefined;
          let count: number = 10;
          for (let i = 3; i < args.length; i++) {
            const opt = (args[i] as any).value?.toString('utf8').toUpperCase();
            if (opt === 'MATCH' && i + 1 < args.length) {
              match = (args[++i] as any).value?.toString('utf8');
            } else if (opt === 'COUNT' && i + 1 < args.length) {
              count = parseInt((args[++i] as any).value?.toString('utf8'), 10);
              if (isNaN(count) || count < 0) return RESPSerializer.encodeError('value is not an integer or out of range');
            }
          }

          const res = this.engine.hscan(key, cursor, match, count);
          return RESPSerializer.encodeArray([res.cursor.toString(), res.entries]);
        }

        case 'SSCAN': {
          if (args.length < 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'sscan' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const cursor = parseInt((args[2] as any).value.toString('utf8'), 10);
          if (isNaN(cursor) || cursor < 0) return RESPSerializer.encodeError('value is not an integer or out of range');

          let match: string | undefined;
          let count: number = 10;
          for (let i = 3; i < args.length; i++) {
            const opt = (args[i] as any).value?.toString('utf8').toUpperCase();
            if (opt === 'MATCH' && i + 1 < args.length) {
              match = (args[++i] as any).value?.toString('utf8');
            } else if (opt === 'COUNT' && i + 1 < args.length) {
              count = parseInt((args[++i] as any).value?.toString('utf8'), 10);
              if (isNaN(count) || count < 0) return RESPSerializer.encodeError('value is not an integer or out of range');
            }
          }

          const res = this.engine.sscan(key, cursor, match, count);
          return RESPSerializer.encodeArray([res.cursor.toString(), res.members]);
        }

        case 'ZSCAN': {
          if (args.length < 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'zscan' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const cursor = parseInt((args[2] as any).value.toString('utf8'), 10);
          if (isNaN(cursor) || cursor < 0) return RESPSerializer.encodeError('value is not an integer or out of range');

          let match: string | undefined;
          let count: number = 10;
          for (let i = 3; i < args.length; i++) {
            const opt = (args[i] as any).value?.toString('utf8').toUpperCase();
            if (opt === 'MATCH' && i + 1 < args.length) {
              match = (args[++i] as any).value?.toString('utf8');
            } else if (opt === 'COUNT' && i + 1 < args.length) {
              count = parseInt((args[++i] as any).value?.toString('utf8'), 10);
              if (isNaN(count) || count < 0) return RESPSerializer.encodeError('value is not an integer or out of range');
            }
          }

          const res = this.engine.zscan(key, cursor, match, count);
          return RESPSerializer.encodeArray([res.cursor.toString(), res.entries]);
        }

        case 'SLOWLOG': {
          if (args.length < 2) {
            return RESPSerializer.encodeError("wrong number of arguments for 'slowlog' command");
          }
          const sub = (args[1] as any).value?.toString('utf8').toUpperCase();
          if (sub === 'GET') {
            const count = args.length >= 3 ? parseInt((args[2] as any).value?.toString('utf8'), 10) : undefined;
            const entries = this.slowlog.get(count);
            const respEntries = entries.map((e) => [
              e.id,
              e.timestamp,
              e.durationMicroseconds,
              e.args,
              e.clientAddress ?? '',
              e.clientName ?? '',
            ]);
            return RESPSerializer.encodeArray(respEntries);
          } else if (sub === 'LEN') {
            return RESPSerializer.encodeInteger(this.slowlog.len());
          } else if (sub === 'RESET') {
            this.slowlog.reset();
            return RESP_CONSTANTS.OK;
          }
          return RESPSerializer.encodeError(`Unknown SLOWLOG subcommand '${sub}'`);
        }

        case 'SETBIT': {
          if (args.length !== 4) {
            return RESPSerializer.encodeError("wrong number of arguments for 'setbit' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const offset = parseInt((args[2] as any).value.toString('utf8'), 10);
          if (isNaN(offset) || offset < 0 || offset > 4294967295) {
            return RESPSerializer.encodeError('bit offset is not an integer or out of range');
          }
          const bitVal = parseInt((args[3] as any).value.toString('utf8'), 10);
          if (isNaN(bitVal) || (bitVal !== 0 && bitVal !== 1)) {
            return RESPSerializer.encodeError('bit is not an integer or out of range');
          }
          const oldBit = this.engine.setbit(key, offset, bitVal as 0 | 1);
          return RESPSerializer.encodeInteger(oldBit);
        }

        case 'GETBIT': {
          if (args.length !== 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'getbit' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const offset = parseInt((args[2] as any).value.toString('utf8'), 10);
          if (isNaN(offset) || offset < 0) {
            return RESPSerializer.encodeError('bit offset is not an integer or out of range');
          }
          const bit = this.engine.getbit(key, offset);
          return RESPSerializer.encodeInteger(bit);
        }

        case 'BITCOUNT': {
          if (args.length !== 2 && args.length !== 4) {
            return RESPSerializer.encodeError("syntax error");
          }
          const key = (args[1] as any).value.toString('utf8');
          if (args.length === 4) {
            const start = parseInt((args[2] as any).value.toString('utf8'), 10);
            const end = parseInt((args[3] as any).value.toString('utf8'), 10);
            if (isNaN(start) || isNaN(end)) {
              return RESPSerializer.encodeError('value is not an integer or out of range');
            }
            return RESPSerializer.encodeInteger(this.engine.bitcount(key, start, end));
          }
          return RESPSerializer.encodeInteger(this.engine.bitcount(key));
        }

        case 'BITPOS': {
          if (args.length < 3 || args.length > 5) {
            return RESPSerializer.encodeError("wrong number of arguments for 'bitpos' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const bit = parseInt((args[2] as any).value.toString('utf8'), 10);
          if (isNaN(bit) || (bit !== 0 && bit !== 1)) {
            return RESPSerializer.encodeError('bit is not an integer or out of range');
          }
          let start: number | undefined;
          let end: number | undefined;
          if (args.length >= 4) {
            start = parseInt((args[3] as any).value.toString('utf8'), 10);
            if (isNaN(start)) return RESPSerializer.encodeError('value is not an integer or out of range');
          }
          if (args.length === 5) {
            end = parseInt((args[4] as any).value.toString('utf8'), 10);
            if (isNaN(end)) return RESPSerializer.encodeError('value is not an integer or out of range');
          }
          return RESPSerializer.encodeInteger(this.engine.bitpos(key, bit as 0 | 1, start, end));
        }

        case 'BITOP': {
          if (args.length < 4) {
            return RESPSerializer.encodeError("wrong number of arguments for 'bitop' command");
          }
          const op = (args[1] as any).value.toString('utf8').toUpperCase();
          if (op !== 'AND' && op !== 'OR' && op !== 'XOR' && op !== 'NOT') {
            return RESPSerializer.encodeError("syntax error");
          }
          if (op === 'NOT' && args.length !== 4) {
            return RESPSerializer.encodeError("BITOP NOT takes exactly one source key.");
          }
          const destKey = (args[2] as any).value.toString('utf8');
          const srcKeys = args.slice(3).map((a: any) => a.value.toString('utf8'));
          const len = this.engine.bitop(op as any, destKey, ...srcKeys);
          return RESPSerializer.encodeInteger(len);
        }

        case 'PFADD': {
          if (args.length < 2) {
            return RESPSerializer.encodeError("wrong number of arguments for 'pfadd' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const elements = args.slice(2).map((a: any) => a.value);
          const modified = this.engine.pfadd(key, ...elements);
          return RESPSerializer.encodeInteger(modified);
        }

        case 'PFCOUNT': {
          if (args.length < 2) {
            return RESPSerializer.encodeError("wrong number of arguments for 'pfcount' command");
          }
          const keys = args.slice(1).map((a: any) => a.value.toString('utf8'));
          const count = this.engine.pfcount(...keys);
          return RESPSerializer.encodeInteger(count);
        }

        case 'PFMERGE': {
          if (args.length < 2) {
            return RESPSerializer.encodeError("wrong number of arguments for 'pfmerge' command");
          }
          const destKey = (args[1] as any).value.toString('utf8');
          const srcKeys = args.slice(2).map((a: any) => a.value.toString('utf8'));
          this.engine.pfmerge(destKey, ...srcKeys);
          return RESP_CONSTANTS.OK;
        }

        case 'COMMAND': {
          return RESPSerializer.encodeArray([]);
        }

        case 'QUIT': {
          socket.write(RESP_CONSTANTS.OK);
          socket.end();
          return null;
        }

        default:
          return RESPSerializer.encodeError(`unknown command '${commandName}'`);
      }
    } catch (err) {
      if (err instanceof WrongTypeError) {
        return RESPSerializer.encodeError(err.message, 'WRONGTYPE');
      } else if (err instanceof OOMError) {
        return RESPSerializer.encodeError(err.message, 'OOM');
      }
      throw err;
    }
  }
}
