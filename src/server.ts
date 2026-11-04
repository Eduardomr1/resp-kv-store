import net from 'node:net';
import { performance } from 'node:perf_hooks';
import { RESPParser, ProtocolError } from './resp/parser.ts';
import { RESPSerializer, RESP_CONSTANTS } from './resp/serializer.ts';
import type { RESPValue } from './resp/types.ts';
import { StorageEngine } from './storage/engine.ts';
import { WrongTypeError, type SetOptions, type GetExOptions } from './storage/types.ts';
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
  private lastCommandMutated: boolean = false;
  private lastMutatedKeys: string[] | null = null;

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
    'SET', 'MSET', 'MSETNX', 'INCR', 'INCRBY', 'DECR', 'DECRBY', 'APPEND', 'GETDEL', 'GETEX',
    'DEL', 'HSET', 'HSETNX', 'HINCRBY', 'HDEL',
    'LPUSH', 'RPUSH', 'LPOP', 'RPOP', 'LSET', 'LTRIM', 'LMOVE',
    'SADD', 'SREM', 'SPOP', 'SMOVE', 'SUNIONSTORE', 'SINTERSTORE', 'SDIFFSTORE',
    'ZADD', 'ZINCRBY', 'ZREM', 'ZPOPMIN', 'ZPOPMAX', 'ZMPOP',
    'EXPIRE', 'PEXPIRE', 'EXPIREAT', 'PEXPIREAT', 'PERSIST',
    'FLUSHALL', 'FLUSHDB', 'XADD', 'SETBIT', 'BITOP', 'PFADD', 'PFMERGE',
    'BF.RESERVE', 'BF.ADD', 'BF.MADD', 'CMS.INITBYDIM', 'CMS.INITBYPROB', 'CMS.INCRBY', 'CMS.MERGE'
  ]);

  private static parseStrictInt(raw: string): number | null {
    if (!/^[+-]?\d+$/.test(raw)) return null;
    const num = Number(raw);
    if (!Number.isSafeInteger(num)) return null;
    return num;
  }

  private touchKeysForCommand(commandName: string, args: RESPValue[]): void {
    if (this.lastMutatedKeys !== null) {
      for (const k of this.lastMutatedKeys) {
        this.transactions.touchKey(k);
      }
      return;
    }
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
    if (commandName === 'MSET' || commandName === 'MSETNX') {
      for (let i = 1; i < args.length; i += 2) {
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
          this.lastCommandMutated = TCPServer.WRITE_COMMANDS.has(commandName);
          this.lastMutatedKeys = null;
          const res = this.dispatchCommand(commandName, args, socket, session) ?? RESP_CONSTANTS.OK;
          if (res && this.lastCommandMutated && res[0] !== 0x2d) {
            this.replication.propagate(RESPSerializer.encodeArray(args));
            this.touchKeysForCommand(commandName, args);
          }
          return res;
        }
      );
      return RESP_CONSTANTS.QUEUED;
    }

    this.lastCommandMutated = TCPServer.WRITE_COMMANDS.has(commandName);
    this.lastMutatedKeys = null;
    const result = this.dispatchCommand(commandName, args, socket, session);
    if (result && this.lastCommandMutated) {
      if (result.length > 0 && result[0] !== 0x2d) {
        this.replication.propagate(RESPSerializer.encodeArray(args));
        this.touchKeysForCommand(commandName, args);
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
          let returnOld = false;
          let ttlFlagCount = 0;

          for (let i = 3; i < args.length; i++) {
            const opt = (args[i] as any).value?.toString('utf8').toUpperCase();
            if (opt === 'NX') {
              if (options.xx) return RESPSerializer.encodeError('syntax error');
              options.nx = true;
            } else if (opt === 'XX') {
              if (options.nx) return RESPSerializer.encodeError('syntax error');
              options.xx = true;
            } else if (opt === 'GET') {
              returnOld = true;
            } else if (opt === 'KEEPTTL') {
              ttlFlagCount++;
              options.keepttl = true;
            } else if (opt === 'EX' && i + 1 < args.length) {
              ttlFlagCount++;
              const sec = Number((args[++i] as any).value?.toString('utf8'));
              if (!Number.isInteger(sec) || sec <= 0) {
                return RESPSerializer.encodeError("invalid expire time in 'set' command");
              }
              options.ex = sec;
            } else if (opt === 'PX' && i + 1 < args.length) {
              ttlFlagCount++;
              const ms = Number((args[++i] as any).value?.toString('utf8'));
              if (!Number.isInteger(ms) || ms <= 0) {
                return RESPSerializer.encodeError("invalid expire time in 'set' command");
              }
              options.px = ms;
            } else if (opt === 'EXAT' && i + 1 < args.length) {
              ttlFlagCount++;
              const sec = Number((args[++i] as any).value?.toString('utf8'));
              if (!Number.isInteger(sec) || sec <= 0) {
                return RESPSerializer.encodeError("invalid expire time in 'set' command");
              }
              options.exat = sec;
            } else if (opt === 'PXAT' && i + 1 < args.length) {
              ttlFlagCount++;
              const ms = Number((args[++i] as any).value?.toString('utf8'));
              if (!Number.isInteger(ms) || ms <= 0) {
                return RESPSerializer.encodeError("invalid expire time in 'set' command");
              }
              options.pxat = ms;
            } else {
              return RESPSerializer.encodeError(`syntax error near '${opt}'`);
            }
          }

          if (ttlFlagCount > 1) {
            return RESPSerializer.encodeError('syntax error');
          }

          const oldValue = returnOld ? this.engine.get(key) : null;
          const success = this.engine.set(key, val, options);
          this.lastCommandMutated = success;

          if (returnOld) {
            return RESPSerializer.encodeBulkString(oldValue);
          }
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

        case 'MSET': {
          if (args.length < 3 || (args.length - 1) % 2 !== 0) {
            return RESPSerializer.encodeError("wrong number of arguments for 'mset' command");
          }
          const pairs: Array<[string, Buffer]> = [];
          for (let i = 1; i < args.length; i += 2) {
            pairs.push([(args[i] as any).value.toString('utf8'), (args[i + 1] as any).value]);
          }
          this.engine.mset(pairs);
          return RESP_CONSTANTS.OK;
        }

        case 'MSETNX': {
          if (args.length < 3 || (args.length - 1) % 2 !== 0) {
            return RESPSerializer.encodeError("wrong number of arguments for 'msetnx' command");
          }
          const pairs: Array<[string, Buffer]> = [];
          for (let i = 1; i < args.length; i += 2) {
            pairs.push([(args[i] as any).value.toString('utf8'), (args[i + 1] as any).value]);
          }
          const ok = this.engine.msetnx(pairs);
          this.lastCommandMutated = ok;
          return RESPSerializer.encodeInteger(ok ? 1 : 0);
        }

        case 'INCR': {
          if (args.length !== 2) {
            return RESPSerializer.encodeError("wrong number of arguments for 'incr' command");
          }
          try {
            const val = this.engine.incr((args[1] as any).value.toString('utf8'));
            return RESPSerializer.encodeInteger(val);
          } catch (err: any) {
            if (err instanceof WrongTypeError || err instanceof OOMError) throw err;
            return RESPSerializer.encodeError(err.message);
          }
        }

        case 'INCRBY': {
          if (args.length !== 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'incrby' command");
          }
          try {
            const key = (args[1] as any).value.toString('utf8');
            const inc = (args[2] as any).value.toString('utf8');
            const val = this.engine.incrby(key, inc);
            return RESPSerializer.encodeInteger(val);
          } catch (err: any) {
            if (err instanceof WrongTypeError || err instanceof OOMError) throw err;
            return RESPSerializer.encodeError(err.message);
          }
        }

        case 'DECR': {
          if (args.length !== 2) {
            return RESPSerializer.encodeError("wrong number of arguments for 'decr' command");
          }
          try {
            const val = this.engine.decr((args[1] as any).value.toString('utf8'));
            return RESPSerializer.encodeInteger(val);
          } catch (err: any) {
            if (err instanceof WrongTypeError || err instanceof OOMError) throw err;
            return RESPSerializer.encodeError(err.message);
          }
        }

        case 'DECRBY': {
          if (args.length !== 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'decrby' command");
          }
          try {
            const key = (args[1] as any).value.toString('utf8');
            const dec = (args[2] as any).value.toString('utf8');
            const val = this.engine.decrby(key, dec);
            return RESPSerializer.encodeInteger(val);
          } catch (err: any) {
            if (err instanceof WrongTypeError || err instanceof OOMError) throw err;
            return RESPSerializer.encodeError(err.message);
          }
        }

        case 'APPEND': {
          if (args.length !== 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'append' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const val = (args[2] as any).value;
          return RESPSerializer.encodeInteger(this.engine.append(key, val));
        }

        case 'STRLEN': {
          if (args.length !== 2) {
            return RESPSerializer.encodeError("wrong number of arguments for 'strlen' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          return RESPSerializer.encodeInteger(this.engine.strlen(key));
        }

        case 'GETDEL': {
          if (args.length !== 2) {
            return RESPSerializer.encodeError("wrong number of arguments for 'getdel' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const val = this.engine.getdel(key);
          this.lastCommandMutated = val !== null;
          return RESPSerializer.encodeBulkString(val);
        }

        case 'GETEX': {
          if (args.length < 2) {
            return RESPSerializer.encodeError("wrong number of arguments for 'getex' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const opts: GetExOptions = {};
          let optCount = 0;

          for (let i = 2; i < args.length; i++) {
            const opt = (args[i] as any).value?.toString('utf8').toUpperCase();
            if (opt === 'PERSIST') {
              optCount++;
              opts.persist = true;
            } else if (opt === 'EX' && i + 1 < args.length) {
              optCount++;
              const sec = Number((args[++i] as any).value?.toString('utf8'));
              if (!Number.isInteger(sec) || sec <= 0) {
                return RESPSerializer.encodeError("invalid expire time in 'getex' command");
              }
              opts.ex = sec;
            } else if (opt === 'PX' && i + 1 < args.length) {
              optCount++;
              const ms = Number((args[++i] as any).value?.toString('utf8'));
              if (!Number.isInteger(ms) || ms <= 0) {
                return RESPSerializer.encodeError("invalid expire time in 'getex' command");
              }
              opts.px = ms;
            } else if (opt === 'EXAT' && i + 1 < args.length) {
              optCount++;
              const sec = Number((args[++i] as any).value?.toString('utf8'));
              if (!Number.isInteger(sec) || sec <= 0) {
                return RESPSerializer.encodeError("invalid expire time in 'getex' command");
              }
              opts.exat = sec;
            } else if (opt === 'PXAT' && i + 1 < args.length) {
              optCount++;
              const ms = Number((args[++i] as any).value?.toString('utf8'));
              if (!Number.isInteger(ms) || ms <= 0) {
                return RESPSerializer.encodeError("invalid expire time in 'getex' command");
              }
              opts.pxat = ms;
            } else {
              return RESPSerializer.encodeError('syntax error');
            }
          }

          if (optCount > 1) {
            return RESPSerializer.encodeError('syntax error');
          }

          const val = this.engine.getex(key, opts);
          this.lastCommandMutated = val !== null && optCount > 0;
          return RESPSerializer.encodeBulkString(val);
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

        case 'HSETNX': {
          if (args.length !== 4) {
            return RESPSerializer.encodeError("wrong number of arguments for 'hsetnx' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const field = (args[2] as any).value.toString('utf8');
          const val = (args[3] as any).value;
          const created = this.engine.hsetnx(key, field, val);
          this.lastCommandMutated = created;
          return RESPSerializer.encodeInteger(created ? 1 : 0);
        }

        case 'HINCRBY': {
          if (args.length !== 4) {
            return RESPSerializer.encodeError("wrong number of arguments for 'hincrby' command");
          }
          try {
            const key = (args[1] as any).value.toString('utf8');
            const field = (args[2] as any).value.toString('utf8');
            const inc = (args[3] as any).value.toString('utf8');
            const next = this.engine.hincrby(key, field, inc);
            return RESPSerializer.encodeInteger(next);
          } catch (err: any) {
            if (err instanceof WrongTypeError || err instanceof OOMError) throw err;
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError(err.message);
          }
        }

        case 'HSTRLEN': {
          if (args.length !== 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'hstrlen' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const field = (args[2] as any).value.toString('utf8');
          return RESPSerializer.encodeInteger(this.engine.hstrlen(key, field));
        }

        case 'HRANDFIELD': {
          if (args.length < 2 || args.length > 4) {
            return RESPSerializer.encodeError("wrong number of arguments for 'hrandfield' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          if (args.length === 2) {
            const picked = this.engine.hrandfield(key) as string | null;
            return RESPSerializer.encodeBulkString(picked);
          }
          const count = TCPServer.parseStrictInt((args[2] as any).value.toString('utf8'));
          if (count === null) {
            return RESPSerializer.encodeError('value is not an integer or out of range');
          }
          let withValues = false;
          if (args.length === 4) {
            const flag = (args[3] as any).value.toString('utf8').toUpperCase();
            if (flag !== 'WITHVALUES') {
              return RESPSerializer.encodeError('syntax error');
            }
            withValues = true;
          }
          const pickedList = this.engine.hrandfield(key, count, withValues) as (string | Buffer)[];
          return RESPSerializer.encodeArray(pickedList);
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

        case 'LINDEX': {
          if (args.length !== 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'lindex' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const index = TCPServer.parseStrictInt((args[2] as any).value.toString('utf8'));
          if (index === null) {
            return RESPSerializer.encodeError('value is not an integer or out of range');
          }
          return RESPSerializer.encodeBulkString(this.engine.lindex(key, index));
        }

        case 'LSET': {
          if (args.length !== 4) {
            return RESPSerializer.encodeError("wrong number of arguments for 'lset' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const index = TCPServer.parseStrictInt((args[2] as any).value.toString('utf8'));
          if (index === null) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError('value is not an integer or out of range');
          }
          const val = (args[3] as any).value;
          try {
            this.engine.lset(key, index, val);
            return RESP_CONSTANTS.OK;
          } catch (err: any) {
            if (err instanceof WrongTypeError || err instanceof OOMError) throw err;
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError(err.message);
          }
        }

        case 'LTRIM': {
          if (args.length !== 4) {
            return RESPSerializer.encodeError("wrong number of arguments for 'ltrim' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const start = TCPServer.parseStrictInt((args[2] as any).value.toString('utf8'));
          const stop = TCPServer.parseStrictInt((args[3] as any).value.toString('utf8'));
          if (start === null || stop === null) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError('value is not an integer or out of range');
          }
          this.engine.ltrim(key, start, stop);
          return RESP_CONSTANTS.OK;
        }

        case 'LMOVE': {
          if (args.length !== 5) {
            return RESPSerializer.encodeError("wrong number of arguments for 'lmove' command");
          }
          const source = (args[1] as any).value.toString('utf8');
          const destination = (args[2] as any).value.toString('utf8');
          const whereFrom = (args[3] as any).value.toString('utf8').toUpperCase();
          const whereTo = (args[4] as any).value.toString('utf8').toUpperCase();
          if ((whereFrom !== 'LEFT' && whereFrom !== 'RIGHT') || (whereTo !== 'LEFT' && whereTo !== 'RIGHT')) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError('syntax error');
          }
          const moved = this.engine.lmove(source, destination, whereFrom, whereTo);
          if (moved === null) {
            this.lastCommandMutated = false;
            return RESP_CONSTANTS.NULL_BULK;
          }
          if (this.blocking.hasBlockedClients(destination)) {
            const poppedForBlocked =
              whereTo === 'LEFT'
                ? (this.engine.lpop(destination, 1) as Buffer | null)
                : (this.engine.rpop(destination, 1) as Buffer | null);
            if (poppedForBlocked !== null) {
              if (!this.blocking.signalPush(destination, poppedForBlocked, whereTo === 'LEFT' ? 'left' : 'right')) {
                if (whereTo === 'LEFT') {
                  this.engine.lpush(destination, poppedForBlocked);
                } else {
                  this.engine.rpush(destination, poppedForBlocked);
                }
              }
            }
          }
          this.lastMutatedKeys = source === destination ? [source] : [source, destination];
          return RESPSerializer.encodeBulkString(moved);
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

        case 'SMISMEMBER': {
          if (args.length < 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'smismember' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const members = args.slice(2).map((a: any) => a.value.toString('utf8'));
          return RESPSerializer.encodeArray(this.engine.smismember(key, members));
        }

        case 'SRANDMEMBER': {
          if (args.length < 2 || args.length > 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'srandmember' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          if (args.length === 2) {
            return RESPSerializer.encodeBulkString(this.engine.srandmember(key) as string | null);
          }
          const count = TCPServer.parseStrictInt((args[2] as any).value.toString('utf8'));
          if (count === null) {
            return RESPSerializer.encodeError('value is not an integer or out of range');
          }
          return RESPSerializer.encodeArray(this.engine.srandmember(key, count) as string[]);
        }

        case 'SPOP': {
          if (args.length < 2 || args.length > 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'spop' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          if (args.length === 2) {
            const popped = this.engine.spop(key) as string | null;
            this.lastCommandMutated = popped !== null;
            return RESPSerializer.encodeBulkString(popped);
          }
          const count = TCPServer.parseStrictInt((args[2] as any).value.toString('utf8'));
          if (count === null || count < 0) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError('value is out of range, must be positive');
          }
          const poppedList = this.engine.spop(key, count) as string[];
          this.lastCommandMutated = poppedList.length > 0;
          return RESPSerializer.encodeArray(poppedList);
        }

        case 'SMOVE': {
          if (args.length !== 4) {
            return RESPSerializer.encodeError("wrong number of arguments for 'smove' command");
          }
          const source = (args[1] as any).value.toString('utf8');
          const destination = (args[2] as any).value.toString('utf8');
          const member = (args[3] as any).value.toString('utf8');
          const moved = this.engine.smove(source, destination, member);
          this.lastCommandMutated = moved;
          if (moved) {
            this.lastMutatedKeys = source === destination ? [source] : [source, destination];
          }
          return RESPSerializer.encodeInteger(moved ? 1 : 0);
        }

        case 'SUNION': {
          if (args.length < 2) {
            return RESPSerializer.encodeError("wrong number of arguments for 'sunion' command");
          }
          const keys = args.slice(1).map((a: any) => a.value.toString('utf8'));
          return RESPSerializer.encodeArray(this.engine.sunion(...keys));
        }

        case 'SINTER': {
          if (args.length < 2) {
            return RESPSerializer.encodeError("wrong number of arguments for 'sinter' command");
          }
          const keys = args.slice(1).map((a: any) => a.value.toString('utf8'));
          return RESPSerializer.encodeArray(this.engine.sinter(...keys));
        }

        case 'SDIFF': {
          if (args.length < 2) {
            return RESPSerializer.encodeError("wrong number of arguments for 'sdiff' command");
          }
          const keys = args.slice(1).map((a: any) => a.value.toString('utf8'));
          return RESPSerializer.encodeArray(this.engine.sdiff(keys[0], ...keys.slice(1)));
        }

        case 'SUNIONSTORE': {
          if (args.length < 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'sunionstore' command");
          }
          const dest = (args[1] as any).value.toString('utf8');
          const keys = args.slice(2).map((a: any) => a.value.toString('utf8'));
          return RESPSerializer.encodeInteger(this.engine.sunionstore(dest, ...keys));
        }

        case 'SINTERSTORE': {
          if (args.length < 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'sinterstore' command");
          }
          const dest = (args[1] as any).value.toString('utf8');
          const keys = args.slice(2).map((a: any) => a.value.toString('utf8'));
          return RESPSerializer.encodeInteger(this.engine.sinterstore(dest, ...keys));
        }

        case 'SDIFFSTORE': {
          if (args.length < 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'sdiffstore' command");
          }
          const dest = (args[1] as any).value.toString('utf8');
          const keys = args.slice(2).map((a: any) => a.value.toString('utf8'));
          return RESPSerializer.encodeInteger(this.engine.sdiffstore(dest, keys[0], ...keys.slice(1)));
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

        case 'ZINCRBY': {
          if (args.length !== 4) {
            return RESPSerializer.encodeError("wrong number of arguments for 'zincrby' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const incRaw = (args[2] as any).value.toString('utf8').trim();
          const inc = Number(incRaw);
          if (incRaw.length === 0 || Number.isNaN(inc)) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError('value is not a valid float');
          }
          const member = (args[3] as any).value.toString('utf8');
          try {
            const next = this.engine.zincrby(key, inc, member);
            return RESPSerializer.encodeBulkString(next.toString());
          } catch (err: any) {
            if (err instanceof WrongTypeError || err instanceof OOMError) throw err;
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError(err.message);
          }
        }

        case 'ZRANK': {
          if (args.length !== 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'zrank' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const member = (args[2] as any).value.toString('utf8');
          const rank = this.engine.zrank(key, member);
          return rank === null ? RESP_CONSTANTS.NULL_BULK : RESPSerializer.encodeInteger(rank);
        }

        case 'ZREVRANK': {
          if (args.length !== 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'zrevrank' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const member = (args[2] as any).value.toString('utf8');
          const rank = this.engine.zrevrank(key, member);
          return rank === null ? RESP_CONSTANTS.NULL_BULK : RESPSerializer.encodeInteger(rank);
        }

        case 'ZREVRANGE': {
          if (args.length < 4 || args.length > 5) {
            return RESPSerializer.encodeError("wrong number of arguments for 'zrevrange' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const start = TCPServer.parseStrictInt((args[2] as any).value.toString('utf8'));
          const stop = TCPServer.parseStrictInt((args[3] as any).value.toString('utf8'));
          if (start === null || stop === null) {
            return RESPSerializer.encodeError('value is not an integer or out of range');
          }
          let withScores = false;
          if (args.length === 5) {
            const flag = (args[4] as any).value.toString('utf8').toUpperCase();
            if (flag !== 'WITHSCORES') {
              return RESPSerializer.encodeError('syntax error');
            }
            withScores = true;
          }
          if (withScores) {
            const list = this.engine.zrevrangeWithScores(key, start, stop);
            const flat: string[] = [];
            for (const item of list) {
              flat.push(item.member, item.score.toString());
            }
            return RESPSerializer.encodeArray(flat);
          }
          return RESPSerializer.encodeArray(this.engine.zrevrange(key, start, stop));
        }

        case 'ZRANGEBYSCORE':
        case 'ZREVRANGEBYSCORE': {
          if (args.length < 4) {
            return RESPSerializer.encodeError(
              `wrong number of arguments for '${commandName.toLowerCase()}' command`
            );
          }
          const isRev = commandName === 'ZREVRANGEBYSCORE';
          const key = (args[1] as any).value.toString('utf8');
          const minSpec = (args[isRev ? 3 : 2] as any).value.toString('utf8');
          const maxSpec = (args[isRev ? 2 : 3] as any).value.toString('utf8');

          let withScores = false;
          let offset: number | undefined;
          let count: number | undefined;

          for (let i = 4; i < args.length; i++) {
            const opt = (args[i] as any).value.toString('utf8').toUpperCase();
            if (opt === 'WITHSCORES') {
              withScores = true;
            } else if (opt === 'LIMIT' && i + 2 < args.length) {
              const parsedOffset = TCPServer.parseStrictInt((args[++i] as any).value.toString('utf8'));
              const parsedCount = TCPServer.parseStrictInt((args[++i] as any).value.toString('utf8'));
              if (parsedOffset === null || parsedCount === null) {
                return RESPSerializer.encodeError('value is not an integer or out of range');
              }
              offset = parsedOffset;
              count = parsedCount;
            } else {
              return RESPSerializer.encodeError('syntax error');
            }
          }

          try {
            const items = this.engine.zrangebyscore(key, minSpec, maxSpec, {
              offset,
              count,
              reverse: isRev,
            });
            if (withScores) {
              const flat: string[] = [];
              for (const item of items) {
                flat.push(item.member, item.score.toString());
              }
              return RESPSerializer.encodeArray(flat);
            }
            return RESPSerializer.encodeArray(items.map((x) => x.member));
          } catch (err: any) {
            if (err instanceof WrongTypeError) throw err;
            return RESPSerializer.encodeError(err.message);
          }
        }

        case 'ZCOUNT': {
          if (args.length !== 4) {
            return RESPSerializer.encodeError("wrong number of arguments for 'zcount' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const minSpec = (args[2] as any).value.toString('utf8');
          const maxSpec = (args[3] as any).value.toString('utf8');
          try {
            return RESPSerializer.encodeInteger(this.engine.zcount(key, minSpec, maxSpec));
          } catch (err: any) {
            if (err instanceof WrongTypeError) throw err;
            return RESPSerializer.encodeError(err.message);
          }
        }

        case 'ZPOPMIN':
        case 'ZPOPMAX': {
          if (args.length < 2 || args.length > 3) {
            return RESPSerializer.encodeError(
              `wrong number of arguments for '${commandName.toLowerCase()}' command`
            );
          }
          const key = (args[1] as any).value.toString('utf8');
          let count = 1;
          if (args.length === 3) {
            const parsedCount = TCPServer.parseStrictInt((args[2] as any).value.toString('utf8'));
            if (parsedCount === null || parsedCount < 0) {
              this.lastCommandMutated = false;
              return RESPSerializer.encodeError('value is out of range, must be positive');
            }
            count = parsedCount;
          }
          const popped =
            commandName === 'ZPOPMIN'
              ? this.engine.zpopmin(key, count)
              : this.engine.zpopmax(key, count);
          this.lastCommandMutated = popped.length > 0;
          const flat: string[] = [];
          for (const item of popped) {
            flat.push(item.member, item.score.toString());
          }
          return RESPSerializer.encodeArray(flat);
        }

        case 'ZMPOP': {
          if (args.length < 4) {
            return RESPSerializer.encodeError("wrong number of arguments for 'zmpop' command");
          }
          const numKeys = TCPServer.parseStrictInt((args[1] as any).value.toString('utf8'));
          if (numKeys === null || numKeys <= 0) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError('numkeys should be greater than 0');
          }
          if (args.length < 2 + numKeys + 1) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError('syntax error');
          }
          const keys = args.slice(2, 2 + numKeys).map((a: any) => a.value.toString('utf8'));
          const dirToken = (args[2 + numKeys] as any).value.toString('utf8').toUpperCase();
          if (dirToken !== 'MIN' && dirToken !== 'MAX') {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError('syntax error');
          }

          let count = 1;
          const afterDir = 2 + numKeys + 1;
          if (afterDir < args.length) {
            if (
              afterDir + 1 !== args.length - 1 ||
              (args[afterDir] as any).value.toString('utf8').toUpperCase() !== 'COUNT'
            ) {
              this.lastCommandMutated = false;
              return RESPSerializer.encodeError('syntax error');
            }
            const parsedCount = TCPServer.parseStrictInt(
              (args[afterDir + 1] as any).value.toString('utf8')
            );
            if (parsedCount === null || parsedCount <= 0) {
              this.lastCommandMutated = false;
              return RESPSerializer.encodeError('count should be greater than 0');
            }
            count = parsedCount;
          }

          const popped = this.engine.zmpop(keys, dirToken, count);
          if (popped === null) {
            this.lastCommandMutated = false;
            return RESP_CONSTANTS.NULL_ARRAY;
          }
          this.lastMutatedKeys = [popped.key];
          return RESPSerializer.encodeArray([
            popped.key,
            popped.elements.map((e) => [e.member, e.score.toString()]),
          ]);
        }

        // =======================
        // GEOSPATIAL (GEO)
        // =======================
        case 'GEOADD': {
          if (args.length < 5 || (args.length - 2) % 3 !== 0) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError("wrong number of arguments for 'geoadd' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const items: Array<{ longitude: number; latitude: number; member: string }> = [];
          for (let i = 2; i < args.length; i += 3) {
            const lon = Number((args[i] as any).value.toString('utf8'));
            const lat = Number((args[i + 1] as any).value.toString('utf8'));
            if (isNaN(lon) || isNaN(lat)) {
              this.lastCommandMutated = false;
              return RESPSerializer.encodeError('value is not a valid float');
            }
            const member = (args[i + 2] as any).value.toString('utf8');
            items.push({ longitude: lon, latitude: lat, member });
          }
          try {
            const added = this.engine.geoadd(key, items);
            this.lastMutatedKeys = [key];
            return RESPSerializer.encodeInteger(added);
          } catch (err: any) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError(err.message);
          }
        }

        case 'GEOPOS': {
          if (args.length < 3) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError("wrong number of arguments for 'geopos' command");
          }
          this.lastCommandMutated = false;
          const key = (args[1] as any).value.toString('utf8');
          const members = args.slice(2).map((a: any) => a.value.toString('utf8'));
          const positions = this.engine.geopos(key, members);
          const out = positions.map((pos) => {
            if (!pos) return null;
            return [pos.longitude.toString(), pos.latitude.toString()];
          });
          return RESPSerializer.encodeArray(out as any);
        }

        case 'GEODIST': {
          if (args.length < 4 || args.length > 5) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError("wrong number of arguments for 'geodist' command");
          }
          this.lastCommandMutated = false;
          const key = (args[1] as any).value.toString('utf8');
          const m1 = (args[2] as any).value.toString('utf8');
          const m2 = (args[3] as any).value.toString('utf8');
          const unit = args.length === 5 ? (args[4] as any).value.toString('utf8').toLowerCase() : 'm';
          const dist = this.engine.geodist(key, m1, m2, unit as any);
          if (dist === null) return RESP_CONSTANTS.NULL_BULK;
          return RESPSerializer.encodeBulkString(dist.toFixed(4));
        }

        case 'GEOSEARCH': {
          if (args.length < 6) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError("wrong number of arguments for 'geosearch' command");
          }
          this.lastCommandMutated = false;
          const key = (args[1] as any).value.toString('utf8');
          let centerLon = 0;
          let centerLat = 0;
          let radius = 0;
          let unit = 'm';
          for (let i = 2; i < args.length; i++) {
            const token = (args[i] as any).value.toString('utf8').toUpperCase();
            if (token === 'FROMLONLAT' && i + 2 < args.length) {
              centerLon = Number((args[i + 1] as any).value.toString('utf8'));
              centerLat = Number((args[i + 2] as any).value.toString('utf8'));
              i += 2;
            } else if (token === 'BYRADIUS' && i + 2 < args.length) {
              radius = Number((args[i + 1] as any).value.toString('utf8'));
              unit = (args[i + 2] as any).value.toString('utf8').toLowerCase();
              i += 2;
            }
          }
          const results = this.engine.geosearch(key, { longitude: centerLon, latitude: centerLat }, radius, unit as any);
          return RESPSerializer.encodeArray(results.map((r) => r.member));
        }

        // =======================
        // BLOOM FILTER (BF)
        // =======================
        case 'BF.RESERVE': {
          if (args.length < 4) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError("wrong number of arguments for 'bf.reserve' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const errorRate = Number((args[2] as any).value.toString('utf8'));
          const capacity = Number((args[3] as any).value.toString('utf8'));
          if (isNaN(errorRate) || isNaN(capacity)) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError('value is not a valid float or integer');
          }
          try {
            this.engine.bfReserve(key, errorRate, capacity);
            this.lastMutatedKeys = [key];
            return RESPSerializer.encodeSimpleString('OK');
          } catch (err: any) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError(err.message);
          }
        }

        case 'BF.ADD': {
          if (args.length < 3) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError("wrong number of arguments for 'bf.add' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const item = (args[2] as any).value.toString('utf8');
          try {
            const added = this.engine.bfAdd(key, item);
            this.lastMutatedKeys = [key];
            return RESPSerializer.encodeInteger(added ? 1 : 0);
          } catch (err: any) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError(err.message);
          }
        }

        case 'BF.MADD': {
          if (args.length < 3) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError("wrong number of arguments for 'bf.madd' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const results: number[] = [];
          for (let i = 2; i < args.length; i++) {
            const item = (args[i] as any).value.toString('utf8');
            const added = this.engine.bfAdd(key, item);
            results.push(added ? 1 : 0);
          }
          this.lastMutatedKeys = [key];
          return RESPSerializer.encodeArray(results);
        }

        case 'BF.EXISTS': {
          if (args.length < 3) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError("wrong number of arguments for 'bf.exists' command");
          }
          this.lastCommandMutated = false;
          const key = (args[1] as any).value.toString('utf8');
          const item = (args[2] as any).value.toString('utf8');
          try {
            const exists = this.engine.bfExists(key, item);
            return RESPSerializer.encodeInteger(exists ? 1 : 0);
          } catch (err: any) {
            return RESPSerializer.encodeError(err.message);
          }
        }

        case 'BF.MEXISTS': {
          if (args.length < 3) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError("wrong number of arguments for 'bf.mexists' command");
          }
          this.lastCommandMutated = false;
          const key = (args[1] as any).value.toString('utf8');
          const results: number[] = [];
          try {
            for (let i = 2; i < args.length; i++) {
              const item = (args[i] as any).value.toString('utf8');
              results.push(this.engine.bfExists(key, item) ? 1 : 0);
            }
            return RESPSerializer.encodeArray(results);
          } catch (err: any) {
            return RESPSerializer.encodeError(err.message);
          }
        }

        case 'BF.INFO': {
          if (args.length < 2) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError("wrong number of arguments for 'bf.info' command");
          }
          this.lastCommandMutated = false;
          const key = (args[1] as any).value.toString('utf8');
          const info = this.engine.bfInfo(key);
          if (!info) return RESPSerializer.encodeError('ERR not found');
          return RESPSerializer.encodeArray([
            'Capacity',
            info.capacity,
            'Size',
            info.size,
            'Number of items inserted',
            info.numberOfItemsInserted,
          ]);
        }

        // =======================
        // COUNT-MIN SKETCH (CMS)
        // =======================
        case 'CMS.INITBYDIM': {
          if (args.length !== 4) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError("wrong number of arguments for 'cms.initbydim' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const width = Number((args[2] as any).value.toString('utf8'));
          const depth = Number((args[3] as any).value.toString('utf8'));
          if (!Number.isInteger(width) || width <= 0 || !Number.isInteger(depth) || depth <= 0) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError('ERR width and depth must be positive integers');
          }
          try {
            this.engine.cmsInitByDim(key, width, depth);
            return RESP_CONSTANTS.OK;
          } catch (err: any) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError(err.message);
          }
        }

        case 'CMS.INITBYPROB': {
          if (args.length !== 4) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError("wrong number of arguments for 'cms.initbyprob' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const errorRate = Number((args[2] as any).value.toString('utf8'));
          const probability = Number((args[3] as any).value.toString('utf8'));
          if (isNaN(errorRate) || isNaN(probability) || errorRate <= 0 || errorRate >= 1 || probability <= 0 || probability >= 1) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError('ERR error rate and probability must be between 0 and 1');
          }
          try {
            this.engine.cmsInitByProb(key, errorRate, probability);
            return RESP_CONSTANTS.OK;
          } catch (err: any) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError(err.message);
          }
        }

        case 'CMS.INCRBY': {
          if (args.length < 4 || (args.length - 2) % 2 !== 0) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError("wrong number of arguments for 'cms.incrby' command");
          }
          const key = (args[1] as any).value.toString('utf8');
          const pairs: Array<{ item: string; increment: number }> = [];
          for (let i = 2; i < args.length; i += 2) {
            const item = (args[i] as any).value.toString('utf8');
            const inc = Number((args[i + 1] as any).value.toString('utf8'));
            if (!Number.isInteger(inc) || inc < 0) {
              this.lastCommandMutated = false;
              return RESPSerializer.encodeError('ERR increment must be a non-negative integer');
            }
            pairs.push({ item, increment: inc });
          }
          try {
            const results = this.engine.cmsIncrBy(key, pairs);
            return RESPSerializer.encodeArray(results);
          } catch (err: any) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError(err.message);
          }
        }

        case 'CMS.QUERY': {
          if (args.length < 3) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError("wrong number of arguments for 'cms.query' command");
          }
          this.lastCommandMutated = false;
          const key = (args[1] as any).value.toString('utf8');
          const items = args.slice(2).map((a: any) => a.value.toString('utf8'));
          try {
            const results = this.engine.cmsQuery(key, items);
            return RESPSerializer.encodeArray(results);
          } catch (err: any) {
            return RESPSerializer.encodeError(err.message);
          }
        }

        case 'CMS.INFO': {
          if (args.length !== 2) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError("wrong number of arguments for 'cms.info' command");
          }
          this.lastCommandMutated = false;
          const key = (args[1] as any).value.toString('utf8');
          const info = this.engine.cmsInfo(key);
          if (!info) return RESPSerializer.encodeError('ERR not found');
          return RESPSerializer.encodeArray([
            'width',
            info.width,
            'depth',
            info.depth,
            'count',
            info.count,
          ]);
        }

        case 'CMS.MERGE': {
          if (args.length < 4) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError("wrong number of arguments for 'cms.merge' command");
          }
          const destKey = (args[1] as any).value.toString('utf8');
          const numKeys = Number((args[2] as any).value.toString('utf8'));
          if (!Number.isInteger(numKeys) || numKeys <= 0) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError('ERR numKeys must be a positive integer');
          }
          if (args.length < 3 + numKeys) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError("wrong number of arguments for 'cms.merge' command");
          }
          const srcKeys: string[] = [];
          for (let i = 0; i < numKeys; i++) {
            srcKeys.push((args[3 + i] as any).value.toString('utf8'));
          }

          let weights: number[] | undefined;
          let idx = 3 + numKeys;
          if (idx < args.length) {
            const opt = (args[idx] as any).value.toString('utf8').toUpperCase();
            if (opt !== 'WEIGHTS' || args.length !== idx + 1 + numKeys) {
              this.lastCommandMutated = false;
              return RESPSerializer.encodeError('ERR syntax error in CMS.MERGE');
            }
            weights = [];
            for (let i = 0; i < numKeys; i++) {
              const w = Number((args[idx + 1 + i] as any).value.toString('utf8'));
              if (!Number.isInteger(w) || w <= 0) {
                this.lastCommandMutated = false;
                return RESPSerializer.encodeError('ERR weight must be a positive integer');
              }
              weights.push(w);
            }
          }

          try {
            this.engine.cmsMerge(destKey, srcKeys, weights);
            return RESP_CONSTANTS.OK;
          } catch (err: any) {
            this.lastCommandMutated = false;
            return RESPSerializer.encodeError(err.message);
          }
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
          const applied = this.engine.pexpire(key, ms);
          this.lastCommandMutated = applied;
          return RESPSerializer.encodeInteger(applied ? 1 : 0);
        }

        case 'EXPIREAT': {
          if (args.length !== 3) return RESPSerializer.encodeError("wrong number of arguments for 'expireat' command");
          const key = (args[1] as any).value.toString('utf8');
          const sec = Number((args[2] as any).value.toString('utf8'));
          if (!Number.isInteger(sec)) return RESPSerializer.encodeError('value is not an integer or out of range');
          const applied = this.engine.expireat(key, sec);
          this.lastCommandMutated = applied;
          return RESPSerializer.encodeInteger(applied ? 1 : 0);
        }

        case 'PEXPIREAT': {
          if (args.length !== 3) return RESPSerializer.encodeError("wrong number of arguments for 'pexpireat' command");
          const key = (args[1] as any).value.toString('utf8');
          const ms = Number((args[2] as any).value.toString('utf8'));
          if (!Number.isInteger(ms)) return RESPSerializer.encodeError('value is not an integer or out of range');
          const applied = this.engine.pexpireat(key, ms);
          this.lastCommandMutated = applied;
          return RESPSerializer.encodeInteger(applied ? 1 : 0);
        }

        case 'EXPIRETIME': {
          if (args.length !== 2) return RESPSerializer.encodeError("wrong number of arguments for 'expiretime' command");
          return RESPSerializer.encodeInteger(this.engine.expiretime((args[1] as any).value.toString('utf8')));
        }

        case 'PEXPIRETIME': {
          if (args.length !== 2) return RESPSerializer.encodeError("wrong number of arguments for 'pexpiretime' command");
          return RESPSerializer.encodeInteger(this.engine.pexpiretime((args[1] as any).value.toString('utf8')));
        }

        case 'PERSIST': {
          if (args.length !== 2) return RESPSerializer.encodeError("wrong number of arguments for 'persist' command");
          const applied = this.engine.persist((args[1] as any).value.toString('utf8'));
          this.lastCommandMutated = applied;
          return RESPSerializer.encodeInteger(applied ? 1 : 0);
        }

        case 'KEYS': {
          if (args.length !== 2) return RESPSerializer.encodeError("wrong number of arguments for 'keys' command");
          return RESPSerializer.encodeArray(this.engine.keys((args[1] as any).value.toString('utf8')));
        }

        case 'DBSIZE': {
          return RESPSerializer.encodeInteger(this.engine.dbsize());
        }

        case 'MEMORY': {
          this.lastCommandMutated = false;
          if (args.length < 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'memory' command");
          }
          const subcmd = (args[1] as any).value.toString('utf8').toUpperCase();
          if (subcmd === 'USAGE') {
            const key = (args[2] as any).value.toString('utf8');
            const usage = this.engine.memoryUsage(key);
            if (usage === null) return RESP_CONSTANTS.NULL_BULK;
            return RESPSerializer.encodeInteger(usage);
          }
          return RESPSerializer.encodeError(`unknown subcommand '${subcmd}' for 'MEMORY'`);
        }

        case 'OBJECT': {
          this.lastCommandMutated = false;
          if (args.length < 3) {
            return RESPSerializer.encodeError("wrong number of arguments for 'object' command");
          }
          const subcmd = (args[1] as any).value.toString('utf8').toUpperCase();
          if (subcmd === 'ENCODING') {
            const key = (args[2] as any).value.toString('utf8');
            const enc = this.engine.objectEncoding(key);
            if (enc === null) return RESP_CONSTANTS.NULL_BULK;
            return RESPSerializer.encodeBulkString(enc);
          }
          return RESPSerializer.encodeError(`unknown subcommand '${subcmd}' for 'OBJECT'`);
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
