import { RESPSerializer, RESP_CONSTANTS } from '../resp/serializer.ts';
import type {
  QueuedCommand,
  WatchedKeys,
  ITransactionSession,
  TransactionStatus,
  TransactionState,
} from './types.ts';

/**
 * Transaction session state maintained per client connection.
 */
export class TransactionSession implements ITransactionSession {
  public isMulti: boolean = false;
  public queue: QueuedCommand[] = [];
  public watchedKeys: WatchedKeys = new Map();
  public isDirty: boolean = false;

  constructor() {
    this.reset();
  }

  /**
   * Enqueues a command for deferred execution within the MULTI block.
   */
  public enqueue(commandName: string, rawArgs: (string | Buffer)[], execute: () => Buffer): void {
    this.queue.push({ commandName, rawArgs, execute });
  }

  /**
   * Returns current transaction status snapshot.
   */
  public getStatus(): TransactionStatus {
    let state: TransactionState = 'IDLE';
    if (this.isMulti) {
      state = this.isDirty ? 'DIRTY' : 'MULTI';
    }
    return {
      state,
      isMulti: this.isMulti,
      isDirty: this.isDirty,
      queueLength: this.queue.length,
      watchedKeysCount: this.watchedKeys.size,
    };
  }

  /**
   * Resets transaction session state back to default.
   */
  public reset(): void {
    this.isMulti = false;
    this.queue = [];
    this.watchedKeys.clear();
    this.isDirty = false;
  }
}

/**
 * Transaction Manager handling MULTI, EXEC, DISCARD, and WATCH optimistic concurrency control.
 */
export class TransactionManager {
  // Key version tracker for optimistic concurrency control (OCC)
  public keyVersions: Map<string, number> = new Map();

  // Registry of sessions actively watching each key: key -> Set<TransactionSession>
  private watchingSessions: Map<string, Set<TransactionSession>> = new Map();

  /**
   * Factory method to create a new transaction session.
   */
  public createSession(): TransactionSession {
    return new TransactionSession();
  }

  /**
   * Increments key version and flags all sessions watching that key as dirty.
   */
  public touchKey(key: string): number {
    const currentVersion = this.keyVersions.get(key) ?? 0;
    const newVersion = currentVersion + 1;
    this.keyVersions.set(key, newVersion);

    const watchers = this.watchingSessions.get(key);
    if (watchers) {
      for (const session of watchers) {
        session.isDirty = true;
      }
    }

    return newVersion;
  }

  /**
   * Increments versions for multiple keys and flags their watchers as dirty.
   */
  public touchKeys(...keys: string[]): void {
    for (const key of keys) {
      this.touchKey(key);
    }
  }

  /**
   * Registers keys with current versions for optimistic locking.
   * If already in MULTI, returns error ('ERR WATCH inside MULTI is not allowed').
   */
  public watch(session: TransactionSession, ...keys: string[]): Buffer {
    if (session.isMulti) {
      return RESPSerializer.encodeError('ERR WATCH inside MULTI is not allowed');
    }

    for (const key of keys) {
      if (!this.keyVersions.has(key)) {
        this.keyVersions.set(key, 0);
      }
      const version = this.keyVersions.get(key)!;
      session.watchedKeys.set(key, version);

      let watchers = this.watchingSessions.get(key);
      if (!watchers) {
        watchers = new Set<TransactionSession>();
        this.watchingSessions.set(key, watchers);
      }
      watchers.add(session);
    }

    return RESP_CONSTANTS.OK;
  }

  /**
   * Clears watched keys for session and unregisters from watchers registry.
   */
  public unwatch(session: TransactionSession): Buffer {
    for (const key of session.watchedKeys.keys()) {
      const watchers = this.watchingSessions.get(key);
      if (watchers) {
        watchers.delete(session);
        if (watchers.size === 0) {
          this.watchingSessions.delete(key);
        }
      }
    }
    session.watchedKeys.clear();
    session.isDirty = false;

    return RESP_CONSTANTS.OK;
  }

  /**
   * Begins transaction block, sets isMulti = true.
   * If already in MULTI, returns error ('ERR MULTI calls can not be nested').
   */
  public multi(session: TransactionSession): Buffer {
    if (session.isMulti) {
      return RESPSerializer.encodeError('ERR MULTI calls can not be nested');
    }
    session.isMulti = true;
    return RESP_CONSTANTS.OK;
  }

  /**
   * Enqueues a command into the session and returns RESP +QUEUED\r\n.
   */
  public queueCommand(session: TransactionSession, cmd: QueuedCommand): Buffer {
    session.queue.push(cmd);
    return RESP_CONSTANTS.QUEUED;
  }

  /**
   * Aborts transaction, clears queue and watched keys, resets isMulti = false.
   * If not in MULTI, returns error ('ERR DISCARD without MULTI').
   */
  public discard(session: TransactionSession): Buffer {
    if (!session.isMulti) {
      return RESPSerializer.encodeError('ERR DISCARD without MULTI');
    }

    session.queue = [];
    this.unwatch(session);
    session.isMulti = false;

    return RESP_CONSTANTS.OK;
  }

  /**
   * Executes all queued commands atomically.
   * - If not in MULTI, returns error ('ERR EXEC without MULTI').
   * - If session is dirty (a watched key changed), aborts transaction, clears queue,
   *   resets MULTI, and returns RESP Null Array (*-1\r\n).
   * - If not dirty, executes all queued commands in sequence atomically, resets MULTI state,
   *   and returns RESP Array (*<count>\r\n...) containing the results of each queued command.
   */
  public exec(session: TransactionSession): Buffer {
    if (!session.isMulti) {
      return RESPSerializer.encodeError('ERR EXEC without MULTI');
    }

    // Verify dirty flag, or check if any watched key was modified
    let isDirty = session.isDirty;
    if (!isDirty) {
      for (const [key, watchedVersion] of session.watchedKeys.entries()) {
        const currentVersion = this.keyVersions.get(key) ?? 0;
        if (currentVersion !== watchedVersion) {
          isDirty = true;
          session.isDirty = true;
          break;
        }
      }
    }

    if (isDirty) {
      session.queue = [];
      session.isMulti = false;
      this.unwatch(session);
      return RESP_CONSTANTS.NULL_ARRAY;
    }

    const queuedCommands = session.queue;
    session.queue = [];
    session.isMulti = false;
    this.unwatch(session);

    const results: Buffer[] = [];
    for (const cmd of queuedCommands) {
      try {
        const res = cmd.execute();
        results.push(res);
      } catch (err: any) {
        results.push(RESPSerializer.encodeError(err?.message ?? 'ERR transaction command execution error'));
      }
    }

    const header = Buffer.from(`*${results.length}\r\n`, 'ascii');
    return Buffer.concat([header, ...results]);
  }

  /**
   * Cleanup session upon client socket disconnect.
   */
  public cleanupSession(session: TransactionSession): void {
    this.unwatch(session);
    session.queue = [];
    session.isMulti = false;
  }

  /**
   * Returns whether a session is watching a key.
   */
  public isWatching(session: TransactionSession, key: string): boolean {
    return session.watchedKeys.has(key);
  }

  /**
   * Returns count of sessions watching a key.
   */
  public getWatchersCount(key: string): number {
    return this.watchingSessions.get(key)?.size ?? 0;
  }
}
