/**
 * Transaction subsystem types and interfaces.
 * Supports MULTI / EXEC / DISCARD / WATCH optimistic concurrency control (OCC).
 */

export interface QueuedCommand {
  commandName: string;
  rawArgs: (string | Buffer)[];
  execute: () => Buffer;
}

export type WatchedKeys = Map<string, number>;

export type TransactionState = 'IDLE' | 'MULTI' | 'DIRTY';

export interface TransactionStatus {
  state: TransactionState;
  isMulti: boolean;
  isDirty: boolean;
  queueLength: number;
  watchedKeysCount: number;
}

export interface ITransactionSession {
  isMulti: boolean;
  queue: QueuedCommand[];
  watchedKeys: WatchedKeys;
  isDirty: boolean;
}
