/**
 * Asynchronous Blocking List Operations Types (BLPOP, BRPOP)
 * Zero external dependencies (native Node.js modules only)
 */

export type PopDirection = 'left' | 'right';

/**
 * Representation of a client sink/session.
 * Compatible with net.Socket, EventEmitter, or custom mock objects.
 */
export interface ClientSink {
  id?: string;
  write?(data: Buffer | Uint8Array | string): boolean | void;
  send?(data: Buffer): void;
  [key: string]: any;
}

/**
 * Internal record for a blocked client waiting on one or more keys.
 */
export interface BlockedRequest {
  id: string;
  clientSink: any;
  keys: string[];
  direction: PopDirection;
  timeoutSeconds: number;
  timer: NodeJS.Timeout | null;
  resolve: (value: Buffer | null) => void;
  reject: (reason?: any) => void;
  createdAt: number;
  completed: boolean;
  onClose?: () => void;
}

/**
 * Map tracking blocked requests per key.
 */
export type BlockedKeyMap = Map<string, Set<BlockedRequest>>;

/**
 * Minimal storage interface required by BlockingManager for immediate pop operations.
 */
export interface BlockingStorage {
  lpop(key: string, count?: number): Buffer[] | Buffer | null;
  rpop(key: string, count?: number): Buffer[] | Buffer | null;
  llen?(key: string): number;
  [key: string]: any;
}

/**
 * Configuration options for BlockingManager.
 */
export interface BlockingManagerOptions {
  storage?: BlockingStorage;
}
