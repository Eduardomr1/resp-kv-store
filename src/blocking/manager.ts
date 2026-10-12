/**
 * Zero-dependency Asynchronous Blocking List Operations Manager (BLPOP, BRPOP)
 * Supports FIFO unblocking, multi-key waiting, TTL timeout expiration, and socket cleanup.
 */

import { RESPSerializer, RESP_CONSTANTS } from '../resp/serializer.ts';
import type {
  BlockedRequest,
  BlockedKeyMap,
  PopDirection,
  BlockingStorage,
  BlockingManagerOptions,
} from './types.ts';

export class BlockingManager {
  /**
   * Tracks blocked clients per key: Map<string, Set<BlockedRequest>>
   * Maintains FIFO queue ordering based on Set insertion order.
   */
  public readonly blockedKeys: BlockedKeyMap = new Map();

  /**
   * Secondary index tracking all pending blocked requests for each clientSink.
   */
  private readonly clientRequests: Map<any, Set<BlockedRequest>> = new Map();

  private storage: BlockingStorage | null = null;
  private requestIdCounter: number = 0;

  constructor(storageOrOptions?: BlockingStorage | BlockingManagerOptions) {
    if (storageOrOptions) {
      if ('storage' in storageOrOptions && typeof (storageOrOptions as any).storage !== 'undefined') {
        this.storage = (storageOrOptions as BlockingManagerOptions).storage ?? null;
      } else if (typeof (storageOrOptions as any).lpop === 'function') {
        this.storage = storageOrOptions as BlockingStorage;
      }
    }
  }

  public setStorage(storage: BlockingStorage | null): void {
    this.storage = storage;
  }

  public getStorage(): BlockingStorage | null {
    return this.storage;
  }

  /**
   * Formats a pop response array: *2\r\n$<keyLen>\r\n<key>\r\n$<valLen>\r\n<val>\r\n
   */
  public static formatPopResponse(key: string, value: Buffer | string): Buffer {
    const keyBuf = Buffer.isBuffer(key) ? key : Buffer.from(key, 'utf8');
    const valBuf = Buffer.isBuffer(value) ? value : Buffer.from(value, 'utf8');
    return RESPSerializer.encodeArray([keyBuf, valBuf]);
  }

  public formatPopResponse(key: string, value: Buffer | string): Buffer {
    return BlockingManager.formatPopResponse(key, value);
  }

  /**
   * Initiates a blocking pop operation across the specified keys.
   * If any key already has elements, pops immediately and returns RESP array:
   *   *2\r\n$<keyLen>\r\n<key>\r\n$<valLen>\r\n<val>\r\n
   * If all keys are empty:
   *   - Suspends client request.
   *   - Registers client under each requested key in BlockingManager.
   *   - If timeoutSeconds > 0, starts timeout timer; on timeout returns RESP Null Array *-1\r\n.
   *   - If timeoutSeconds === 0, blocks indefinitely until push arrives or client disconnects.
   */
  public block(
    clientSink: any,
    keys: string[],
    timeoutSeconds: number,
    direction: PopDirection
  ): Promise<Buffer | null> {
    if (!keys || keys.length === 0) {
      return Promise.reject(new Error("wrong number of arguments for blocking pop command"));
    }

    // 1. Immediate check: If any key already has elements, pop immediately
    if (this.storage) {
      for (const key of keys) {
        const popped = direction === 'left' ? this.storage.lpop(key) : this.storage.rpop(key);
        if (popped !== null && popped !== undefined) {
          const val = Array.isArray(popped) ? popped[0] : popped;
          if (val !== null && val !== undefined) {
            return Promise.resolve(this.formatPopResponse(key, val));
          }
        }
      }
    }

    // 2. All keys are empty: Suspend client request
    return new Promise<Buffer | null>((resolve, reject) => {
      const uniqueKeys = Array.from(new Set(keys));
      const request: BlockedRequest = {
        id: `blk_${++this.requestIdCounter}_${Date.now()}`,
        clientSink,
        keys: uniqueKeys,
        direction,
        timeoutSeconds,
        timer: null,
        resolve,
        reject,
        createdAt: Date.now(),
        completed: false,
      };

      // Set up timeout if timeoutSeconds > 0
      if (timeoutSeconds > 0) {
        const timeoutMs = Math.max(1, Math.floor(timeoutSeconds * 1000));
        const timer = setTimeout(() => {
          this.handleTimeout(request);
        }, timeoutMs);
        request.timer = timer;
      }

      // Automatically clean up if client socket/sink emits close or error
      if (clientSink && typeof clientSink.once === 'function') {
        const onClose = () => {
          this.removeClient(clientSink);
        };
        request.onClose = onClose;
        clientSink.once('close', onClose);
        clientSink.once('error', onClose);
      }

      // Register client request under each requested key in BlockingManager
      for (const key of uniqueKeys) {
        let set = this.blockedKeys.get(key);
        if (!set) {
          set = new Set<BlockedRequest>();
          this.blockedKeys.set(key, set);
        }
        set.add(request);
      }

      // Track request under clientSink
      let clientSet = this.clientRequests.get(clientSink);
      if (!clientSet) {
        clientSet = new Set<BlockedRequest>();
        this.clientRequests.set(clientSink, clientSet);
      }
      clientSet.add(request);
    });
  }

  /**
   * Handles timeout expiration for a blocked request.
   */
  private handleTimeout(request: BlockedRequest): void {
    if (request.completed) return;
    request.completed = true;

    // Clear and reset timer
    if (request.timer !== null) {
      clearTimeout(request.timer);
      request.timer = null;
    }

    // Clean up event listeners on clientSink if attached
    this.detachListeners(request);

    // Unregister client from all keys
    for (const key of request.keys) {
      const set = this.blockedKeys.get(key);
      if (set) {
        set.delete(request);
        if (set.size === 0) {
          this.blockedKeys.delete(key);
        }
      }
    }

    // Unregister from clientRequests index
    const clientSet = this.clientRequests.get(request.clientSink);
    if (clientSet) {
      clientSet.delete(request);
      if (clientSet.size === 0) {
        this.clientRequests.delete(request.clientSink);
      }
    }

    // When timeout fires, unregisters client from all keys and returns RESP Null Array *-1\r\n
    request.resolve(RESP_CONSTANTS.NULL_ARRAY);
  }

  /**
   * Signals that an element was pushed to the specified key.
   * If any client is blocked waiting on key:
   *   - Retrieves the oldest blocked client (FIFO queue order).
   *   - Resolves that client with *2\r\n$<keyLen>\r\n<key>\r\n$<valLen>\r\n<val>\r\n.
   *   - Unregisters that client from any other keys it was watching.
   *   - Clears timeout timer.
   *   - Returns true (indicating the pushed element was consumed directly by a waiting client).
   * If no client is blocked, returns false (element should be stored in the list).
   */
  public signalPush(
    key: string,
    value: Buffer | string,
    direction: PopDirection = 'left'
  ): boolean {
    const set = this.blockedKeys.get(key);
    if (!set || set.size === 0) {
      return false;
    }

    // Retrieve the oldest blocked client (FIFO queue order from Set insertion order)
    let oldest: BlockedRequest | null = null;
    for (const req of set) {
      if (!req.completed) {
        oldest = req;
        break;
      }
    }

    if (!oldest) {
      return false;
    }

    // Mark completed
    oldest.completed = true;

    // Clear timeout timer
    if (oldest.timer !== null) {
      clearTimeout(oldest.timer);
      oldest.timer = null;
    }

    // Clean up event listeners on clientSink if attached
    this.detachListeners(oldest);

    // Unregister that client from any other keys it was watching (and from this key)
    for (const k of oldest.keys) {
      const kSet = this.blockedKeys.get(k);
      if (kSet) {
        kSet.delete(oldest);
        if (kSet.size === 0) {
          this.blockedKeys.delete(k);
        }
      }
    }

    // Unregister from clientRequests index
    const clientSet = this.clientRequests.get(oldest.clientSink);
    if (clientSet) {
      clientSet.delete(oldest);
      if (clientSet.size === 0) {
        this.clientRequests.delete(oldest.clientSink);
      }
    }

    // Format response: *2\r\n$<keyLen>\r\n<key>\r\n$<valLen>\r\n<val>\r\n
    const valBuf = Buffer.isBuffer(value) ? value : Buffer.from(value, 'utf8');
    const response = this.formatPopResponse(key, valBuf);

    // Resolves that client
    oldest.resolve(response);

    return true;
  }

  /**
   * Cleans up all pending registrations if client socket closes or errors.
   */
  public removeClient(clientSink: any): void {
    const clientSet = this.clientRequests.get(clientSink);
    if (!clientSet || clientSet.size === 0) {
      this.clientRequests.delete(clientSink);
      return;
    }

    const requests = Array.from(clientSet);
    this.clientRequests.delete(clientSink);

    for (const req of requests) {
      if (req.completed) continue;
      req.completed = true;

      // Clear timeout timer
      if (req.timer !== null) {
        clearTimeout(req.timer);
        req.timer = null;
      }

      this.detachListeners(req);

      // Remove from all keys
      for (const key of req.keys) {
        const set = this.blockedKeys.get(key);
        if (set) {
          set.delete(req);
          if (set.size === 0) {
            this.blockedKeys.delete(key);
          }
        }
      }

      // Resolve with null indicating client disconnected
      req.resolve(null);
    }
  }

  private detachListeners(request: BlockedRequest): void {
    if (request.onClose && request.clientSink && typeof request.clientSink.removeListener === 'function') {
      try {
        request.clientSink.removeListener('close', request.onClose);
        request.clientSink.removeListener('error', request.onClose);
      } catch {}
      request.onClose = undefined;
    }
  }

  /**
   * Returns whether any clients are currently blocked waiting on the given key.
   */
  public hasBlockedClients(key: string): boolean {
    const set = this.blockedKeys.get(key);
    return !!set && set.size > 0;
  }

  /**
   * Returns the count of blocked clients waiting on a specific key,
   * or the total number of blocked requests across all keys if key is omitted.
   */
  public getBlockedCount(key?: string): number {
    if (key !== undefined) {
      return this.blockedKeys.get(key)?.size ?? 0;
    }
    let total = 0;
    for (const set of this.clientRequests.values()) {
      total += set.size;
    }
    return total;
  }

  /**
   * Returns the count of active timeout timers.
   */
  public getPendingTimerCount(): number {
    let count = 0;
    for (const set of this.clientRequests.values()) {
      for (const req of set) {
        if (req.timer !== null && !req.completed) {
          count++;
        }
      }
    }
    return count;
  }

  /**
   * Clears all pending requests and timers.
   */
  public clear(): void {
    for (const clientSink of Array.from(this.clientRequests.keys())) {
      this.removeClient(clientSink);
    }
    this.blockedKeys.clear();
    this.clientRequests.clear();
  }
}
