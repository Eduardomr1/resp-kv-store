/**
 * Eviction subsystem type definitions for zero-dependency RESP Key-Value store.
 */

export type EvictionPolicy =
  | 'noeviction'
  | 'allkeys-lru'
  | 'volatile-lru'
  | 'allkeys-lfu'
  | 'volatile-lfu'
  | 'volatile-ttl'
  | 'allkeys-random'
  | 'volatile-random';

/**
 * Standard Redis Out-Of-Memory Error thrown when memory limit is exceeded.
 */
export class OOMError extends Error {
  constructor(message: string = "OOM command not allowed when used memory > 'maxmemory'") {
    super(message);
    this.name = 'OOMError';
    Object.setPrototypeOf(this, OOMError.prototype);
  }
}

/**
 * Metadata tracked on each storage entry for LRU and LFU cache eviction.
 */
export interface EntryEvictionMetadata {
  /**
   * Last access timestamp in milliseconds (LRU clock).
   */
  lastAccessTime: number;

  /**
   * Morris 8-bit logarithmic counter (0 - 255) for LFU.
   */
  lfuCounter: number;

  /**
   * Timestamp in milliseconds when the LFU counter was last decayed / accessed.
   */
  lfuDecayTime: number;
}

/**
 * Candidate entry stored inside the 16-key eviction pool.
 */
export interface EvictionCandidate {
  /**
   * The storage key.
   */
  key: string;

  /**
   * The computed eviction metric score according to policy:
   * - LRU: idle time in ms (higher idle time => evicted first)
   * - LFU: 255 - lfuCounter (lower frequency => higher score => evicted first)
   * - TTL: remaining ms until expiration (lower remaining time => evicted first)
   * - Random: random candidate metric
   */
  metric: number;

  /**
   * Idle time in milliseconds (LRU).
   */
  idleTime?: number;

  /**
   * Inverted LFU score (255 - lfuCounter).
   */
  lfuScore?: number;

  /**
   * Remaining time until expiration in milliseconds (TTL).
   */
  ttlRemaining?: number;
}

/**
 * Telemetry and statistics tracked by Memory Governor.
 */
export interface EvictionStats {
  evictedKeys: number;
  evictionCycles: number;
  evictionErrors: number;
}

/**
 * Interface that storage engines or mock stores implement to support eviction.
 */
export interface EvictionStoreTarget {
  getMemoryUsage(): number;
  getKeys(): string[];
  getVolatileKeys?(): string[];
  getEntry(key: string): {
    expiresAt?: number | null;
    lastAccessTime?: number;
    lfuCounter?: number;
    lfuDecayTime?: number;
    [key: string]: any;
  } | undefined;
  delete(key: string): boolean;
  sampleRandomKeys?(count: number, sampleVolatileOnly: boolean): string[];
}
