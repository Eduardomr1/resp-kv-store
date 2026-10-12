import {
  type EvictionPolicy,
  type EvictionStoreTarget,
  type EvictionStats,
  type EntryEvictionMetadata,
  OOMError,
} from './types.ts';
import { EvictionPool, DEFAULT_POOL_SIZE } from './pool.ts';
import {
  lfuDecay,
  updateLFU,
  LFU_INIT_VAL,
  DEFAULT_LFU_DECAY_INTERVAL,
  DEFAULT_LFU_LOG_FACTOR,
} from './lfu.ts';

export const DEFAULT_SAMPLE_SIZE = 5;

export interface MemoryGovernorOptions {
  maxmemory?: number;
  policy?: EvictionPolicy;
  sampleSize?: number;
  poolSize?: number;
  decayInterval?: number;
  lfuLogFactor?: number;
  target?: EvictionStoreTarget;
}

interface InternalStoreEntry {
  size: number;
  expiresAt: number | null;
  lastAccessTime: number;
  lfuCounter: number;
  lfuDecayTime: number;
}

/**
 * Memory Governor managing memory usage thresholds and executing cache eviction policies.
 */
export class MemoryGovernor {
  private maxmemory: number;
  private policy: EvictionPolicy;
  private sampleSize: number;
  private poolSize: number;
  private decayInterval: number;
  private lfuLogFactor: number;
  private pool: EvictionPool;
  private target?: EvictionStoreTarget;

  // Standalone mode tracking
  private internalEntries: Map<string, InternalStoreEntry> = new Map();
  private usedMemory: number = 0;

  private stats: EvictionStats = {
    evictedKeys: 0,
    evictionCycles: 0,
    evictionErrors: 0,
  };

  constructor(options: MemoryGovernorOptions = {}) {
    this.maxmemory = options.maxmemory ?? 0;
    this.policy = options.policy ?? 'noeviction';
    this.sampleSize = options.sampleSize ?? DEFAULT_SAMPLE_SIZE;
    this.poolSize = options.poolSize ?? DEFAULT_POOL_SIZE;
    this.decayInterval = options.decayInterval ?? DEFAULT_LFU_DECAY_INTERVAL;
    this.lfuLogFactor = options.lfuLogFactor ?? DEFAULT_LFU_LOG_FACTOR;
    this.target = options.target;

    this.pool = new EvictionPool(this.poolSize, this.policy);
  }

  // ==========================================
  // CONFIGURATION & GETTERS
  // ==========================================

  public getMaxMemory(): number {
    return this.maxmemory;
  }

  public setMaxMemory(bytes: number): void {
    this.maxmemory = Math.max(0, bytes);
  }

  public getPolicy(): EvictionPolicy {
    return this.policy;
  }

  public setPolicy(policy: EvictionPolicy): void {
    this.policy = policy;
    this.pool.setPolicy(policy);
  }

  public getSampleSize(): number {
    return this.sampleSize;
  }

  public setSampleSize(size: number): void {
    this.sampleSize = Math.max(1, size);
  }

  public getPool(): EvictionPool {
    return this.pool;
  }

  public getStats(): EvictionStats {
    return { ...this.stats };
  }

  public resetStats(): void {
    this.stats = {
      evictedKeys: 0,
      evictionCycles: 0,
      evictionErrors: 0,
    };
  }

  public attachTarget(target: EvictionStoreTarget): void {
    this.target = target;
  }

  public getTarget(): EvictionStoreTarget | undefined {
    return this.target;
  }

  // ==========================================
  // MEMORY TRACKING
  // ==========================================

  public getMemoryUsage(): number {
    if (this.target) {
      return this.target.getMemoryUsage();
    }
    return this.usedMemory;
  }

  public setMemoryUsage(bytes: number): void {
    this.usedMemory = Math.max(0, bytes);
  }

  public adjustMemoryUsage(delta: number): void {
    this.usedMemory = Math.max(0, this.usedMemory + delta);
  }

  // ==========================================
  // STANDALONE / REGISTRATION HELPERS
  // ==========================================

  public registerEntry(
    key: string,
    metadata: Partial<EntryEvictionMetadata & { expiresAt: number | null }> = {},
    sizeBytes: number = 0
  ): void {
    const now = Date.now();
    const existing = this.internalEntries.get(key);
    const oldSize = existing?.size ?? 0;

    const entry: InternalStoreEntry = {
      size: sizeBytes,
      expiresAt: metadata.expiresAt ?? null,
      lastAccessTime: metadata.lastAccessTime ?? now,
      lfuCounter: metadata.lfuCounter ?? LFU_INIT_VAL,
      lfuDecayTime: metadata.lfuDecayTime ?? now,
    };

    this.internalEntries.set(key, entry);
    this.adjustMemoryUsage(sizeBytes - oldSize);
  }

  public recordAccess(key: string, now: number = Date.now()): void {
    const entry = this.getEntry(key);
    if (!entry) return;

    entry.lastAccessTime = now;
    updateLFU(entry, now, this.decayInterval, this.lfuLogFactor);
  }

  public getEntry(key: string): {
    expiresAt?: number | null;
    lastAccessTime?: number;
    lfuCounter?: number;
    lfuDecayTime?: number;
    [key: string]: any;
  } | undefined {
    if (this.target) {
      return this.target.getEntry(key);
    }
    return this.internalEntries.get(key);
  }

  public deleteEntry(key: string): boolean {
    return this.deleteKey(key);
  }

  private deleteKey(key: string): boolean {
    if (this.target) {
      return this.target.delete(key);
    }
    const existing = this.internalEntries.get(key);
    if (!existing) return false;
    this.adjustMemoryUsage(-existing.size);
    return this.internalEntries.delete(key);
  }

  // ==========================================
  // EVICTION METRIC COMPUTATION
  // ==========================================

  /**
   * Computes the eviction metric for a given entry according to the policy.
   */
  public computeMetric(
    policy: EvictionPolicy,
    entry: {
      lastAccessTime?: number;
      lfuCounter?: number;
      lfuDecayTime?: number;
      expiresAt?: number | null;
    },
    now: number = Date.now()
  ): number {
    switch (policy) {
      case 'allkeys-lru':
      case 'volatile-lru': {
        const lastAccess = entry.lastAccessTime ?? now;
        return Math.max(0, now - lastAccess); // idle time
      }

      case 'allkeys-lfu':
      case 'volatile-lfu': {
        const rawCounter = entry.lfuCounter ?? LFU_INIT_VAL;
        const decayTime = entry.lfuDecayTime ?? now;
        const decayed = lfuDecay(rawCounter, decayTime, now, this.decayInterval).counter;
        return 255 - decayed; // lower frequency => higher score => evicted first
      }

      case 'volatile-ttl': {
        if (entry.expiresAt === null || entry.expiresAt === undefined) {
          return Number.MAX_SAFE_INTEGER;
        }
        return Math.max(0, entry.expiresAt - now); // remaining ms until expiration
      }

      case 'allkeys-random':
      case 'volatile-random':
        return Math.random();

      case 'noeviction':
      default:
        return 0;
    }
  }

  // ==========================================
  // SAMPLING & EVICTION EXECUTION
  // ==========================================

  /**
   * Samples up to `count` eligible keys for eviction.
   */
  public sampleEligibleKeys(count: number, volatileOnly: boolean): string[] {
    if (this.target?.sampleRandomKeys) {
      return this.target.sampleRandomKeys(count, volatileOnly);
    }

    let candidates: string[] = [];
    if (this.target) {
      if (volatileOnly && this.target.getVolatileKeys) {
        candidates = this.target.getVolatileKeys();
      } else {
        const allKeys = this.target.getKeys();
        if (volatileOnly) {
          candidates = allKeys.filter((k) => {
            const e = this.target!.getEntry(k);
            return e && e.expiresAt !== null && e.expiresAt !== undefined;
          });
        } else {
          candidates = allKeys;
        }
      }
    } else {
      for (const [k, e] of this.internalEntries.entries()) {
        if (!volatileOnly || (e.expiresAt !== null && e.expiresAt !== undefined)) {
          candidates.push(k);
        }
      }
    }

    if (candidates.length <= count) {
      return [...candidates];
    }

    // Uniform random sampling without replacement
    const sampled: string[] = [];
    const poolCopy = [...candidates];
    const n = Math.min(count, poolCopy.length);

    for (let i = 0; i < n; i++) {
      const idx = Math.floor(Math.random() * poolCopy.length);
      sampled.push(poolCopy[idx]);
      poolCopy[idx] = poolCopy[poolCopy.length - 1];
      poolCopy.pop();
    }

    return sampled;
  }

  /**
   * Performs eviction if memory exceeds maxmemory.
   * Throws OOMError if policy is 'noeviction' or if memory limit cannot be met.
   *
   * @returns Total number of keys evicted
   */
  public evictIfNeeded(): number {
    if (this.maxmemory <= 0) {
      return 0; // 0 = unlimited memory
    }

    if (this.getMemoryUsage() <= this.maxmemory) {
      return 0; // Memory within acceptable limits
    }

    if (this.policy === 'noeviction') {
      this.stats.evictionErrors++;
      throw new OOMError();
    }

    this.stats.evictionCycles++;
    this.pool.setPolicy(this.policy);

    let totalEvicted = 0;
    const isVolatilePolicy = this.policy.startsWith('volatile-');
    const isRandomPolicy = this.policy.endsWith('-random');
    let consecutiveFailures = 0;
    const maxIterations = 10000;
    let iterations = 0;

    while (this.getMemoryUsage() > this.maxmemory && iterations++ < maxIterations) {
      if (isRandomPolicy) {
        const sampled = this.sampleEligibleKeys(1, isVolatilePolicy);
        if (sampled.length === 0) {
          this.stats.evictionErrors++;
          throw new OOMError();
        }

        const keyToEvict = sampled[0];
        const deleted = this.deleteKey(keyToEvict);
        if (deleted) {
          totalEvicted++;
          this.stats.evictedKeys++;
          consecutiveFailures = 0;
        } else {
          consecutiveFailures++;
          if (consecutiveFailures > 5) break;
        }
        continue;
      }

      // Sample keys to populate eviction pool
      const sampledKeys = this.sampleEligibleKeys(this.sampleSize, isVolatilePolicy);
      if (sampledKeys.length === 0 && this.pool.size === 0) {
        // No keys eligible for eviction
        this.stats.evictionErrors++;
        throw new OOMError();
      }

      const now = Date.now();
      for (const key of sampledKeys) {
        const entry = this.getEntry(key);
        if (!entry) continue;
        if (isVolatilePolicy && (entry.expiresAt === null || entry.expiresAt === undefined)) {
          continue;
        }

        const metric = this.computeMetric(this.policy, entry, now);
        this.pool.insert(key, metric, {
          idleTime: this.policy.includes('lru') ? metric : undefined,
          lfuScore: this.policy.includes('lfu') ? metric : undefined,
          ttlRemaining: this.policy === 'volatile-ttl' ? metric : undefined,
        });
      }

      const bestCandidate = this.pool.popBestCandidate();
      if (!bestCandidate) {
        if (this.getMemoryUsage() > this.maxmemory) {
          this.stats.evictionErrors++;
          throw new OOMError();
        }
        break;
      }

      const deleted = this.deleteKey(bestCandidate.key);
      if (deleted) {
        totalEvicted++;
        this.stats.evictedKeys++;
        consecutiveFailures = 0;
      } else {
        consecutiveFailures++;
        if (consecutiveFailures > 10) break;
      }
    }

    if (this.getMemoryUsage() > this.maxmemory) {
      this.stats.evictionErrors++;
      throw new OOMError();
    }

    return totalEvicted;
  }

  /**
   * Enforces memory limit (alias for evictIfNeeded).
   */
  public enforceMemoryLimit(): number {
    return this.evictIfNeeded();
  }
}
