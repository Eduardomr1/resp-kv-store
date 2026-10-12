import type { EvictionPolicy, EvictionCandidate } from './types.ts';

export const DEFAULT_POOL_SIZE = 16;

/**
 * 16-Key Eviction Candidate Pool implementing the Redis eviction pool algorithm.
 *
 * Maintains candidate keys ordered by eviction desirability:
 * - Index 0 is always the BEST candidate to evict.
 * - Index (length - 1) is the WORST candidate in the pool.
 * - When full, candidates worse than the pool's worst are rejected.
 * - Better candidates displace the worst candidate, preserving sorted order.
 */
export class EvictionPool {
  public readonly maxSize: number;
  public policy: EvictionPolicy;
  public entries: EvictionCandidate[] = [];
  private comparator?: (a: EvictionCandidate, b: EvictionCandidate) => number;

  constructor(
    maxSize: number = DEFAULT_POOL_SIZE,
    policy: EvictionPolicy = 'allkeys-lru',
    comparator?: (a: EvictionCandidate, b: EvictionCandidate) => number
  ) {
    this.maxSize = maxSize;
    this.policy = policy;
    this.comparator = comparator;
  }

  public setPolicy(policy: EvictionPolicy): void {
    this.policy = policy;
  }

  /**
   * Evaluates if candidate A is a higher-priority target for eviction than candidate B.
   */
  public isBetter(a: EvictionCandidate, b: EvictionCandidate): boolean {
    if (this.comparator) {
      return this.comparator(a, b) > 0;
    }

    if (this.policy === 'volatile-ttl') {
      // Lower remaining time until expiration gets evicted first
      return a.metric < b.metric;
    }

    // LRU (higher idle time), LFU (higher 255 - lfuCounter), and Random:
    // Higher metric gets evicted first
    return a.metric > b.metric;
  }

  /**
   * Inserts or updates a candidate into the eviction pool.
   *
   * @returns true if candidate was accepted, false if rejected due to lower priority than pool's worst
   */
  public insert(
    candidateOrKey: string | EvictionCandidate,
    metric?: number,
    extra?: Partial<EvictionCandidate>
  ): boolean {
    let cand: EvictionCandidate;
    if (typeof candidateOrKey === 'string') {
      cand = {
        key: candidateOrKey,
        metric: metric ?? 0,
        ...extra,
      };
    } else {
      cand = {
        ...candidateOrKey,
        ...(metric !== undefined ? { metric } : {}),
        ...extra,
      };
    }

    // If key already exists in pool, remove previous occurrence to update position
    const existingIndex = this.entries.findIndex((e) => e.key === cand.key);
    if (existingIndex !== -1) {
      this.entries.splice(existingIndex, 1);
    }

    // Pool capacity check
    if (this.entries.length >= this.maxSize) {
      const worst = this.entries[this.entries.length - 1];
      if (!this.isBetter(cand, worst)) {
        return false; // Rejected: worse or equal to the worst in pool
      }
      // Evict worst entry from candidate pool to make space
      this.entries.pop();
    }

    // Binary / linear search for sorted insertion position (best -> worst)
    let insertIdx = 0;
    while (insertIdx < this.entries.length && this.isBetter(this.entries[insertIdx], cand)) {
      insertIdx++;
    }

    this.entries.splice(insertIdx, 0, cand);
    return true;
  }

  /**
   * Alias for insert.
   */
  public push(
    candidateOrKey: string | EvictionCandidate,
    metric?: number,
    extra?: Partial<EvictionCandidate>
  ): boolean {
    return this.insert(candidateOrKey, metric, extra);
  }

  /**
   * Returns the best candidate to evict without removing it.
   */
  public getBestCandidate(): EvictionCandidate | undefined {
    return this.entries[0];
  }

  /**
   * Removes and returns the best candidate to evict.
   */
  public popBestCandidate(): EvictionCandidate | undefined {
    return this.entries.shift();
  }

  /**
   * Returns the worst candidate currently in the pool.
   */
  public getWorstCandidate(): EvictionCandidate | undefined {
    if (this.entries.length === 0) return undefined;
    return this.entries[this.entries.length - 1];
  }

  /**
   * Removes a key from the pool.
   */
  public remove(key: string): boolean {
    const idx = this.entries.findIndex((e) => e.key === key);
    if (idx !== -1) {
      this.entries.splice(idx, 1);
      return true;
    }
    return false;
  }

  /**
   * Checks if a key exists in the candidate pool.
   */
  public has(key: string): boolean {
    return this.entries.some((e) => e.key === key);
  }

  /**
   * Retrieves an entry by key.
   */
  public get(key: string): EvictionCandidate | undefined {
    return this.entries.find((e) => e.key === key);
  }

  /**
   * Clears all candidates from the pool.
   */
  public clear(): void {
    this.entries = [];
  }

  public get size(): number {
    return this.entries.length;
  }

  public get length(): number {
    return this.entries.length;
  }
}
