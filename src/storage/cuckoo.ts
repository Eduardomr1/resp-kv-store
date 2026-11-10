/**
 * Cuckoo Filter (CF) Probabilistic Data Structure
 *
 * Implements RedisBloom-compatible space-efficient probabilistic set membership
 * with support for dynamic deletion (unlike standard Bloom Filters), based on
 * partial-key cuckoo hashing.
 *
 * Mathematical properties:
 * - Number of buckets m is a power of 2: m = 2^k >= capacity / bucketSize
 * - Alternating index involution: i2 = (i1 ^ hash(fingerprint)) & (m - 1)
 *   satisfies (i2 ^ hash(fingerprint)) & (m - 1) = i1.
 * - Supports deletion: CF.DEL decrements/clears matching fingerprint.
 */

export interface CuckooFilterOptions {
  capacity?: number;
  bucketSize?: number;
  maxIterations?: number;
  expansion?: number;
}

export interface CuckooInfo {
  size: number;
  numBuckets: number;
  numFilters: number;
  numItemsInserted: number;
  numItemsDeleted: number;
  bucketSize: number;
  maxIterations: number;
  expansion: number;
}

function nextPowerOf2(n: number): number {
  let count = 1;
  while (count < n) {
    count <<= 1;
  }
  return Math.max(4, count);
}

function hashItem32(str: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

function computeFingerprint(str: string): number {
  let h = 0xcbf29ce4;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 0x1000193);
  }
  // 16-bit non-zero fingerprint
  const fp = (h & 0xffff) >>> 0;
  return fp === 0 ? 1 : fp;
}

function hashFingerprint(fp: number): number {
  let h = Math.imul(fp ^ 0x5bd1e995, 0x5bd1e995);
  h = (h ^ (h >>> 15)) >>> 0;
  return h;
}

export class CuckooFilter {
  public readonly capacity: number;
  public readonly bucketSize: number;
  public readonly maxIterations: number;
  public readonly expansion: number;
  public readonly numBuckets: number;

  private readonly table: Uint16Array;
  private insertedCount = 0;
  private deletedCount = 0;
  private currentCount = 0;

  constructor(capacity = 1000, bucketSize = 2, maxIterations = 500, expansion = 1) {
    if (capacity <= 0) throw new Error('ERR capacity must be > 0');
    if (bucketSize <= 0) throw new Error('ERR bucket size must be > 0');

    this.capacity = capacity;
    this.bucketSize = bucketSize;
    this.maxIterations = maxIterations > 0 ? maxIterations : 500;
    this.expansion = expansion > 0 ? expansion : 1;

    this.numBuckets = nextPowerOf2(Math.ceil(capacity / bucketSize));
    this.table = new Uint16Array(this.numBuckets * this.bucketSize);
  }

  public get byteSize(): number {
    return this.table.byteLength + 64;
  }

  public get count(): number {
    return this.currentCount;
  }

  private getBucketIndices(item: string): { fp: number; i1: number; i2: number } {
    const fp = computeFingerprint(item);
    const i1 = hashItem32(item) & (this.numBuckets - 1);
    const fpH = hashFingerprint(fp);
    const i2 = (i1 ^ fpH) & (this.numBuckets - 1);
    return { fp, i1, i2 };
  }

  /**
   * Adds an item to the Cuckoo Filter.
   * If both alternate buckets are full, evicts an entry and follows kickout chain
   * up to maxIterations.
   */
  public add(item: string): boolean {
    const { fp, i1, i2 } = this.getBucketIndices(item);

    // Try primary bucket
    if (this.insertIntoBucket(i1, fp)) {
      this.insertedCount++;
      this.currentCount++;
      return true;
    }

    // Try secondary bucket
    if (this.insertIntoBucket(i2, fp)) {
      this.insertedCount++;
      this.currentCount++;
      return true;
    }

    // Both buckets full: kickout relocations
    let currIdx = Math.random() < 0.5 ? i1 : i2;
    let currFp = fp;

    for (let iter = 0; iter < this.maxIterations; iter++) {
      // Pick random slot in currIdx
      const slot = Math.floor(Math.random() * this.bucketSize);
      const tableOffset = currIdx * this.bucketSize + slot;
      const oldFp = this.table[tableOffset]!;
      this.table[tableOffset] = currFp;

      currFp = oldFp;
      currIdx = (currIdx ^ hashFingerprint(currFp)) & (this.numBuckets - 1);

      if (this.insertIntoBucket(currIdx, currFp)) {
        this.insertedCount++;
        this.currentCount++;
        return true;
      }
    }

    throw new Error('ERR Filter is full');
  }

  /**
   * Adds item only if it does not already exist in the filter.
   */
  public addNx(item: string): boolean {
    if (this.exists(item)) {
      return false;
    }
    return this.add(item);
  }

  /**
   * Checks whether an item exists in the filter.
   */
  public exists(item: string): boolean {
    const { fp, i1, i2 } = this.getBucketIndices(item);
    return this.bucketContains(i1, fp) || this.bucketContains(i2, fp);
  }

  /**
   * Deletes one instance of the item from the filter.
   * Returns true if found and removed, false if not present.
   */
  public delete(item: string): boolean {
    const { fp, i1, i2 } = this.getBucketIndices(item);

    if (this.removeFromBucket(i1, fp)) {
      this.deletedCount++;
      this.currentCount--;
      return true;
    }

    if (this.removeFromBucket(i2, fp)) {
      this.deletedCount++;
      this.currentCount--;
      return true;
    }

    return false;
  }

  /**
   * Counts the frequency / duplicate presence of an item.
   */
  public countOccurrences(item: string): number {
    const { fp, i1, i2 } = this.getBucketIndices(item);
    let occurrences = 0;

    for (let s = 0; s < this.bucketSize; s++) {
      if (this.table[i1 * this.bucketSize + s] === fp) occurrences++;
    }
    if (i1 !== i2) {
      for (let s = 0; s < this.bucketSize; s++) {
        if (this.table[i2 * this.bucketSize + s] === fp) occurrences++;
      }
    }
    return occurrences;
  }

  public info(): CuckooInfo {
    return {
      size: this.byteSize,
      numBuckets: this.numBuckets,
      numFilters: 1,
      numItemsInserted: this.insertedCount,
      numItemsDeleted: this.deletedCount,
      bucketSize: this.bucketSize,
      maxIterations: this.maxIterations,
      expansion: this.expansion,
    };
  }

  private insertIntoBucket(bucketIdx: number, fp: number): boolean {
    const base = bucketIdx * this.bucketSize;
    for (let s = 0; s < this.bucketSize; s++) {
      if (this.table[base + s] === 0) {
        this.table[base + s] = fp;
        return true;
      }
    }
    return false;
  }

  private bucketContains(bucketIdx: number, fp: number): boolean {
    const base = bucketIdx * this.bucketSize;
    for (let s = 0; s < this.bucketSize; s++) {
      if (this.table[base + s] === fp) return true;
    }
    return false;
  }

  private removeFromBucket(bucketIdx: number, fp: number): boolean {
    const base = bucketIdx * this.bucketSize;
    for (let s = 0; s < this.bucketSize; s++) {
      if (this.table[base + s] === fp) {
        this.table[base + s] = 0;
        return true;
      }
    }
    return false;
  }
}
