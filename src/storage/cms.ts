/**
 * Count-Min Sketch (CMS) Probabilistic Frequency Estimator
 *
 * Implements RedisBloom-compatible sub-linear space frequency estimation
 * using a 2D counter array and Kirsch-Mitzenmacher double-hashing.
 *
 * Guarantees:
 * - Never underestimates the true count (no false undercounts): estimate >= trueCount
 * - Overestimation error bounded by (e / width) * totalCount with probability >= (1 - delta)
 *   where delta = e^(-depth) or (1/2)^depth.
 */

export interface CMSInfo {
  width: number;
  depth: number;
  count: number;
}

export class CountMinSketch {
  public readonly width: number;
  public readonly depth: number;
  private readonly table: Uint32Array;
  private totalCount: number = 0;

  constructor(width: number, depth: number) {
    if (width <= 0 || !Number.isInteger(width)) {
      throw new Error('ERR width must be a positive integer');
    }
    if (depth <= 0 || !Number.isInteger(depth)) {
      throw new Error('ERR depth must be a positive integer');
    }

    this.width = width;
    this.depth = depth;
    this.table = new Uint32Array(width * depth);
  }

  /**
   * Initializes a Count-Min Sketch using error rate (overestimation tolerance)
   * and probability (confidence level 1 - delta).
   *
   * width = ceil(2 / errorRate)
   * depth = ceil(ln(1 - probability) / ln(0.5))
   */
  public static createByProb(errorRate: number, probability: number): CountMinSketch {
    if (isNaN(errorRate) || errorRate <= 0 || errorRate >= 1) {
      throw new Error('ERR error rate must be between 0 and 1');
    }
    if (isNaN(probability) || probability <= 0 || probability >= 1) {
      throw new Error('ERR probability must be between 0 and 1');
    }

    const width = Math.max(1, Math.ceil(2 / errorRate));
    const depth = Math.max(1, Math.ceil(Math.log(1 - probability) / Math.log(0.5)));
    return new CountMinSketch(width, depth);
  }

  /**
   * Hashes item into a pair of 32-bit seeds (FNV-1a & Murmur-inspired mixing).
   */
  private hashPair(item: string): [number, number] {
    let h1 = 0x811c9dc5;
    for (let i = 0; i < item.length; i++) {
      h1 ^= item.charCodeAt(i);
      h1 = Math.imul(h1, 0x01000193) >>> 0;
    }

    let h2 = 0x9747b28c;
    for (let i = item.length - 1; i >= 0; i--) {
      h2 ^= item.charCodeAt(i);
      h2 = Math.imul(h2, 0x5bd1e995) >>> 0;
      h2 = (h2 ^ (h2 >>> 13)) >>> 0;
    }
    if (h2 === 0) h2 = 1;

    return [h1 >>> 0, h2 >>> 0];
  }

  /**
   * Computes the column index for row i using Kirsch-Mitzenmacher double-hashing.
   */
  private getIndex(h1: number, h2: number, row: number): number {
    const combined = (BigInt(h1) + BigInt(row) * BigInt(h2)) % BigInt(this.width);
    const col = Number(combined);
    return col < 0 ? col + this.width : col;
  }

  /**
   * Increments the frequency count for an item by increment.
   * Returns the updated minimum estimated count for the item.
   */
  public incrBy(item: string, increment: number = 1): number {
    if (!Number.isInteger(increment) || increment < 0) {
      throw new Error('ERR increment must be a non-negative integer');
    }
    if (increment === 0) {
      return this.query(item);
    }

    const [h1, h2] = this.hashPair(item);
    let minVal = 0xffffffff;

    for (let row = 0; row < this.depth; row++) {
      const col = this.getIndex(h1, h2, row);
      const idx = row * this.width + col;
      const current = this.table[idx]!;
      // Prevent 32-bit unsigned overflow
      const next = current + increment > 0xffffffff ? 0xffffffff : current + increment;
      this.table[idx] = next;
      if (next < minVal) {
        minVal = next;
      }
    }

    this.totalCount += increment;
    return minVal;
  }

  /**
   * Queries the estimated frequency count for a single item.
   */
  public query(item: string): number {
    const [h1, h2] = this.hashPair(item);
    let minVal = 0xffffffff;

    for (let row = 0; row < this.depth; row++) {
      const col = this.getIndex(h1, h2, row);
      const val = this.table[row * this.width + col]!;
      if (val < minVal) {
        minVal = val;
      }
    }

    return minVal;
  }

  /**
   * Merges another Count-Min Sketch into this one with optional integer weight multiplier.
   */
  public merge(source: CountMinSketch, weight: number = 1): void {
    if (this.width !== source.width || this.depth !== source.depth) {
      throw new Error('ERR sketches must have identical width and depth to merge');
    }
    if (!Number.isInteger(weight) || weight <= 0) {
      throw new Error('ERR weight must be a positive integer');
    }

    for (let i = 0; i < this.table.length; i++) {
      const added = source.table[i]! * weight;
      const current = this.table[i]!;
      this.table[i] = current + added > 0xffffffff ? 0xffffffff : current + added;
    }

    this.totalCount += source.totalCount * weight;
  }

  /**
   * Returns metadata and total accumulated increments.
   */
  public get info(): CMSInfo {
    return {
      width: this.width,
      depth: this.depth,
      count: this.totalCount,
    };
  }

  /**
   * Returns memory consumption in bytes for the table.
   */
  public get byteSize(): number {
    return this.table.byteLength;
  }
}
