/**
 * Bloom Filter Probabilistic Data Structure.
 *
 * Implements RedisBloom-compatible space-efficient probabilistic set membership
 * testing using Kirsch-Mitzenmacher double-hashing:
 *   hash_i(x) = (h1(x) + i * h2(x)) mod m
 *
 * Guaranteed zero false negatives, configurable false positive error rate (default 0.01 = 1%).
 */

export interface BloomFilterOptions {
  errorRate?: number;
  capacity?: number;
  expansion?: number;
}

export class BloomFilter {
  public readonly errorRate: number;
  public readonly capacity: number;
  public readonly expansion: number;
  public readonly bitSize: number;
  public readonly numHashes: number;
  private readonly bits: Uint8Array;
  private itemsCount: number = 0;

  constructor(capacity = 1000, errorRate = 0.01, expansion = 2) {
    if (capacity <= 0) throw new Error('Capacity must be > 0');
    if (errorRate <= 0 || errorRate >= 1) throw new Error('Error rate must be between 0 and 1');

    this.capacity = capacity;
    this.errorRate = errorRate;
    this.expansion = expansion;

    // m = - (n * ln(p)) / (ln(2)^2)
    const m = Math.ceil(-((capacity * Math.log(errorRate)) / (Math.LN2 * Math.LN2)));
    this.bitSize = Math.max(64, m);

    // k = (m / n) * ln(2)
    const k = Math.ceil((this.bitSize / capacity) * Math.LN2);
    this.numHashes = Math.max(1, Math.min(30, k));

    this.bits = new Uint8Array(Math.ceil(this.bitSize / 8));
  }

  public add(item: string): boolean {
    const [h1, h2] = this.hashPair(item);
    let newlySet = false;

    for (let i = 0; i < this.numHashes; i++) {
      let bitIndex = Number((BigInt(h1) + BigInt(i) * BigInt(h2)) % BigInt(this.bitSize));
      if (bitIndex < 0) bitIndex += this.bitSize;
      const bytePos = Math.floor(bitIndex / 8);
      const bitPos = bitIndex % 8;
      const mask = 1 << bitPos;

      if ((this.bits[bytePos]! & mask) === 0) {
        this.bits[bytePos]! |= mask;
        newlySet = true;
      }
    }

    if (newlySet) {
      this.itemsCount++;
    }
    return newlySet;
  }

  public exists(item: string): boolean {
    const [h1, h2] = this.hashPair(item);

    for (let i = 0; i < this.numHashes; i++) {
      let bitIndex = Number((BigInt(h1) + BigInt(i) * BigInt(h2)) % BigInt(this.bitSize));
      if (bitIndex < 0) bitIndex += this.bitSize;
      const bytePos = Math.floor(bitIndex / 8);
      const bitPos = bitIndex % 8;
      const mask = 1 << bitPos;

      if ((this.bits[bytePos]! & mask) === 0) {
        return false;
      }
    }

    return true;
  }

  public get count(): number {
    return this.itemsCount;
  }

  public get byteSize(): number {
    return this.bits.byteLength;
  }

  private hashPair(str: string): [number, number] {
    // 32-bit unsigned FNV-1a hash
    let h1 = 0x811c9dc5;
    for (let i = 0; i < str.length; i++) {
      h1 ^= str.charCodeAt(i);
      h1 = Math.imul(h1, 0x01000193) >>> 0;
    }

    // 32-bit unsigned second hash
    let h2 = 0x9747b28c;
    for (let i = str.length - 1; i >= 0; i--) {
      h2 ^= str.charCodeAt(i);
      h2 = Math.imul(h2, 0x5bd1e995) >>> 0;
      h2 = (h2 ^ (h2 >>> 13)) >>> 0;
    }
    if (h2 === 0) h2 = 1;

    return [h1 >>> 0, h2 >>> 0];
  }
}
