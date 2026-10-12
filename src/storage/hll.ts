/**
 * HyperLogLog Probabilistic Cardinality Estimator
 * Implements the Flajolet et al. algorithm with 16,384 registers (p=14),
 * 64-bit hashing, Linear Counting bias correction, and register merging.
 */

import crypto from 'node:crypto';

export const HLL_REGISTERS = 16384; // 2^14 registers
export const HLL_PRECISION = 14;     // First 14 bits identify register
const ALPHA_M = 0.72125;             // 0.7213 / (1 + 1.079 / 16384)

export class HyperLogLog {
  public readonly registers: Uint8Array;

  constructor(existingRegisters?: Uint8Array) {
    if (existingRegisters && existingRegisters.length === HLL_REGISTERS) {
      this.registers = new Uint8Array(existingRegisters);
    } else {
      this.registers = new Uint8Array(HLL_REGISTERS);
    }
  }

  /**
   * Hashes an arbitrary buffer or string into a 64-bit BigInt using SHA-256.
   */
  public static hash64(value: Buffer | string): bigint {
    const buf = Buffer.isBuffer(value) ? value : Buffer.from(value, 'utf8');
    const hash = crypto.createHash('sha256').update(buf).digest();
    return hash.readBigUInt64BE(0);
  }

  /**
   * Adds an element to the HyperLogLog register array.
   * Returns true if at least one register was updated (cardinality altered).
   */
  public add(element: Buffer | string): boolean {
    const hash = HyperLogLog.hash64(element);

    // Register index: highest 14 bits (bits 50..63)
    const registerIndex = Number((hash >> 50n) & 0x3FFFn);

    // Remaining 50 bits (bits 0..49)
    const remaining = hash & 0x0003FFFFFFFFFFFFn;

    // Count leading zeros in 50-bit number + 1 (rank)
    let rank = 1;
    if (remaining === 0n) {
      rank = 51;
    } else {
      // Find position of most significant set bit in 50 bits
      for (let bit = 49n; bit >= 0n; bit--) {
        if ((remaining & (1n << bit)) !== 0n) {
          rank = Number(50n - bit);
          break;
        }
      }
    }

    if (rank > this.registers[registerIndex]) {
      this.registers[registerIndex] = rank;
      return true;
    }

    return false;
  }

  /**
   * Estimates unique cardinality with Harmonic Mean and Linear Counting bias correction.
   */
  public count(): number {
    let sum = 0;
    let zeroRegisters = 0;

    for (let i = 0; i < HLL_REGISTERS; i++) {
      const val = this.registers[i];
      sum += Math.pow(2, -val);
      if (val === 0) {
        zeroRegisters++;
      }
    }

    // Raw estimate: E = alpha_m * m^2 / sum(2^-M[j])
    const rawEstimate = (ALPHA_M * (HLL_REGISTERS * HLL_REGISTERS)) / sum;

    // Small range correction (Linear Counting when E <= 2.5 * m)
    if (rawEstimate <= 2.5 * HLL_REGISTERS && zeroRegisters > 0) {
      return Math.round(HLL_REGISTERS * Math.log(HLL_REGISTERS / zeroRegisters));
    }

    return Math.round(rawEstimate);
  }

  /**
   * Merges multiple HyperLogLog instances into this one by computing element-wise max.
   */
  public merge(...others: HyperLogLog[]): void {
    for (const other of others) {
      for (let i = 0; i < HLL_REGISTERS; i++) {
        if (other.registers[i] > this.registers[i]) {
          this.registers[i] = other.registers[i];
        }
      }
    }
  }

  /**
   * Exports registers as a Buffer.
   */
  public toBuffer(): Buffer {
    return Buffer.from(this.registers.buffer);
  }

  /**
   * Instantiates HyperLogLog from raw Buffer.
   */
  public static fromBuffer(buf: Buffer): HyperLogLog {
    const registers = new Uint8Array(HLL_REGISTERS);
    buf.copy(Buffer.from(registers.buffer), 0, 0, Math.min(buf.length, HLL_REGISTERS));
    return new HyperLogLog(registers);
  }
}
