/**
 * Redis Bitmap Operations Helper
 * Implements byte-level bit manipulation, fast Hamming weight popcount,
 * bitpos searches, and bitwise operations (AND, OR, XOR, NOT).
 */

export class BitmapHelper {
  /**
   * Sets bit at offset to value (0 or 1).
   * Big-endian bit ordering: bit 0 is the most significant bit (0x80) of byte 0.
   * Expands buffer if offset exceeds current buffer capacity.
   */
  public static setBit(
    source: Buffer,
    offset: number,
    value: 0 | 1
  ): { buffer: Buffer; oldBit: 0 | 1 } {
    if (offset < 0) {
      throw new Error('ERR bit offset is not an integer or out of range');
    }

    const byteIndex = Math.floor(offset / 8);
    const bitOffset = 7 - (offset % 8);
    const mask = 1 << bitOffset;

    let target = source;
    if (byteIndex >= source.length) {
      target = Buffer.alloc(byteIndex + 1);
      source.copy(target, 0, 0, source.length);
    }

    const oldByte = target[byteIndex];
    const oldBit: 0 | 1 = (oldByte & mask) !== 0 ? 1 : 0;

    if (value === 1) {
      target[byteIndex] = oldByte | mask;
    } else {
      target[byteIndex] = oldByte & ~mask;
    }

    return { buffer: target, oldBit };
  }

  /**
   * Gets bit at offset (0 or 1).
   */
  public static getBit(source: Buffer, offset: number): 0 | 1 {
    if (offset < 0) {
      throw new Error('ERR bit offset is not an integer or out of range');
    }

    const byteIndex = Math.floor(offset / 8);
    if (byteIndex >= source.length) {
      return 0;
    }

    const bitOffset = 7 - (offset % 8);
    const mask = 1 << bitOffset;
    return (source[byteIndex] & mask) !== 0 ? 1 : 0;
  }

  /**
   * Computes population count (Hamming weight) of set bits in byte range.
   */
  public static bitCount(source: Buffer, start?: number, end?: number): number {
    if (source.length === 0) return 0;

    let s = start !== undefined ? start : 0;
    let e = end !== undefined ? end : source.length - 1;

    // Normalizes negative indices
    if (s < 0) s = source.length + s;
    if (e < 0) e = source.length + e;
    s = Math.max(0, s);
    e = Math.min(source.length - 1, e);

    if (s > e || s >= source.length) {
      return 0;
    }

    let count = 0;
    for (let i = s; i <= e; i++) {
      let byte = source[i];
      // 8-bit popcount using Kernighan's bit trick
      while (byte > 0) {
        byte &= byte - 1;
        count++;
      }
    }

    return count;
  }

  /**
   * Finds the first position of bit (0 or 1) in byte range.
   * Returns -1 if not found.
   */
  public static bitPos(
    source: Buffer,
    bit: 0 | 1,
    start?: number,
    end?: number
  ): number {
    if (source.length === 0) {
      return bit === 0 ? 0 : -1;
    }

    let s = start !== undefined ? start : 0;
    let e = end !== undefined ? end : source.length - 1;

    if (s < 0) s = source.length + s;
    if (e < 0) e = source.length + e;
    s = Math.max(0, s);
    e = Math.min(source.length - 1, e);

    if (s > e || s >= source.length) {
      return -1;
    }

    for (let i = s; i <= e; i++) {
      const byte = source[i];
      if (bit === 1 && byte === 0) continue;
      if (bit === 0 && byte === 0xff) continue;

      for (let b = 7; b >= 0; b--) {
        const currentBit: 0 | 1 = (byte & (1 << b)) !== 0 ? 1 : 0;
        if (currentBit === bit) {
          return i * 8 + (7 - b);
        }
      }
    }

    // If searching for 0 and reached end of buffer without explicit end boundary,
    // bit after the last byte is an implicit 0
    if (bit === 0 && end === undefined) {
      return source.length * 8;
    }

    return -1;
  }

  /**
   * Bitwise operations across multiple buffers: AND, OR, XOR, NOT.
   */
  public static bitOp(
    operation: 'AND' | 'OR' | 'XOR' | 'NOT',
    buffers: Buffer[]
  ): Buffer {
    if (operation === 'NOT') {
      if (buffers.length !== 1) {
        throw new Error("ERR BITOP NOT takes exactly 1 source key");
      }
      const src = buffers[0];
      const result = Buffer.alloc(src.length);
      for (let i = 0; i < src.length; i++) {
        result[i] = ~src[i] & 0xff;
      }
      return result;
    }

    if (buffers.length === 0) {
      return Buffer.alloc(0);
    }

    // Find maximum length among source buffers
    let maxLen = 0;
    for (const buf of buffers) {
      if (buf.length > maxLen) maxLen = buf.length;
    }

    const result = Buffer.alloc(maxLen);

    for (let i = 0; i < maxLen; i++) {
      let byte = buffers[0][i] ?? 0;
      for (let b = 1; b < buffers.length; b++) {
        const nextByte = buffers[b][i] ?? 0;
        if (operation === 'AND') {
          byte &= nextByte;
        } else if (operation === 'OR') {
          byte |= nextByte;
        } else if (operation === 'XOR') {
          byte ^= nextByte;
        }
      }
      result[i] = byte;
    }

    return result;
  }
}
