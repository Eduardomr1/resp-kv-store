/**
 * ReplicationBacklog: High-performance circular ring buffer for the replication stream.
 *
 * Tracks the master's monotonic byte offset (`master_repl_offset`) and retains the last N bytes
 * of written commands. Allows connected replicas to perform partial resynchronization (PSYNC)
 * without requiring expensive full-state transfers as long as their replication offset falls
 * within the circular buffer window.
 */
export class ReplicationBacklog {
  public master_repl_offset: bigint = 0n;
  private buffer: Buffer;
  private readonly capacity: number;

  constructor(size: number = 1024 * 1024) {
    if (size <= 0) {
      throw new Error(`Backlog size must be a positive integer, received: ${size}`);
    }
    this.capacity = size;
    this.buffer = Buffer.alloc(size);
    this.master_repl_offset = 0n;
  }

  /**
   * Backlog circular buffer capacity in bytes.
   */
  public get size(): number {
    return this.capacity;
  }

  /**
   * Current global replication offset.
   */
  public get offset(): bigint {
    return this.master_repl_offset;
  }

  /**
   * Returns the oldest global byte offset still stored in the circular ring buffer.
   */
  public get oldestOffset(): bigint {
    const cap = BigInt(this.capacity);
    return this.master_repl_offset > cap ? this.master_repl_offset - cap : 0n;
  }

  /**
   * Returns the number of valid backlogged bytes currently available.
   */
  public get bufferedBytes(): number {
    const cap = BigInt(this.capacity);
    return Number(this.master_repl_offset > cap ? cap : this.master_repl_offset);
  }

  /**
   * Appends write command data to backlog and updates the global replication offset.
   *
   * Handles circular ring buffer wrap-around and handles inputs exceeding buffer capacity.
   */
  public write(data: Buffer): void {
    if (data.length === 0) return;

    const len = data.length;
    const cap = this.capacity;
    const capBig = BigInt(cap);

    if (len >= cap) {
      // Data size exceeds or equals circular buffer capacity.
      // Only the most recent `capacity` bytes will fit in the ring buffer.
      const slice = data.subarray(len - cap);
      const startOffset = this.master_repl_offset + BigInt(len - cap);
      const bufferStart = Number(startOffset % capBig);
      const bytesToEnd = cap - bufferStart;

      if (cap <= bytesToEnd) {
        slice.copy(this.buffer, bufferStart);
      } else {
        slice.copy(this.buffer, bufferStart, 0, bytesToEnd);
        slice.copy(this.buffer, 0, bytesToEnd);
      }
    } else {
      // Data size is less than capacity.
      const bufferStart = Number(this.master_repl_offset % capBig);
      const bytesToEnd = cap - bufferStart;

      if (len <= bytesToEnd) {
        data.copy(this.buffer, bufferStart);
      } else {
        data.copy(this.buffer, bufferStart, 0, bytesToEnd);
        data.copy(this.buffer, 0, bytesToEnd);
      }
    }

    this.master_repl_offset += BigInt(len);
  }

  /**
   * Retrieves backlogged stream bytes starting from `offset` up to the current offset
   * if still available within the backlog window.
   * Returns `null` if offset has fallen off the circular window (requiring full resync)
   * or if offset is invalid / ahead of master.
   */
  public getStreamFrom(offset: bigint): Buffer | null {
    if (offset < 0n) return null;
    if (offset > this.master_repl_offset) return null;

    const capBig = BigInt(this.capacity);
    const minOffset = this.master_repl_offset > capBig ? this.master_repl_offset - capBig : 0n;

    if (offset < minOffset) {
      return null; // Fallen off the circular window!
    }

    const bytesToRead = Number(this.master_repl_offset - offset);
    if (bytesToRead === 0) {
      return Buffer.alloc(0);
    }

    const result = Buffer.allocUnsafe(bytesToRead);
    const startIdx = Number(offset % capBig);
    const bytesToEnd = this.capacity - startIdx;

    if (bytesToRead <= bytesToEnd) {
      this.buffer.copy(result, 0, startIdx, startIdx + bytesToRead);
    } else {
      this.buffer.copy(result, 0, startIdx, this.capacity);
      this.buffer.copy(result, bytesToEnd, 0, bytesToRead - bytesToEnd);
    }

    return result;
  }

  /**
   * Resets the backlog buffer and offsets.
   */
  public reset(): void {
    this.buffer.fill(0);
    this.master_repl_offset = 0n;
  }
}
