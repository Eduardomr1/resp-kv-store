/**
 * Redis Stream Data Structure Implementation
 * Supports monotonic millisecond-sequence ID generation, field-value entries,
 * range slicing (XRANGE, XREVRANGE), and XREAD streaming semantics.
 */

export interface StreamId {
  ms: number;
  seq: number;
}

export interface StreamEntry {
  id: string; // e.g. "1710000000000-0"
  ms: number;
  seq: number;
  fields: [string, Buffer][];
}

export class StreamError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'StreamError';
  }
}

export class RedisStream {
  private entries: StreamEntry[] = [];
  private lastId: StreamId = { ms: 0, seq: 0 };

  constructor() {}

  public static compareIds(a: StreamId, b: StreamId): number {
    if (a.ms !== b.ms) {
      return a.ms < b.ms ? -1 : 1;
    }
    if (a.seq !== b.seq) {
      return a.seq < b.seq ? -1 : 1;
    }
    return 0;
  }

  public static formatId(id: StreamId): string {
    return `${id.ms}-${id.seq}`;
  }

  public static parseId(idStr: string): StreamId {
    const parts = idStr.split('-');
    if (parts.length !== 2) {
      throw new StreamError('ERR Invalid stream ID specified as stream command argument');
    }
    const ms = Number(parts[0]);
    const seq = Number(parts[1]);
    if (isNaN(ms) || isNaN(seq) || ms < 0 || seq < 0 || !Number.isInteger(ms) || !Number.isInteger(seq)) {
      throw new StreamError('ERR Invalid stream ID specified as stream command argument');
    }
    return { ms, seq };
  }

  public getLastId(): StreamId {
    return { ...this.lastId };
  }

  public getLastIdString(): string {
    return RedisStream.formatId(this.lastId);
  }

  public len(): number {
    return this.entries.length;
  }

  public getEntries(): StreamEntry[] {
    return this.entries;
  }

  /**
   * Adds an entry to the stream with explicit or auto-generated ID ('*').
   */
  public add(
    idSpec: string,
    fields: [string, Buffer][],
    maxlen?: number
  ): string {
    let targetId: StreamId;

    if (idSpec === '*') {
      // Auto-generate ID: <now>-<seq>
      const now = Date.now();
      if (now > this.lastId.ms) {
        targetId = { ms: now, seq: 0 };
      } else {
        // Monotonic sequence increment within same ms
        targetId = { ms: this.lastId.ms, seq: this.lastId.seq + 1 };
      }
    } else if (idSpec.endsWith('-*')) {
      // Explicit ms, auto-generate sequence: <ms>-*
      const msPart = Number(idSpec.slice(0, -2));
      if (isNaN(msPart) || msPart < 0 || !Number.isInteger(msPart)) {
        throw new StreamError('ERR Invalid stream ID specified as stream command argument');
      }
      if (msPart < this.lastId.ms) {
        throw new StreamError('ERR The ID specified in XADD is equal or smaller than the target stream top item');
      } else if (msPart === this.lastId.ms) {
        targetId = { ms: msPart, seq: this.lastId.seq + 1 };
      } else {
        targetId = { ms: msPart, seq: 0 };
      }
    } else {
      targetId = RedisStream.parseId(idSpec);
      // Validates strict monotonicity: must be > (0, 0) and > lastId
      if (targetId.ms === 0 && targetId.seq === 0) {
        throw new StreamError('ERR The ID specified in XADD must be greater than 0-0');
      }
      if (this.entries.length > 0 && RedisStream.compareIds(targetId, this.lastId) <= 0) {
        throw new StreamError('ERR The ID specified in XADD is equal or smaller than the target stream top item');
      }
    }

    const formattedId = RedisStream.formatId(targetId);
    const entry: StreamEntry = {
      id: formattedId,
      ms: targetId.ms,
      seq: targetId.seq,
      fields,
    };

    this.entries.push(entry);
    this.lastId = targetId;

    if (maxlen !== undefined && maxlen >= 0 && this.entries.length > maxlen) {
      this.trim(maxlen);
    }

    return formattedId;
  }

  /**
   * Trims stream entries to maxlen oldest elements discarded.
   */
  public trim(maxlen: number): number {
    if (this.entries.length <= maxlen) return 0;
    const toRemove = this.entries.length - maxlen;
    this.entries.splice(0, toRemove);
    return toRemove;
  }

  /**
   * Parses boundary ID string for range queries:
   * '-' -> { ms: 0, seq: 0 }
   * '+' -> { ms: Infinity, seq: Infinity }
   * '123' -> { ms: 123, seq: 0 } (start) or { ms: 123, seq: Infinity } (end)
   */
  private parseBoundaryId(boundary: string, isEnd: boolean): StreamId {
    if (boundary === '-') return { ms: 0, seq: 0 };
    if (boundary === '+') return { ms: Infinity, seq: Infinity };

    if (!boundary.includes('-')) {
      const ms = Number(boundary);
      if (isNaN(ms) || ms < 0) {
        throw new StreamError('ERR Invalid stream ID specified as stream command argument');
      }
      return { ms, seq: isEnd ? Infinity : 0 };
    }

    return RedisStream.parseId(boundary);
  }

  /**
   * XRANGE key start end [COUNT count]
   */
  public range(startStr: string, endStr: string, count?: number): StreamEntry[] {
    const startId = this.parseBoundaryId(startStr, false);
    const endId = this.parseBoundaryId(endStr, true);

    const result: StreamEntry[] = [];
    const limit = count !== undefined && count > 0 ? count : Infinity;

    for (const entry of this.entries) {
      const entryId = { ms: entry.ms, seq: entry.seq };
      if (RedisStream.compareIds(entryId, startId) < 0) {
        continue;
      }
      if (RedisStream.compareIds(entryId, endId) > 0) {
        break;
      }
      result.push(entry);
      if (result.length >= limit) {
        break;
      }
    }

    return result;
  }

  /**
   * XREVRANGE key end start [COUNT count]
   */
  public revrange(endStr: string, startStr: string, count?: number): StreamEntry[] {
    const endId = this.parseBoundaryId(endStr, true);
    const startId = this.parseBoundaryId(startStr, false);

    const result: StreamEntry[] = [];
    const limit = count !== undefined && count > 0 ? count : Infinity;

    for (let i = this.entries.length - 1; i >= 0; i--) {
      const entry = this.entries[i];
      const entryId = { ms: entry.ms, seq: entry.seq };
      if (RedisStream.compareIds(entryId, endId) > 0) {
        continue;
      }
      if (RedisStream.compareIds(entryId, startId) < 0) {
        break;
      }
      result.push(entry);
      if (result.length >= limit) {
        break;
      }
    }

    return result;
  }

  /**
   * Reads entries strictly greater than `lastIdStr`
   */
  public readAfter(lastIdStr: string, count?: number): StreamEntry[] {
    let boundary: StreamId;
    if (lastIdStr === '$') {
      boundary = this.lastId;
    } else {
      boundary = RedisStream.parseId(lastIdStr);
    }

    const result: StreamEntry[] = [];
    const limit = count !== undefined && count > 0 ? count : Infinity;

    for (const entry of this.entries) {
      const entryId = { ms: entry.ms, seq: entry.seq };
      if (RedisStream.compareIds(entryId, boundary) > 0) {
        result.push(entry);
        if (result.length >= limit) {
          break;
        }
      }
    }

    return result;
  }
}
