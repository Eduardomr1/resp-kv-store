import { type RESPValue, type RESPParserOptions, RESP_PREFIX } from './types.ts';

export class ProtocolError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ProtocolError';
  }
}

const DEFAULT_INITIAL_CAPACITY = 64 * 1024;
const DEFAULT_MAX_BULK_LENGTH = 64 * 1024 * 1024; // 64 MB
const DEFAULT_MAX_ARRAY_LENGTH = 65536;
const DEFAULT_MAX_INLINE_LENGTH = 64 * 1024; // 64 KB
const DEFAULT_MAX_DEPTH = 16;

const STRICT_LENGTH_REGEX = /^-?\d+$/;
const STRICT_INTEGER_REGEX = /^[+-]?\d+$/;

/**
 * Streaming zero-copy RESP Parser.
 *
 * Implements a linear buffer with a cursor that handles:
 * - TCP packet fragmentation (partial frames)
 * - Pipelined commands (multiple frames per TCP segment)
 * - Binary-safe bulk strings (containing \r\n or \0 inside data)
 * - Inline commands (telnet/netcat style without RESP prefixes)
 * - Amortized O(1) buffer compaction to eliminate memory fragmentation
 * - Protocol framing bounds (max bulk size, array count, nesting depth, and line length)
 */
export class RESPParser {
  private buffer: Buffer;
  private readOffset: number = 0;
  private writeOffset: number = 0;
  private readonly maxBulkLength: number;
  private readonly maxArrayLength: number;
  private readonly maxInlineLength: number;
  private readonly maxDepth: number;

  constructor(optionsOrCapacity: number | RESPParserOptions = DEFAULT_INITIAL_CAPACITY) {
    if (typeof optionsOrCapacity === 'number') {
      this.buffer = Buffer.allocUnsafe(optionsOrCapacity);
      this.maxBulkLength = DEFAULT_MAX_BULK_LENGTH;
      this.maxArrayLength = DEFAULT_MAX_ARRAY_LENGTH;
      this.maxInlineLength = DEFAULT_MAX_INLINE_LENGTH;
      this.maxDepth = DEFAULT_MAX_DEPTH;
    } else {
      const cap = optionsOrCapacity.initialCapacity ?? DEFAULT_INITIAL_CAPACITY;
      this.buffer = Buffer.allocUnsafe(cap);
      this.maxBulkLength = optionsOrCapacity.maxBulkLength ?? DEFAULT_MAX_BULK_LENGTH;
      this.maxArrayLength = optionsOrCapacity.maxArrayLength ?? DEFAULT_MAX_ARRAY_LENGTH;
      this.maxInlineLength = optionsOrCapacity.maxInlineLength ?? DEFAULT_MAX_INLINE_LENGTH;
      this.maxDepth = optionsOrCapacity.maxDepth ?? DEFAULT_MAX_DEPTH;
    }
  }

  /**
   * Appends incoming TCP stream chunk to internal buffer,
   * compacting or growing buffer if necessary.
   */
  public feed(chunk: Buffer): void {
    const requiredSize = this.writeOffset + chunk.length;

    // Check if we need to resize or compact
    if (requiredSize > this.buffer.length) {
      const unreadLength = this.writeOffset - this.readOffset;

      // If compacting frees up enough space, slide unread bytes to start
      if (unreadLength + chunk.length <= this.buffer.length) {
        if (unreadLength > 0) {
          this.buffer.copy(this.buffer, 0, this.readOffset, this.writeOffset);
        }
        this.readOffset = 0;
        this.writeOffset = unreadLength;
      } else {
        // Expand buffer capacity (power of 2 or required size)
        let newCapacity = this.buffer.length * 2;
        while (newCapacity < unreadLength + chunk.length) {
          newCapacity *= 2;
        }

        const newBuffer = Buffer.allocUnsafe(newCapacity);
        if (unreadLength > 0) {
          this.buffer.copy(newBuffer, 0, this.readOffset, this.writeOffset);
        }
        this.buffer = newBuffer;
        this.readOffset = 0;
        this.writeOffset = unreadLength;
      }
    }

    // Write chunk
    chunk.copy(this.buffer, this.writeOffset);
    this.writeOffset += chunk.length;
  }

  /**
   * Parses all fully available RESP frames from the accumulated stream.
   * Leaves partial / incomplete frames in the buffer for future feed() calls.
   */
  public parse(): RESPValue[] {
    const values: RESPValue[] = [];

    while (this.readOffset < this.writeOffset) {
      const savedOffset = this.readOffset;
      try {
        const val = this.parseFrame(0);
        if (val === null) {
          // Frame is incomplete; backtrack cursor and await more bytes
          this.readOffset = savedOffset;
          break;
        }
        values.push(val);
      } catch (err) {
        if (err instanceof ProtocolError) {
          // Reset buffer state on fatal protocol error
          this.readOffset = 0;
          this.writeOffset = 0;
          throw err;
        }
        throw err;
      }
    }

    // Reset offsets when fully drained
    if (this.readOffset === this.writeOffset) {
      this.readOffset = 0;
      this.writeOffset = 0;
    } else if (this.readOffset > 32 * 1024 && this.readOffset > (this.buffer.length >> 1)) {
      // Periodic compaction to prevent unbounded drift
      const unreadLength = this.writeOffset - this.readOffset;
      this.buffer.copy(this.buffer, 0, this.readOffset, this.writeOffset);
      this.readOffset = 0;
      this.writeOffset = unreadLength;
    }

    return values;
  }

  /**
   * Parses all fully available RESP frames along with their raw byte lengths.
   * Crucial for replication offset tracking.
   */
  public parseWithByteCount(): { value: RESPValue; byteLength: number }[] {
    const results: { value: RESPValue; byteLength: number }[] = [];

    while (this.readOffset < this.writeOffset) {
      const savedOffset = this.readOffset;
      try {
        const val = this.parseFrame(0);
        if (val === null) {
          this.readOffset = savedOffset;
          break;
        }
        results.push({ value: val, byteLength: this.readOffset - savedOffset });
      } catch (err) {
        if (err instanceof ProtocolError) {
          this.readOffset = 0;
          this.writeOffset = 0;
          throw err;
        }
        throw err;
      }
    }

    if (this.readOffset === this.writeOffset) {
      this.readOffset = 0;
      this.writeOffset = 0;
    } else if (this.readOffset > 32 * 1024 && this.readOffset > (this.buffer.length >> 1)) {
      const unreadLength = this.writeOffset - this.readOffset;
      this.buffer.copy(this.buffer, 0, this.readOffset, this.writeOffset);
      this.readOffset = 0;
      this.writeOffset = unreadLength;
    }

    return results;
  }

  /**
   * Internal recursive frame parser.
   * Returns RESPValue if frame is complete, or null if more bytes are needed.
   */
  private parseFrame(depth: number): RESPValue | null {
    if (this.readOffset >= this.writeOffset) {
      return null;
    }

    const firstByte = this.buffer[this.readOffset];

    switch (firstByte) {
      case RESP_PREFIX.SIMPLE_STRING:
        return this.parseSimpleString();

      case RESP_PREFIX.ERROR:
        return this.parseError();

      case RESP_PREFIX.INTEGER:
        return this.parseInteger();

      case RESP_PREFIX.BULK_STRING:
        return this.parseBulkString();

      case RESP_PREFIX.ARRAY:
        return this.parseArray(depth);

      default:
        // Inline command fallback (e.g. "PING\r\n" or "SET foo bar\r\n")
        return this.parseInlineCommand();
    }
  }

  /**
   * Finds the next CRLF (\r\n) starting from start.
   * Enforces maxInlineLength on unterminated header/inline lines to prevent unbounded scans.
   * Returns index of '\r', or -1 if not found yet.
   */
  private findCRLF(start: number = this.readOffset): number {
    for (let i = start; i < this.writeOffset - 1; i++) {
      if (this.buffer[i] === RESP_PREFIX.CR && this.buffer[i + 1] === RESP_PREFIX.LF) {
        if (i - start > this.maxInlineLength) {
          throw new ProtocolError(
            `Protocol line length ${i - start} exceeds maximum allowed ${this.maxInlineLength}`
          );
        }
        return i;
      }
    }

    if (this.writeOffset - start > this.maxInlineLength) {
      throw new ProtocolError(
        `Unterminated protocol line exceeds maximum allowed ${this.maxInlineLength} bytes`
      );
    }

    return -1;
  }

  private parseStrictLength(raw: string, label: string): number {
    if (!STRICT_LENGTH_REGEX.test(raw)) {
      throw new ProtocolError(`Invalid ${label} length: "${raw}"`);
    }
    const num = Number(raw);
    if (!Number.isSafeInteger(num)) {
      throw new ProtocolError(`Out-of-range ${label} length: "${raw}"`);
    }
    return num;
  }

  /**
   * Parses Simple String: +<str>\r\n
   */
  private parseSimpleString(): RESPValue | null {
    const crlf = this.findCRLF(this.readOffset + 1);
    if (crlf === -1) return null;

    const str = this.buffer.toString('utf8', this.readOffset + 1, crlf);
    this.readOffset = crlf + 2;
    return { type: 'simple_string', value: str };
  }

  /**
   * Parses Error: -<message>\r\n
   */
  private parseError(): RESPValue | null {
    const crlf = this.findCRLF(this.readOffset + 1);
    if (crlf === -1) return null;

    const str = this.buffer.toString('utf8', this.readOffset + 1, crlf);
    this.readOffset = crlf + 2;
    return { type: 'error', value: str };
  }

  /**
   * Parses Integer: :[<+|->]<number>\r\n
   */
  private parseInteger(): RESPValue | null {
    const crlf = this.findCRLF(this.readOffset + 1);
    if (crlf === -1) return null;

    const raw = this.buffer.toString('ascii', this.readOffset + 1, crlf);
    if (!STRICT_INTEGER_REGEX.test(raw)) {
      throw new ProtocolError(`Invalid integer representation: ${raw}`);
    }

    const num = Number(raw);
    if (!Number.isSafeInteger(num)) {
      try {
        const bigIntVal = BigInt(raw);
        this.readOffset = crlf + 2;
        return { type: 'integer', value: bigIntVal };
      } catch {
        throw new ProtocolError(`Invalid integer representation: ${raw}`);
      }
    }

    this.readOffset = crlf + 2;
    return { type: 'integer', value: num };
  }

  /**
   * Parses Bulk String: $<length>\r\n<binary-data>\r\n or $-1\r\n (null)
   */
  private parseBulkString(): RESPValue | null {
    const crlf = this.findCRLF(this.readOffset + 1);
    if (crlf === -1) return null;

    const lenStr = this.buffer.toString('ascii', this.readOffset + 1, crlf);
    const length = this.parseStrictLength(lenStr, 'bulk string');

    // Null Bulk String ($-1\r\n)
    if (length === -1) {
      this.readOffset = crlf + 2;
      return { type: 'null' };
    }

    if (length < -1) {
      throw new ProtocolError(`Negative bulk string length invalid: ${length}`);
    }

    if (length > this.maxBulkLength) {
      throw new ProtocolError(
        `Bulk string length ${length} exceeds maximum allowed ${this.maxBulkLength}`
      );
    }

    const dataStart = crlf + 2;
    const dataEnd = dataStart + length;
    const frameEnd = dataEnd + 2; // includes trailing \r\n

    // Check if we have enough bytes for payload + \r\n
    if (this.writeOffset < frameEnd) {
      return null;
    }

    // Validate trailing \r\n
    if (this.buffer[dataEnd] !== RESP_PREFIX.CR || this.buffer[dataEnd + 1] !== RESP_PREFIX.LF) {
      throw new ProtocolError(
        `Bulk string expected CRLF at end of data at offset ${dataEnd}`
      );
    }

    // Slice buffer payload (creating independent buffer copy to preserve data across ring buffer reuse)
    const payload = Buffer.allocUnsafe(length);
    this.buffer.copy(payload, 0, dataStart, dataEnd);

    this.readOffset = frameEnd;
    return { type: 'bulk_string', value: payload };
  }

  /**
   * Parses Array: *<count>\r\n<element_1>...<element_n> or *-1\r\n (null)
   */
  private parseArray(depth: number): RESPValue | null {
    if (depth >= this.maxDepth) {
      throw new ProtocolError(
        `Array nesting depth exceeds maximum allowed ${this.maxDepth}`
      );
    }

    const crlf = this.findCRLF(this.readOffset + 1);
    if (crlf === -1) return null;

    const countStr = this.buffer.toString('ascii', this.readOffset + 1, crlf);
    const count = this.parseStrictLength(countStr, 'array');

    // Null Array (*-1\r\n)
    if (count === -1) {
      this.readOffset = crlf + 2;
      return { type: 'null' };
    }

    if (count < -1) {
      throw new ProtocolError(`Negative array length invalid: ${count}`);
    }

    if (count > this.maxArrayLength) {
      throw new ProtocolError(
        `Array length ${count} exceeds maximum allowed ${this.maxArrayLength}`
      );
    }

    if (count === 0) {
      this.readOffset = crlf + 2;
      return { type: 'array', value: [] };
    }

    const currentOffset = this.readOffset;
    this.readOffset = crlf + 2;

    const elements: RESPValue[] = [];
    for (let i = 0; i < count; i++) {
      const element = this.parseFrame(depth + 1);
      if (element === null) {
        // Rollback whole array if any child element is incomplete
        this.readOffset = currentOffset;
        return null;
      }
      elements.push(element);
    }

    return { type: 'array', value: elements };
  }

  /**
   * Handles inline telnet commands: e.g. "PING\r\n" or "SET key \"hello world\"\r\n"
   * Supports double-quoted and single-quoted arguments with escape sequences.
   */
  private parseInlineCommand(): RESPValue | null {
    const crlf = this.findCRLF(this.readOffset);
    if (crlf === -1) return null;

    const line = this.buffer.toString('utf8', this.readOffset, crlf).trim();
    this.readOffset = crlf + 2;

    if (line.length === 0) {
      return null;
    }

    const tokens = RESPParser.splitInlineArgs(line);
    const elements: RESPValue[] = tokens.map((token) => ({
      type: 'bulk_string',
      value: Buffer.from(token, 'utf8'),
    }));

    return { type: 'array', value: elements };
  }

  /**
   * Splits an inline telnet command line into tokens respecting single/double quotes and escapes.
   */
  public static splitInlineArgs(line: string): string[] {
    const args: string[] = [];
    let i = 0;
    const len = line.length;

    while (i < len) {
      while (i < len && (line[i] === ' ' || line[i] === '\t')) {
        i++;
      }
      if (i >= len) break;

      let current = '';
      let inDoubleQuotes = false;
      let inSingleQuotes = false;
      let done = false;

      while (!done) {
        if (inDoubleQuotes) {
          if (i >= len) {
            throw new ProtocolError('Unbalanced quotes in inline request');
          }
          if (line[i] === '\\' && i + 1 < len) {
            i++;
            const esc = line[i];
            if (esc === 'n') current += '\n';
            else if (esc === 'r') current += '\r';
            else if (esc === 't') current += '\t';
            else current += esc;
          } else if (line[i] === '"') {
            if (i + 1 < len && line[i + 1] !== ' ' && line[i + 1] !== '\t') {
              throw new ProtocolError('Closing quote must be followed by whitespace in inline request');
            }
            done = true;
          } else {
            current += line[i];
          }
          i++;
        } else if (inSingleQuotes) {
          if (i >= len) {
            throw new ProtocolError('Unbalanced quotes in inline request');
          }
          if (line[i] === '\\' && i + 1 < len && line[i + 1] === "'") {
            i++;
            current += "'";
          } else if (line[i] === "'") {
            if (i + 1 < len && line[i + 1] !== ' ' && line[i + 1] !== '\t') {
              throw new ProtocolError('Closing quote must be followed by whitespace in inline request');
            }
            done = true;
          } else {
            current += line[i];
          }
          i++;
        } else {
          if (i >= len || line[i] === ' ' || line[i] === '\t') {
            done = true;
          } else if (line[i] === '"' && current.length === 0) {
            inDoubleQuotes = true;
            i++;
          } else if (line[i] === "'" && current.length === 0) {
            inSingleQuotes = true;
            i++;
          } else {
            current += line[i];
            i++;
          }
        }
      }

      args.push(current);
    }

    return args;
  }
}
