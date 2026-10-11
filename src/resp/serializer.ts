import type { RESPValue } from './types.ts';

export const RESP_CONSTANTS = {
  OK: Buffer.from('+OK\r\n'),
  PONG: Buffer.from('+PONG\r\n'),
  QUEUED: Buffer.from('+QUEUED\r\n'),
  NULL_BULK: Buffer.from('$-1\r\n'),
  NULL_ARRAY: Buffer.from('*-1\r\n'),
  EMPTY_ARRAY: Buffer.from('*0\r\n'),
  ZERO_INT: Buffer.from(':0\r\n'),
  ONE_INT: Buffer.from(':1\r\n'),
  CRLF: Buffer.from('\r\n'),
} as const;

export class RESPSerializer {
  /**
   * Encodes a Simple String: +<str>\r\n
   */
  public static encodeSimpleString(str: string): Buffer {
    if (str === 'OK') return RESP_CONSTANTS.OK;
    if (str === 'PONG') return RESP_CONSTANTS.PONG;
    return Buffer.from(`+${str}\r\n`, 'utf8');
  }

  /**
   * Encodes an Error: -<error>\r\n
   */
  public static encodeError(err: string, code: string = 'ERR'): Buffer {
    if (err.startsWith(code) || err.startsWith('ERR') || err.startsWith('WRONGTYPE')) {
      return Buffer.from(`-${err}\r\n`, 'utf8');
    }
    return Buffer.from(`-${code} ${err}\r\n`, 'utf8');
  }

  /**
   * Encodes an Integer: :<num>\r\n
   */
  public static encodeInteger(num: number | bigint): Buffer {
    if (num === 0 || num === 0n) return RESP_CONSTANTS.ZERO_INT;
    if (num === 1 || num === 1n) return RESP_CONSTANTS.ONE_INT;
    return Buffer.from(`:${num.toString()}\r\n`, 'ascii');
  }

  /**
   * Encodes a Bulk String: $<len>\r\n<data>\r\n or $-1\r\n
   */
  public static encodeBulkString(data: Buffer | string | null | undefined): Buffer {
    if (data === null || data === undefined) {
      return RESP_CONSTANTS.NULL_BULK;
    }

    const payload = Buffer.isBuffer(data) ? data : Buffer.from(data, 'utf8');
    const header = Buffer.from(`$${payload.length}\r\n`, 'ascii');

    return Buffer.concat([header, payload, RESP_CONSTANTS.CRLF]);
  }

  /**
   * Encodes an Array of elements: *<len>\r\n...
   */
  public static encodeArray(
    elements: (RESPValue | Buffer | string | number | bigint | null)[] | null
  ): Buffer {
    if (elements === null) {
      return RESP_CONSTANTS.NULL_ARRAY;
    }

    if (elements.length === 0) {
      return RESP_CONSTANTS.EMPTY_ARRAY;
    }

    const chunks: Buffer[] = [Buffer.from(`*${elements.length}\r\n`, 'ascii')];

    for (const elem of elements) {
      if (elem === null) {
        chunks.push(RESP_CONSTANTS.NULL_BULK);
      } else if (Buffer.isBuffer(elem) || typeof elem === 'string') {
        chunks.push(RESPSerializer.encodeBulkString(elem));
      } else if (typeof elem === 'number' || typeof elem === 'bigint') {
        chunks.push(RESPSerializer.encodeInteger(elem));
      } else if (Array.isArray(elem)) {
        chunks.push(RESPSerializer.encodeArray(elem));
      } else if (typeof elem === 'object' && 'type' in elem) {
        chunks.push(RESPSerializer.serialize(elem));
      }
    }

    return Buffer.concat(chunks);
  }

  /**
   * Encodes generic RESPValue structure.
   */
  public static serialize(value: RESPValue): Buffer {
    switch (value.type) {
      case 'simple_string':
        return RESPSerializer.encodeSimpleString(value.value);
      case 'error':
        return Buffer.from(`-${value.value}\r\n`, 'utf8');
      case 'integer':
        return RESPSerializer.encodeInteger(value.value);
      case 'bulk_string':
        return RESPSerializer.encodeBulkString(value.value);
      case 'null':
        return RESP_CONSTANTS.NULL_BULK;
      case 'array':
        return RESPSerializer.encodeArray(value.value);
      default:
        throw new Error(`Unsupported RESP value type: ${(value as any).type}`);
    }
  }
}
