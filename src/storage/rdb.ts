import fs from 'node:fs';
import path from 'node:path';
import { Buffer } from 'node:buffer';
import type { StorageEngine } from './engine.ts';
import type { StorageEntry, StorageData, ZSetData } from './types.ts';
import { SkipList } from './skiplist.ts';
import {
  RDB_MAGIC,
  OPCODE_SELECTDB,
  OPCODE_EXPIRETIME_MS,
  OPCODE_EXPIRETIME_SEC,
  OPCODE_AUX,
  OPCODE_RESIZEDB,
  OPCODE_EOF,
  TYPE_STRING,
  TYPE_LIST,
  TYPE_SET,
  TYPE_ZSET,
  TYPE_HASH,
  RDBError,
  CorruptRDBError,
} from './rdb_types.ts';

/**
 * Precomputed 256-entry lookup table for CRC64 (Jones polynomial 0xad93d23594c935a9n).
 * Conforms to standard Redis 64-bit checksum verification.
 */
const CRC64_POLY = 0xad93d23594c935a9n;
const crc64Table = new BigUint64Array(256);
for (let i = 0; i < 256; i++) {
  let crc = BigInt(i);
  for (let j = 0; j < 8; j++) {
    if ((crc & 1n) !== 0n) {
      crc = (crc >> 1n) ^ CRC64_POLY;
    } else {
      crc = crc >> 1n;
    }
  }
  crc64Table[i] = crc;
}

/**
 * Computes standard 64-bit CRC over given Buffer.
 */
export function crc64(buf: Buffer): bigint {
  let crc = 0n;
  for (let i = 0; i < buf.length; i++) {
    const tableIndex = Number((crc ^ BigInt(buf[i])) & 0xffn);
    crc = (crc >> 8n) ^ crc64Table[tableIndex];
  }
  return crc;
}

/**
 * Length prefix encoding adhering to standard Redis RDB format:
 * - 00xxxxxx: 6-bit length (0..63) -> 1 byte
 * - 01xxxxxx xxxxxxxx: 14-bit length (64..16383) -> 2 bytes
 * - 10000000 + 4-byte BE: 32-bit length (16384..2^32-1) -> 5 bytes
 * - 10000001 + 8-byte BE: 64-bit length -> 9 bytes
 */
export function encodeLength(len: number): Buffer {
  if (len < 0) {
    throw new RDBError('Length cannot be negative');
  }
  if (len <= 0x3f) {
    return Buffer.from([len & 0x3f]);
  } else if (len <= 0x3fff) {
    return Buffer.from([0x40 | ((len >> 8) & 0x3f), len & 0xff]);
  } else if (len <= 0xffffffff) {
    const buf = Buffer.allocUnsafe(5);
    buf[0] = 0x80;
    buf.writeUInt32BE(len, 1);
    return buf;
  } else {
    const buf = Buffer.allocUnsafe(9);
    buf[0] = 0x81;
    buf.writeBigUInt64BE(BigInt(len), 1);
    return buf;
  }
}

/**
 * Decodes length prefix from buffer.
 */
export function decodeLength(buffer: Buffer, offsetRef: { offset: number }): number {
  if (offsetRef.offset >= buffer.length) {
    throw new CorruptRDBError('Unexpected EOF while decoding length');
  }
  const b = buffer[offsetRef.offset++];
  const encType = (b >> 6) & 0x03;

  if (encType === 0) {
    return b & 0x3f;
  } else if (encType === 1) {
    if (offsetRef.offset >= buffer.length) {
      throw new CorruptRDBError('Unexpected EOF while decoding 14-bit length');
    }
    const b2 = buffer[offsetRef.offset++];
    return ((b & 0x3f) << 8) | b2;
  } else if (encType === 2) {
    if (b === 0x80 || (b & 0x3f) === 0) {
      if (offsetRef.offset + 4 > buffer.length) {
        throw new CorruptRDBError('Unexpected EOF while decoding 32-bit length');
      }
      const val = buffer.readUInt32BE(offsetRef.offset);
      offsetRef.offset += 4;
      return val;
    } else if (b === 0x81) {
      if (offsetRef.offset + 8 > buffer.length) {
        throw new CorruptRDBError('Unexpected EOF while decoding 64-bit length');
      }
      const val = buffer.readBigUInt64BE(offsetRef.offset);
      offsetRef.offset += 8;
      return Number(val);
    } else {
      throw new CorruptRDBError(`Unsupported length format byte 0x${b.toString(16)}`);
    }
  } else {
    throw new CorruptRDBError(`Unexpected special length encoding byte 0x${b.toString(16)}`);
  }
}

/**
 * Encodes string or Buffer as length-prefixed bytes.
 */
export function encodeString(val: string | Buffer): Buffer {
  const buf = Buffer.isBuffer(val) ? val : Buffer.from(val, 'utf8');
  const lenBuf = encodeLength(buf.length);
  return Buffer.concat([lenBuf, buf]);
}

/**
 * Encodes an integer using special integer encoding (0xC0, 0xC1, 0xC2) or string fallback.
 */
export function encodeInteger(val: number): Buffer {
  if (val >= -128 && val <= 127) {
    const buf = Buffer.allocUnsafe(2);
    buf[0] = 0xc0;
    buf.writeInt8(val, 1);
    return buf;
  } else if (val >= -32768 && val <= 32767) {
    const buf = Buffer.allocUnsafe(3);
    buf[0] = 0xc1;
    buf.writeInt16LE(val, 1);
    return buf;
  } else if (val >= -2147483648 && val <= 2147483647) {
    const buf = Buffer.allocUnsafe(5);
    buf[0] = 0xc2;
    buf.writeInt32LE(val, 1);
    return buf;
  } else {
    return encodeString(val.toString());
  }
}

/**
 * Decodes string, handling both length-prefixed binary strings and special integer encodings.
 */
export function decodeString(buffer: Buffer, offsetRef: { offset: number }): Buffer {
  if (offsetRef.offset >= buffer.length) {
    throw new CorruptRDBError('Unexpected EOF while decoding string length');
  }
  const b = buffer[offsetRef.offset];
  const encType = (b >> 6) & 0x03;

  if (encType === 3) {
    offsetRef.offset++;
    if (b === 0xc0) {
      if (offsetRef.offset >= buffer.length) throw new CorruptRDBError('Unexpected EOF reading int8 string');
      const val = buffer.readInt8(offsetRef.offset++);
      return Buffer.from(val.toString(), 'utf8');
    } else if (b === 0xc1) {
      if (offsetRef.offset + 2 > buffer.length) throw new CorruptRDBError('Unexpected EOF reading int16 string');
      const val = buffer.readInt16LE(offsetRef.offset);
      offsetRef.offset += 2;
      return Buffer.from(val.toString(), 'utf8');
    } else if (b === 0xc2) {
      if (offsetRef.offset + 4 > buffer.length) throw new CorruptRDBError('Unexpected EOF reading int32 string');
      const val = buffer.readInt32LE(offsetRef.offset);
      offsetRef.offset += 4;
      return Buffer.from(val.toString(), 'utf8');
    } else {
      throw new CorruptRDBError(`Unknown special string format byte: 0x${b.toString(16)}`);
    }
  }

  const len = decodeLength(buffer, offsetRef);
  if (offsetRef.offset + len > buffer.length) {
    throw new CorruptRDBError(`Unexpected EOF: string length ${len} exceeds remaining buffer`);
  }
  const result = Buffer.from(buffer.subarray(offsetRef.offset, offsetRef.offset + len));
  offsetRef.offset += len;
  return result;
}

/**
 * Decodes an integer from buffer.
 */
export function decodeInteger(buffer: Buffer, offsetRef: { offset: number }): number {
  if (offsetRef.offset >= buffer.length) {
    throw new CorruptRDBError('Unexpected EOF while decoding integer');
  }
  const b = buffer[offsetRef.offset];
  if ((b >> 6) === 3) {
    offsetRef.offset++;
    if (b === 0xc0) {
      if (offsetRef.offset >= buffer.length) throw new CorruptRDBError('Unexpected EOF reading int8');
      return buffer.readInt8(offsetRef.offset++);
    } else if (b === 0xc1) {
      if (offsetRef.offset + 2 > buffer.length) throw new CorruptRDBError('Unexpected EOF reading int16');
      const val = buffer.readInt16LE(offsetRef.offset);
      offsetRef.offset += 2;
      return val;
    } else if (b === 0xc2) {
      if (offsetRef.offset + 4 > buffer.length) throw new CorruptRDBError('Unexpected EOF reading int32');
      const val = buffer.readInt32LE(offsetRef.offset);
      offsetRef.offset += 4;
      return val;
    }
  }

  const str = decodeString(buffer, offsetRef).toString('utf8');
  const num = parseInt(str, 10);
  if (isNaN(num)) throw new CorruptRDBError(`Failed to decode integer from: ${str}`);
  return num;
}

/**
 * Encodes score in Redis RDB TYPE_ZSET format:
 * - 1 byte length followed by ASCII characters of the float
 * - Special byte 253 for NaN, 254 for +Inf, 255 for -Inf
 */
export function encodeScore(score: number): Buffer {
  if (Number.isNaN(score)) {
    return Buffer.from([253]);
  }
  if (score === Infinity) {
    return Buffer.from([254]);
  }
  if (score === -Infinity) {
    return Buffer.from([255]);
  }
  const scoreStr = score.toString();
  const scoreBuf = Buffer.from(scoreStr, 'utf8');
  return Buffer.concat([Buffer.from([scoreBuf.length]), scoreBuf]);
}

/**
 * Decodes score in Redis RDB TYPE_ZSET format.
 */
export function decodeScore(buffer: Buffer, offsetRef: { offset: number }): number {
  if (offsetRef.offset >= buffer.length) {
    throw new CorruptRDBError('Unexpected EOF reading score');
  }
  const len = buffer[offsetRef.offset++];
  if (len === 253) return NaN;
  if (len === 254) return Infinity;
  if (len === 255) return -Infinity;

  if (offsetRef.offset + len > buffer.length) {
    throw new CorruptRDBError(`Unexpected EOF reading score string of length ${len}`);
  }
  const scoreStr = buffer.toString('utf8', offsetRef.offset, offsetRef.offset + len);
  offsetRef.offset += len;
  const num = parseFloat(scoreStr);
  if (isNaN(num)) {
    throw new CorruptRDBError(`Invalid float score string: ${scoreStr}`);
  }
  return num;
}

/**
 * RDB Binary Snapshot Persistence Encoder.
 */
export class RDBEncoder {
  public static encodeLength = encodeLength;
  public static encodeString = encodeString;
  public static encodeInteger = encodeInteger;
  public static encodeScore = encodeScore;
  public static crc64 = crc64;

  /**
   * Encodes the current state of StorageEngine into a Redis RDB binary buffer:
   * - Header: 'REDIS0011'
   * - Database Selection: SELECTDB 0
   * - Key entries:
   *   - Optional OPCODE_EXPIRETIME_MS + 8-byte uint64 LE timestamp
   *   - Value type opcode (0x00..0x04)
   *   - Length-prefixed key name
   *   - Value payload (string, list, set, zset with scores, hash)
   * - OPCODE_EOF (0xFF)
   * - 8-byte uint64 LE CRC64 checksum
   */
  public static encode(engine: StorageEngine): Buffer {
    const chunks: Buffer[] = [];

    // 1. Magic header
    chunks.push(Buffer.from(RDB_MAGIC, 'ascii'));

    // 2. Select database 0 (0xFE, 0x00)
    chunks.push(Buffer.from([OPCODE_SELECTDB, 0x00]));

    // 3. Extract active non-expired entries
    let entries: Array<{ key: string; entry: StorageEntry }>;
    if (typeof (engine as any).getAllEntries === 'function') {
      entries = (engine as any).getAllEntries();
    } else {
      entries = [];
      const storeMap = (engine as any).store as Map<string, StorageEntry>;
      if (storeMap) {
        for (const [key, entry] of storeMap.entries()) {
          if (entry.expiresAt !== null && entry.expiresAt <= Date.now()) continue;
          entries.push({ key, entry });
        }
      }
    }

    // 4. Encode each key-value pair
    for (const { key, entry } of entries) {
      // Expiration timestamp
      if (entry.expiresAt !== null) {
        chunks.push(Buffer.from([OPCODE_EXPIRETIME_MS]));
        const expBuf = Buffer.allocUnsafe(8);
        expBuf.writeBigUInt64LE(BigInt(entry.expiresAt), 0);
        chunks.push(expBuf);
      }

      // Value Type Opcode & Key
      switch (entry.data.type) {
        case 'string': {
          chunks.push(Buffer.from([TYPE_STRING]));
          chunks.push(encodeString(key));
          chunks.push(encodeString(entry.data.value));
          break;
        }

        case 'list': {
          chunks.push(Buffer.from([TYPE_LIST]));
          chunks.push(encodeString(key));
          const list = entry.data.value as Buffer[];
          chunks.push(encodeLength(list.length));
          for (const item of list) {
            chunks.push(encodeString(item));
          }
          break;
        }

        case 'set': {
          chunks.push(Buffer.from([TYPE_SET]));
          chunks.push(encodeString(key));
          const set = entry.data.value as Set<string>;
          chunks.push(encodeLength(set.size));
          for (const member of set) {
            chunks.push(encodeString(member));
          }
          break;
        }

        case 'zset': {
          chunks.push(Buffer.from([TYPE_ZSET]));
          chunks.push(encodeString(key));
          const zdata = entry.data as ZSetData;
          const range = zdata.value.getRange(0, -1);
          chunks.push(encodeLength(range.length));
          for (const item of range) {
            chunks.push(encodeString(item.member));
            chunks.push(encodeScore(item.score));
          }
          break;
        }

        case 'hash': {
          chunks.push(Buffer.from([TYPE_HASH]));
          chunks.push(encodeString(key));
          const map = entry.data.value as Map<string, Buffer>;
          chunks.push(encodeLength(map.size));
          for (const [field, val] of map.entries()) {
            chunks.push(encodeString(field));
            chunks.push(encodeString(val));
          }
          break;
        }
      }
    }

    // 5. EOF Opcode
    chunks.push(Buffer.from([OPCODE_EOF]));

    // 6. Concatenate payload & calculate 8-byte uint64 LE CRC64 checksum
    const bodyBuf = Buffer.concat(chunks);
    const checksum = crc64(bodyBuf);
    const checkBuf = Buffer.allocUnsafe(8);
    checkBuf.writeBigUInt64LE(checksum, 0);

    return Buffer.concat([bodyBuf, checkBuf]);
  }

  /**
   * Saves the RDB snapshot to disk atomically using a temp file and rename.
   */
  public static save(filePath: string, engine: StorageEngine): void {
    const resolvedPath = path.resolve(filePath);
    const dir = path.dirname(resolvedPath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }

    const tmpPath = `${resolvedPath}.tmp.${Date.now()}.${process.pid}.${Math.random().toString(36).slice(2, 8)}`;
    const buffer = RDBEncoder.encode(engine);

    try {
      const fd = fs.openSync(tmpPath, 'w');
      fs.writeSync(fd, buffer);
      fs.fsyncSync(fd);
      fs.closeSync(fd);
      fs.renameSync(tmpPath, resolvedPath);
    } catch (err) {
      if (fs.existsSync(tmpPath)) {
        try { fs.unlinkSync(tmpPath); } catch {}
      }
      throw err;
    }
  }

  /**
   * Asynchronously saves RDB snapshot to disk (BGSAVE).
   */
  public static async bgsave(filePath: string, engine: StorageEngine): Promise<void> {
    const resolvedPath = path.resolve(filePath);
    const dir = path.dirname(resolvedPath);
    if (!fs.existsSync(dir)) {
      await fs.promises.mkdir(dir, { recursive: true });
    }

    const tmpPath = `${resolvedPath}.tmp.${Date.now()}.${process.pid}.${Math.random().toString(36).slice(2, 8)}`;
    const buffer = RDBEncoder.encode(engine);

    try {
      const handle = await fs.promises.open(tmpPath, 'w');
      await handle.write(buffer);
      await handle.sync();
      await handle.close();
      await fs.promises.rename(tmpPath, resolvedPath);
    } catch (err) {
      if (fs.existsSync(tmpPath)) {
        try { await fs.promises.unlink(tmpPath); } catch {}
      }
      throw err;
    }
  }

  public encode(engine: StorageEngine): Buffer {
    return RDBEncoder.encode(engine);
  }

  public save(filePath: string, engine: StorageEngine): void {
    return RDBEncoder.save(filePath, engine);
  }

  public bgsave(filePath: string, engine: StorageEngine): Promise<void> {
    return RDBEncoder.bgsave(filePath, engine);
  }
}

/**
 * RDB Binary Snapshot Persistence Decoder.
 */
export class RDBDecoder {
  public static decodeLength = decodeLength;
  public static decodeString = decodeString;
  public static decodeInteger = decodeInteger;
  public static decodeScore = decodeScore;

  /**
   * Decodes an RDB binary buffer and reconstructs keys, values, and expiration
   * timestamps directly into the provided StorageEngine instance.
   */
  public static decode(buffer: Buffer, engine: StorageEngine): void {
    if (!Buffer.isBuffer(buffer) || buffer.length < 9) {
      throw new CorruptRDBError('Invalid RDB file: buffer too short');
    }

    // 1. Magic Header validation
    const magic = buffer.subarray(0, 5).toString('ascii');
    if (magic !== 'REDIS') {
      throw new CorruptRDBError(`Invalid RDB magic header: expected 'REDIS', got '${magic}'`);
    }

    const versionStr = buffer.subarray(5, 9).toString('ascii');
    const versionNum = parseInt(versionStr, 10);
    if (isNaN(versionNum)) {
      throw new CorruptRDBError(`Invalid RDB version format: '${versionStr}'`);
    }

    // 2. Minimum length check for header + EOF + checksum
    if (buffer.length < 18) {
      throw new CorruptRDBError('Truncated RDB file: missing EOF or checksum');
    }

    // 3. Checksum validation (last 8 bytes uint64 LE)
    const expectedChecksum = buffer.readBigUInt64LE(buffer.length - 8);
    if (expectedChecksum !== 0n) {
      const computedChecksum = crc64(buffer.subarray(0, buffer.length - 8));
      if (computedChecksum !== expectedChecksum) {
        throw new CorruptRDBError(
          `RDB checksum mismatch: file is corrupt (expected 0x${expectedChecksum.toString(16)}, got 0x${computedChecksum.toString(16)})`
        );
      }
    }

    // 4. Parse stream opcodes
    const offsetRef = { offset: 9 };
    let currentExpiresAt: number | null = null;
    let currentDb = 0;
    let eofFound = false;

    while (offsetRef.offset < buffer.length - 8) {
      const opcode = buffer[offsetRef.offset++];

      if (opcode === OPCODE_EOF) {
        eofFound = true;
        break;
      }

      if (opcode === OPCODE_SELECTDB) {
        currentDb = decodeLength(buffer, offsetRef);
        continue;
      }

      if (opcode === OPCODE_EXPIRETIME_MS) {
        if (offsetRef.offset + 8 > buffer.length - 8) {
          throw new CorruptRDBError('Unexpected EOF reading expiretime ms');
        }
        const ms = buffer.readBigUInt64LE(offsetRef.offset);
        offsetRef.offset += 8;
        currentExpiresAt = Number(ms);
        continue;
      }

      if (opcode === OPCODE_EXPIRETIME_SEC) {
        if (offsetRef.offset + 4 > buffer.length - 8) {
          throw new CorruptRDBError('Unexpected EOF reading expiretime sec');
        }
        const sec = buffer.readUInt32LE(offsetRef.offset);
        offsetRef.offset += 4;
        currentExpiresAt = Number(sec) * 1000;
        continue;
      }

      if (opcode === OPCODE_AUX) {
        decodeString(buffer, offsetRef); // aux key
        decodeString(buffer, offsetRef); // aux value
        continue;
      }

      if (opcode === OPCODE_RESIZEDB) {
        decodeLength(buffer, offsetRef); // db size
        decodeLength(buffer, offsetRef); // expires size
        continue;
      }

      // Reconstruct value types
      switch (opcode) {
        case TYPE_STRING: {
          const key = decodeString(buffer, offsetRef).toString('utf8');
          const val = decodeString(buffer, offsetRef);
          RDBDecoder.restoreValue(engine, key, { type: 'string', value: val }, currentExpiresAt);
          currentExpiresAt = null;
          break;
        }

        case TYPE_LIST: {
          const key = decodeString(buffer, offsetRef).toString('utf8');
          const len = decodeLength(buffer, offsetRef);
          const list: Buffer[] = [];
          for (let i = 0; i < len; i++) {
            list.push(decodeString(buffer, offsetRef));
          }
          RDBDecoder.restoreValue(engine, key, { type: 'list', value: list }, currentExpiresAt);
          currentExpiresAt = null;
          break;
        }

        case TYPE_SET: {
          const key = decodeString(buffer, offsetRef).toString('utf8');
          const len = decodeLength(buffer, offsetRef);
          const set = new Set<string>();
          for (let i = 0; i < len; i++) {
            set.add(decodeString(buffer, offsetRef).toString('utf8'));
          }
          RDBDecoder.restoreValue(engine, key, { type: 'set', value: set }, currentExpiresAt);
          currentExpiresAt = null;
          break;
        }

        case TYPE_ZSET: {
          const key = decodeString(buffer, offsetRef).toString('utf8');
          const len = decodeLength(buffer, offsetRef);
          const skipList = new SkipList();
          const memberScores = new Map<string, number>();
          for (let i = 0; i < len; i++) {
            const member = decodeString(buffer, offsetRef).toString('utf8');
            const score = decodeScore(buffer, offsetRef);
            skipList.insert(member, score);
            memberScores.set(member, score);
          }
          RDBDecoder.restoreValue(
            engine,
            key,
            { type: 'zset', value: skipList, memberScores },
            currentExpiresAt
          );
          currentExpiresAt = null;
          break;
        }

        case TYPE_HASH: {
          const key = decodeString(buffer, offsetRef).toString('utf8');
          const len = decodeLength(buffer, offsetRef);
          const map = new Map<string, Buffer>();
          for (let i = 0; i < len; i++) {
            const field = decodeString(buffer, offsetRef).toString('utf8');
            const val = decodeString(buffer, offsetRef);
            map.set(field, val);
          }
          RDBDecoder.restoreValue(engine, key, { type: 'hash', value: map }, currentExpiresAt);
          currentExpiresAt = null;
          break;
        }

        default:
          throw new CorruptRDBError(
            `Unknown or unsupported RDB opcode: 0x${opcode.toString(16)} at offset ${offsetRef.offset - 1}`
          );
      }
    }

    if (!eofFound) {
      throw new CorruptRDBError('Truncated RDB file: missing OPCODE_EOF');
    }

    // Verify EOF position directly precedes the 8-byte checksum
    if (offsetRef.offset !== buffer.length - 8) {
      throw new CorruptRDBError('Unexpected extra bytes between EOF and checksum');
    }
  }

  /**
   * Helper to restore data structure into storage engine.
   */
  private static restoreValue(
    engine: StorageEngine,
    key: string,
    data: StorageData,
    expiresAt: number | null
  ): void {
    if (typeof (engine as any).restoreEntry === 'function') {
      (engine as any).restoreEntry(key, data, expiresAt);
    } else {
      // Fallback if custom engine is provided
      switch (data.type) {
        case 'string':
          engine.set(key, data.value);
          break;
        case 'list':
          if (data.value.length > 0) engine.rpush(key, ...data.value);
          break;
        case 'set':
          if (data.value.size > 0) engine.sadd(key, ...Array.from(data.value.values()));
          break;
        case 'zset': {
          for (const [m, s] of data.memberScores.entries()) {
            engine.zadd(key, s, m);
          }
          break;
        }
        case 'hash': {
          for (const [f, v] of data.value.entries()) {
            engine.hset(key, f, v);
          }
          break;
        }
      }
      if (expiresAt !== null) {
        engine.pexpire(key, Math.max(1, expiresAt - Date.now()));
      }
    }
  }

  /**
   * Loads an RDB snapshot file from disk into the storage engine.
   * Returns true if loaded, or false if file does not exist.
   */
  public static load(filePath: string, engine: StorageEngine): boolean {
    const resolvedPath = path.resolve(filePath);
    if (!fs.existsSync(resolvedPath)) {
      return false;
    }

    const buffer = fs.readFileSync(resolvedPath);
    RDBDecoder.decode(buffer, engine);
    return true;
  }

  public decode(buffer: Buffer, engine: StorageEngine): void {
    return RDBDecoder.decode(buffer, engine);
  }

  public load(filePath: string, engine: StorageEngine): boolean {
    return RDBDecoder.load(filePath, engine);
  }
}
