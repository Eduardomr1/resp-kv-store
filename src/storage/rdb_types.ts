/**
 * RDB Binary Snapshot Persistence Specification Constants and Types.
 * Compatible with Redis RDB Version 11 format.
 */

// Magic header constant: REDIS followed by 4-digit version '0011'
export const RDB_MAGIC = 'REDIS0011';
export const REDIS_MAGIC = 'REDIS0011';
export const MAGIC_HEADER = 'REDIS0011';

// RDB Control Opcodes
export const OPCODE_MODULE_AUX = 0xf7;
export const OPCODE_IDLE = 0xf8;
export const OPCODE_FREQ = 0xf9;
export const OPCODE_AUX = 0xfa;
export const OPCODE_RESIZEDB = 0xfb;
export const OPCODE_EXPIRETIME_MS = 0xfc;
export const OPCODE_EXPIRETIME_SEC = 0xfd;
export const OPCODE_SELECTDB = 0xfe;
export const OPCODE_EOF = 0xff;

// RDB Value Type Opcodes
export const TYPE_STRING = 0x00;
export const TYPE_LIST = 0x01;
export const TYPE_SET = 0x02;
export const TYPE_ZSET = 0x03;
export const TYPE_HASH = 0x04;

// Standard Length Encoding Types
export const RDB_6BITLEN = 0;
export const RDB_14BITLEN = 1;
export const RDB_32BITLEN = 2;
export const RDB_ENCVAL = 3;

// Special Integer Encodings
export const RDB_ENC_INT8 = 0xc0;
export const RDB_ENC_INT16 = 0xc1;
export const RDB_ENC_INT32 = 0xc2;
export const RDB_ENC_LZF = 0xc3;

/**
 * Custom Error for RDB serialization/deserialization failures.
 */
export class RDBError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'RDBError';
  }
}

/**
 * Custom Error for RDB corruption or invalid format.
 */
export class CorruptRDBError extends RDBError {
  constructor(message: string) {
    super(message);
    this.name = 'CorruptRDBError';
  }
}
