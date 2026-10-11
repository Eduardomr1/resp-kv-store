/**
 * RESP (Redis Serialization Protocol) Data Types
 * Supports RESP2 specification with binary-safe primitives.
 */

export type RESPType =
  | 'simple_string'
  | 'error'
  | 'integer'
  | 'bulk_string'
  | 'null'
  | 'array';

export interface RESPSimpleString {
  type: 'simple_string';
  value: string;
}

export interface RESPError {
  type: 'error';
  value: string;
}

export interface RESPInteger {
  type: 'integer';
  value: bigint | number;
}

export interface RESPBulkString {
  type: 'bulk_string';
  value: Buffer;
}

export interface RESPNull {
  type: 'null';
}

export interface RESPArray {
  type: 'array';
  value: RESPValue[];
}

export type RESPValue =
  | RESPSimpleString
  | RESPError
  | RESPInteger
  | RESPBulkString
  | RESPNull
  | RESPArray;

export const RESP_PREFIX = {
  SIMPLE_STRING: 0x2b, // '+'
  ERROR: 0x2d,         // '-'
  INTEGER: 0x3a,       // ':'
  BULK_STRING: 0x24,   // '$'
  ARRAY: 0x2a,         // '*'
  CR: 0x0d,            // '\r'
  LF: 0x0a,            // '\n'
} as const;
