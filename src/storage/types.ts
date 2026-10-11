import type { SkipList } from './skiplist.ts';
import type { RedisStream } from './stream.ts';
import type { EvictionPolicy } from '../eviction/types.ts';

export type RedisDataType = 'string' | 'hash' | 'list' | 'set' | 'zset' | 'stream';

export class WrongTypeError extends Error {
  constructor() {
    super('WRONGTYPE Operation against a key holding the wrong kind of value');
    this.name = 'WrongTypeError';
  }
}

export interface StringData {
  type: 'string';
  value: Buffer;
}

export interface HashData {
  type: 'hash';
  value: Map<string, Buffer>;
}

export interface ListData {
  type: 'list';
  value: Buffer[];
}

export interface SetData {
  type: 'set';
  value: Set<string>;
}

export interface ZSetData {
  type: 'zset';
  value: SkipList;
  memberScores: Map<string, number>;
}

export interface StreamData {
  type: 'stream';
  value: RedisStream;
}

export type StorageData =
  | StringData
  | HashData
  | ListData
  | SetData
  | ZSetData
  | StreamData;

export interface StorageEntry {
  data: StorageData;
  expiresAt: number | null; // Milliseconds timestamp, null = persistent
  createdAt: number;
  updatedAt: number;
  lastAccessTime?: number;
  lfuCounter?: number;
  lfuDecayTime?: number;
}

export interface SetOptions {
  ex?: number;       // Expire time in seconds
  px?: number;       // Expire time in milliseconds
  exat?: number;     // Expire absolute Unix timestamp in seconds
  pxat?: number;     // Expire absolute Unix timestamp in milliseconds
  nx?: boolean;      // Only set if key does not exist
  xx?: boolean;      // Only set if key already exists
  keepttl?: boolean; // Retain current TTL
}

export type FsyncPolicy = 'always' | 'everysec' | 'no';

export interface EngineOptions {
  activeExpireIntervalMs?: number;      // Interval between active sweeps (default: 100ms)
  activeExpireSampleSize?: number;      // Number of random expiring keys sampled per iteration (default: 20)
  activeExpireMaxTimeBudgetMs?: number; // Max execution time budget per cycle in ms (default: 5ms)
  activeExpireThresholdRatio?: number;  // Threshold of expired sampled keys to trigger consecutive cycle (default: 0.25)
  aofEnabled?: boolean;                 // Enable Append-Only File persistence (default: false)
  aofFilePath?: string;                 // Path to AOF file (default: './appendonly.aof')
  aofFsyncPolicy?: FsyncPolicy;         // Fsync flush policy (default: 'everysec')
  maxmemory?: number;                   // Max memory in bytes (0 = unlimited, default: 0)
  maxmemoryPolicy?: EvictionPolicy;     // Cache eviction policy (default: 'noeviction')
  evictionSampleSize?: number;          // Eviction pool candidate sample size (default: 5)
}

export interface StorageStats {
  totalKeys: number;
  expiringKeys: number;
  hits: number;
  misses: number;
  expiredPassive: number;
  expiredActive: number;
  activeCyclesCount: number;
  memoryEstimateBytes: number;
  aofEnabled: boolean;
  aofFileSize: number;
  aofRewritesCount: number;
}
