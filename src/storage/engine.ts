import { SkipList } from './skiplist.ts';
import { RedisStream, type StreamEntry } from './stream.ts';
import { BitmapHelper } from './bitmap.ts';
import { HyperLogLog } from './hll.ts';
import { AOFManager } from './aof.ts';
import {
  type StorageEntry,
  type StorageData,
  type RedisDataType,
  type SetOptions,
  type EngineOptions,
  type StorageStats,
  WrongTypeError,
} from './types.ts';
import { MemoryGovernor } from '../eviction/governor.ts';
import { updateLFU, LFU_INIT_VAL } from '../eviction/lfu.ts';

/**
 * High-Performance Concurrent-friendly In-Memory Storage Engine.
 * Supports:
 * - Polymorphic data structures (String, Hash, List, Set, Sorted Set with SkipList)
 * - Dual-layer expiration strategy (Passive + Adaptive Active Probabilistic Sweeps)
 * - Dense array swap-and-pop index for O(1) random TTL sampling
 * - Optional transactional Append-Only File (AOF) persistence & online compaction
 */
export class StorageEngine {
  // Primary key-value storage: key -> StorageEntry
  private store: Map<string, StorageEntry> = new Map();

  // Secondary index for expiring keys: key -> expiresAt (epoch ms)
  private expires: Map<string, number> = new Map();

  // Dense array of expiring keys for O(1) uniform random sampling
  private expiringKeyList: string[] = [];
  private expiringKeyIndex: Map<string, number> = new Map();

  private options: Required<EngineOptions>;
  private activeTimer: NodeJS.Timeout | null = null;
  private aof: AOFManager | null = null;
  private isReplayingAOF: boolean = false;
  private memoryGovernor: MemoryGovernor;

  // Telemetry & metrics
  private statsCounters = {
    hits: 0,
    misses: 0,
    expiredPassive: 0,
    expiredActive: 0,
    activeCyclesCount: 0,
    memoryEstimateBytes: 0,
  };

  constructor(options: EngineOptions = {}) {
    this.options = {
      activeExpireIntervalMs: options.activeExpireIntervalMs ?? 100,
      activeExpireSampleSize: options.activeExpireSampleSize ?? 20,
      activeExpireMaxTimeBudgetMs: options.activeExpireMaxTimeBudgetMs ?? 5,
      activeExpireThresholdRatio: options.activeExpireThresholdRatio ?? 0.25,
      aofEnabled: options.aofEnabled ?? false,
      aofFilePath: options.aofFilePath ?? './appendonly.aof',
      aofFsyncPolicy: options.aofFsyncPolicy ?? 'everysec',
      maxmemory: options.maxmemory ?? 0,
      maxmemoryPolicy: options.maxmemoryPolicy ?? 'noeviction',
      evictionSampleSize: options.evictionSampleSize ?? 5,
    };

    this.memoryGovernor = new MemoryGovernor({
      maxmemory: this.options.maxmemory,
      policy: this.options.maxmemoryPolicy,
      sampleSize: this.options.evictionSampleSize,
      target: {
        getMemoryUsage: () => this.statsCounters.memoryEstimateBytes,
        getKeys: () => Array.from(this.store.keys()),
        getVolatileKeys: () => Array.from(this.expires.keys()),
        getEntry: (k) => this.store.get(k),
        delete: (k) => this.deleteInternal(k, false),
        sampleRandomKeys: (count, volatileOnly) => {
          const source = volatileOnly ? this.expiringKeyList : Array.from(this.store.keys());
          if (source.length === 0) return [];
          if (source.length <= count) return [...source];

          const result: string[] = [];
          const copy = [...source];
          for (let i = 0; i < count; i++) {
            const idx = Math.floor(Math.random() * copy.length);
            result.push(copy[idx]);
            copy[idx] = copy[copy.length - 1];
            copy.pop();
          }
          return result;
        },
      },
    });

    if (this.options.aofEnabled) {
      this.aof = new AOFManager(this.options.aofFilePath, this.options.aofFsyncPolicy);
      this.aof.open();
      // Replay existing AOF data before accepting traffic
      this.isReplayingAOF = true;
      try {
        this.aof.recover(this);
      } finally {
        this.isReplayingAOF = false;
      }
    }

    this.startActiveExpiration();
  }

  private startActiveExpiration(): void {
    if (this.activeTimer !== null) return;

    this.activeTimer = setInterval(() => {
      this.activeExpireCycle();
    }, this.options.activeExpireIntervalMs);

    this.activeTimer.unref();
  }

  public close(): void {
    if (this.activeTimer !== null) {
      clearInterval(this.activeTimer);
      this.activeTimer = null;
    }
    if (this.aof !== null) {
      this.aof.close();
      this.aof = null;
    }
  }

  public getAOF(): AOFManager | null {
    return this.aof;
  }

  public getMemoryGovernor(): MemoryGovernor {
    return this.memoryGovernor;
  }

  public recordAccess(entry: StorageEntry): void {
    const now = Date.now();
    entry.lastAccessTime = now;
    if (entry.lfuCounter === undefined) entry.lfuCounter = LFU_INIT_VAL;
    if (entry.lfuDecayTime === undefined) entry.lfuDecayTime = now;
    updateLFU(entry as any, now);
  }

  private logAOF(commandArgs: (string | Buffer)[]): void {
    if (this.aof !== null && !this.isReplayingAOF) {
      this.aof.append(commandArgs);
    }
  }

  /**
   * Passive (Lazy) eviction check.
   */
  private isExpired(key: string, entry: StorageEntry): boolean {
    if (entry.expiresAt !== null && entry.expiresAt <= Date.now()) {
      this.deleteInternal(key, true /* isPassive */);
      return true;
    }
    return false;
  }

  /**
   * Removes key from storage and secondary expiration index.
   */
  private deleteInternal(key: string, isPassive: boolean): boolean {
    const entry = this.store.get(key);
    if (!entry) return false;

    this.reclaimMemoryEstimate(key, entry.data);
    this.store.delete(key);

    if (this.expires.has(key)) {
      this.expires.delete(key);
      this.removeFromExpiringList(key);
    }

    if (isPassive) {
      this.statsCounters.expiredPassive++;
    } else {
      this.statsCounters.expiredActive++;
    }

    return true;
  }

  private removeFromExpiringList(key: string): void {
    const index = this.expiringKeyIndex.get(key);
    if (index === undefined) return;

    const lastIdx = this.expiringKeyList.length - 1;
    const lastKey = this.expiringKeyList[lastIdx];

    if (index !== lastIdx) {
      this.expiringKeyList[index] = lastKey;
      this.expiringKeyIndex.set(lastKey, index);
    }

    this.expiringKeyList.pop();
    this.expiringKeyIndex.delete(key);
  }

  private addToExpiringList(key: string, expiresAt: number): void {
    this.expires.set(key, expiresAt);
    if (!this.expiringKeyIndex.has(key)) {
      this.expiringKeyList.push(key);
      this.expiringKeyIndex.set(key, this.expiringKeyList.length - 1);
    }
  }

  /**
   * Active Probabilistic Expiration Cycle (Redis adaptive algorithm).
   */
  public activeExpireCycle(): void {
    this.statsCounters.activeCyclesCount++;
    const totalExpiring = this.expiringKeyList.length;
    if (totalExpiring === 0) return;

    const startTime = performance.now();
    const now = Date.now();

    do {
      const currentCount = this.expiringKeyList.length;
      if (currentCount === 0) break;

      const sampleSize = Math.min(this.options.activeExpireSampleSize, currentCount);
      let expiredCount = 0;

      for (let i = 0; i < sampleSize; i++) {
        const randomIndex = Math.floor(Math.random() * this.expiringKeyList.length);
        const candidateKey = this.expiringKeyList[randomIndex];
        const candidateExp = this.expires.get(candidateKey);

        if (candidateExp !== undefined && candidateExp <= now) {
          this.deleteInternal(candidateKey, false);
          expiredCount++;
        }
      }

      const ratio = expiredCount / sampleSize;
      const elapsed = performance.now() - startTime;

      if (
        ratio < this.options.activeExpireThresholdRatio ||
        elapsed >= this.options.activeExpireMaxTimeBudgetMs
      ) {
        break;
      }
    } while (this.expiringKeyList.length > 0);
  }

  /**
   * TYPE key
   */
  public type(key: string): RedisDataType | 'none' {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) {
      return 'none';
    }
    return entry.data.type;
  }

  // ==========================================
  // STRING OPERATIONS
  // ==========================================

  public set(key: string, value: Buffer | string, options: SetOptions = {}): boolean {
    const existing = this.store.get(key);
    const isExisted = existing !== undefined && !this.isExpired(key, existing);

    if (options.nx && isExisted) return false;
    if (options.xx && !isExisted) return false;

    const valBuffer = Buffer.isBuffer(value) ? value : Buffer.from(value, 'utf8');
    const now = Date.now();

    let expiresAt: number | null = null;
    if (options.keepttl && isExisted && existing) {
      expiresAt = existing.expiresAt;
    } else if (options.ex !== undefined) {
      expiresAt = now + Math.round(options.ex * 1000);
    } else if (options.px !== undefined) {
      expiresAt = now + Math.round(options.px);
    } else if (options.exat !== undefined) {
      expiresAt = Math.round(options.exat * 1000);
    } else if (options.pxat !== undefined) {
      expiresAt = Math.round(options.pxat);
    }

    if (expiresAt !== null && expiresAt <= now) {
      if (isExisted) this.deleteInternal(key, true);
      return true;
    }

    if (existing) {
      this.reclaimMemoryEstimate(key, existing.data);
    }

    const data: StorageData = { type: 'string', value: valBuffer };
    this.trackMemoryEstimate(key, data);

    try {
      this.memoryGovernor.evictIfNeeded();
    } catch (err) {
      this.reclaimMemoryEstimate(key, data);
      if (existing) {
        this.trackMemoryEstimate(key, existing.data);
      }
      throw err;
    }

    const entry: StorageEntry = {
      data,
      expiresAt,
      createdAt: isExisted && existing ? existing.createdAt : now,
      updatedAt: now,
      lastAccessTime: now,
      lfuCounter: isExisted && existing?.lfuCounter !== undefined ? existing.lfuCounter : LFU_INIT_VAL,
      lfuDecayTime: isExisted && existing?.lfuDecayTime !== undefined ? existing.lfuDecayTime : now,
    };

    this.store.set(key, entry);

    if (expiresAt !== null) {
      this.addToExpiringList(key, expiresAt);
    } else if (this.expires.has(key)) {
      this.expires.delete(key);
      this.removeFromExpiringList(key);
    }

    // AOF logging
    const aofArgs: (string | Buffer)[] = ['SET', key, valBuffer];
    if (expiresAt !== null) {
      aofArgs.push('PXAT', expiresAt.toString());
    }
    this.logAOF(aofArgs);

    return true;
  }

  public get(key: string): Buffer | null {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) {
      this.statsCounters.misses++;
      return null;
    }

    if (entry.data.type !== 'string') {
      throw new WrongTypeError();
    }

    this.statsCounters.hits++;
    this.recordAccess(entry);
    return entry.data.value;
  }

  public mget(keys: string[]): (Buffer | null)[] {
    return keys.map((key) => {
      try {
        return this.get(key);
      } catch (err) {
        if (err instanceof WrongTypeError) return null;
        throw err;
      }
    });
  }

  // ==========================================
  // BITMAP OPERATIONS
  // ==========================================

  public setbit(key: string, offset: number, value: 0 | 1): number {
    let entry = this.store.get(key);
    if (entry && this.isExpired(key, entry)) entry = undefined;

    let buf: Buffer;
    if (!entry) {
      buf = Buffer.alloc(0);
    } else {
      if (entry.data.type !== 'string') throw new WrongTypeError();
      buf = entry.data.value;
    }

    const { buffer: newBuf, oldBit } = BitmapHelper.setBit(buf, offset, value);
    this.set(key, newBuf);
    return oldBit;
  }

  public getbit(key: string, offset: number): number {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) return 0;
    if (entry.data.type !== 'string') throw new WrongTypeError();
    this.recordAccess(entry);
    return BitmapHelper.getBit(entry.data.value, offset);
  }

  public bitcount(key: string, start?: number, end?: number): number {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) return 0;
    if (entry.data.type !== 'string') throw new WrongTypeError();
    this.recordAccess(entry);
    return BitmapHelper.bitCount(entry.data.value, start, end);
  }

  public bitpos(key: string, bit: 0 | 1, start?: number, end?: number): number {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) return bit === 0 ? 0 : -1;
    if (entry.data.type !== 'string') throw new WrongTypeError();
    this.recordAccess(entry);
    return BitmapHelper.bitPos(entry.data.value, bit, start, end);
  }

  public bitop(op: 'AND' | 'OR' | 'XOR' | 'NOT', destKey: string, ...srcKeys: string[]): number {
    const buffers: Buffer[] = [];
    for (const k of srcKeys) {
      const entry = this.store.get(k);
      if (!entry || this.isExpired(k, entry)) {
        buffers.push(Buffer.alloc(0));
      } else {
        if (entry.data.type !== 'string') throw new WrongTypeError();
        this.recordAccess(entry);
        buffers.push(entry.data.value);
      }
    }

    const result = BitmapHelper.bitOp(op, buffers);
    this.set(destKey, result);
    return result.length;
  }

  // ==========================================
  // HYPERLOGLOG OPERATIONS
  // ==========================================

  public pfadd(key: string, ...elements: (string | Buffer)[]): number {
    let entry = this.store.get(key);
    if (entry && this.isExpired(key, entry)) entry = undefined;

    let hll: HyperLogLog;
    if (!entry) {
      hll = new HyperLogLog();
    } else {
      if (entry.data.type !== 'string') throw new WrongTypeError();
      hll = HyperLogLog.fromBuffer(entry.data.value);
    }

    let modified = false;
    for (const elem of elements) {
      if (hll.add(elem)) {
        modified = true;
      }
    }

    if (modified || !entry) {
      this.set(key, hll.toBuffer());
      return modified ? 1 : 0;
    }
    return 0;
  }

  public pfcount(...keys: string[]): number {
    if (keys.length === 0) return 0;

    if (keys.length === 1) {
      const entry = this.store.get(keys[0]);
      if (!entry || this.isExpired(keys[0], entry)) return 0;
      if (entry.data.type !== 'string') throw new WrongTypeError();
      this.recordAccess(entry);
      const hll = HyperLogLog.fromBuffer(entry.data.value);
      return hll.count();
    }

    // Merge in-memory
    const merged = new HyperLogLog();
    for (const k of keys) {
      const entry = this.store.get(k);
      if (!entry || this.isExpired(k, entry)) continue;
      if (entry.data.type !== 'string') throw new WrongTypeError();
      this.recordAccess(entry);
      const hll = HyperLogLog.fromBuffer(entry.data.value);
      merged.merge(hll);
    }
    return merged.count();
  }

  public pfmerge(destKey: string, ...srcKeys: string[]): boolean {
    const destEntry = this.store.get(destKey);
    let targetHll: HyperLogLog;
    if (destEntry && !this.isExpired(destKey, destEntry)) {
      if (destEntry.data.type !== 'string') throw new WrongTypeError();
      targetHll = HyperLogLog.fromBuffer(destEntry.data.value);
    } else {
      targetHll = new HyperLogLog();
    }

    for (const k of srcKeys) {
      const entry = this.store.get(k);
      if (!entry || this.isExpired(k, entry)) continue;
      if (entry.data.type !== 'string') throw new WrongTypeError();
      this.recordAccess(entry);
      const hll = HyperLogLog.fromBuffer(entry.data.value);
      targetHll.merge(hll);
    }

    this.set(destKey, targetHll.toBuffer());
    return true;
  }

  // ==========================================
  // HASH OPERATIONS
  // ==========================================

  public hset(key: string, field: string, value: Buffer | string): number {
    let entry = this.store.get(key);
    if (entry && this.isExpired(key, entry)) entry = undefined;

    const valBuffer = Buffer.isBuffer(value) ? value : Buffer.from(value, 'utf8');
    let isNewKey = false;
    let isNewField = false;

    if (!entry) {
      const hashData: StorageData = {
        type: 'hash',
        value: new Map<string, Buffer>(),
      };
      entry = {
        data: hashData,
        expiresAt: null,
        createdAt: Date.now(),
        updatedAt: Date.now(),
      };
      this.store.set(key, entry);
      isNewKey = true;
    } else if (entry.data.type !== 'hash') {
      throw new WrongTypeError();
    }

    const map = entry.data.value as Map<string, Buffer>;
    if (!map.has(field)) {
      isNewField = true;
    }

    map.set(field, valBuffer);
    entry.updatedAt = Date.now();
    this.statsCounters.memoryEstimateBytes += Buffer.byteLength(field) + valBuffer.length + 32;

    this.logAOF(['HSET', key, field, valBuffer]);
    return isNewField ? 1 : 0;
  }

  public hget(key: string, field: string): Buffer | null {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) {
      this.statsCounters.misses++;
      return null;
    }
    if (entry.data.type !== 'hash') throw new WrongTypeError();

    const val = (entry.data.value as Map<string, Buffer>).get(field);
    if (!val) {
      this.statsCounters.misses++;
      return null;
    }

    this.statsCounters.hits++;
    return val;
  }

  public hmget(key: string, fields: string[]): (Buffer | null)[] {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) {
      return fields.map(() => null);
    }
    if (entry.data.type !== 'hash') throw new WrongTypeError();

    const map = entry.data.value as Map<string, Buffer>;
    return fields.map((f) => map.get(f) ?? null);
  }

  public hdel(key: string, ...fields: string[]): number {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) return 0;
    if (entry.data.type !== 'hash') throw new WrongTypeError();

    const map = entry.data.value as Map<string, Buffer>;
    let count = 0;
    for (const f of fields) {
      const existing = map.get(f);
      if (existing) {
        map.delete(f);
        count++;
        this.statsCounters.memoryEstimateBytes -= Buffer.byteLength(f) + existing.length + 32;
      }
    }

    if (map.size === 0) {
      this.deleteInternal(key, true);
    }

    if (count > 0) {
      this.logAOF(['HDEL', key, ...fields]);
    }
    return count;
  }

  public hexists(key: string, field: string): boolean {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) return false;
    if (entry.data.type !== 'hash') throw new WrongTypeError();
    return (entry.data.value as Map<string, Buffer>).has(field);
  }

  public hgetall(key: string): [string, Buffer][] {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) return [];
    if (entry.data.type !== 'hash') throw new WrongTypeError();

    return Array.from((entry.data.value as Map<string, Buffer>).entries());
  }

  public hkeys(key: string): string[] {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) return [];
    if (entry.data.type !== 'hash') throw new WrongTypeError();

    return Array.from((entry.data.value as Map<string, Buffer>).keys());
  }

  public hvals(key: string): Buffer[] {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) return [];
    if (entry.data.type !== 'hash') throw new WrongTypeError();

    return Array.from((entry.data.value as Map<string, Buffer>).values());
  }

  public hlen(key: string): number {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) return 0;
    if (entry.data.type !== 'hash') throw new WrongTypeError();
    return (entry.data.value as Map<string, Buffer>).size;
  }

  // ==========================================
  // LIST OPERATIONS
  // ==========================================

  public lpush(key: string, ...values: (Buffer | string)[]): number {
    let entry = this.store.get(key);
    if (entry && this.isExpired(key, entry)) entry = undefined;

    if (!entry) {
      entry = {
        data: { type: 'list', value: [] },
        expiresAt: null,
        createdAt: Date.now(),
        updatedAt: Date.now(),
      };
      this.store.set(key, entry);
    } else if (entry.data.type !== 'list') {
      throw new WrongTypeError();
    }

    const list = entry.data.value as Buffer[];
    for (const val of values) {
      const buf = Buffer.isBuffer(val) ? val : Buffer.from(val, 'utf8');
      list.unshift(buf);
      this.statsCounters.memoryEstimateBytes += buf.length + 16;
    }
    entry.updatedAt = Date.now();

    this.logAOF(['LPUSH', key, ...values.map((v) => (Buffer.isBuffer(v) ? v : Buffer.from(v)))]);
    return list.length;
  }

  public rpush(key: string, ...values: (Buffer | string)[]): number {
    let entry = this.store.get(key);
    if (entry && this.isExpired(key, entry)) entry = undefined;

    if (!entry) {
      entry = {
        data: { type: 'list', value: [] },
        expiresAt: null,
        createdAt: Date.now(),
        updatedAt: Date.now(),
      };
      this.store.set(key, entry);
    } else if (entry.data.type !== 'list') {
      throw new WrongTypeError();
    }

    const list = entry.data.value as Buffer[];
    for (const val of values) {
      const buf = Buffer.isBuffer(val) ? val : Buffer.from(val, 'utf8');
      list.push(buf);
      this.statsCounters.memoryEstimateBytes += buf.length + 16;
    }
    entry.updatedAt = Date.now();

    this.logAOF(['RPUSH', key, ...values.map((v) => (Buffer.isBuffer(v) ? v : Buffer.from(v)))]);
    return list.length;
  }

  public lpop(key: string, count: number = 1): Buffer[] | Buffer | null {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) return null;
    if (entry.data.type !== 'list') throw new WrongTypeError();

    const list = entry.data.value as Buffer[];
    if (list.length === 0) return null;

    if (count === 1) {
      const popped = list.shift()!;
      this.statsCounters.memoryEstimateBytes -= popped.length + 16;
      if (list.length === 0) this.deleteInternal(key, true);
      this.logAOF(['LPOP', key]);
      return popped;
    }

    const poppedItems: Buffer[] = [];
    const numToPop = Math.min(count, list.length);
    for (let i = 0; i < numToPop; i++) {
      const item = list.shift()!;
      this.statsCounters.memoryEstimateBytes -= item.length + 16;
      poppedItems.push(item);
    }
    if (list.length === 0) this.deleteInternal(key, true);
    this.logAOF(['LPOP', key, count.toString()]);
    return poppedItems;
  }

  public rpop(key: string, count: number = 1): Buffer[] | Buffer | null {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) return null;
    if (entry.data.type !== 'list') throw new WrongTypeError();

    const list = entry.data.value as Buffer[];
    if (list.length === 0) return null;

    if (count === 1) {
      const popped = list.pop()!;
      this.statsCounters.memoryEstimateBytes -= popped.length + 16;
      if (list.length === 0) this.deleteInternal(key, true);
      this.logAOF(['RPOP', key]);
      return popped;
    }

    const poppedItems: Buffer[] = [];
    const numToPop = Math.min(count, list.length);
    for (let i = 0; i < numToPop; i++) {
      const item = list.pop()!;
      this.statsCounters.memoryEstimateBytes -= item.length + 16;
      poppedItems.push(item);
    }
    if (list.length === 0) this.deleteInternal(key, true);
    this.logAOF(['RPOP', key, count.toString()]);
    return poppedItems;
  }

  public lrange(key: string, start: number, stop: number): Buffer[] {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) return [];
    if (entry.data.type !== 'list') throw new WrongTypeError();

    const list = entry.data.value as Buffer[];
    const len = list.length;
    if (len === 0) return [];

    let startIdx = start < 0 ? len + start : start;
    let stopIdx = stop < 0 ? len + stop : stop;

    if (startIdx < 0) startIdx = 0;
    if (stopIdx >= len) stopIdx = len - 1;
    if (startIdx > stopIdx || startIdx >= len) return [];

    return list.slice(startIdx, stopIdx + 1);
  }

  public llen(key: string): number {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) return 0;
    if (entry.data.type !== 'list') throw new WrongTypeError();
    return (entry.data.value as Buffer[]).length;
  }

  // ==========================================
  // SET OPERATIONS
  // ==========================================

  public sadd(key: string, ...members: string[]): number {
    let entry = this.store.get(key);
    if (entry && this.isExpired(key, entry)) entry = undefined;

    if (!entry) {
      entry = {
        data: { type: 'set', value: new Set<string>() },
        expiresAt: null,
        createdAt: Date.now(),
        updatedAt: Date.now(),
      };
      this.store.set(key, entry);
    } else if (entry.data.type !== 'set') {
      throw new WrongTypeError();
    }

    const set = entry.data.value as Set<string>;
    let added = 0;
    for (const mem of members) {
      if (!set.has(mem)) {
        set.add(mem);
        added++;
        this.statsCounters.memoryEstimateBytes += Buffer.byteLength(mem) + 16;
      }
    }
    entry.updatedAt = Date.now();

    if (added > 0) {
      this.logAOF(['SADD', key, ...members]);
    }
    return added;
  }

  public srem(key: string, ...members: string[]): number {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) return 0;
    if (entry.data.type !== 'set') throw new WrongTypeError();

    const set = entry.data.value as Set<string>;
    let removed = 0;
    for (const mem of members) {
      if (set.delete(mem)) {
        removed++;
        this.statsCounters.memoryEstimateBytes -= Buffer.byteLength(mem) + 16;
      }
    }

    if (set.size === 0) {
      this.deleteInternal(key, true);
    }

    if (removed > 0) {
      this.logAOF(['SREM', key, ...members]);
    }
    return removed;
  }

  public smembers(key: string): string[] {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) return [];
    if (entry.data.type !== 'set') throw new WrongTypeError();

    return Array.from((entry.data.value as Set<string>).values());
  }

  public sismember(key: string, member: string): boolean {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) return false;
    if (entry.data.type !== 'set') throw new WrongTypeError();

    return (entry.data.value as Set<string>).has(member);
  }

  public scard(key: string): number {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) return 0;
    if (entry.data.type !== 'set') throw new WrongTypeError();
    return (entry.data.value as Set<string>).size;
  }

  // ==========================================
  // SORTED SET OPERATIONS (SKIPLIST)
  // ==========================================

  public zadd(key: string, score: number, member: string): number {
    let entry = this.store.get(key);
    if (entry && this.isExpired(key, entry)) entry = undefined;

    if (!entry) {
      entry = {
        data: {
          type: 'zset',
          value: new SkipList(),
          memberScores: new Map<string, number>(),
        },
        expiresAt: null,
        createdAt: Date.now(),
        updatedAt: Date.now(),
      };
      this.store.set(key, entry);
    } else if (entry.data.type !== 'zset') {
      throw new WrongTypeError();
    }

    const zsetData = entry.data as { type: 'zset'; value: SkipList; memberScores: Map<string, number> };
    const prevScore = zsetData.memberScores.get(member);
    let added = 0;

    if (prevScore !== undefined) {
      if (prevScore !== score) {
        zsetData.value.delete(member, prevScore);
        zsetData.value.insert(member, score);
        zsetData.memberScores.set(member, score);
      }
    } else {
      zsetData.value.insert(member, score);
      zsetData.memberScores.set(member, score);
      added = 1;
      this.statsCounters.memoryEstimateBytes += Buffer.byteLength(member) + 48;
    }
    entry.updatedAt = Date.now();

    this.logAOF(['ZADD', key, score.toString(), member]);
    return added;
  }

  public zscore(key: string, member: string): number | null {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) return null;
    if (entry.data.type !== 'zset') throw new WrongTypeError();

    const zsetData = entry.data as { type: 'zset'; value: SkipList; memberScores: Map<string, number> };
    return zsetData.memberScores.get(member) ?? null;
  }

  public zrange(key: string, start: number, stop: number): string[] {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) return [];
    if (entry.data.type !== 'zset') throw new WrongTypeError();

    const zsetData = entry.data as { type: 'zset'; value: SkipList; memberScores: Map<string, number> };
    const elements = zsetData.value.getRange(start, stop);
    return elements.map((e) => e.member);
  }

  public zrangeWithScores(
    key: string,
    start: number,
    stop: number
  ): Array<{ member: string; score: number }> {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) return [];
    if (entry.data.type !== 'zset') throw new WrongTypeError();

    const zsetData = entry.data as { type: 'zset'; value: SkipList; memberScores: Map<string, number> };
    return zsetData.value.getRange(start, stop);
  }

  public zrem(key: string, ...members: string[]): number {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) return 0;
    if (entry.data.type !== 'zset') throw new WrongTypeError();

    const zsetData = entry.data as { type: 'zset'; value: SkipList; memberScores: Map<string, number> };
    let removed = 0;

    for (const mem of members) {
      const score = zsetData.memberScores.get(mem);
      if (score !== undefined) {
        zsetData.value.delete(mem, score);
        zsetData.memberScores.delete(mem);
        removed++;
        this.statsCounters.memoryEstimateBytes -= Buffer.byteLength(mem) + 48;
      }
    }

    if (zsetData.memberScores.size === 0) {
      this.deleteInternal(key, true);
    }

    if (removed > 0) {
      this.logAOF(['ZREM', key, ...members]);
    }
    return removed;
  }

  public zcard(key: string): number {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) return 0;
    if (entry.data.type !== 'zset') throw new WrongTypeError();

    const zsetData = entry.data as { type: 'zset'; value: SkipList; memberScores: Map<string, number> };
    return zsetData.value.size;
  }

  // ==========================================
  // STREAM OPERATIONS
  // ==========================================

  public xadd(
    key: string,
    idSpec: string,
    fields: [string, Buffer][],
    maxlen?: number
  ): string {
    let entry = this.store.get(key);
    let stream: RedisStream;

    if (!entry || this.isExpired(key, entry)) {
      stream = new RedisStream();
      const data: StorageData = { type: 'stream', value: stream };
      this.trackMemoryEstimate(key, data);
      this.memoryGovernor.evictIfNeeded();

      const now = Date.now();
      entry = {
        data,
        expiresAt: null,
        createdAt: now,
        updatedAt: now,
        lastAccessTime: now,
        lfuCounter: LFU_INIT_VAL,
        lfuDecayTime: now,
      };
      this.store.set(key, entry);
    } else {
      if (entry.data.type !== 'stream') throw new WrongTypeError();
      stream = entry.data.value;
      entry.updatedAt = Date.now();
      this.recordAccess(entry);
    }

    const addedId = stream.add(idSpec, fields, maxlen);

    let memDelta = 32;
    for (const [k, v] of fields) {
      memDelta += Buffer.byteLength(k) + v.length + 16;
    }
    this.statsCounters.memoryEstimateBytes += memDelta;

    const aofArgs: (string | Buffer)[] = ['XADD', key, addedId];
    for (const [k, v] of fields) {
      aofArgs.push(k, v);
    }
    this.logAOF(aofArgs);

    return addedId;
  }

  public xrange(key: string, start: string, end: string, count?: number): StreamEntry[] {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) return [];
    if (entry.data.type !== 'stream') throw new WrongTypeError();
    this.recordAccess(entry);
    return entry.data.value.range(start, end, count);
  }

  public xrevrange(key: string, end: string, start: string, count?: number): StreamEntry[] {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) return [];
    if (entry.data.type !== 'stream') throw new WrongTypeError();
    this.recordAccess(entry);
    return entry.data.value.revrange(end, start, count);
  }

  public xlen(key: string): number {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) return 0;
    if (entry.data.type !== 'stream') throw new WrongTypeError();
    return entry.data.value.len();
  }

  public xread(
    streams: { key: string; lastId: string }[],
    count?: number
  ): { key: string; entries: StreamEntry[] }[] {
    const results: { key: string; entries: StreamEntry[] }[] = [];
    for (const { key, lastId } of streams) {
      const entry = this.store.get(key);
      if (!entry || this.isExpired(key, entry)) continue;
      if (entry.data.type !== 'stream') throw new WrongTypeError();
      this.recordAccess(entry);
      const items = entry.data.value.readAfter(lastId, count);
      if (items.length > 0) {
        results.push({ key, entries: items });
      }
    }
    return results;
  }

  // ==========================================
  // CURSOR SCAN OPERATIONS
  // ==========================================

  public scan(
    cursor: number,
    matchPattern?: string,
    count: number = 10
  ): { cursor: number; keys: string[] } {
    const allKeys: string[] = [];
    for (const [k, entry] of this.store.entries()) {
      if (!this.isExpired(k, entry)) {
        allKeys.push(k);
      }
    }

    if (cursor >= allKeys.length) {
      return { cursor: 0, keys: [] };
    }

    const regex = matchPattern ? this.globToRegex(matchPattern) : null;
    const matchedKeys: string[] = [];
    const endIndex = Math.min(cursor + count, allKeys.length);

    for (let i = cursor; i < endIndex; i++) {
      const key = allKeys[i];
      if (!regex || regex.test(key)) {
        matchedKeys.push(key);
      }
    }

    const nextCursor = endIndex >= allKeys.length ? 0 : endIndex;
    return { cursor: nextCursor, keys: matchedKeys };
  }

  public hscan(
    key: string,
    cursor: number,
    matchPattern?: string,
    count: number = 10
  ): { cursor: number; entries: (string | Buffer)[] } {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) {
      return { cursor: 0, entries: [] };
    }
    if (entry.data.type !== 'hash') throw new WrongTypeError();
    this.recordAccess(entry);

    const hash = entry.data.value;
    const fields = Array.from(hash.keys());
    if (cursor >= fields.length) {
      return { cursor: 0, entries: [] };
    }

    const regex = matchPattern ? this.globToRegex(matchPattern) : null;
    const matched: (string | Buffer)[] = [];
    const endIndex = Math.min(cursor + count, fields.length);

    for (let i = cursor; i < endIndex; i++) {
      const f = fields[i];
      if (!regex || regex.test(f)) {
        matched.push(f, hash.get(f)!);
      }
    }

    const nextCursor = endIndex >= fields.length ? 0 : endIndex;
    return { cursor: nextCursor, entries: matched };
  }

  public sscan(
    key: string,
    cursor: number,
    matchPattern?: string,
    count: number = 10
  ): { cursor: number; members: string[] } {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) {
      return { cursor: 0, members: [] };
    }
    if (entry.data.type !== 'set') throw new WrongTypeError();
    this.recordAccess(entry);

    const set = entry.data.value;
    const members = Array.from(set.values());
    if (cursor >= members.length) {
      return { cursor: 0, members: [] };
    }

    const regex = matchPattern ? this.globToRegex(matchPattern) : null;
    const matched: string[] = [];
    const endIndex = Math.min(cursor + count, members.length);

    for (let i = cursor; i < endIndex; i++) {
      const m = members[i];
      if (!regex || regex.test(m)) {
        matched.push(m);
      }
    }

    const nextCursor = endIndex >= members.length ? 0 : endIndex;
    return { cursor: nextCursor, members: matched };
  }

  public zscan(
    key: string,
    cursor: number,
    matchPattern?: string,
    count: number = 10
  ): { cursor: number; entries: (string | string)[] } {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) {
      return { cursor: 0, entries: [] };
    }
    if (entry.data.type !== 'zset') throw new WrongTypeError();
    this.recordAccess(entry);

    const zsetData = entry.data as { type: 'zset'; value: SkipList; memberScores: Map<string, number> };
    const members = Array.from(zsetData.memberScores.keys());
    if (cursor >= members.length) {
      return { cursor: 0, entries: [] };
    }

    const regex = matchPattern ? this.globToRegex(matchPattern) : null;
    const matched: string[] = [];
    const endIndex = Math.min(cursor + count, members.length);

    for (let i = cursor; i < endIndex; i++) {
      const m = members[i];
      if (!regex || regex.test(m)) {
        matched.push(m, zsetData.memberScores.get(m)!.toString());
      }
    }

    const nextCursor = endIndex >= members.length ? 0 : endIndex;
    return { cursor: nextCursor, entries: matched };
  }

  // ==========================================
  // GENERAL KEY MANAGEMENT
  // ==========================================

  public del(...keys: string[]): number {
    let count = 0;
    const deletedKeys: string[] = [];
    for (const key of keys) {
      const entry = this.store.get(key);
      if (entry) {
        if (!this.isExpired(key, entry)) {
          this.deleteInternal(key, true);
          deletedKeys.push(key);
          count++;
        }
      }
    }

    if (deletedKeys.length > 0) {
      this.logAOF(['DEL', ...deletedKeys]);
    }
    return count;
  }

  public exists(...keys: string[]): number {
    let count = 0;
    for (const key of keys) {
      const entry = this.store.get(key);
      if (entry && !this.isExpired(key, entry)) {
        count++;
      }
    }
    return count;
  }

  public ttl(key: string): number {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) return -2;
    if (entry.expiresAt === null) return -1;
    return Math.max(0, Math.floor((entry.expiresAt - Date.now()) / 1000));
  }

  public pttl(key: string): number {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) return -2;
    if (entry.expiresAt === null) return -1;
    return Math.max(0, entry.expiresAt - Date.now());
  }

  public expire(key: string, seconds: number): boolean {
    return this.pexpire(key, seconds * 1000);
  }

  public pexpire(key: string, ms: number): boolean {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) return false;

    const expiresAt = Date.now() + Math.round(ms);
    if (expiresAt <= Date.now()) {
      this.deleteInternal(key, true);
      this.logAOF(['DEL', key]);
      return true;
    }

    entry.expiresAt = expiresAt;
    this.addToExpiringList(key, expiresAt);
    this.logAOF(['PEXPIREAT', key, expiresAt.toString()]);
    return true;
  }

  public persist(key: string): boolean {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) return false;
    if (entry.expiresAt === null) return false;

    entry.expiresAt = null;
    this.expires.delete(key);
    this.removeFromExpiringList(key);
    this.logAOF(['PERSIST', key]);
    return true;
  }

  public keys(pattern: string): string[] {
    const regex = this.globToRegex(pattern);
    const matched: string[] = [];

    for (const [key, entry] of this.store.entries()) {
      if (!this.isExpired(key, entry) && regex.test(key)) {
        matched.push(key);
      }
    }
    return matched;
  }

  public dbsize(): number {
    let count = 0;
    for (const [key, entry] of this.store.entries()) {
      if (!this.isExpired(key, entry)) count++;
    }
    return count;
  }

  public flushall(): void {
    this.store.clear();
    this.expires.clear();
    this.expiringKeyList = [];
    this.expiringKeyIndex.clear();
    this.statsCounters.memoryEstimateBytes = 0;
    this.logAOF(['FLUSHALL']);
  }

  public stats(): StorageStats {
    return {
      totalKeys: this.dbsize(),
      expiringKeys: this.expiringKeyList.length,
      hits: this.statsCounters.hits,
      misses: this.statsCounters.misses,
      expiredPassive: this.statsCounters.expiredPassive,
      expiredActive: this.statsCounters.expiredActive,
      activeCyclesCount: this.statsCounters.activeCyclesCount,
      memoryEstimateBytes: this.statsCounters.memoryEstimateBytes,
      aofEnabled: this.aof !== null,
      aofFileSize: this.aof?.getFileSize() ?? 0,
      aofRewritesCount: this.aof?.getRewritesCount() ?? 0,
    };
  }

  // ==========================================
  // RECOVERY & REWRITE COMPACTION HELPERS
  // ==========================================

  public executeRecoveredCommand(args: Buffer[]): void {
    if (args.length === 0) return;
    const cmd = args[0].toString('utf8').toUpperCase();

    if (cmd === 'SET' && args.length >= 3) {
      const key = args[1].toString('utf8');
      const val = args[2];
      const opts: SetOptions = {};
      for (let i = 3; i < args.length; i++) {
        const flag = args[i].toString('utf8').toUpperCase();
        if (flag === 'PXAT' && i + 1 < args.length) {
          opts.pxat = Number(args[++i].toString('utf8'));
        }
      }
      this.set(key, val, opts);
    } else if (cmd === 'HSET' && args.length >= 4) {
      const key = args[1].toString('utf8');
      const field = args[2].toString('utf8');
      const val = args[3];
      this.hset(key, field, val);
    } else if (cmd === 'LPUSH' && args.length >= 3) {
      const key = args[1].toString('utf8');
      const vals = args.slice(2);
      this.lpush(key, ...vals);
    } else if (cmd === 'RPUSH' && args.length >= 3) {
      const key = args[1].toString('utf8');
      const vals = args.slice(2);
      this.rpush(key, ...vals);
    } else if (cmd === 'SADD' && args.length >= 3) {
      const key = args[1].toString('utf8');
      const members = args.slice(2).map((b) => b.toString('utf8'));
      this.sadd(key, ...members);
    } else if (cmd === 'ZADD' && args.length >= 4) {
      const key = args[1].toString('utf8');
      const score = Number(args[2].toString('utf8'));
      const member = args[3].toString('utf8');
      this.zadd(key, score, member);
    } else if (cmd === 'DEL' && args.length >= 2) {
      const keys = args.slice(1).map((b) => b.toString('utf8'));
      this.del(...keys);
    } else if (cmd === 'PEXPIREAT' && args.length >= 3) {
      const key = args[1].toString('utf8');
      const pxat = Number(args[2].toString('utf8'));
      const entry = this.store.get(key);
      if (entry) {
        entry.expiresAt = pxat;
        this.addToExpiringList(key, pxat);
      }
    } else if (cmd === 'FLUSHALL') {
      this.flushall();
    }
  }

  public dumpAllToAOFCommands(): (string | Buffer)[][] {
    const commands: (string | Buffer)[][] = [];

    for (const [key, entry] of this.store.entries()) {
      if (this.isExpired(key, entry)) continue;

      switch (entry.data.type) {
        case 'string':
          commands.push(['SET', key, entry.data.value]);
          break;

        case 'hash': {
          const map = entry.data.value as Map<string, Buffer>;
          for (const [f, v] of map.entries()) {
            commands.push(['HSET', key, f, v]);
          }
          break;
        }

        case 'list': {
          const list = entry.data.value as Buffer[];
          if (list.length > 0) {
            commands.push(['RPUSH', key, ...list]);
          }
          break;
        }

        case 'set': {
          const set = entry.data.value as Set<string>;
          if (set.size > 0) {
            commands.push(['SADD', key, ...Array.from(set.values())]);
          }
          break;
        }

        case 'zset': {
          const zsetData = entry.data as { type: 'zset'; value: SkipList; memberScores: Map<string, number> };
          const range = zsetData.value.getRange(0, -1);
          for (const item of range) {
            commands.push(['ZADD', key, item.score.toString(), item.member]);
          }
          break;
        }

        case 'stream': {
          const stream = entry.data.value as RedisStream;
          for (const sEntry of stream.getEntries()) {
            const flatFields: (string | Buffer)[] = [];
            for (const [k, v] of sEntry.fields) {
              flatFields.push(k, v);
            }
            commands.push(['XADD', key, sEntry.id, ...flatFields]);
          }
          break;
        }
      }

      if (entry.expiresAt !== null) {
        commands.push(['PEXPIREAT', key, entry.expiresAt.toString()]);
      }
    }

    return commands;
  }

  /**
   * Retrieves all non-expired entries in the storage engine (used for snapshots and AOF).
   */
  public getAllEntries(): Array<{ key: string; entry: StorageEntry }> {
    const entries: Array<{ key: string; entry: StorageEntry }> = [];
    for (const [key, entry] of this.store.entries()) {
      if (this.isExpired(key, entry)) continue;
      entries.push({ key, entry });
    }
    return entries;
  }

  /**
   * Returns a raw StorageEntry if it exists and has not expired.
   */
  public getEntry(key: string): StorageEntry | undefined {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(key, entry)) return undefined;
    return entry;
  }

  /**
   * Directly restores an entry into the storage engine (used by RDB binary loader).
   */
  public restoreEntry(key: string, data: StorageData, expiresAt: number | null = null): void {
    const existing = this.store.get(key);
    if (existing) {
      this.reclaimMemoryEstimate(key, existing.data);
    }

    this.trackMemoryEstimate(key, data);
    const now = Date.now();
    const entry: StorageEntry = {
      data,
      expiresAt,
      createdAt: existing ? existing.createdAt : now,
      updatedAt: now,
    };

    this.store.set(key, entry);

    if (expiresAt !== null) {
      this.addToExpiringList(key, expiresAt);
    } else if (this.expires.has(key)) {
      this.expires.delete(key);
      this.removeFromExpiringList(key);
    }
  }

  // ==========================================
  // MEMORY TRACKING & HELPERS
  // ==========================================

  private calculateDataMemory(data: StorageData): number {
    switch (data.type) {
      case 'string':
        return data.value.length;
      case 'hash': {
        let size = 0;
        for (const [f, v] of (data.value as Map<string, Buffer>).entries()) {
          size += Buffer.byteLength(f) + v.length + 32;
        }
        return size;
      }
      case 'list': {
        let size = 0;
        for (const item of (data.value as Buffer[])) {
          size += item.length + 16;
        }
        return size;
      }
      case 'set': {
        let size = 0;
        for (const mem of (data.value as Set<string>)) {
          size += Buffer.byteLength(mem) + 16;
        }
        return size;
      }
      case 'zset': {
        let size = 0;
        const zdata = data as { memberScores: Map<string, number> };
        for (const mem of zdata.memberScores.keys()) {
          size += Buffer.byteLength(mem) + 48;
        }
        return size;
      }
      case 'stream': {
        let size = 0;
        const stream = data.value as RedisStream;
        for (const sEntry of stream.getEntries()) {
          size += 32;
          for (const [k, v] of sEntry.fields) {
            size += Buffer.byteLength(k) + v.length + 16;
          }
        }
        return size;
      }
      default:
        return 0;
    }
  }

  private trackMemoryEstimate(key: string, data: StorageData): void {
    const keySize = Buffer.byteLength(key) + 64;
    const dataSize = this.calculateDataMemory(data);
    this.statsCounters.memoryEstimateBytes += keySize + dataSize;
  }

  private reclaimMemoryEstimate(key: string, data: StorageData): void {
    const keySize = Buffer.byteLength(key) + 64;
    const dataSize = this.calculateDataMemory(data);
    this.statsCounters.memoryEstimateBytes = Math.max(
      0,
      this.statsCounters.memoryEstimateBytes - (keySize + dataSize)
    );
  }

  private globToRegex(pattern: string): RegExp {
    let regStr = '^';
    let inCharClass = false;

    for (let i = 0; i < pattern.length; i++) {
      const c = pattern[i];
      if (c === '*' && !inCharClass) {
        regStr += '.*';
      } else if (c === '?' && !inCharClass) {
        regStr += '.';
      } else if (c === '[' && !inCharClass) {
        inCharClass = true;
        regStr += '[';
      } else if (c === ']' && inCharClass) {
        inCharClass = false;
        regStr += ']';
      } else if (['.', '+', '(', ')', '{', '}', '^', '$', '|', '\\'].includes(c)) {
        regStr += '\\' + c;
      } else {
        regStr += c;
      }
    }
    regStr += '$';
    return new RegExp(regStr);
  }
}
