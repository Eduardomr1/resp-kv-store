/**
 * Top-K Heavy Hitters (HeavyKeeper) Probabilistic Data Structure
 *
 * Implements RedisBloom-compatible streaming heavy-hitter tracking using the
 * HeavyKeeper algorithm paired with a bounded Min-Heap of size k.
 *
 * Algorithmic properties:
 * - d x w matrix of (32-bit fingerprint, 32-bit counter) cells backed by TypedArrays
 * - Count-with-Exponential-Decay: colliding keys decay existing counters with
 *   probability `decay^count`, protecting true heavy hitters ("elephants") while
 *   rapidly evicting low-frequency noise ("mice")
 * - Bounded Min-Heap of capacity `k` with O(1) membership index (`Map<string, number>`)
 *   for O(log k) heap maintenance and O(1) TOPK.QUERY checks
 */

export interface TopKOptions {
  k: number;
  width?: number;
  depth?: number;
  decay?: number;
}

export interface TopKInfo {
  k: number;
  width: number;
  depth: number;
  decay: number;
}

export interface TopKListEntry {
  item: string;
  count: number;
}

interface HeapNode {
  item: string;
  count: number;
}

function hashFNV1a32(str: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

function hashMurmurMix32(str: string): number {
  let h = 0x9e3779b9;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 0x85ebca6b);
    h ^= h >>> 13;
  }
  h = Math.imul(h, 0xc2b2ae35);
  h ^= h >>> 16;
  return (h >>> 0) | 1; // Ensure non-zero odd stride for row hashing
}

function computeFingerprint32(str: string): number {
  let h = 0xcbf29ce4;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
    h ^= h >>> 11;
  }
  const fp = h >>> 0;
  // Reserve 0 exclusively for empty HeavyKeeper buckets
  return fp === 0 ? 1 : fp;
}

export class TopK {
  public readonly k: number;
  public readonly width: number;
  public readonly depth: number;
  public readonly decay: number;

  private readonly fingerprints: Uint32Array;
  private readonly counts: Uint32Array;
  private readonly heap: HeapNode[] = [];
  private readonly heapIndex: Map<string, number> = new Map();

  constructor(k: number, width = 8, depth = 7, decay = 0.9) {
    if (!Number.isInteger(k) || k <= 0) {
      throw new Error('ERR k must be a positive integer');
    }
    if (!Number.isInteger(width) || width <= 0) {
      throw new Error('ERR width must be a positive integer');
    }
    if (!Number.isInteger(depth) || depth <= 0) {
      throw new Error('ERR depth must be a positive integer');
    }
    if (!Number.isFinite(decay) || decay <= 0 || decay > 1) {
      throw new Error('ERR decay must be in (0, 1]');
    }

    this.k = k;
    this.width = width;
    this.depth = depth;
    this.decay = decay;

    const totalCells = depth * width;
    this.fingerprints = new Uint32Array(totalCells);
    this.counts = new Uint32Array(totalCells);
  }

  public get byteSize(): number {
    let heapBytes = 64;
    for (const entry of this.heap) {
      heapBytes += Buffer.byteLength(entry.item) + 48;
    }
    return this.fingerprints.byteLength + this.counts.byteLength + heapBytes;
  }

  /**
   * Adds one or more items (increment = 1 each).
   * Returns an array aligned with `items` containing the evicted item name
   * if an entry was displaced from the Top-K heap, or null otherwise.
   */
  public add(items: string[]): (string | null)[] {
    const evicted: (string | null)[] = [];
    for (const item of items) {
      evicted.push(this.updateItem(item, 1));
    }
    return evicted;
  }

  /**
   * Increments items by arbitrary positive integer amounts.
   * Returns evicted item names (or null) aligned with `entries`.
   */
  public incrBy(entries: Array<{ item: string; increment: number }>): (string | null)[] {
    for (const { increment } of entries) {
      if (!Number.isInteger(increment) || increment <= 0) {
        throw new Error('ERR increment must be a positive integer');
      }
    }

    const evicted: (string | null)[] = [];
    for (const { item, increment } of entries) {
      evicted.push(this.updateItem(item, increment));
    }
    return evicted;
  }

  /**
   * Checks whether each item is currently tracked in the Top-K Min-Heap.
   */
  public query(items: string[]): boolean[] {
    return items.map((item) => this.heapIndex.has(item));
  }

  /**
   * Returns estimated frequency count for each item.
   * Uses the tracked heap count if present, otherwise inspects matching HeavyKeeper cells.
   */
  public count(items: string[]): number[] {
    return items.map((item) => {
      const idx = this.heapIndex.get(item);
      if (idx !== undefined) {
        return this.heap[idx]!.count;
      }

      const fp = computeFingerprint32(item);
      const h1 = hashFNV1a32(item);
      const h2 = hashMurmurMix32(item);
      let maxCount = 0;

      for (let row = 0; row < this.depth; row++) {
        const col = ((h1 + Math.imul(row, h2)) >>> 0) % this.width;
        const cellIdx = row * this.width + col;
        if (this.fingerprints[cellIdx] === fp && this.counts[cellIdx]! > maxCount) {
          maxCount = this.counts[cellIdx]!;
        }
      }

      return maxCount;
    });
  }

  /**
   * Returns the current Top-K heavy hitters sorted in descending frequency order.
   */
  public list(withCount = false): Array<string | TopKListEntry> {
    const sorted = [...this.heap].sort((a, b) => {
      if (b.count !== a.count) return b.count - a.count;
      return a.item.localeCompare(b.item);
    });

    if (withCount) {
      return sorted.map((entry) => ({ item: entry.item, count: entry.count }));
    }
    return sorted.map((entry) => entry.item);
  }

  public info(): TopKInfo {
    return {
      k: this.k,
      width: this.width,
      depth: this.depth,
      decay: this.decay,
    };
  }

  private updateItem(item: string, increment: number): string | null {
    const fp = computeFingerprint32(item);
    const h1 = hashFNV1a32(item);
    const h2 = hashMurmurMix32(item);

    let maxCount = 0;

    for (let row = 0; row < this.depth; row++) {
      const col = ((h1 + Math.imul(row, h2)) >>> 0) % this.width;
      const cellIdx = row * this.width + col;
      const cellCount = this.counts[cellIdx]!;
      const cellFp = this.fingerprints[cellIdx]!;

      if (cellCount === 0) {
        this.fingerprints[cellIdx] = fp;
        this.counts[cellIdx] = increment;
        if (increment > maxCount) maxCount = increment;
      } else if (cellFp === fp) {
        const updated = cellCount + increment;
        this.counts[cellIdx] = updated;
        if (updated > maxCount) maxCount = updated;
      } else {
        // Exponential decay on fingerprint collision
        let remaining = increment;
        let current = cellCount;

        // Guard loop bound for very large increments when decay probability is negligible
        while (remaining > 0) {
          const decayProb = Math.pow(this.decay, current);
          if (remaining * decayProb < 1e-9) {
            break;
          }

          if (Math.random() < decayProb) {
            current--;
            remaining--;
            if (current === 0) {
              this.fingerprints[cellIdx] = fp;
              const takeoverCount = remaining + 1;
              this.counts[cellIdx] = takeoverCount;
              if (takeoverCount > maxCount) {
                maxCount = takeoverCount;
              }
              remaining = 0;
              break;
            }
          } else {
            remaining--;
          }
        }

        if (current > 0) {
          this.counts[cellIdx] = current;
        }
      }
    }

    return this.updateHeap(item, maxCount);
  }

  private updateHeap(item: string, estimatedCount: number): string | null {
    const existingIdx = this.heapIndex.get(item);
    if (existingIdx !== undefined) {
      const node = this.heap[existingIdx]!;
      if (estimatedCount > node.count) {
        node.count = estimatedCount;
        this.siftDown(existingIdx);
      }
      return null;
    }

    if (this.heap.length < this.k) {
      const idx = this.heap.length;
      this.heap.push({ item, count: estimatedCount });
      this.heapIndex.set(item, idx);
      this.siftUp(idx);
      return null;
    }

    const minNode = this.heap[0]!;
    if (estimatedCount > minNode.count) {
      const evicted = minNode.item;
      this.heapIndex.delete(evicted);
      this.heap[0] = { item, count: estimatedCount };
      this.heapIndex.set(item, 0);
      this.siftDown(0);
      return evicted;
    }

    return null;
  }

  private siftUp(index: number): void {
    let idx = index;
    while (idx > 0) {
      const parentIdx = (idx - 1) >> 1;
      if (this.heap[idx]!.count < this.heap[parentIdx]!.count) {
        this.swap(idx, parentIdx);
        idx = parentIdx;
      } else {
        break;
      }
    }
  }

  private siftDown(index: number): void {
    let idx = index;
    const len = this.heap.length;
    while (true) {
      const left = idx * 2 + 1;
      const right = idx * 2 + 2;
      let smallest = idx;

      if (left < len && this.heap[left]!.count < this.heap[smallest]!.count) {
        smallest = left;
      }
      if (right < len && this.heap[right]!.count < this.heap[smallest]!.count) {
        smallest = right;
      }
      if (smallest === idx) {
        break;
      }
      this.swap(idx, smallest);
      idx = smallest;
    }
  }

  private swap(i: number, j: number): void {
    const a = this.heap[i]!;
    const b = this.heap[j]!;
    this.heap[i] = b;
    this.heap[j] = a;
    this.heapIndex.set(b.item, i);
    this.heapIndex.set(a.item, j);
  }
}
