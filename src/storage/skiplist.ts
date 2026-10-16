/**
 * Order-Statistic SkipList implementation for Redis-compatible Sorted Sets (ZSET).
 *
 * Provides O(log N) insertion, deletion, rank lookup, score-bounded range queries,
 * and O(log N + K) rank-sliced range scanning via per-level span counters.
 * Nodes are ordered by score ascending; ties are broken lexicographically by member.
 */

const SKIPLIST_MAXLEVEL = 16;
const SKIPLIST_P = 0.25;

export interface SkipListNode {
  member: string;
  score: number;
  forward: (SkipListNode | null)[];
  span: number[];
}

export interface ScoreBound {
  value: number;
  inclusive: boolean;
}

export interface ScoreRangeOptions {
  offset?: number;
  count?: number;
  reverse?: boolean;
}

/**
 * Parses a Redis score bound expression such as "-inf", "+inf", "100", or "(100".
 */
export function parseScoreBound(raw: string): ScoreBound {
  const trimmed = raw.trim();
  if (trimmed.length === 0) {
    throw new Error('ERR min or max is not a float');
  }

  let inclusive = true;
  let token = trimmed;
  if (token.startsWith('(')) {
    inclusive = false;
    token = token.slice(1);
    if (token.length === 0) {
      throw new Error('ERR min or max is not a float');
    }
  }

  const lower = token.toLowerCase();
  if (lower === '-inf') {
    return { value: -Infinity, inclusive };
  }
  if (lower === '+inf' || lower === 'inf') {
    return { value: Infinity, inclusive };
  }

  const num = Number(token);
  if (Number.isNaN(num)) {
    throw new Error('ERR min or max is not a float');
  }
  return { value: num, inclusive };
}

export class SkipList {
  private header: SkipListNode;
  private level: number;
  private length: number;

  constructor() {
    this.level = 1;
    this.length = 0;
    this.header = {
      member: '',
      score: -Infinity,
      forward: new Array(SKIPLIST_MAXLEVEL).fill(null),
      span: new Array(SKIPLIST_MAXLEVEL).fill(0),
    };
  }

  public get size(): number {
    return this.length;
  }

  /**
   * Generates a random level for a new node with geometric distribution.
   */
  private randomLevel(): number {
    let lvl = 1;
    while (Math.random() < SKIPLIST_P && lvl < SKIPLIST_MAXLEVEL) {
      lvl++;
    }
    return lvl;
  }

  private static scoreGteMin(score: number, min: ScoreBound): boolean {
    return min.inclusive ? score >= min.value : score > min.value;
  }

  private static scoreLteMax(score: number, max: ScoreBound): boolean {
    return max.inclusive ? score <= max.value : score < max.value;
  }

  /**
   * Inserts a member with given score into the SkipList.
   * Returns true if newly inserted, or false if identical (member, score) already existed.
   */
  public insert(member: string, score: number): boolean {
    const update: SkipListNode[] = new Array(SKIPLIST_MAXLEVEL);
    const rank: number[] = new Array(SKIPLIST_MAXLEVEL).fill(0);
    let curr: SkipListNode = this.header;

    // Search insertion path across levels while accumulating rank spans
    for (let i = this.level - 1; i >= 0; i--) {
      rank[i] = i === this.level - 1 ? 0 : rank[i + 1];
      while (
        curr.forward[i] !== null &&
        (curr.forward[i]!.score < score ||
          (curr.forward[i]!.score === score && curr.forward[i]!.member < member))
      ) {
        rank[i] += curr.span[i];
        curr = curr.forward[i]!;
      }
      update[i] = curr;
    }

    // Check if member already exists at exact location
    const next = curr.forward[0];
    if (next !== null && next.score === score && next.member === member) {
      return false;
    }

    const lvl = this.randomLevel();
    if (lvl > this.level) {
      for (let i = this.level; i < lvl; i++) {
        rank[i] = 0;
        update[i] = this.header;
        update[i].span[i] = this.length;
      }
      this.level = lvl;
    }

    const newNode: SkipListNode = {
      member,
      score,
      forward: new Array(lvl).fill(null),
      span: new Array(lvl).fill(0),
    };

    for (let i = 0; i < lvl; i++) {
      newNode.forward[i] = update[i].forward[i];
      update[i].forward[i] = newNode;

      newNode.span[i] = update[i].span[i] - (rank[0] - rank[i]);
      update[i].span[i] = rank[0] - rank[i] + 1;
    }

    for (let i = lvl; i < this.level; i++) {
      update[i].span[i]++;
    }

    this.length++;
    return true;
  }

  /**
   * Deletes a member with the specified score.
   */
  public delete(member: string, score: number): boolean {
    const update: SkipListNode[] = new Array(SKIPLIST_MAXLEVEL);
    let curr: SkipListNode = this.header;

    for (let i = this.level - 1; i >= 0; i--) {
      while (
        curr.forward[i] !== null &&
        (curr.forward[i]!.score < score ||
          (curr.forward[i]!.score === score && curr.forward[i]!.member < member))
      ) {
        curr = curr.forward[i]!;
      }
      update[i] = curr;
    }

    const target = curr.forward[0];
    if (target !== null && target.score === score && target.member === member) {
      for (let i = 0; i < this.level; i++) {
        if (update[i].forward[i] === target) {
          update[i].span[i] += target.span[i] - 1;
          update[i].forward[i] = target.forward[i];
        } else {
          update[i].span[i] -= 1;
        }
      }

      // Shrink levels if top levels became empty
      while (this.level > 1 && this.header.forward[this.level - 1] === null) {
        this.header.span[this.level - 1] = 0;
        this.level--;
      }

      this.length--;
      return true;
    }

    return false;
  }

  /**
   * Computes 0-based ascending rank of (member, score) in O(log N) time, or null if absent.
   */
  public getRank(member: string, score: number): number | null {
    let traversed = 0;
    let curr: SkipListNode = this.header;

    for (let i = this.level - 1; i >= 0; i--) {
      while (
        curr.forward[i] !== null &&
        (curr.forward[i]!.score < score ||
          (curr.forward[i]!.score === score && curr.forward[i]!.member <= member))
      ) {
        traversed += curr.span[i];
        curr = curr.forward[i]!;
      }
      if (curr !== this.header && curr.score === score && curr.member === member) {
        return traversed - 1;
      }
    }

    return null;
  }

  /**
   * Locates the node at 0-based rank in O(log N) using level spans.
   */
  private getNodeByRank(zeroBasedRank: number): SkipListNode | null {
    if (zeroBasedRank < 0 || zeroBasedRank >= this.length) return null;
    const targetTraversed = zeroBasedRank + 1;
    let traversed = 0;
    let curr: SkipListNode = this.header;

    for (let i = this.level - 1; i >= 0; i--) {
      while (curr.forward[i] !== null && traversed + curr.span[i] <= targetTraversed) {
        traversed += curr.span[i];
        curr = curr.forward[i]!;
      }
      if (traversed === targetTraversed) {
        return curr;
      }
    }
    return null;
  }

  /**
   * Retrieves elements in zero-indexed rank range [start, stop] in O(log N + K).
   * Supports negative indices (e.g. -1 for last element).
   */
  public getRange(
    start: number,
    stop: number
  ): Array<{ member: string; score: number }> {
    if (this.length === 0) return [];

    let startIdx = start < 0 ? this.length + start : start;
    let stopIdx = stop < 0 ? this.length + stop : stop;

    if (startIdx < 0) startIdx = 0;
    if (stopIdx >= this.length) stopIdx = this.length - 1;
    if (startIdx > stopIdx) return [];

    const result: Array<{ member: string; score: number }> = [];
    let curr: SkipListNode | null = this.getNodeByRank(startIdx);
    let index = startIdx;

    while (curr !== null && index <= stopIdx) {
      result.push({ member: curr.member, score: curr.score });
      curr = curr.forward[0];
      index++;
    }

    return result;
  }

  /**
   * Locates the first node whose score satisfies the lower ScoreBound in O(log N).
   */
  private firstInRange(min: ScoreBound, max: ScoreBound): SkipListNode | null {
    if (this.length === 0) return null;
    if (min.value > max.value) return null;
    if (min.value === max.value && (!min.inclusive || !max.inclusive)) return null;

    let curr: SkipListNode = this.header;
    for (let i = this.level - 1; i >= 0; i--) {
      while (
        curr.forward[i] !== null &&
        !SkipList.scoreGteMin(curr.forward[i]!.score, min)
      ) {
        curr = curr.forward[i]!;
      }
    }

    const candidate = curr.forward[0];
    if (candidate === null || !SkipList.scoreLteMax(candidate.score, max)) {
      return null;
    }
    return candidate;
  }

  /**
   * Retrieves elements whose scores fall within [min, max] (inclusive/exclusive) in O(log N + K).
   */
  public getRangeByScore(
    min: ScoreBound,
    max: ScoreBound,
    options: ScoreRangeOptions = {}
  ): Array<{ member: string; score: number }> {
    let curr = this.firstInRange(min, max);
    if (curr === null) return [];

    const offset = options.offset ?? 0;
    const count = options.count;
    if (offset < 0 || count === 0) return [];

    if (!options.reverse) {
      const result: Array<{ member: string; score: number }> = [];
      let skipped = 0;

      while (curr !== null && SkipList.scoreLteMax(curr.score, max)) {
        if (skipped < offset) {
          skipped++;
        } else {
          result.push({ member: curr.member, score: curr.score });
          if (count !== undefined && count > 0 && result.length >= count) {
            break;
          }
        }
        curr = curr.forward[0];
      }
      return result;
    }

    // Reverse order collection
    const matched: Array<{ member: string; score: number }> = [];
    while (curr !== null && SkipList.scoreLteMax(curr.score, max)) {
      matched.push({ member: curr.member, score: curr.score });
      curr = curr.forward[0];
    }
    matched.reverse();

    if (offset >= matched.length) return [];
    if (count === undefined || count < 0) {
      return matched.slice(offset);
    }
    return matched.slice(offset, offset + count);
  }

  /**
   * Counts elements whose scores fall within [min, max] without intermediate array allocation.
   */
  public countByScore(min: ScoreBound, max: ScoreBound): number {
    let curr = this.firstInRange(min, max);
    let count = 0;
    while (curr !== null && SkipList.scoreLteMax(curr.score, max)) {
      count++;
      curr = curr.forward[0];
    }
    return count;
  }
}
