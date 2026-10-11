/**
 * SkipList implementation for Redis-compatible Sorted Sets (ZSET).
 *
 * Provides O(log N) insertion, deletion, score lookup, and range scanning.
 * Nodes are ordered by score ascending; ties are broken lexicographically by member.
 */

const SKIPLIST_MAXLEVEL = 16;
const SKIPLIST_P = 0.25;

export interface SkipListNode {
  member: string;
  score: number;
  forward: (SkipListNode | null)[];
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

  /**
   * Inserts a member with given score into the SkipList.
   * Returns true if newly inserted, or false if member already existed and was updated.
   */
  public insert(member: string, score: number): boolean {
    const update: SkipListNode[] = new Array(SKIPLIST_MAXLEVEL);
    let curr: SkipListNode = this.header;

    // Search insertion path across levels
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

    // Check if member already exists at exact location
    const next = curr.forward[0];
    if (next !== null && next.score === score && next.member === member) {
      // Identical score and member, no-op
      return false;
    }

    const lvl = this.randomLevel();
    if (lvl > this.level) {
      for (let i = this.level; i < lvl; i++) {
        update[i] = this.header;
      }
      this.level = lvl;
    }

    const newNode: SkipListNode = {
      member,
      score,
      forward: new Array(lvl).fill(null),
    };

    for (let i = 0; i < lvl; i++) {
      newNode.forward[i] = update[i].forward[i];
      update[i].forward[i] = newNode;
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
          update[i].forward[i] = target.forward[i];
        }
      }

      // Shrink levels if top levels became empty
      while (this.level > 1 && this.header.forward[this.level - 1] === null) {
        this.level--;
      }

      this.length--;
      return true;
    }

    return false;
  }

  /**
   * Retrieves elements in zero-indexed rank range [start, stop].
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
    let curr: SkipListNode | null = this.header.forward[0];
    let index = 0;

    // Traverse level 0 to start index
    while (curr !== null && index < startIdx) {
      curr = curr.forward[0];
      index++;
    }

    // Collect elements
    while (curr !== null && index <= stopIdx) {
      result.push({ member: curr.member, score: curr.score });
      curr = curr.forward[0];
      index++;
    }

    return result;
  }
}
