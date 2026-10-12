/**
 * Redis-compliant Least Frequently Used (LFU) Morris Logarithmic Counter & Temporal Decay.
 *
 * Implements:
 * - 8-bit Morris logarithmic counter (0 - 255) with logarithmic probability scaling.
 * - Temporal decay based on elapsed minutes and configurable decay interval.
 */

export const LFU_INIT_VAL = 5;
export const LFU_MAX_VALUE = 255;
export const DEFAULT_LFU_DECAY_INTERVAL = 1; // 1 minute
export const DEFAULT_LFU_LOG_FACTOR = 10;

/**
 * Reduces the LFU counter based on elapsed minutes and decay interval.
 *
 * @param counter Current counter value (0 - 255)
 * @param elapsedMinutes Number of elapsed minutes since last decay
 * @param decayInterval Decay interval in minutes (default: 1)
 * @returns Decayed counter value (minimum 0)
 */
export function lfuDecayCounter(
  counter: number,
  elapsedMinutes: number,
  decayInterval: number = DEFAULT_LFU_DECAY_INTERVAL
): number {
  if (decayInterval <= 0 || elapsedMinutes <= 0) {
    return counter;
  }
  const decayPeriods = Math.floor(elapsedMinutes / decayInterval);
  return Math.max(0, counter - decayPeriods);
}

/**
 * Computes decayed counter given timestamp of last decay and current time.
 *
 * @param counter Current counter value
 * @param lastDecayTime Epoch ms of last decay
 * @param now Current epoch ms (default: Date.now())
 * @param decayInterval Decay interval in minutes (default: 1)
 * @returns Object with decayed counter and updated lastDecayTime
 */
export function lfuDecay(
  counter: number,
  lastDecayTime: number,
  now: number = Date.now(),
  decayInterval: number = DEFAULT_LFU_DECAY_INTERVAL
): { counter: number; lastDecayTime: number } {
  if (decayInterval <= 0) {
    return { counter, lastDecayTime };
  }

  const elapsedMs = Math.max(0, now - lastDecayTime);
  const elapsedMinutes = Math.floor(elapsedMs / 60000);
  const decayPeriods = Math.floor(elapsedMinutes / decayInterval);

  if (decayPeriods > 0) {
    const newCounter = Math.max(0, counter - decayPeriods);
    // Advance decay time by the exact decayed periods to preserve fractional remainder
    const newDecayTime = lastDecayTime + decayPeriods * decayInterval * 60000;
    return { counter: newCounter, lastDecayTime: newDecayTime };
  }

  return { counter, lastDecayTime };
}

/**
 * Performs Morris logarithmic increment on the 8-bit LFU counter.
 * Probability of increment is inversely proportional to current counter value:
 * p = 1 / ((counter - LFU_INIT_VAL) * logFactor + 1)
 *
 * @param counter Current counter value (0 - 255)
 * @param logFactor Sensitivity factor (default: 10, Redis standard)
 * @param randomFn Random number generator returning [0, 1) (default: Math.random)
 * @returns New counter value (incremented or unchanged, capped at 255)
 */
export function lfuLogIncr(
  counter: number,
  logFactor: number = DEFAULT_LFU_LOG_FACTOR,
  randomFn: () => number = Math.random
): number {
  if (counter >= LFU_MAX_VALUE) {
    return LFU_MAX_VALUE;
  }

  const baseval = Math.max(0, counter - LFU_INIT_VAL);
  const p = 1.0 / (baseval * logFactor + 1);
  const r = randomFn();

  if (r < p) {
    return Math.min(LFU_MAX_VALUE, counter + 1);
  }

  return counter;
}

/**
 * Alias for lfuLogIncr.
 */
export const lfuIncrement = lfuLogIncr;

/**
 * Atomically decays and increments an entry's LFU metadata on access.
 */
export function updateLFU(
  entry: { lfuCounter: number; lfuDecayTime: number; lastAccessTime?: number },
  now: number = Date.now(),
  decayInterval: number = DEFAULT_LFU_DECAY_INTERVAL,
  logFactor: number = DEFAULT_LFU_LOG_FACTOR,
  randomFn: () => number = Math.random
): void {
  const decayed = lfuDecay(entry.lfuCounter, entry.lfuDecayTime, now, decayInterval);
  entry.lfuCounter = decayed.counter;
  entry.lfuDecayTime = decayed.lastDecayTime;
  entry.lfuCounter = lfuLogIncr(entry.lfuCounter, logFactor, randomFn);
  if (entry.lastAccessTime !== undefined) {
    entry.lastAccessTime = now;
  }
}
