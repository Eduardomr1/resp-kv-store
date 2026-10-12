/**
 * Redis Slowlog Telemetry Engine
 * Records command execution times exceeding a microsecond threshold in an in-memory ring buffer.
 */

export interface SlowlogEntry {
  id: number;
  timestamp: number;
  durationMicroseconds: number;
  args: (string | Buffer)[];
  clientAddress?: string;
  clientName?: string;
}

export class SlowlogManager {
  private entries: SlowlogEntry[] = [];
  private idCounter: number = 0;
  private logSlowerThanUs: number;
  private maxLen: number;

  constructor(options: { logSlowerThanUs?: number; maxLen?: number } = {}) {
    this.logSlowerThanUs = options.logSlowerThanUs ?? 10000; // default: 10ms
    this.maxLen = options.maxLen ?? 128;
  }

  public getLogSlowerThan(): number {
    return this.logSlowerThanUs;
  }

  public setLogSlowerThan(us: number): void {
    this.logSlowerThanUs = us;
  }

  public getMaxLen(): number {
    return this.maxLen;
  }

  public setMaxLen(len: number): void {
    this.maxLen = Math.max(0, len);
    if (this.entries.length > this.maxLen) {
      this.entries = this.entries.slice(0, this.maxLen);
    }
  }

  /**
   * Evaluates command execution duration and conditionally logs if it breaches threshold.
   */
  public record(
    durationMicroseconds: number,
    args: (string | Buffer)[],
    clientAddress?: string,
    clientName?: string
  ): void {
    if (this.logSlowerThanUs < 0) return; // Logging disabled
    if (durationMicroseconds < this.logSlowerThanUs) return; // Faster than threshold

    const entry: SlowlogEntry = {
      id: ++this.idCounter,
      timestamp: Math.floor(Date.now() / 1000),
      durationMicroseconds: Math.round(durationMicroseconds),
      args,
      clientAddress: clientAddress ?? '',
      clientName: clientName ?? '',
    };

    // Prepend (newest first as in Redis)
    this.entries.unshift(entry);

    if (this.entries.length > this.maxLen) {
      this.entries.pop();
    }
  }

  public get(count?: number): SlowlogEntry[] {
    if (count === undefined || count <= 0) {
      return [...this.entries];
    }
    return this.entries.slice(0, count);
  }

  public len(): number {
    return this.entries.length;
  }

  public reset(): void {
    this.entries = [];
  }
}
