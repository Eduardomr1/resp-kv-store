import fs from 'node:fs';
import path from 'node:path';
import { RESPSerializer } from '../resp/serializer.ts';
import { RESPParser } from '../resp/parser.ts';
import type { StorageEngine } from './engine.ts';
import type { FsyncPolicy } from './types.ts';

export class AOFManager {
  private filePath: string;
  private policy: FsyncPolicy;
  private fd: number | null = null;
  private syncTimer: NodeJS.Timeout | null = null;
  private isRewriting: boolean = false;
  private rewritesCount: number = 0;

  constructor(filePath: string = './appendonly.aof', policy: FsyncPolicy = 'everysec') {
    this.filePath = path.resolve(filePath);
    this.policy = policy;
  }

  public open(): void {
    if (this.fd !== null) return;

    // Open file in append/read mode, creating if it doesn't exist
    this.fd = fs.openSync(this.filePath, 'a+');

    if (this.policy === 'everysec') {
      this.syncTimer = setInterval(() => {
        if (this.fd !== null) {
          try {
            fs.fdatasyncSync(this.fd);
          } catch (err) {
            console.error('[AOF sync error]', err);
          }
        }
      }, 1000);
      this.syncTimer.unref();
    }
  }

  public close(): void {
    if (this.syncTimer !== null) {
      clearInterval(this.syncTimer);
      this.syncTimer = null;
    }

    if (this.fd !== null) {
      try {
        fs.fdatasyncSync(this.fd);
        fs.closeSync(this.fd);
      } catch {}
      this.fd = null;
    }
  }

  /**
   * Appends an encoded RESP command to the AOF log.
   */
  public append(commandArgs: (string | Buffer)[]): void {
    if (this.fd === null) return;

    const payload = RESPSerializer.encodeArray(commandArgs);
    fs.writeSync(this.fd, payload);

    if (this.policy === 'always') {
      fs.fdatasyncSync(this.fd);
    }
  }

  public getFileSize(): number {
    try {
      if (fs.existsSync(this.filePath)) {
        return fs.statSync(this.filePath).size;
      }
    } catch {}
    return 0;
  }

  public getRewritesCount(): number {
    return this.rewritesCount;
  }

  /**
   * Replays commands from AOF file into the storage engine upon boot.
   */
  public recover(engine: StorageEngine): void {
    if (!fs.existsSync(this.filePath)) return;

    const stats = fs.statSync(this.filePath);
    if (stats.size === 0) return;

    const fd = fs.openSync(this.filePath, 'r');
    const parser = new RESPParser(128 * 1024);
    const chunk = Buffer.allocUnsafe(64 * 1024);
    let bytesRead = 0;
    let pos = 0;

    try {
      while ((bytesRead = fs.readSync(fd, chunk, 0, chunk.length, pos)) > 0) {
        pos += bytesRead;
        parser.feed(chunk.subarray(0, bytesRead));
        const commands = parser.parse();

        for (const cmd of commands) {
          if (cmd.type === 'array') {
            const rawArgs = cmd.value.map((v: any) =>
              v.type === 'bulk_string' ? v.value : Buffer.from(v.value?.toString() ?? '')
            );
            engine.executeRecoveredCommand(rawArgs);
          }
        }
      }
    } finally {
      fs.closeSync(fd);
    }
  }

  /**
   * Compaction / AOF Rewrite:
   * Generates a minimal snapshot of current in-memory state and atomically replaces the log file.
   */
  public async rewrite(engine: StorageEngine): Promise<void> {
    if (this.isRewriting) return;
    this.isRewriting = true;

    const tmpPath = `${this.filePath}.tmp.${Date.now()}`;
    const tmpFd = fs.openSync(tmpPath, 'w');

    try {
      const dumpCommands = engine.dumpAllToAOFCommands();

      for (const cmdArgs of dumpCommands) {
        const payload = RESPSerializer.encodeArray(cmdArgs);
        fs.writeSync(tmpFd, payload);
      }

      fs.fdatasyncSync(tmpFd);
      fs.closeSync(tmpFd);

      // Re-point log to compacted file atomically
      if (this.fd !== null) {
        fs.closeSync(this.fd);
        this.fd = null;
      }

      fs.renameSync(tmpPath, this.filePath);
      this.fd = fs.openSync(this.filePath, 'a+');
      this.rewritesCount++;
    } catch (err) {
      if (fs.existsSync(tmpPath)) {
        try { fs.unlinkSync(tmpPath); } catch {}
      }
      throw err;
    } finally {
      this.isRewriting = false;
    }
  }
}
