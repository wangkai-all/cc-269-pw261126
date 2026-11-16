import { appendFile, mkdir, open, readFile, truncate, writeFile } from 'node:fs/promises';
import { dirname } from 'node:path';

/**
 * Append-only JSONL event journal.
 *
 * Guarantees used by the release protocol:
 * - every record is written as one line and fdatasync'd before append() resolves;
 * - sequence numbers are dense and strictly increasing;
 * - a torn trailing line (process died mid-write) is discarded on recovery;
 * - replay is deterministic, so in-memory state can always be rebuilt from disk.
 */
export class EventJournal {
  constructor(file) {
    this.file = file;
    this.events = [];
    this.lastSeq = 0;
    this.tailBroken = false;
    this.writeChain = Promise.resolve();
    this.initialized = false;
  }

  async init() {
    await mkdir(dirname(this.file), { recursive: true });
    let text = '';
    try {
      text = await readFile(this.file, 'utf8');
    } catch (err) {
      if (err.code !== 'ENOENT') throw err;
      await writeFile(this.file, '');
    }

    const lines = text.split('\n');
    let complete = '';
    for (let i = 0; i < lines.length; i += 1) {
      const line = lines[i];
      const isLast = i === lines.length - 1;
      if (isLast && line === '') break;
      if (isLast && !text.endsWith('\n')) {
        // A record without a trailing newline can only be a torn write.
        this.tailBroken = true;
        break;
      }
      if (!line.trim()) continue;
      let event;
      try {
        event = JSON.parse(line);
      } catch {
        this.tailBroken = true;
        break;
      }
      const expected = this.lastSeq + 1;
      if (event.seq !== expected) {
        throw new Error(
          `Journal ${this.file} is corrupt: expected seq ${expected}, found ${event.seq}`,
        );
      }
      this.events.push(event);
      this.lastSeq = event.seq;
      complete += `${line}\n`;
    }

    if (this.tailBroken) {
      // Cut the torn suffix so future appends start on a clean line.
      const handle = await open(this.file, 'r+');
      try {
        await truncate(this.file, Buffer.byteLength(complete));
        await handle.sync();
      } finally {
        await handle.close();
      }
      process.stderr.write(
        `[journal] discarded a torn tail from ${this.file}; recovered ${this.lastSeq} events\n`,
      );
    }
    this.initialized = true;
    return this;
  }

  get head() {
    return this.lastSeq;
  }

  /**
   * Append one event. The returned promise settles after fdatasync.
   * Writes and the optional onPersist callback are serialized, so in-memory
   * state is updated in exactly the same order as the journal.
   */
  append(type, payload = {}, onPersist = null) {
    const task = this.writeChain.then(async () => {
      if (!this.initialized) throw new Error('journal not initialized');
      const event = {
        seq: this.lastSeq + 1,
        at: new Date().toISOString(),
        type,
        ...payload,
      };
      const line = `${JSON.stringify(event)}\n`;
      const handle = await open(this.file, 'a');
      try {
        await handle.write(line);
        await handle.sync();
      } finally {
        await handle.close();
      }
      this.events.push(event);
      this.lastSeq = event.seq;
      if (onPersist) onPersist(event);
      return event;
    });
    this.writeChain = task.catch(() => {});
    return task;
  }

  /** Replay all recorded events through a reducer. */
  replay(reducer) {
    for (const event of this.events) reducer(event);
  }

  async close() {
    await this.writeChain;
  }
}
