import test from 'node:test';
import assert from 'node:assert/strict';
import { appendFile, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventJournal } from '../src/shared/journal.js';

async function withDir(fn) {
  const dir = await mkdtemp(join(tmpdir(), 'rc-journal-'));
  try { await fn(dir); } finally { await rm(dir, { recursive: true, force: true }); }
}

test('journal assigns dense sequences and replays in order', async () => {
  await withDir(async (dir) => {
    const file = join(dir, 'j.jsonl');
    const journal = await new EventJournal(file).init();
    const a = await journal.append('one', { n: 1 });
    const b = await journal.append('two', { n: 2 });
    assert.equal(a.seq, 1);
    assert.equal(b.seq, 2);

    const reopened = await new EventJournal(file).init();
    assert.equal(reopened.head, 2);
    const seen = [];
    reopened.replay((e) => seen.push(e.type));
    assert.deepEqual(seen, ['one', 'two']);
    await reopened.close();
  });
});

test('a torn trailing write is discarded and never becomes state', async () => {
  await withDir(async (dir) => {
    const file = join(dir, 'j.jsonl');
    const journal = await new EventJournal(file).init();
    await journal.append('good', { n: 1 });
    await journal.append('good', { n: 2 });
    await journal.close();

    // Simulate a crash mid-write: a partial JSON record with no newline.
    await appendFile(file, '{"seq":3,"at":"2026-01-01T00:00:00Z","type":"release.applied","releaseId":"rel-');

    const recovered = await new EventJournal(file).init();
    assert.equal(recovered.tailBroken, true);
    assert.equal(recovered.head, 2);
    const types = [];
    recovered.replay((e) => types.push(e.type));
    assert.deepEqual(types, ['good', 'good']);

    const third = await recovered.append('good', { n: 3 });
    assert.equal(third.seq, 3);

    const rechecked = await new EventJournal(file).init();
    assert.equal(rechecked.head, 3);
    assert.equal(rechecked.tailBroken, false);
    await rechecked.close();
  });
});

test('a corrupted middle record refuses to load instead of guessing', async () => {
  await withDir(async (dir) => {
    const file = join(dir, 'j.jsonl');
    await writeFile(file, [
      '{"seq":1,"at":"x","type":"a"}',
      '{"seq":2,"at":"x","type":"b"}',
      '{"seq":4,"at":"x","type":"c"}',
      '',
    ].join('\n'));
    await assert.rejects(() => new EventJournal(file).init(), /expected seq 3, found 4/);
  });
});

test('ack signature is stable and distinguishes dispatch attempts', async () => {
  const { ackSignature } = await import('../src/shared/protocol.js');
  const base = {
    consumer: 'alpha', releaseId: 'rel-1', version: 'v1',
    appliedSeq: 1, dispatchSeq: 2, idempotencyKey: 'k',
  };
  assert.equal(ackSignature(base), ackSignature({ ...base }));
  assert.notEqual(ackSignature(base), ackSignature({ ...base, dispatchSeq: 3 }));
});
