import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { startConsumer } from '../src/consumer/agent.js';
import { buildRelease } from '../src/shared/schema.js';

function delivery(payload, overrides = {}) {
  return {
    protocol: 1,
    releaseId: 'rel-test-1',
    ordinal: 1,
    version: payload.manifest.version,
    schema: payload.manifest.schema,
    cohort: payload.manifest.cohort,
    payloadHash: payload.payloadHash,
    payload,
    dispatchSeq: 1,
    idempotencyKey: 'key-1',
    ...overrides,
  };
}

async function withConsumer(fn, { supportedSchema = 2 } = {}) {
  const dir = await mkdtemp(join(tmpdir(), 'rc-consumer-'));
  const consumer = await startConsumer({
    name: 'alpha',
    port: 0,
    dataDir: dir,
    publisherUrl: 'http://127.0.0.1:9', // unreachable on purpose
    supportedSchema,
    autoRetryAcks: false,
  });
  try {
    await fn(consumer);
  } finally {
    await consumer.close();
    await rm(dir, { recursive: true, force: true });
  }
}

test('consumer applies a valid release exactly once and journals before ack', async () => {
  await withConsumer(async (consumer) => {
    const payload = buildRelease({ version: 'v1', schema: 2, cohort: 'c1', rowCount: 6 });
    const first = await consumer.store.receiveDelivery(delivery(payload));
    assert.equal(first.body.applied, true);
    assert.equal(first.body.appliedSeq, 1);
    assert.equal(consumer.store.state.currentVersion, 'v1');

    const duplicate = await consumer.store.receiveDelivery(delivery(payload, {
      dispatchSeq: 2, idempotencyKey: 'key-2',
    }));
    assert.equal(duplicate.body.applied, false);
    assert.equal(duplicate.body.duplicate, true);
    assert.equal(consumer.store.state.currentReleaseId, 'rel-test-1');
    assert.equal(consumer.store.state.counters.applied, 1);
    assert.equal(consumer.store.state.counters.duplicateDeliveries, 1);

    // The durable outbox holds exactly one acknowledgement.
    assert.equal(consumer.store.state.outbox.size, 1);
    const ad = [...consumer.store.state.outbox.values()][0];
    assert.equal(ad.dispatchSeq, 2, 'outbox advances to the newest dispatch attempt');
  });
});

test('consumer rejects incompatible schema without storing or applying it', async () => {
  await withConsumer(async (consumer) => {
    const payload = buildRelease({ version: 'v2', schema: 2, cohort: 'c2', rowCount: 4 });
    const result = await consumer.store.receiveDelivery(delivery(payload));
    assert.equal(result.body.applied, false);
    assert.equal(result.body.rejected, true);
    assert.match(result.body.reason, /unsupported schema v2/);
    assert.equal(result.body.detail.compatible, false);
    assert.equal(consumer.store.state.applications.size, 0);
    assert.equal(consumer.store.state.currentVersion, null);
    assert.equal(consumer.store.state.rejected.size, 1);
  }, { supportedSchema: 1 });
});

test('consumer refuses tampered payloads and stale ordinals', async () => {
  await withConsumer(async (consumer) => {
    const v2 = buildRelease({ version: 'v2', schema: 2, cohort: 'c2', rowCount: 4 });
    await consumer.store.receiveDelivery(delivery(v2, { ordinal: 2, releaseId: 'rel-2' }));

    const tampered = structuredClone(buildRelease({
      version: 'v3', schema: 2, cohort: 'c3', rowCount: 4,
    }));
    tampered.rows[0].score = 999;
    const bad = await consumer.store.receiveDelivery(delivery(tampered, {
      ordinal: 3, releaseId: 'rel-3', version: 'v3',
    }));
    assert.equal(bad.body.rejected, true);
    assert.match(bad.body.reason, /payloadHash mismatch/);

    const old = buildRelease({ version: 'v1', schema: 2, cohort: 'c1', rowCount: 4 });
    const stale = await consumer.store.receiveDelivery(delivery(old, {
      ordinal: 1, releaseId: 'rel-1',
    }));
    assert.equal(stale.body.applied, false);
    assert.equal(stale.body.stale, true);
    assert.equal(consumer.store.state.currentReleaseId, 'rel-2');
    assert.equal(consumer.store.state.counters.staleDeliveries, 1);
  });
});

test('revocation quarantines consumed data and tombstones unseen releases', async () => {
  await withConsumer(async (consumer) => {
    const v1 = buildRelease({ version: 'v1', schema: 2, cohort: 'c1', rowCount: 3 });
    await consumer.store.receiveDelivery(delivery(v1));

    const cancel = await consumer.store.cancelRelease('rel-test-1', 'emergency');
    assert.equal(cancel.disposition, 'quarantined');
    assert.equal(consumer.store.state.serving, false);
    assert.equal(consumer.store.state.counters.quarantined, 1);
    assert.equal(consumer.store.state.tombstones.has('rel-test-1'), true);

    // A delivery that was already in flight must not resurrect the release.
    const retry = await consumer.store.receiveDelivery(delivery(v1, { dispatchSeq: 3 }));
    assert.equal(retry.body.applied, false);
    assert.equal(retry.body.revoked, true);
    assert.equal(consumer.store.state.counters.applied, 1);

    const unseen = await consumer.store.cancelRelease('rel-never-seen', 'emergency');
    assert.equal(unseen.disposition, 'not-consumed');
    const late = await consumer.store.receiveDelivery(delivery(v1, {
      releaseId: 'rel-never-seen', dispatchSeq: 9,
    }));
    assert.equal(late.body.applied, false);
    assert.equal(late.body.revoked, true);
  });
});
