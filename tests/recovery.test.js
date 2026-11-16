import test from 'node:test';
import assert from 'node:assert/strict';
import { rm } from 'node:fs/promises';
import { join } from 'node:path';
import { bootStack, jsonFetch, waitFor } from './helpers.js';
import { startPublisher } from '../src/publisher/server.js';

async function allAcked(stack, releaseId) {
  await waitFor(async () => {
    await stack.sweep(2);
    const s = await stack.state();
    const rel = s.releases.find((r) => r.releaseId === releaseId);
    return rel && rel.ackedCount === 3;
  }, { label: 'all acked' });
}

test('publisher crash + replay preserves every bit of confirmed state', async () => {
  const stack = await bootStack();
  let publisher = stack.publisher;
  try {
    const rel = await stack.createRelease({ version: 'v1', schema: 1 });
    await stack.publish(rel.releaseId);
    await allAcked(stack, rel.releaseId);
    await stack.jsonFetch(`${stack.base}/api/releases/${rel.releaseId}/promote`, { method: 'POST' });
    const before = await stack.state();

    await publisher.close();
    // Full process-style restart: same data dir, same consumer endpoints.
    publisher = await startPublisher({
      port: 0,
      dataDir: join(stack.root, 'publisher'),
      consumerUrls: {
        alpha: stack.consumerBase('alpha'),
        beta: stack.consumerBase('beta'),
        gamma: stack.consumerBase('gamma'),
      },
      autoPoll: false,
    });
    stack.rebind(`http://127.0.0.1:${publisher.port}`);
    stack.publisher = publisher;
    await publisher.sweep();
    const after = await stack.state();

    assert.equal(after.epoch, before.epoch, 'journal head unchanged by restart');
    assert.equal(after.production.releaseId, rel.releaseId);
    assert.equal(after.production.state, 'PROMOTED');
    assert.equal(after.releases[0].ackedCount, 3);
    assert.deepEqual(
      after.consumers.map((c) => [c.name, c.effectiveVersion]),
      before.consumers.map((c) => [c.name, c.effectiveVersion]),
    );
    assert.deepEqual(after.counters, before.counters);

    // A repeated ack after restart is still a duplicate.
    const dup = await stack.jsonFetch(`${stack.base}/api/releases/${rel.releaseId}/simulate/duplicate-ack`, {
      method: 'POST', body: { consumer: 'alpha' },
    });
    assert.equal(dup.data.result.duplicate, true);
    assert.equal((await stack.state()).releases[0].ackedCount, 3);
  } finally {
    await stack.close();
  }
});

test('a late ack stale before the crash stays stale after the crash', async () => {
  const stack = await bootStack();
  let publisher = stack.publisher;
  try {
    const rel = await stack.createRelease({ version: 'v1', schema: 1 });
    await stack.publish(rel.releaseId);
    await allAcked(stack, rel.releaseId);
    const stale = await stack.jsonFetch(`${stack.base}/api/releases/${rel.releaseId}/simulate/late-ack`, {
      method: 'POST', body: { consumer: 'beta' },
    });
    assert.equal(stale.data.result.stale, true);
    const before = await stack.state();

    await publisher.close();
    publisher = await startPublisher({
      port: 0,
      dataDir: join(stack.root, 'publisher'),
      consumerUrls: {
        alpha: stack.consumerBase('alpha'),
        beta: stack.consumerBase('beta'),
        gamma: stack.consumerBase('gamma'),
      },
      autoPoll: false,
    });
    stack.rebind(`http://127.0.0.1:${publisher.port}`);
    stack.publisher = publisher;
    const after = await stack.state();
    assert.equal(after.counters.staleAcks, before.counters.staleAcks);
    assert.equal(after.releases[0].ackedCount, 3);
  } finally {
    await stack.close();
  }
});

test('revocation is durable: crashed publisher retries it, no ack can undo it', async () => {
  const stack = await bootStack();
  let publisher = stack.publisher;
  try {
    // beta is offline from before the release, so it never consumes anything.
    const betaPort = stack.consumers.beta.port;
    await stack.consumers.beta.close();

    const rel = await stack.createRelease({ version: 'v1', schema: 1 });
    await stack.publish(rel.releaseId);
    await waitFor(async () => {
      await stack.sweep(2);
      const s = await stack.state();
      const r = s.releases.find((x) => x.releaseId === rel.releaseId);
      return r.ackedCount === 2;
    }, { label: 'alpha+gamma acks' });
    await stack.jsonFetch(`${stack.base}/api/releases/${rel.releaseId}/promote`, { method: 'POST' });

    const revoked = await stack.jsonFetch(`${stack.base}/api/releases/${rel.releaseId}/revoke`, {
      method: 'POST', body: { reason: 'bad upstream data' },
    });
    assert.equal(revoked.data.release.state, 'REVOKED');
    assert.equal(revoked.data.release.revocation.targets.alpha.consumed, true);
    assert.equal(revoked.data.release.revocation.targets.beta.consumed, false);
    assert.equal(revoked.data.release.revocation.targets.beta.deliveryStatus, 'PENDING');
    assert.equal(revoked.data.release.revocation.results.alpha.disposition, 'quarantined');
    assert.equal(stack.consumers.alpha.store.snapshot().serving, false);
    assert.equal(stack.consumers.gamma.store.snapshot().serving, false);

    // Any ack arriving after the revoke is refused, even if well-formed.
    const lateAck = await jsonFetch(`${stack.base}/bus/ack`, {
      method: 'POST',
      body: {
        consumer: 'alpha', releaseId: rel.releaseId, version: 'v1',
        appliedSeq: 1, dispatchSeq: 1, idempotencyKey: 'x',
      },
    });
    assert.equal(lateAck.data.accepted, false);
    assert.equal(lateAck.data.revoked, true);

    await publisher.close();
    publisher = await startPublisher({
      port: 0,
      dataDir: join(stack.root, 'publisher'),
      consumerUrls: {
        alpha: stack.consumerBase('alpha'),
        beta: stack.consumerBase('beta'),
        gamma: stack.consumerBase('gamma'),
      },
      autoPoll: false,
    });
    stack.rebind(`http://127.0.0.1:${publisher.port}`);
    stack.publisher = publisher;

    // beta comes back after the crash; the next sweep reconciles its pending
    // revocation, and because it never consumed, it is tombstoned instead.
    const { startConsumer } = await import('../src/consumer/agent.js');
    const beta = await startConsumer({
      name: 'beta',
      port: betaPort,
      dataDir: join(stack.root, 'beta'),
      publisherUrl: stack.base,
      supportedSchema: 2,
      autoRetryAcks: false,
    });
    stack.consumers.beta = beta;
    await stack.sweep(2);
    const st = await stack.state();
    assert.equal(st.production, null, 'no production release after revocation');
    assert.equal(st.releases[0].state, 'REVOKED');
    assert.equal(st.releases[0].revocation.results.alpha.ok, true);
    assert.equal(st.releases[0].revocation.results.beta.ok, true);
    assert.equal(st.releases[0].revocation.results.beta.disposition, 'not-consumed');
    assert.equal(beta.store.state.tombstones.has(rel.releaseId), true);
    assert.equal(beta.store.state.applications.size, 0);

    // Even if the stale release is re-delivered to beta, it stays unapplied.
    await stack.sweep(2);
    assert.equal((await stack.state()).releases[0].state, 'REVOKED');
    assert.equal(beta.store.state.applications.size, 0);
  } finally {
    await stack.close();
  }
});
