import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { join } from 'node:path';
import { bootStack, jsonFetch, waitFor } from './helpers.js';
import { startConsumer } from '../src/consumer/agent.js';
import { CONSUMER_NAMES } from '../src/shared/protocol.js';

async function released(stack, body) {
  const rel = await stack.createRelease(body);
  await stack.publish(rel.releaseId);
  return rel;
}

test('publisher never counts dispatch as delivery; acks are the only proof', async () => {
  const stack = await bootStack();
  try {
    await stack.sweep(1);
    const rel = await released(stack, { version: 'v1', schema: 1 });
    // First sweep only dispatches; no state can be ACKED yet in the same tick.
    await stack.sweep(1);
    let st = await stack.state();
    const dispatched = st.releases[0].deliveries;
    for (const name of CONSUMER_NAMES) {
      assert.ok(['DISPATCHED', 'ACKED', 'PENDING'].includes(dispatched[name].status));
    }
    await waitFor(async () => {
      const s = await stack.state();
      await stack.sweep(2);
      return s.releases[0].ackedCount === 3;
    }, { label: 'all three acks' });

    st = await stack.state();
    assert.equal(st.releases[0].ackedCount, 3);
    assert.equal(st.promotion.eligible, true);
    for (const c of st.consumers) {
      assert.equal(c.effectiveVersion, 'v1');
      assert.equal(c.consumerReportedVersion, 'v1');
    }
    assert.ok(rel.releaseId);
  } finally {
    await stack.close();
  }
});

test('an HTTP 200 that never acknowledges is never counted as effective', async () => {
  const stack = await bootStack();
  // Replace gamma with a stub that accepts deliveries (200, applied) but never
  // sends an acknowledgement. This is the exact "sent but not in effect" case.
  const gammaPort = stack.consumers.gamma.port;
  await stack.consumers.gamma.close();
  const stub = createServer((req, res) => {
    if (req.method === 'GET' && req.url === '/health') {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ ok: true, name: 'gamma', port: gammaPort, supportedSchema: 2 }));
      return;
    }
    if (req.method === 'GET' && req.url === '/state') {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ name: 'gamma', applications: [], currentVersion: null }));
      return;
    }
    if (req.method === 'POST' && req.url === '/delivery') {
      req.resume();
      req.on('end', () => {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ applied: true, appliedSeq: 1 }));
      });
      return;
    }
    res.writeHead(404).end('{}');
  });
  await new Promise((r) => stub.listen(gammaPort, '127.0.0.1', r));
  try {
    const rel = await released(stack, { version: 'v1', schema: 1 });
    await waitFor(async () => {
      await stack.sweep(2);
      const s = await stack.state();
      return s.releases[0].deliveries.gamma.status === 'DISPATCHED';
    }, { label: 'gamma dispatched' });
    await stack.sweep(3);
    const st = await stack.state();
    const gamma = st.releases[0].deliveries.gamma;
    assert.equal(gamma.status, 'DISPATCHED', 'HTTP 200 is not a confirmation');
    assert.equal(gamma.attempts >= 1, true);
    assert.equal(gamma.ackAt, null);
    assert.equal(st.releases[0].ackedCount, 2);
    assert.equal(st.promotion.eligible, false);
    assert.equal(st.consumers.find((c) => c.name === 'gamma').effectiveVersion, null);

    // Keep dispatching: the lease expires and the delivery times out rather
    // than silently becoming "delivered".
    await new Promise((r) => setTimeout(r, 2700));
    await stack.sweep(2);
    const timedOut = (await stack.state()).releases[0].deliveries.gamma;
    assert.ok(['TIMEOUT', 'DISPATCHED'].includes(timedOut.status));
    assert.equal((await stack.state()).releases[0].ackedCount, 2);
  } finally {
    await new Promise((r) => stub.close(r));
    await stack.close();
  }
});

test('structural rejection blocks all-converged promotion and is terminal', async () => {
  const stack = await bootStack();
  try {
    await released(stack, { version: 'v1', schema: 1 });
    await waitFor(async () => {
      await stack.sweep(2);
      return (await stack.state()).releases[0].ackedCount === 3;
    }, { label: 'v1 acked' });
    const v1 = (await stack.state()).releases[0];
    const promoted = await stack.jsonFetch(`${stack.base}/api/releases/${v1.releaseId}/promote`, { method: 'POST' });
    assert.equal(promoted.status, 200);

    const v2 = await released(stack, { version: 'v2', schema: 2 });
    await waitFor(async () => {
      await stack.sweep(2);
      const s = await stack.state();
      const rel = s.releases.find((r) => r.releaseId === v2.releaseId);
      return rel.deliveries.gamma.status === 'REJECTED' && rel.ackedCount === 2;
    }, { label: 'gamma rejection' });

    const attempt = await stack.jsonFetch(`${stack.base}/api/releases/${v2.releaseId}/promote`, { method: 'POST' });
    assert.equal(attempt.status, 409);
    assert.ok(attempt.data.details.blockers.some((b) => b.includes('gamma')));

    // Retrying a rejected delivery does not turn it into success.
    await stack.jsonFetch(`${stack.base}/api/releases/${v2.releaseId}/deliveries/gamma/retry`, { method: 'POST' });
    await stack.sweep(2);
    const st = await stack.state();
    const rel = st.releases.find((r) => r.releaseId === v2.releaseId);
    assert.equal(rel.deliveries.gamma.status, 'REJECTED');
    assert.equal(st.production.releaseId, v1.releaseId);

    // Quorum mode can promote while accepting the lagging consumer.
    await stack.jsonFetch(`${stack.base}/api/policy`, {
      method: 'POST', body: { promotionMode: 'quorum-with-lag', quorum: 2 },
    });
    const quorum = await stack.jsonFetch(`${stack.base}/api/releases/${v2.releaseId}/promote`, { method: 'POST' });
    assert.equal(quorum.status, 200);
    assert.equal(quorum.data.release.state, 'PROMOTED');
    assert.equal(quorum.data.release.promotion.forced, true);
    assert.equal(st.consumers.find((c) => c.name === 'gamma').effectiveVersion, 'v1');

    const s2 = await stack.state();
    assert.equal(s2.production.version, 'v2');
    const gamma = s2.consumers.find((c) => c.name === 'gamma');
    assert.equal(gamma.effectiveVersion, 'v1', 'lagging consumer keeps its own effective version');
    assert.ok(gamma.lagDeadline, 'lag deadline recorded at promotion');
  } finally {
    await stack.close();
  }
});

test('duplicate and late acknowledgements never move state twice', async () => {
  const stack = await bootStack();
  try {
    const rel = await released(stack, { version: 'v1', schema: 1 });
    await waitFor(async () => {
      await stack.sweep(2);
      return (await stack.state()).releases[0].ackedCount === 3;
    }, { label: 'acks' });
    const ack = (await stack.state()).releases[0].deliveries.alpha;

    const dup = await stack.jsonFetch(`${stack.base}/api/releases/${rel.releaseId}/simulate/duplicate-ack`, {
      method: 'POST', body: { consumer: 'alpha' },
    });
    assert.equal(dup.data.result.accepted, true);
    assert.equal(dup.data.result.duplicate, true);

    const late = await stack.jsonFetch(`${stack.base}/api/releases/${rel.releaseId}/simulate/late-ack`, {
      method: 'POST', body: { consumer: 'alpha' },
    });
    assert.equal(late.data.result.accepted, false);
    assert.equal(late.data.result.stale, true);

    // Direct protocol misuse: wrong version, low appliedSeq.
    const forged = await jsonFetch(`${stack.base}/bus/ack`, {
      method: 'POST',
      body: {
        consumer: 'beta', releaseId: rel.releaseId, version: 'v999',
        appliedSeq: ack.appliedSeq, dispatchSeq: ack.dispatchSeq,
      },
    });
    assert.equal(forged.data.accepted, false);
    assert.equal(forged.data.error, 'ack-does-not-match-release');

    const before = await stack.state();
    assert.equal(before.counters.duplicateAcks, 1);
    assert.equal(before.counters.staleAcks, 1);
    assert.equal(before.releases[0].ackedCount, 3);
    assert.equal(before.consumers.find((c) => c.name === 'alpha').effectiveVersion, 'v1');
  } finally {
    await stack.close();
  }
});

test('offline consumer lags without blocking others; restart resumes without re-applying', async () => {
  const stack = await bootStack();
  let beta = stack.consumers.beta;
  try {
    const betaDir = join(stack.root, 'beta');
    const betaPort = beta.port;
    await beta.close();
    const rel = await released(stack, { version: 'v1', schema: 1 });
    await waitFor(async () => {
      await stack.sweep(2);
      const s = await stack.state();
      return s.releases.find((r) => r.releaseId === rel.releaseId).ackedCount === 2;
    }, { label: 'two acks while beta offline' });
    let st = await stack.state();
    assert.equal(st.consumers.find((c) => c.name === 'beta').online, false);
    assert.equal(st.promotion.eligible, false);
    assert.equal(st.promotion.blockers.length > 0, true);

    const journalBefore = await readFile(join(betaDir, 'journal.jsonl'), 'utf8');
    assert.equal(journalBefore.trim(), '', 'offline beta never received the release');

    // Restart beta from the same directory on the same port.
    beta = await startConsumer({
      name: 'beta',
      port: betaPort,
      dataDir: betaDir,
      publisherUrl: stack.base,
      supportedSchema: 2,
      autoRetryAcks: false,
    });
    stack.consumers.beta = beta;
    await waitFor(async () => {
      await stack.sweep(2);
      const s = await stack.state();
      return s.releases.find((r) => r.releaseId === rel.releaseId).ackedCount === 3;
    }, { label: 'beta catches up after restart' });
    st = await stack.state();
    assert.equal(st.promotion.eligible, true);
    assert.equal(beta.store.state.counters.applied, 1);

    // A second restart replays the journal and stays exactly where it was.
    await beta.close();
    beta = await startConsumer({
      name: 'beta',
      port: betaPort,
      dataDir: betaDir,
      publisherUrl: stack.base,
      supportedSchema: 2,
      autoRetryAcks: false,
    });
    stack.consumers.beta = beta;
    assert.equal(beta.store.state.currentVersion, 'v1');
    assert.equal(beta.store.state.counters.applied, 1);
    assert.equal(beta.store.snapshot().applications.length, 1);

    // Redelivery after recovery is a duplicate, not a re-application.
    const again = await beta.store.receiveDelivery({
      releaseId: rel.releaseId,
      ordinal: 1,
      version: 'v1',
      schema: 1,
      cohort: rel.cohort,
      payloadHash: rel.payloadHash,
      payload: (await stack.jsonFetch(`${stack.base}/api/releases/${rel.releaseId}`)).data.deliveryView,
      dispatchSeq: 99,
      idempotencyKey: 'post-recovery',
    });
    assert.equal(again.body.applied, false);
    assert.equal(again.body.duplicate, true);
    assert.equal(beta.store.state.counters.applied, 1);
  } finally {
    await stack.close();
  }
});
