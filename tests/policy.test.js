import test from 'node:test';
import assert from 'node:assert/strict';
import { bootStack, waitFor } from './helpers.js';

test('compatibility window expires and requires an explicit business exception', async () => {
  const stack = await bootStack({
    policy: { compatWindowMs: 300, promotionMode: 'quorum-with-lag', quorum: 2 },
  });
  try {
    const rel = await stack.createRelease({ version: 'v2', schema: 2 });
    await stack.publish(rel.releaseId);
    await waitFor(async () => {
      await stack.sweep(2);
      const s = await stack.state();
      const r = s.releases.find((x) => x.releaseId === rel.releaseId);
      return r.ackedCount === 2 && r.deliveries.gamma.status === 'REJECTED';
    }, { label: 'quorum with gamma rejected' });

    await new Promise((r) => setTimeout(r, 400));
    const st = await stack.state();
    const gammaCompat = st.promotion.compat.find((c) => c.consumer === 'gamma');
    assert.equal(gammaCompat.expired, true);
    assert.equal(st.promotion.eligible, false);
    assert.ok(st.promotion.blockers.some((b) => b.includes('兼容窗口已超期')));

    const blocked = await stack.jsonFetch(`${stack.base}/api/releases/${rel.releaseId}/promote`, { method: 'POST' });
    assert.equal(blocked.status, 409);

    const allowed = await stack.jsonFetch(`${stack.base}/api/releases/${rel.releaseId}/promote`, {
      method: 'POST', body: { acknowledgeCompatException: true },
    });
    assert.equal(allowed.status, 200);
    assert.equal(allowed.data.release.state, 'PROMOTED');
    const after = await stack.state();
    assert.equal(after.production.version, 'v2');
    assert.equal(after.consumers.find((c) => c.name === 'gamma').effectiveVersion, null);
  } finally {
    await stack.close();
  }
});

test('waiting window is reported while a downstream is offline', async () => {
  const stack = await bootStack({
    policy: { promotionMode: 'all-converged', waitWindowMs: 250 },
  });
  try {
    await stack.consumers.beta.close();
    const rel = await stack.createRelease({ version: 'v1', schema: 1 });
    await stack.publish(rel.releaseId);
    await waitFor(async () => {
      await stack.sweep(2);
      return (await stack.state()).releases[0].ackedCount === 2;
    }, { label: 'two acks' });
    await new Promise((r) => setTimeout(r, 350));
    const st = await stack.state();
    assert.equal(st.promotion.waitOverdue, true);
    assert.equal(st.promotion.eligible, false);
    assert.ok(st.promotion.lagging.some((l) => l.consumer === 'beta'));
    assert.equal(Date.parse(st.promotion.waitDeadline) > 0, true);
    assert.ok(Date.parse(st.promotion.waitDeadline) < Date.now(), 'wait window has run out');
    // No lag deadline exists until a promotion is actually forced.
    assert.ok(st.promotion.lagging.every((l) => l.lagDeadline === null));
  } finally {
    await stack.close();
  }
});

test('rollback creates a forward release and never moves versions backwards', async () => {
  const stack = await bootStack();
  try {
    const v1 = await stack.createRelease({ version: 'v1', schema: 1 });
    await stack.publish(v1.releaseId);
    await waitFor(async () => {
      await stack.sweep(2);
      return (await stack.state()).releases.find((r) => r.releaseId === v1.releaseId).ackedCount === 3;
    }, { label: 'v1 acked' });
    await stack.jsonFetch(`${stack.base}/api/releases/${v1.releaseId}/promote`, { method: 'POST' });

    const v2 = await stack.createRelease({ version: 'v2', schema: 1 });
    await stack.publish(v2.releaseId);
    await waitFor(async () => {
      await stack.sweep(2);
      return (await stack.state()).releases.find((r) => r.releaseId === v2.releaseId).ackedCount === 3;
    }, { label: 'v2 acked' });
    await stack.jsonFetch(`${stack.base}/api/releases/${v2.releaseId}/promote`, { method: 'POST' });

    const rb = await stack.jsonFetch(`${stack.base}/api/releases/${v2.releaseId}/rollback`, {
      method: 'POST', body: { reason: 'bad metrics' },
    });
    assert.equal(rb.status, 200);
    const replacement = rb.data.replacement;
    assert.notEqual(replacement.releaseId, v1.releaseId, 'restore is a new forward release');
    assert.ok(replacement.ordinal > v2.ordinal);
    assert.equal(rb.data.rolledBack.state, 'ROLLED_BACK');

    await waitFor(async () => {
      await stack.sweep(2);
      const s = await stack.state();
      return s.releases.find((r) => r.releaseId === replacement.releaseId).ackedCount === 3;
    }, { label: 'restore acked' });

    // The restore release auto-promotes; consumer ordinals only increase.
    await stack.sweep(2);
    const st = await stack.state();
    assert.equal(st.production.releaseId, replacement.releaseId);
    for (const c of st.consumers) {
      assert.equal(c.effectiveReleaseId, replacement.releaseId);
      assert.notEqual(c.effectiveVersion, 'v1', 'consumer version names never roll back');
    }
    const release = st.releases.find((r) => r.releaseId === replacement.releaseId);
    assert.equal(release.rollbackOf, v2.releaseId);
  } finally {
    await stack.close();
  }
});
