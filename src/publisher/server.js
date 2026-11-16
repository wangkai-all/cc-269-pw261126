import { createServer } from 'node:http';
import { readFile, stat } from 'node:fs/promises';
import { extname, join, normalize, resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { PublisherStore } from './store.js';
import {
  CONSUMER_NAMES, CONSUMER_PROFILES, DEFAULT_PORTS, consumerUrl,
} from '../shared/protocol.js';
import { buildRelease } from '../shared/schema.js';
import { nowIso, sha256, HttpError } from '../shared/util.js';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const PUBLIC_DIR = join(ROOT, 'public');

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
};

function sendJson(res, status, body) {
  const text = JSON.stringify(body);
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'content-length': Buffer.byteLength(text),
  });
  res.end(text);
}

async function readJsonBody(req, limit = 4 * 1024 * 1024) {
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > limit) throw new HttpError(413, 'request body too large');
    chunks.push(chunk);
  }
  if (!chunks.length) return {};
  try {
    return JSON.parse(Buffer.concat(chunks).toString('utf8'));
  } catch {
    throw new HttpError(400, 'invalid JSON body');
  }
}

function publicRelease(rel) {
  return {
    releaseId: rel.releaseId,
    ordinal: rel.ordinal,
    version: rel.version,
    schema: rel.schema,
    cohort: rel.cohort,
    notes: rel.notes,
    payloadHash: rel.payloadHash,
    state: rel.state,
    createdAt: rel.createdAt,
    publishedAt: rel.publishedAt,
    dispatchedAt: rel.dispatchedAt,
    promotedAt: rel.promotedAt,
    revokedAt: rel.revokedAt,
    rolledBackAt: rel.rolledBackAt,
    supersededAt: rel.supersededAt,
    rollbackOf: rel.rollbackOf,
    promotion: rel.promotion ?? null,
    revocation: rel.revocation ?? null,
    rollback: rel.rollback ?? null,
    rowCount: rel.payload?.manifest?.rowCount ?? 0,
    sampleRows: rel.payload?.rows?.slice(0, 5) ?? [],
    deliveries: Object.fromEntries(
      Object.entries(rel.deliveries).map(([name, d]) => [name, { ...d }]),
    ),
    ackedCount: Object.values(rel.deliveries).filter((d) => d.status === 'ACKED').length,
    total: Object.keys(rel.deliveries).length,
  };
}

function evaluatePromotion(store, rel, { withCompatException = false } = {}) {
  const plain = evaluatePromotionOnce(store, rel);
  if (withCompatException) return evaluatePromotionOnce(store, rel, true);
  // Show the operator that the exception path exists without letting them take
  // it by accident: eligible stays false, eligibleWithException says what a
  // recorded business exception would unlock.
  const exception = evaluatePromotionOnce(store, rel, true);
  return {
    ...plain,
    eligibleWithException: exception.eligible,
    blockersWithException: exception.blockers,
  };
}

function evaluatePromotionOnce(store, rel, withCompatException = false) {
  const policy = store.state.policy;
  const now = Date.now();
  const entries = Object.values(rel.deliveries);
  const acked = entries.filter((d) => d.status === 'ACKED');
  const rejected = entries.filter((d) => d.status === 'REJECTED');
  const lagging = entries.filter((d) => d.status !== 'ACKED');
  const blockers = [];
  if (rel.state !== 'PUBLISHED') blockers.push(`release is ${rel.state}, expected PUBLISHED`);
  if (policy.promotionMode === 'all-converged' && rejected.length) {
    blockers.push(`${rejected.map((d) => d.consumer).join(', ')} 结构性拒绝，无法确认`);
  }
  if (policy.promotionMode === 'all-converged' && lagging.length) {
    blockers.push(`仍有 ${lagging.length} 个下游未确认: ${lagging.map((d) => `${d.consumer}(${d.status})`).join(', ')}`);
  }
  if (policy.promotionMode === 'quorum-with-lag' && acked.length < policy.quorum) {
    blockers.push(`确认数 ${acked.length} 未达到法定人数 ${policy.quorum}`);
  }

  // Compatibility window: a downstream whose schema support is below the
  // release schema may lag during the window. After it expires, promotion in
  // quorum mode requires an explicit, recorded business exception.
  const compat = [];
  const runtimeRef = store.__runtime;
  for (const name of CONSUMER_NAMES) {
    // Prefer what the running consumer reports; fall back to the seeded
    // profile when it is offline and has never reported.
    const supported = runtimeRef?.health?.get(name)?.supportedSchema
      ?? CONSUMER_PROFILES[name].schema;
    if (!Number.isInteger(supported) || supported >= rel.schema) continue;
    const intro = store.state.schemaIntros.get(rel.schema);
    const introducedAt = intro ? Date.parse(intro.introducedAt) : (rel.publishedAt ? Date.parse(rel.publishedAt) : now);
    const expiresAt = introducedAt + policy.compatWindowMs;
    const remainingMs = expiresAt - now;
    compat.push({
      consumer: name,
      supportedSchema: supported,
      requiredSchema: rel.schema,
      introducedAt: new Date(introducedAt).toISOString(),
      expiresAt: new Date(expiresAt).toISOString(),
      remainingMs,
      expired: remainingMs <= 0,
      status: store.state.consumerVersion.get(name)?.version ?? null,
    });
  }
  const expiredCompat = compat.filter((c) => c.expired);
  if (policy.promotionMode === 'quorum-with-lag' && expiredCompat.length && !withCompatException) {
    blockers.push(
      `兼容窗口已超期：${expiredCompat.map((c) => c.consumer).join(', ')}。需确认业务例外后才能按法定人数提升`,
    );
  }
  const newest = store.releasesNewestFirst()[0];
  if (newest && newest.ordinal > rel.ordinal) blockers.push('存在更新的发布，不能提升旧版本');
  const publishedAt = rel.publishedAt ? Date.parse(rel.publishedAt) : now;
  const waitedMs = now - publishedAt;
  const waitOverdue = waitedMs > policy.waitWindowMs && lagging.length > 0;
  const waitDeadline = new Date(publishedAt + policy.waitWindowMs).toISOString();
  return {
    eligible: blockers.length === 0 && rel.state === 'PUBLISHED',
    blockers,
    acked: acked.map((d) => d.consumer),
    lagging: lagging.map((d) => ({
      consumer: d.consumer,
      status: d.status,
      rejection: d.rejection,
      // Before promotion there is no lag deadline yet: the wait window
      // (waitDeadline, reported separately) is what the publisher is in.
      // A forced promotion stamps a fixed per-consumer lag deadline.
      lagDeadline: rel.state === 'PROMOTED'
        ? (rel.promotion?.lagDeadlines?.[d.consumer] ?? null)
        : null,
      lagOverdue: rel.state === 'PROMOTED'
        ? Boolean(rel.promotion?.lagDeadlines?.[d.consumer]
          && Date.parse(rel.promotion.lagDeadlines[d.consumer]) < now)
        : false,
    })),
    rejected: rejected.map((d) => d.consumer),
    compat,
    compatExpired: expiredCompat.length > 0,
    compatWindowMs: policy.compatWindowMs,
    waitedMs,
    waitWindowMs: policy.waitWindowMs,
    waitDeadline,
    waitOverdue,
    mode: policy.promotionMode,
    quorum: policy.quorum,
    ackedCount: acked.length,
    total: entries.length,
  };
}

export async function startPublisher(options = {}) {
  const port = options.port ?? Number(process.env.PORT ?? DEFAULT_PORTS.publisher);
  const dataDir = resolve(options.dataDir ?? process.env.PUBLISHER_DATA_DIR ?? `${ROOT}/.data/publisher`);
  const consumerUrls = options.consumerUrls ?? Object.fromEntries(
    CONSUMER_NAMES.map((name) => [name, consumerUrl(name)]),
  );
  const supervisorUrl = options.supervisorUrl
    ?? (process.env.SUPERVISOR_URL ?? `http://127.0.0.1:${DEFAULT_PORTS.supervisor}`);

  const store = await new PublisherStore({ dataDir }).init();
  if (options.policy) {
    await store.mutate('policy.updated', { policy: options.policy });
  }
  const runtime = {
    online: new Map(CONSUMER_NAMES.map((name) => [name, false])),
    health: new Map(),
    lastSweepAt: null,
    inFlight: new Set(),
    nextAttemptAt: new Map(),
    retryAttempts: new Map(),
    compatExceptionFor: new Set(),
    autoPromoting: null,
    revokeAttemptAt: new Map(),
    timers: [],
    shuttingDown: false,
  };

  store.__runtime = runtime;
  const consumerBase = (name) => consumerUrls[name] ?? consumerUrl(name);

  async function callConsumer(name, path, payload, timeoutMs = 4000) {
    const res = await fetch(`${consumerBase(name)}${path}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(timeoutMs),
    });
    let data = null;
    try { data = await res.json(); } catch { /* ignore */ }
    return { status: res.status, ok: res.ok, data };
  }

  async function pollConsumer(name) {
    try {
      const res = await fetch(`${consumerBase(name)}/health`, {
        signal: AbortSignal.timeout(1200),
      });
      if (!res.ok) throw new Error(`health ${res.status}`);
      const health = await res.json();
      if (!runtime.online.get(name)) {
        process.stdout.write(`[publisher] consumer ${name} is online\n`);
      }
      runtime.online.set(name, true);
      runtime.health.set(name, { ...health, checkedAt: nowIso() });
      return true;
    } catch (err) {
      if (runtime.online.get(name)) {
        process.stdout.write(`[publisher] consumer ${name} went offline: ${err.message}\n`);
      }
      runtime.online.set(name, false);
      runtime.health.set(name, { online: false, error: err.message, checkedAt: nowIso() });
      return false;
    }
  }

  function dispatchDue(rel, delivery, now) {
    if (!runtime.online.get(delivery.consumer)) return false;
    if (['ACKED', 'REJECTED'].includes(delivery.status)) return false;
    if (rel.state === 'REVOKED' || rel.state === 'ROLLED_BACK' || rel.state === 'DRAFT') return false;
    if (runtime.inFlight.has(`${rel.releaseId}:${delivery.consumer}`)) return false;
    if (delivery.status === 'DISPATCHED' && delivery.leaseUntil && Date.parse(delivery.leaseUntil) > now) {
      return false;
    }
    const nextAt = runtime.nextAttemptAt.get(`${rel.releaseId}:${delivery.consumer}`) ?? 0;
    return nextAt <= now;
  }

  async function dispatch(rel, delivery) {
    const key = `${rel.releaseId}:${delivery.consumer}`;
    runtime.inFlight.add(key);
    const dispatchSeq = delivery.dispatchSeq + 1;
    const idempotencyKey = sha256(`${rel.releaseId}|${delivery.consumer}|${dispatchSeq}|${rel.payloadHash}`).slice(0, 24);
    const leaseUntil = new Date(Date.now() + store.state.policy.leaseMs).toISOString();
    try {
      await store.mutate('delivery.dispatched', {
        releaseId: rel.releaseId,
        consumer: delivery.consumer,
        dispatchSeq,
        idempotencyKey,
        leaseUntil,
      });
      const body = {
        protocol: 1,
        releaseId: rel.releaseId,
        ordinal: rel.ordinal,
        version: rel.version,
        schema: rel.schema,
        cohort: rel.cohort,
        payloadHash: rel.payloadHash,
        payload: rel.payload,
        dispatchSeq,
        idempotencyKey,
      };
      let result;
      try {
        result = await callConsumer(delivery.consumer, '/delivery', body, 6000);
      } catch (err) {
        await store.mutate('delivery.rejected', {
          releaseId: rel.releaseId,
          consumer: delivery.consumer,
          reason: 'dispatch-failed',
          detail: err.message,
          terminal: false,
          dispatchSeq,
        }).catch(() => {});
        scheduleRetry(key);
        return;
      }
      if (!result.ok) {
        await store.mutate('delivery.rejected', {
          releaseId: rel.releaseId,
          consumer: delivery.consumer,
          reason: `http-${result.status}`,
          detail: result.data?.error ?? result.data?.reason ?? null,
          terminal: false,
          dispatchSeq,
        }).catch(() => {});
        scheduleRetry(key);
        return;
      }
      if (result.data?.rejected) {
        await store.mutate('delivery.rejected', {
          releaseId: rel.releaseId,
          consumer: delivery.consumer,
          reason: result.data.reason ?? 'rejected',
          detail: result.data.detail ?? null,
          terminal: true,
          dispatchSeq,
        }).catch(() => {});
        return;
      }
      runtime.nextAttemptAt.delete(key);
      runtime.retryAttempts.delete(key);
    } finally {
      runtime.inFlight.delete(key);
    }
  }

  function scheduleRetry(key) {
    const attempts = (runtime.retryAttempts.get(key) ?? 0) + 1;
    runtime.retryAttempts.set(key, attempts);
    const backoff = Math.min(store.state.policy.retryMaxDelayMs, 400 * 2 ** Math.min(4, attempts - 1));
    runtime.nextAttemptAt.set(key, Date.now() + backoff);
  }

  async function revokeAtConsumer(rel, name) {
    const throttleKey = `${rel.releaseId}:${name}`;
    const last = runtime.revokeAttemptAt.get(throttleKey) ?? 0;
    if (Date.now() - last < 3000) return null;
    runtime.revokeAttemptAt.set(throttleKey, Date.now());
    try {
      const result = await callConsumer(name, '/cancel', {
        releaseId: rel.releaseId,
        reason: rel.revocation?.reason ?? 'publisher-revoked',
      });
      await store.mutate('revocation.result', {
        releaseId: rel.releaseId,
        consumer: name,
        disposition: result.data?.disposition ?? 'unknown',
        expect: store.state.consumption.get(name)?.has(rel.releaseId) ? 'quarantined' : 'not-consumed',
        ok: result.ok,
        error: result.ok ? null : (result.data?.error ?? `http-${result.status}`),
      });
      return result;
    } catch (err) {
      await store.mutate('revocation.result', {
        releaseId: rel.releaseId,
        consumer: name,
        disposition: 'unreachable',
        expect: store.state.consumption.get(name)?.has(rel.releaseId) ? 'quarantined' : 'not-consumed',
        ok: false,
        error: err.message,
      }).catch(() => {});
      return null;
    }
  }

  async function sweep() {
    if (runtime.shuttingDown) return;
    runtime.lastSweepAt = nowIso();
    await Promise.all(CONSUMER_NAMES.map((name) => pollConsumer(name)));

    // Handle dispatch timeouts (lease expiry). Never treat a timeout as applied.
    for (const rel of store.state.releases.values()) {
      if (rel.state !== 'PUBLISHED' && rel.state !== 'PROMOTED') continue;
      for (const d of Object.values(rel.deliveries)) {
        if (d.status === 'DISPATCHED' && d.leaseUntil && Date.parse(d.leaseUntil) < Date.now()) {
          await store.mutate('delivery.timed_out', {
            releaseId: rel.releaseId,
            consumer: d.consumer,
            dispatchSeq: d.dispatchSeq,
          }).catch(() => {});
        }
      }
    }

    // Revocation reconciliation: every online consumer must confirm withdrawal.
    // A consumer that was offline at revoke time has no result yet, so iterate
    // the full roster, not just the recorded failures.
    for (const rel of store.state.releases.values()) {
      if (!rel.revocation) continue;
      for (const name of CONSUMER_NAMES) {
        if (rel.revocation.results[name]?.ok) continue;
        if (runtime.online.get(name)) await revokeAtConsumer(rel, name);
      }
    }

    const newest = store.releasesNewestFirst()[0];
    if (!newest) return;
    if (newest.state !== 'PUBLISHED' && newest.state !== 'PROMOTED') return;
    const now = Date.now();
    for (const d of Object.values(newest.deliveries)) {
      if (dispatchDue(newest, d, now)) {
        await dispatch(newest, d);
      }
    }

    // A rollback replacement is restoring already-approved content: switch it
    // back to production automatically as soon as its own delivery gate passes.
    // This avoids leaving the service with no production release after recovery.
    for (const rel of store.state.releases.values()) {
      if (rel.state !== 'PUBLISHED' || !rel.rollbackOf) continue;
      if (runtime.autoPromoting === rel.releaseId) continue;
      const evaluation = evaluatePromotion(store, rel);
      if (!evaluation.eligible) continue;
      runtime.autoPromoting = rel.releaseId;
      try {
        await promoteRelease(rel.releaseId, { auto: true });
        process.stdout.write(`[publisher] auto-promoted rollback replacement ${rel.version}\n`);
      } catch (err) {
        process.stderr.write(`[publisher] auto-promote ${rel.version} failed: ${err.message}\n`);
      } finally {
        runtime.autoPromoting = null;
      }
      break;
    }
  }

  async function pollLoop() {
    try {
      await sweep();
    } catch (err) {
      process.stderr.write(`[publisher] sweep failed: ${err.stack ?? err}\n`);
    } finally {
      if (!runtime.shuttingDown) {
        const timer = setTimeout(pollLoop, 900);
        timer.unref?.();
        runtime.timers.push(timer);
      }
    }
  }

  function stateSnapshot() {
    const releases = store.releasesNewestFirst();
    const active = store.currentRelease();
    const production = store.productionRelease();
    const consumerStates = CONSUMER_NAMES.map((name) => {
      const effective = store.state.consumerVersion.get(name) ?? null;
      const consumed = [...(store.state.consumption.get(name)?.values() ?? [])];
      const health = runtime.health.get(name) ?? null;
      const currentOnConsumer = health?.currentVersion ?? null;
      const effectiveRelease = effective ? store.getRelease(effective.releaseId) : null;
      const withdrawn = ['REVOKED', 'ROLLED_BACK'].includes(effectiveRelease?.state);
      const withdrawalPending = Boolean(
        effectiveRelease?.revocation
        && effectiveRelease.revocation.results?.[name]
        && !effectiveRelease.revocation.results[name].ok,
      );
      // Health is a polled cache; it can predate the acknowledgement it is
      // being compared with. Only compare against a probe that is newer than
      // the confirmation, otherwise "drift" would be an artefact of timing.
      const healthFresh = Boolean(health?.checkedAt && effective?.at
        && Date.parse(health.checkedAt) >= Date.parse(effective.at));
      const drift = Boolean(effective && currentOnConsumer
        && effective.version !== currentOnConsumer && healthFresh);
      const linked = active ? store.getRelease(active.releaseId) : null;
      const lagDeadline = linked?.promotion?.lagDeadlines?.[name] ?? null;
      return {
        name,
        profile: CONSUMER_PROFILES[name],
        online: runtime.online.get(name) ?? false,
        port: health?.port ?? null,
        health,
        effectiveVersion: effective?.version ?? null,
        effectiveReleaseId: effective?.releaseId ?? null,
        effectiveAppliedSeq: effective?.appliedSeq ?? null,
        effectiveWithdrawn: withdrawn,
        withdrawalPending,
        consumerReportedVersion: currentOnConsumer,
        drift,
        lagDeadline,
        lagOverdue: Boolean(lagDeadline && Date.parse(lagDeadline) < Date.now()
          && linked?.promotion?.effectiveVersions?.[name] !== linked.version),
        consumed,
        callbacks: {
          duplicateAcks: store.state.counters.duplicateAcks,
          lateAcks: store.state.counters.lateAcks,
          staleAcks: store.state.counters.staleAcks,
        },
      };
    });
    const promotion = active
      ? evaluatePromotion(store, active, {
        withCompatException: Boolean(active.promotion?.compatException
          || runtime.compatExceptionFor?.has(active.releaseId)),
      })
      : null;
    if (promotion && active.state !== 'PUBLISHED') {
      // Blockers only describe a *pending* promotion. Once promoted (or
      // superseded/revoked) they would read as if something were wrong.
      promotion.blockers = [];
      promotion.eligible = false;
      promotion.applicable = false;
    }
    return {
      protocol: 1,
      serverTime: nowIso(),
      epoch: store.state.epoch,
      publisherOnline: true,
      policy: store.state.policy,
      active: active ? publicRelease(active) : null,
      production: production ? publicRelease(production) : null,
      promotion,
      consumers: consumerStates,
      releases: releases.slice(0, 20).map(publicRelease),
      promotions: store.state.promotions,
      schemaIntros: [...store.state.schemaIntros.values()].map((intro) => ({
        ...intro,
        expiresAt: new Date(Date.parse(intro.introducedAt) + store.state.policy.compatWindowMs).toISOString(),
      })),
      counters: store.state.counters,
      journalHead: store.journal.head,
      lastError: store.state.lastError,
      supervisorUrl,
    };
  }

  async function createRelease(body) {
    return store.transaction(async () => {
      const ordinal = store.state.nextOrdinal;
      const version = String(body.version ?? `v${ordinal}`).trim() || `v${ordinal}`;
      const schema = Number(body.schema ?? 1);
      if (![1, 2].includes(schema)) throw new HttpError(400, 'schema must be 1 or 2');
      const cohort = String(body.cohort ?? `cohort-${ordinal}`).slice(0, 40);
      const rowCount = Math.max(1, Math.min(100, Number(body.rowCount ?? 24)));
      const existing = [...store.state.releases.values()].find(
        (r) => r.version === version && r.state !== 'ROLLED_BACK' && r.state !== 'REVOKED',
      );
      if (existing) throw new HttpError(409, `版本号 ${version} 已存在（${existing.state}）`);
      const payload = buildRelease({
        version, schema, cohort, rowCount, notes: String(body.notes ?? ''),
      });
      const releaseId = store.newReleaseId();
      const event = await store.append('release.created', {
        releaseId,
        ordinal,
        version,
        schema,
        cohort,
        notes: String(body.notes ?? ''),
        payload,
        payloadHash: payload.payloadHash,
        consumers: Object.fromEntries(CONSUMER_NAMES.map((n) => [n, true])),
        rollbackOf: body.rollbackOf ?? null,
      });
      return { releaseId, ordinal, version, event };
    });
  }

  async function publishRelease(releaseId) {
    return store.transaction(async () => {
      const rel = store.getRelease(releaseId);
      if (!rel) throw new HttpError(404, 'release not found');
      if (rel.state !== 'DRAFT') throw new HttpError(409, `release is ${rel.state}`);
      const supersedes = [...store.state.releases.values()]
        .filter((other) => other.releaseId !== releaseId && other.state === 'PUBLISHED')
        .map((other) => other.releaseId);
      await store.append('release.published', { releaseId, supersedes });
      return publicRelease(store.getRelease(releaseId));
    });
  }

  async function promoteRelease(releaseId, body = {}) {
    return store.transaction(async () => {
      const rel = store.getRelease(releaseId);
      if (!rel) throw new HttpError(404, 'release not found');
      const evaluation = evaluatePromotion(store, rel, {
        withCompatException: Boolean(body.acknowledgeCompatException),
      });
      if (!evaluation.eligible) {
        throw new HttpError(409, '该版本当前不可切换正式版', {
          blockers: evaluation.blockers,
          evaluation,
        });
      }
      const effectiveVersions = {};
      const lagDeadlines = {};
      const lagWindowMs = store.state.policy.lagWindowMs;
      for (const d of Object.values(rel.deliveries)) {
        if (d.status === 'ACKED') {
          effectiveVersions[d.consumer] = store.state.consumerVersion.get(d.consumer)?.version ?? rel.version;
        } else {
          effectiveVersions[d.consumer] = store.state.consumerVersion.get(d.consumer)?.version ?? null;
          lagDeadlines[d.consumer] = new Date(Date.now() + lagWindowMs).toISOString();
        }
      }
      if (body.acknowledgeCompatException) runtime.compatExceptionFor.add(releaseId);
      await store.append('release.promoted', {
        releaseId,
        mode: evaluation.mode,
        forced: evaluation.lagging.length > 0,
        lagMs: lagWindowMs,
        effectiveVersions,
        lagDeadlines,
        compatException: Boolean(body.acknowledgeCompatException),
      });
      return publicRelease(store.getRelease(releaseId));
    });
  }

  async function revokeRelease(releaseId, body = {}) {
    const rel = store.getRelease(releaseId);
    if (!rel) throw new HttpError(404, 'release not found');
    if (['REVOKED', 'ROLLED_BACK', 'DRAFT'].includes(rel.state)) {
      throw new HttpError(409, `cannot revoke a ${rel.state} release`);
    }
    await store.transaction(async () => {
      const targets = Object.fromEntries(
        Object.entries(rel.deliveries).map(([name, d]) => [name, {
          consumed: Boolean(store.state.consumption.get(name)?.has(rel.releaseId)),
          deliveryStatus: d.status,
        }]),
      );
      await store.append('release.revoked', {
        releaseId,
        reason: String(body.reason ?? '紧急撤销'),
        policy: 'consumed=>quarantine; unconsumed=>tombstone',
        targets,
      });
    });
    const after = store.getRelease(releaseId);
    for (const name of CONSUMER_NAMES) {
      if (runtime.online.get(name)) await revokeAtConsumer(after, name);
    }
    return publicRelease(store.getRelease(releaseId));
  }

  async function rollbackRelease(releaseId, body = {}) {
    const rel = store.getRelease(releaseId);
    if (!rel) throw new HttpError(404, 'release not found');
    if (!['PUBLISHED', 'PROMOTED', 'SUPERSEDED', 'REVOKED'].includes(rel.state)) {
      throw new HttpError(409, `cannot roll back a ${rel.state} release`);
    }
    const previous = store.state.promotions
      .slice()
      .reverse()
      .filter((p) => p.releaseId !== releaseId)
      .map((p) => store.getRelease(p.releaseId))
      .filter((r) => r && !['REVOKED', 'ROLLED_BACK'].includes(r.state))
      .sort((a, b) => b.ordinal - a.ordinal)[0];
    if (!previous) {
      throw new HttpError(
        409,
        '没有可恢复的上一正式版本（恢复采用向前发布旧内容的方式，不会创建倒退的版本号）',
      );
    }
    return store.transaction(async () => {
      const ordinal = store.state.nextOrdinal;
      const version = String(body.version ?? `${previous.version}-restore-${ordinal}`);
      const payload = buildRelease({
        version,
        schema: previous.schema,
        cohort: previous.cohort,
        rowCount: previous.payload.manifest.rowCount,
        notes: `回退恢复自 ${previous.version}（${rel.version} ${rel.state === 'REVOKED' ? '撤销后恢复' : '回退'}）`,
      });
      const replacementId = store.newReleaseId();
      await store.append('release.rollback', {
        releaseId,
        reason: String(body.reason ?? '回退'),
        replacement: {
          releaseId: replacementId,
          ordinal,
          version,
          schema: previous.schema,
          cohort: previous.cohort,
          notes: payload.manifest.notes,
          payload,
          payloadHash: payload.payloadHash,
          consumers: Object.fromEntries(CONSUMER_NAMES.map((n) => [n, true])),
        },
      });
      return {
        rolledBack: publicRelease(store.getRelease(releaseId)),
        replacement: publicRelease(store.getRelease(replacementId)),
      };
    });
  }

  async function retryDelivery(releaseId, consumer) {
    const rel = store.getRelease(releaseId);
    const d = rel?.deliveries[consumer];
    if (!rel || !d) throw new HttpError(404, 'delivery not found');
    if (d.status === 'ACKED') throw new HttpError(409, '已经确认，无需重试');
    if (d.status === 'REJECTED') {
      throw new HttpError(409, `该下游已结构性拒绝（${d.rejection?.reason ?? '未知原因'}）；重试不会改变结果，需要发布兼容版本或走业务例外`);
    }
    if (rel.state === 'REVOKED' || rel.state === 'ROLLED_BACK') throw new HttpError(409, '该版本已撤销或回退');
    await store.mutate('delivery.retried', { releaseId, consumer });
    runtime.nextAttemptAt.delete(`${releaseId}:${consumer}`);
    await sweep();
    return publicRelease(store.getRelease(releaseId));
  }

  async function acceptAck(body) {
    const { consumer, releaseId, appliedSeq, dispatchSeq } = body;
    if (!consumer || !releaseId) throw new HttpError(400, 'consumer and releaseId required');
    return store.transaction(async () => {
      const rel = store.getRelease(releaseId);
      if (!rel) return { accepted: false, error: 'unknown-release' };
      const d = rel.deliveries[consumer];
      if (!d) return { accepted: false, error: 'unknown-consumer' };
      if (rel.state === 'REVOKED') {
        return { accepted: false, revoked: true, error: 'release-revoked' };
      }
      if (rel.state === 'ROLLED_BACK') {
        return { accepted: false, revoked: true, error: 'release-rolled-back' };
      }
      if (d.status === 'REJECTED') {
        return { accepted: false, error: 'delivery-rejected' };
      }
      if (!Number.isInteger(appliedSeq) || appliedSeq < 1) {
        return { accepted: false, error: 'invalid-applied-seq' };
      }
      const arrivalSeq = Number(dispatchSeq ?? 0);
      if (arrivalSeq < d.dispatchSeq || arrivalSeq < 1) {
        // A confirmation for an older dispatch can never move the state, even
        // if the release is already ACKED via a newer attempt.
        await store.append('ack.stale', {
          releaseId, consumer, arrivalSeq, expectedDispatchSeq: d.dispatchSeq,
        });
        return {
          accepted: false,
          stale: true,
          expectedDispatchSeq: d.dispatchSeq,
        };
      }
      if (body.version !== rel.version || (body.payloadHash && body.payloadHash !== rel.payloadHash)) {
        return { accepted: false, error: 'ack-does-not-match-release' };
      }
      if (d.status === 'ACKED') {
        await store.append('ack.duplicate', { releaseId, consumer, appliedSeq });
        return {
          accepted: true,
          duplicate: true,
          effectiveVersion: store.state.consumerVersion.get(consumer)?.version ?? null,
        };
      }
      await store.append('delivery.acked', {
        releaseId,
        consumer,
        version: rel.version,
        appliedSeq,
        dispatchSeq: arrivalSeq,
        idempotencyKey: body.idempotencyKey ?? null,
        late: d.status === 'TIMEOUT' || arrivalSeq > d.dispatchSeq,
        at: nowIso(),
      });
      return {
        accepted: true,
        duplicate: false,
        effectiveVersion: store.state.consumerVersion.get(consumer)?.version ?? rel.version,
        releaseState: store.getRelease(releaseId).state,
      };
    });
  }

  async function simulateDuplicateAck(releaseId, consumer) {
    const rel = store.getRelease(releaseId);
    const d = rel?.deliveries[consumer];
    if (!d || d.status !== 'ACKED') throw new HttpError(409, '该下游尚无确认可重放');
    return acceptAck({
      consumer,
      releaseId,
      version: rel.version,
      payloadHash: rel.payloadHash,
      appliedSeq: d.appliedSeq,
      dispatchSeq: d.dispatchSeq,
      idempotencyKey: d.idempotencyKey,
    });
  }

  async function simulateLateAck(releaseId, consumer) {
    const rel = store.getRelease(releaseId);
    const d = rel?.deliveries[consumer];
    if (!rel || !d) throw new HttpError(404, 'delivery not found');
    // A delayed confirmation from an older dispatch attempt.
    const staleSeq = Math.max(0, d.dispatchSeq - 1);
    return acceptAck({
      consumer,
      releaseId,
      version: rel.version,
      payloadHash: rel.payloadHash,
      appliedSeq: d.appliedSeq ?? 1,
      dispatchSeq: staleSeq,
      idempotencyKey: 'simulated-late-ack',
    });
  }

  async function dispatchNow(releaseId) {
    const rel = store.getRelease(releaseId);
    if (!rel) throw new HttpError(404, 'release not found');
    for (const d of Object.values(rel.deliveries)) {
      if (!['ACKED', 'REJECTED'].includes(d.status)) {
        await store.mutate('delivery.retried', { releaseId, consumer: d.consumer }).catch(() => {});
        runtime.nextAttemptAt.delete(`${releaseId}:${d.consumer}`);
      }
    }
    await sweep();
    return publicRelease(store.getRelease(releaseId));
  }

  async function supervisorAction(path, body) {
    try {
      const res = await fetch(`${supervisorUrl}${path}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body ?? {}),
        signal: AbortSignal.timeout(2500),
      });
      const data = await res.json().catch(() => null);
      if (!res.ok) throw new HttpError(res.status, data?.error ?? 'supervisor error');
      return data;
    } catch (err) {
      if (err instanceof HttpError) throw err;
      throw new HttpError(503, `进程管理器不可用（${supervisorUrl}）：${err.message}。可单独用 npm run consumer -- --name <name> 启停消费者。`);
    }
  }

  const server = createServer(async (req, res) => {
    const url = new URL(req.url, `http://127.0.0.1:${port}`);
    try {
      if (url.pathname.startsWith('/api/') || url.pathname.startsWith('/bus/')) {
        const body = req.method === 'POST' ? await readJsonBody(req) : {};
        const route = `${req.method} ${url.pathname}`;
        const m = (pattern) => url.pathname.match(pattern);
        let match;

        if (route === 'GET /api/state') {
          sendJson(res, 200, stateSnapshot());
          return;
        }
        if (route === 'GET /api/events') {
          const since = Number(url.searchParams.get('since') ?? 0);
          sendJson(res, 200, {
            head: store.journal.head,
            events: store.journal.events.filter((e) => e.seq > since).slice(-250),
          });
          return;
        }
        if (route === 'GET /api/releases') {
          sendJson(res, 200, { releases: store.releasesNewestFirst().map(publicRelease) });
          return;
        }
        if (route === 'POST /api/releases') {
          const created = await createRelease(body);
          sendJson(res, 201, { release: publicRelease(store.getRelease(created.releaseId)) });
          return;
        }
        if ((match = m(/^\/api\/releases\/([^/]+)$/)) && req.method === 'GET') {
          const rel = store.getRelease(decodeURIComponent(match[1]));
          if (!rel) throw new HttpError(404, 'release not found');
          sendJson(res, 200, { release: publicRelease(rel), deliveryView: rel.payload });
          return;
        }
        if ((match = m(/^\/api\/releases\/([^/]+)\/publish$/)) && req.method === 'POST') {
          sendJson(res, 200, { release: await publishRelease(decodeURIComponent(match[1])) });
          return;
        }
        if ((match = m(/^\/api\/releases\/([^/]+)\/promote$/)) && req.method === 'POST') {
          sendJson(res, 200, { release: await promoteRelease(decodeURIComponent(match[1]), body) });
          return;
        }
        if ((match = m(/^\/api\/releases\/([^/]+)\/revoke$/)) && req.method === 'POST') {
          sendJson(res, 200, { release: await revokeRelease(decodeURIComponent(match[1]), body) });
          return;
        }
        if ((match = m(/^\/api\/releases\/([^/]+)\/rollback$/)) && req.method === 'POST') {
          sendJson(res, 200, await rollbackRelease(decodeURIComponent(match[1]), body));
          return;
        }
        if ((match = m(/^\/api\/releases\/([^/]+)\/dispatch$/)) && req.method === 'POST') {
          sendJson(res, 200, { release: await dispatchNow(decodeURIComponent(match[1])) });
          return;
        }
        if ((match = m(/^\/api\/releases\/([^/]+)\/deliveries\/([^/]+)\/retry$/)) && req.method === 'POST') {
          sendJson(res, 200, {
            release: await retryDelivery(decodeURIComponent(match[1]), decodeURIComponent(match[2])),
          });
          return;
        }
        if ((match = m(/^\/api\/releases\/([^/]+)\/simulate\/(duplicate-ack|late-ack)$/)) && req.method === 'POST') {
          const consumer = String(body.consumer ?? '');
          if (!CONSUMER_NAMES.includes(consumer)) throw new HttpError(400, 'unknown consumer');
          const releaseId = decodeURIComponent(match[1]);
          const kind = match[2];
          const result = kind === 'duplicate-ack'
            ? await simulateDuplicateAck(releaseId, consumer)
            : await simulateLateAck(releaseId, consumer);
          sendJson(res, 200, { kind, result, state: stateSnapshot() });
          return;
        }
        if (route === 'POST /api/policy') {
          const policy = {};
          if (body.promotionMode) {
            if (!['all-converged', 'quorum-with-lag'].includes(body.promotionMode)) {
              throw new HttpError(400, 'invalid promotionMode');
            }
            policy.promotionMode = body.promotionMode;
          }
          if (body.quorum !== undefined) policy.quorum = Math.max(1, Math.min(3, Number(body.quorum) || 2));
          for (const key of ['waitWindowMs', 'lagWindowMs', 'compatWindowMs', 'leaseMs']) {
            if (body[key] !== undefined) {
              policy[key] = Math.max(1, Math.min(30 * 24 * 3600 * 1000, Number(body[key]) || 0));
            }
          }
          await store.mutate('policy.updated', { policy });
          sendJson(res, 200, { policy: store.state.policy });
          return;
        }
        if (route === 'GET /api/journal') {
          sendJson(res, 200, { head: store.journal.head, events: store.journal.events.slice(-400) });
          return;
        }
        if ((match = m(/^\/api\/consumers\/([^/]+)\/state$/)) && req.method === 'GET') {
          const name = decodeURIComponent(match[1]);
          if (!CONSUMER_NAMES.includes(name)) throw new HttpError(400, 'unknown consumer');
          try {
            const res2 = await fetch(`${consumerBase(name)}/state`, { signal: AbortSignal.timeout(1200) });
            sendJson(res, 200, await res2.json());
          } catch (err) {
            throw new HttpError(502, `consumer ${name} unreachable: ${err.message}`);
          }
          return;
        }
        if ((match = m(/^\/api\/consumers\/([^/]+)\/(start|stop|restart)$/)) && req.method === 'POST') {
          const name = decodeURIComponent(match[1]);
          const action = match[2];
          if (!CONSUMER_NAMES.includes(name)) throw new HttpError(400, 'unknown consumer');
          const result = await supervisorAction(`/${action}/${name}`);
          sendJson(res, 200, result);
          return;
        }
        if ((match = m(/^\/api\/supervisor\/([^/]+)\/(start|stop|restart|crash)$/)) && req.method === 'POST') {
          const name = decodeURIComponent(match[1]);
          if (name !== 'publisher' && !CONSUMER_NAMES.includes(name)) throw new HttpError(400, 'unknown service');
          // For 'publisher' the supervisor answers before it kills this
          // process, so the operator still gets a response.
          const result = await supervisorAction(`/${match[2]}/${name}`, body);
          sendJson(res, 200, result);
          return;
        }
        if (route === 'POST /bus/ack') {
          const result = await acceptAck(body);
          sendJson(res, result.accepted || result.duplicate ? 200 : 200, result);
          return;
        }
        if (route === 'GET /api/health') {
          sendJson(res, 200, { ok: true, journalHead: store.journal.head, uptime: process.uptime() });
          return;
        }
        throw new HttpError(404, 'not found');
      }

      // Static web console
      let pathname = url.pathname === '/' ? '/index.html' : url.pathname;
      const safe = normalize(pathname).replace(/^(\.\.[/\\])+/, '');
      const filePath = join(PUBLIC_DIR, safe);
      if (!filePath.startsWith(PUBLIC_DIR)) throw new HttpError(403, 'forbidden');
      const info = await stat(filePath).catch(() => null);
      if (!info || !info.isFile()) throw new HttpError(404, 'not found');
      const content = await readFile(filePath);
      res.writeHead(200, {
        'content-type': MIME[extname(filePath)] ?? 'application/octet-stream',
        'cache-control': 'no-cache',
        'content-length': content.length,
      });
      res.end(content);
    } catch (err) {
      if (err instanceof HttpError) {
        sendJson(res, err.status, { error: err.message, details: err.details ?? null });
      } else {
        process.stderr.write(`[publisher] request error: ${err.stack ?? err}\n`);
        sendJson(res, 500, { error: err.message });
      }
    }
  });

  await new Promise((resolveListen, reject) => {
    server.once('error', reject);
    server.listen(port, '127.0.0.1', resolveListen);
  });
  const actualPort = server.address().port;

  if (options.autoPoll !== false) {
    const initialTimer = setTimeout(pollLoop, options.pollIntervalMs ?? 400);
    initialTimer.unref?.();
    runtime.timers.push(initialTimer);
  }

  const close = async () => {
    runtime.shuttingDown = true;
    for (const timer of runtime.timers) clearTimeout(timer);
    server.closeAllConnections?.();
    await new Promise((r) => server.close(r));
    await store.close();
  };

  return {
    port: actualPort, store, server, runtime, close,
    stateSnapshot, evaluatePromotion: (id) => evaluatePromotion(store, store.getRelease(id)),
    sweep,
  };
}

async function main() {
  const { values } = parseArgs({ options: { port: { type: 'string' }, data: { type: 'string' } } });
  const publisher = await startPublisher({
    port: values.port ? Number(values.port) : undefined,
    dataDir: values.data,
  });
  process.stdout.write(
    `[publisher] release console on http://127.0.0.1:${publisher.port} (journal head ${publisher.store.journal.head})\n`,
  );
  let closing = false;
  const shutdown = async (signal) => {
    if (closing) return;
    closing = true;
    process.stdout.write(`[publisher] ${signal} received, closing...\n`);
    await publisher.close();
    process.exit(0);
  };
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch((err) => {
    process.stderr.write(`${err.stack ?? err}\n`);
    process.exit(1);
  });
}
