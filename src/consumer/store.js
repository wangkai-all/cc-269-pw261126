import { EventJournal } from '../shared/journal.js';
import { createMutex } from '../shared/util.js';
import { SchemaError, validatePayload } from '../shared/schema.js';

/**
 * Durable consumer replica.
 *
 * Every applied release is journaled before it is acknowledged. A release is
 * applied at most once per releaseId, and never at an ordinal lower than the
 * current one. Revocation quarantines consumed data locally; it never silently
 * deletes it, and it never rolls the visible version backwards.
 */
export class ConsumerStore {
  constructor({ name, dataDir, supportedSchema, publisherUrl }) {
    this.name = name;
    this.dataDir = dataDir;
    this.publisherUrl = publisherUrl;
    this.supportedSchema = supportedSchema;
    this.journal = new EventJournal(`${dataDir}/journal.jsonl`);
    this.mutex = createMutex();
    this.state = {
      name,
      supportedSchema,
      currentReleaseId: null,
      currentVersion: null,
      currentOrdinal: 0,
      appliedSeq: 0,
      serving: true,
      applications: new Map(), // releaseId -> application record
      rejected: new Map(), // releaseId -> {reason, detail, at}
      tombstones: new Set(), // revoked releaseIds the consumer must never apply
      outbox: new Map(), // releaseId -> ack awaiting acceptance
      counters: {
        applied: 0,
        duplicateDeliveries: 0,
        staleDeliveries: 0,
        rejected: 0,
        quarantined: 0,
        acksAccepted: 0,
        acksRejected: 0,
        ackRetries: 0,
      },
      lastError: null,
    };
  }

  async init() {
    await this.journal.init();
    this.journal.replay((event) => this.apply(event));
    return this;
  }

  apply(event) {
    switch (event.type) {
      case 'release.applied': {
        const app = {
          releaseId: event.releaseId,
          ordinal: event.ordinal,
          version: event.version,
          schema: event.schema,
          cohort: event.cohort,
          payloadHash: event.payloadHash,
          payload: event.payload,
          appliedSeq: event.appliedSeq,
          dispatchSeq: event.dispatchSeq,
          idempotencyKey: event.idempotencyKey,
          state: 'serving',
          appliedAt: event.at,
          quarantinedAt: null,
          quarantineReason: null,
          ackDelivered: false,
        };
        this.state.applications.set(event.releaseId, app);
        this.state.counters.applied += 1;
        if (event.ordinal >= this.state.currentOrdinal) {
          this.state.currentOrdinal = event.ordinal;
          this.state.currentReleaseId = event.releaseId;
          this.state.currentVersion = event.version;
          this.state.serving = true;
        }
        this.state.appliedSeq = Math.max(this.state.appliedSeq, event.appliedSeq);
        if (event.acknowledged !== true) {
          this.state.outbox.set(event.releaseId, {
            releaseId: event.releaseId,
            version: event.version,
            dispatchSeq: event.dispatchSeq,
            idempotencyKey: event.idempotencyKey,
            appliedSeq: event.appliedSeq,
            payloadHash: event.payloadHash,
            at: event.at,
          });
        }
        break;
      }
      case 'release.quarantined': {
        const app = this.state.applications.get(event.releaseId);
        if (app && app.state !== 'quarantined') {
          app.state = 'quarantined';
          app.quarantinedAt = event.at;
          app.quarantineReason = event.reason;
          this.state.counters.quarantined += 1;
          if (this.state.currentReleaseId === event.releaseId) {
            this.state.serving = false;
          }
        }
        this.state.tombstones.add(event.releaseId);
        break;
      }
      case 'release.tombstoned': {
        this.state.tombstones.add(event.releaseId);
        break;
      }
      case 'release.rejected': {
        this.state.rejected.set(event.releaseId, {
          releaseId: event.releaseId,
          reason: event.reason,
          detail: event.detail ?? null,
          at: event.at,
        });
        this.state.counters.rejected += 1;
        break;
      }
      case 'delivery.duplicate': {
        this.state.counters.duplicateDeliveries += 1;
        break;
      }
      case 'delivery.stale': {
        this.state.counters.staleDeliveries += 1;
        break;
      }
      case 'ack.accepted': {
        this.state.outbox.delete(event.releaseId);
        const app = this.state.applications.get(event.releaseId);
        if (app) app.ackDelivered = true;
        this.state.counters.acksAccepted += 1;
        break;
      }
      case 'ack.retry': {
        this.state.counters.ackRetries += 1;
        break;
      }
      case 'error': {
        this.state.lastError = { message: event.message, at: event.at };
        break;
      }
      default:
        break;
    }
    this.state.epoch = event.seq;
  }

  async append(type, payload) {
    return this.journal.append(type, payload, (event) => this.apply(event));
  }

  async receiveDelivery(delivery) {
    return this.mutex(async () => {
      const {
        releaseId, ordinal, version, schema, cohort, payloadHash, payload,
        dispatchSeq, idempotencyKey,
      } = delivery;

      if (!releaseId || !Number.isInteger(ordinal) || !version) {
        return { ok: false, status: 400, body: { reason: 'malformed-delivery' } };
      }

      if (this.state.tombstones.has(releaseId)) {
        return {
          ok: true,
          status: 200,
          body: { applied: false, revoked: true, reason: 'release-revoked', dispatchSeq },
        };
      }

      const existing = this.state.applications.get(releaseId);
      if (existing) {
        await this.append('delivery.duplicate', {
          releaseId, dispatchSeq, idempotencyKey,
        });
        this.refreshOutbox(existing, dispatchSeq, idempotencyKey);
        setImmediate(() => { this.flushAcks().catch(() => {}); });
        return {
          ok: true,
          status: 200,
          body: {
            applied: false,
            duplicate: true,
            quarantined: existing.state === 'quarantined',
            appliedSeq: existing.appliedSeq,
            dispatchSeq,
          },
        };
      }

      // Explicit structural rejection. Nothing is stored or applied.
      try {
        validatePayload(payload, this.supportedSchema);
      } catch (err) {
        const detail = err instanceof SchemaError ? err.details : null;
        await this.append('release.rejected', {
          releaseId, reason: err.message, detail,
        });
        return {
          ok: true,
          status: 200,
          body: {
            applied: false, rejected: true, reason: err.message, detail,
            supportedSchema: this.supportedSchema, dispatchSeq,
          },
        };
      }

      if (ordinal <= this.state.currentOrdinal && releaseId !== this.state.currentReleaseId) {
        await this.append('delivery.stale', {
          releaseId, ordinal, currentOrdinal: this.state.currentOrdinal,
        });
        return {
          ok: true,
          status: 200,
          body: {
            applied: false, stale: true,
            currentVersion: this.state.currentVersion,
            currentReleaseId: this.state.currentReleaseId,
            dispatchSeq,
          },
        };
      }

      const appliedSeq = this.state.appliedSeq + 1;
      await this.append('release.applied', {
        releaseId, ordinal, version, schema, cohort, payloadHash, payload,
        appliedSeq, dispatchSeq, idempotencyKey,
      });
      setImmediate(() => { this.flushAcks().catch(() => {}); });
      return {
        ok: true,
        status: 200,
        body: { applied: true, appliedSeq, version, dispatchSeq },
      };
    });
  }

  refreshOutbox(app, dispatchSeq, idempotencyKey) {
    const entry = this.state.outbox.get(app.releaseId);
    if (!entry) {
      if (app.ackDelivered) return;
      // Re-create an outbox entry only if the ack was never accepted.
      this.state.outbox.set(app.releaseId, {
        releaseId: app.releaseId,
        version: app.version,
        dispatchSeq,
        idempotencyKey,
        appliedSeq: app.appliedSeq,
        payloadHash: app.payloadHash,
        at: new Date().toISOString(),
      });
      return;
    }
    if (dispatchSeq > entry.dispatchSeq) {
      entry.dispatchSeq = dispatchSeq;
      entry.idempotencyKey = idempotencyKey;
    }
  }

  /** Apply an emergency revocation. Idempotent and safe for unseen releases. */
  async cancelRelease(releaseId, reason = 'publisher-revoked') {
    return this.mutex(async () => {
      if (this.state.tombstones.has(releaseId)) {
        const app = this.state.applications.get(releaseId);
        return {
          disposition: app?.state === 'quarantined' ? 'quarantined' : 'not-consumed',
          duplicate: true,
        };
      }
      const app = this.state.applications.get(releaseId);
      if (app) {
        await this.append('release.quarantined', { releaseId, reason });
        this.state.outbox.delete(releaseId);
        return { disposition: 'quarantined', appliedSeq: app.appliedSeq };
      }
      await this.append('release.tombstoned', { releaseId, reason });
      return { disposition: 'not-consumed' };
    });
  }

  ackBody(entry) {
    return {
      protocol: 1,
      consumer: this.name,
      releaseId: entry.releaseId,
      version: entry.version,
      payloadHash: entry.payloadHash,
      appliedSeq: entry.appliedSeq,
      dispatchSeq: entry.dispatchSeq,
      idempotencyKey: entry.idempotencyKey,
      at: new Date().toISOString(),
    };
  }

  /**
   * Ack flushing is serialized: the retry timer, request handlers and the
   * post-delivery kick can all fire at once, and concurrent flushes would
   * duplicate acknowledgements and journal noise.
   */
  flushAcks() {
    if (!this.flushChain) this.flushChain = Promise.resolve();
    const run = this.flushChain.then(() => this.flushAcksOnce());
    this.flushChain = run.catch(() => {});
    return run;
  }

  async flushAcksOnce() {
    const entries = [...this.state.outbox.values()];
    for (const entry of entries) {
      let response;
      try {
        response = await fetch(`${this.publisherUrl}/bus/ack`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify(this.ackBody(entry)),
          signal: AbortSignal.timeout(2500),
        });
      } catch {
        // Publisher is down. The outbox is durable; retry later.
        return;
      }
      let data = null;
      try { data = await response.json(); } catch { /* ignore */ }
      if (!response.ok) {
        await this.append('ack.retry', {
          releaseId: entry.releaseId,
          status: response.status,
          reason: data?.error ?? 'ack-rejected',
        });
        continue;
      }
      if (data?.accepted) {
        await this.append('ack.accepted', {
          releaseId: entry.releaseId,
          duplicate: Boolean(data.duplicate),
          revoked: Boolean(data.revoked),
        });
        if (data.revoked) {
          await this.cancelRelease(entry.releaseId, 'publisher-revoked-late-ack');
        }
        continue;
      }
      if (data?.revoked) {
        await this.append('ack.retry', {
          releaseId: entry.releaseId,
          reason: data.error ?? 'release-revoked',
        });
        await this.cancelRelease(entry.releaseId, 'publisher-revoked-late-ack');
        continue;
      }
      if (data?.stale) {
        // A newer dispatch superseded this confirmation. Its ack will carry
        // the new dispatchSeq; do not keep the old one forever.
        await this.append('ack.retry', {
          releaseId: entry.releaseId,
          reason: 'stale-confirmation',
        });
        this.state.outbox.delete(entry.releaseId);
      }
    }
  }

  snapshot() {
    const applications = [...this.state.applications.values()].map((a) => ({
      releaseId: a.releaseId,
      ordinal: a.ordinal,
      version: a.version,
      schema: a.schema,
      cohort: a.cohort,
      state: a.state,
      appliedSeq: a.appliedSeq,
      appliedAt: a.appliedAt,
      quarantinedAt: a.quarantinedAt,
      rowCount: a.payload?.manifest?.rowCount ?? null,
      sample: a.payload?.rows?.slice(0, 4) ?? [],
    }));
    return {
      name: this.name,
      supportedSchema: this.supportedSchema,
      currentReleaseId: this.state.currentReleaseId,
      currentVersion: this.state.currentVersion,
      currentOrdinal: this.state.currentOrdinal,
      serving: this.state.serving,
      appliedSeq: this.state.appliedSeq,
      applications,
      rejected: [...this.state.rejected.values()],
      outbox: [...this.state.outbox.values()],
      tombstones: [...this.state.tombstones],
      counters: this.state.counters,
      lastError: this.state.lastError,
      epoch: this.state.epoch,
      recentEvents: this.journal.events.slice(-40),
    };
  }
}
