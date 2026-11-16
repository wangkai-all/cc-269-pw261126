import { EventJournal } from '../shared/journal.js';
import { createMutex, nowIso, sha256 } from '../shared/util.js';

/**
 * Publisher state machine, persisted as an append-only event journal and
 * rebuilt purely by replay. No state lives only in memory.
 *
 * Release lifecycle
 *   DRAFT ──publish──▶ PUBLISHED ──promote──▶ PROMOTED ──supersede──▶ SUPERSEDED
 *                         │  │                  │
 *                         │  └──revoke──▶ REVOKED
 *                         └─────rollback──▶ ROLLED_BACK (a new forward release is created)
 *
 * Delivery status per downstream, per release:
 *   PENDING ─▶ DISPATCHED ─▶ ACKED          (ACKED is the only "effective" proof)
 *      ▲           │
 *      └── retry ──┴──▶ FAILED / TIMEOUT     (attempt, never treated as delivered)
 *                  └──▶ REJECTED             (structural refusal; retrying alone cannot fix)
 */
export class PublisherStore {
  constructor({ dataDir }) {
    this.dataDir = dataDir;
    this.journal = new EventJournal(`${dataDir}/journal.jsonl`);
    this.mutex = createMutex();
    this.state = {
      releases: new Map(),
      consumerVersion: new Map(),
      consumption: new Map(),
      promotions: [],
      schemaIntros: new Map(), // schema -> introducedAt (compat window origin)
      nextOrdinal: 1,
      lastError: null,
      epoch: 0,
      counters: { duplicateAcks: 0, lateAcks: 0, staleAcks: 0, dispatches: 0, rejections: 0 },
      policy: {
        // Business policy knobs; see docs/RELEASE_POLICY.md
        promotionMode: 'all-converged', // 'all-converged' | 'quorum-with-lag'
        quorum: 2,
        waitWindowMs: 15 * 60 * 1000,
        lagWindowMs: 15 * 60 * 1000,
        compatWindowMs: 24 * 60 * 60 * 1000,
        leaseMs: 2500,
        retryMaxDelayMs: 4000,
      },
    };
  }

  async init() {
    await this.journal.init();
    this.journal.replay((event) => this.apply(event));
    return this;
  }

  apply(event) {
    switch (event.type) {
      case 'release.created': {
        this.state.releases.set(event.releaseId, {
          releaseId: event.releaseId,
          ordinal: event.ordinal,
          version: event.version,
          schema: event.schema,
          cohort: event.cohort,
          notes: event.notes ?? '',
          payload: event.payload,
          payloadHash: event.payloadHash,
          state: 'DRAFT',
          createdAt: event.at,
          publishedAt: null,
          dispatchedAt: null,
          promotedAt: null,
          revokedAt: null,
          rolledBackAt: null,
          supersededAt: null,
          rollbackOf: event.rollbackOf ?? null,
          revocation: null,
          deliveries: Object.fromEntries(
            Object.keys(event.consumers).map((name) => [name, {
              consumer: name,
              status: 'PENDING',
              dispatchSeq: 0,
              attempts: 0,
              dispatchedAt: null,
              leaseUntil: null,
              ackAt: null,
              appliedSeq: null,
              idempotencyKey: null,
              rejection: null,
              lastError: null,
            }]),
          ),
        });
        this.state.nextOrdinal = Math.max(this.state.nextOrdinal, event.ordinal + 1);
        break;
      }
      case 'release.published': {
        const rel = this.state.releases.get(event.releaseId);
        if (!rel || rel.state !== 'DRAFT') break;
        rel.state = 'PUBLISHED';
        rel.publishedAt = event.at;
        // Supersede older in-flight releases in the same atomic transition, so
        // a crash can never leave a release stranded without an owner.
        for (const olderId of event.supersedes ?? []) {
          const older = this.state.releases.get(olderId);
          if (older && older.state === 'PUBLISHED') {
            older.state = 'SUPERSEDED';
            older.supersededAt = event.at;
          }
        }
        // The compatibility window for a schema starts when that schema is
        // first made visible to downstreams, not when the draft was written.
        if (!this.state.schemaIntros.has(rel.schema)) {
          this.state.schemaIntros.set(rel.schema, {
            schema: rel.schema,
            introducedAt: event.at,
            releaseId: rel.releaseId,
          });
        }
        break;
      }
      case 'release.promoted': {
        const rel = this.state.releases.get(event.releaseId);
        if (!rel || !['PUBLISHED', 'PROMOTED'].includes(rel.state)) break;
        if (rel.state === 'PROMOTED') break;
        rel.state = 'PROMOTED';
        rel.promotedAt = event.at;
        rel.promotion = {
          mode: event.mode,
          forced: Boolean(event.forced),
          lagMs: event.lagMs,
          effectiveVersions: event.effectiveVersions,
          lagDeadlines: event.lagDeadlines ?? {},
          compatException: Boolean(event.compatException),
          at: event.at,
        };
        for (const other of this.state.releases.values()) {
          if (other.releaseId !== rel.releaseId && other.state === 'PROMOTED') {
            other.state = 'SUPERSEDED';
            other.supersededAt = event.at;
          }
        }
        this.state.promotions.push({
          releaseId: rel.releaseId,
          ordinal: rel.ordinal,
          version: rel.version,
          schema: rel.schema,
          at: event.at,
          mode: event.mode,
          forced: Boolean(event.forced),
          effectiveVersions: event.effectiveVersions,
        });
        break;
      }
      case 'release.rollback': {
        // One atomic transition: the target becomes ROLLED_BACK and the
        // forward replacement is created already PUBLISHED. A crash can never
        // observe "rolled back but no replacement".
        const target = this.state.releases.get(event.releaseId);
        const repl = event.replacement;
        if (!target || !repl) break;
        // Revocation is terminal: the replacement is still a valid forward
        // release, but a revoked target keeps its state.
        if (['PUBLISHED', 'PROMOTED', 'SUPERSEDED'].includes(target.state)) {
          target.state = 'ROLLED_BACK';
          target.rolledBackAt = event.at;
          target.rollback = {
            reason: event.reason,
            at: event.at,
            replacementReleaseId: repl.releaseId,
          };
        }
        this.state.releases.set(repl.releaseId, {
          releaseId: repl.releaseId,
          ordinal: repl.ordinal,
          version: repl.version,
          schema: repl.schema,
          cohort: repl.cohort,
          notes: repl.notes ?? '',
          payload: repl.payload,
          payloadHash: repl.payloadHash,
          state: 'PUBLISHED',
          createdAt: event.at,
          publishedAt: event.at,
          dispatchedAt: null,
          promotedAt: null,
          revokedAt: null,
          rolledBackAt: null,
          supersededAt: null,
          rollbackOf: event.releaseId,
          revocation: null,
          deliveries: Object.fromEntries(
            Object.keys(repl.consumers).map((name) => [name, {
              consumer: name,
              status: 'PENDING',
              dispatchSeq: 0,
              attempts: 0,
              dispatchedAt: null,
              leaseUntil: null,
              ackAt: null,
              appliedSeq: null,
              idempotencyKey: null,
              rejection: null,
              lastError: null,
            }]),
          ),
        });
        this.state.nextOrdinal = Math.max(this.state.nextOrdinal, repl.ordinal + 1);
        if (!this.state.schemaIntros.has(repl.schema)) {
          this.state.schemaIntros.set(repl.schema, {
            schema: repl.schema,
            introducedAt: event.at,
            releaseId: repl.releaseId,
          });
        }
        break;
      }
      case 'release.revoked': {
        const rel = this.state.releases.get(event.releaseId);
        if (!rel || ['REVOKED', 'ROLLED_BACK'].includes(rel.state)) break;
        rel.state = 'REVOKED';
        rel.revokedAt = event.at;
        rel.revocation = {
          reason: event.reason,
          at: event.at,
          policy: event.policy,
          targets: event.targets,
          results: {},
        };
        break;
      }
      case 'revocation.result': {
        const rel = this.state.releases.get(event.releaseId);
        if (!rel?.revocation) break;
        rel.revocation.results[event.consumer] = {
          disposition: event.disposition,
          expect: event.expect,
          ok: Boolean(event.ok),
          error: event.error ?? null,
          at: event.at,
        };
        // A consumer that reports "quarantined" had applied the release even
        // if its acknowledgement never reached us. Record the observed fact,
        // but never upgrade it to a confirmed/effective version.
        if (event.disposition === 'quarantined') {
          const known = this.state.consumption.get(event.consumer)?.get(event.releaseId);
          if (!known) {
            if (!this.state.consumption.has(event.consumer)) {
              this.state.consumption.set(event.consumer, new Map());
            }
            this.state.consumption.get(event.consumer).set(event.releaseId, {
              releaseId: rel.releaseId,
              ordinal: rel.ordinal,
              version: rel.version,
              schema: rel.schema,
              payloadHash: rel.payloadHash,
              appliedSeq: null,
              at: event.at,
              quarantined: true,
              confirmed: false,
            });
          } else {
            known.quarantined = true;
          }
        }
        break;
      }
      case 'delivery.dispatched': {
        const rel = this.state.releases.get(event.releaseId);
        const d = rel?.deliveries[event.consumer];
        if (!d) break;
        if (event.dispatchSeq < d.dispatchSeq) break; // never regress on replay
        d.status = 'DISPATCHED';
        d.dispatchSeq = event.dispatchSeq;
        d.attempts += 1;
        d.dispatchedAt = event.at;
        d.leaseUntil = event.leaseUntil;
        d.idempotencyKey = event.idempotencyKey;
        d.rejection = null;
        d.lastError = null;
        this.state.counters.dispatches += 1;
        if (rel.dispatchedAt === null) rel.dispatchedAt = event.at;
        break;
      }
      case 'delivery.rejected': {
        const rel = this.state.releases.get(event.releaseId);
        const d = rel?.deliveries[event.consumer];
        if (!d || d.status === 'ACKED' || event.dispatchSeq < d.dispatchSeq) break;
        d.status = event.terminal ? 'REJECTED' : 'FAILED';
        d.rejection = {
          reason: event.reason,
          detail: event.detail ?? null,
          at: event.at,
          terminal: Boolean(event.terminal),
        };
        this.state.counters.rejections += 1;
        break;
      }
      case 'delivery.timed_out': {
        const rel = this.state.releases.get(event.releaseId);
        const d = rel?.deliveries[event.consumer];
        if (!d || d.status === 'ACKED' || d.status === 'REJECTED' || event.dispatchSeq < d.dispatchSeq) break;
        d.status = 'TIMEOUT';
        d.lastError = 'no acknowledgement before lease expired';
        break;
      }
      case 'delivery.retried': {
        const rel = this.state.releases.get(event.releaseId);
        const d = rel?.deliveries[event.consumer];
        if (!d || d.status === 'ACKED' || d.status === 'REJECTED') break;
        d.status = 'PENDING';
        d.rejection = null;
        d.lastError = null;
        d.leaseUntil = null;
        break;
      }
      case 'ack.duplicate': {
        this.state.counters.duplicateAcks += 1;
        break;
      }
      case 'ack.stale': {
        this.state.counters.staleAcks += 1;
        break;
      }
      case 'delivery.acked': {
        const rel = this.state.releases.get(event.releaseId);
        const d = rel?.deliveries[event.consumer];
        if (!d) break;
        if (event.late) this.state.counters.lateAcks += 1;
        d.status = 'ACKED';
        d.ackAt = event.at;
        d.appliedSeq = event.appliedSeq;
        d.dispatchSeq = Math.max(d.dispatchSeq, event.dispatchSeq ?? 0);
        d.idempotencyKey = event.idempotencyKey;
        d.rejection = null;
        d.lastError = null;
        if (!d.dispatchedAt) d.dispatchedAt = event.at;
        if (rel.ackedAt == null) rel.ackedAt = event.at;
        this.markConsumed(event.consumer, rel, event);
        break;
      }
      case 'policy.updated': {
        Object.assign(this.state.policy, event.policy);
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

  markConsumed(name, rel, event) {
    if (!this.state.consumption.has(name)) this.state.consumption.set(name, new Map());
    this.state.consumption.get(name).set(rel.releaseId, {
      releaseId: rel.releaseId,
      ordinal: rel.ordinal,
      version: rel.version,
      schema: rel.schema,
      payloadHash: rel.payloadHash,
      appliedSeq: event.appliedSeq ?? null,
      at: event.at,
      quarantined: rel.state === 'REVOKED',
      confirmed: true,
    });
    const current = this.state.consumerVersion.get(name);
    if (!current || (event.appliedSeq ?? 0) >= (current.appliedSeq ?? 0)) {
      this.state.consumerVersion.set(name, {
        version: rel.version,
        releaseId: rel.releaseId,
        ordinal: rel.ordinal,
        schema: rel.schema,
        appliedSeq: event.appliedSeq ?? null,
        at: event.at,
      });
    }
  }

  async append(type, payload) {
    return this.journal.append(type, payload, (event) => this.apply(event));
  }

  mutate(type, payload) {
    return this.mutex(() => this.append(type, payload));
  }

  async transaction(fn) {
    return this.mutex(fn);
  }

  getRelease(releaseId) {
    return this.state.releases.get(releaseId) ?? null;
  }

  releasesNewestFirst() {
    return [...this.state.releases.values()].sort((a, b) => b.ordinal - a.ordinal);
  }

  /** Newest release still in flight or in production — the one to watch. */
  currentRelease() {
    return this.releasesNewestFirst().find((r) => ['PUBLISHED', 'PROMOTED'].includes(r.state)) ?? null;
  }

  /** The release currently marked as production, if any. */
  productionRelease() {
    return this.releasesNewestFirst().find((r) => r.state === 'PROMOTED') ?? null;
  }

  nextVersionName() {
    return `v${this.state.nextOrdinal}`;
  }

  newReleaseId() {
    return `rel-${sha256(`${nowIso()}:${this.state.nextOrdinal}:${Math.random()}`).slice(0, 10)}`;
  }

  close() {
    return this.journal.close();
  }
}
