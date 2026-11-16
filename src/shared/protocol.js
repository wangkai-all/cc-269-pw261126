/**
 * Wire protocol shared by publisher, consumers and tests.
 *
 * The "bus" is local HTTP owned by the publisher. No external message broker
 * is involved: every dispatch is an explicit, persisted HTTP request so the
 * console can show exactly which version reached which downstream.
 */

export const PROTOCOL_VERSION = 1;

export const CONSUMER_NAMES = ['alpha', 'beta', 'gamma'];

export const DEFAULT_PORTS = {
  supervisor: 4600,
  publisher: 4601,
  alpha: 4611,
  beta: 4612,
  gamma: 4613,
};

/** Consumer capabilities. v2 consumers understand the v2 schema; gamma cannot. */
export const CONSUMER_PROFILES = {
  alpha: { schema: 2 },
  beta: { schema: 2 },
  gamma: { schema: 1 },
};

export const CONSUMER_API = {
  delivery: '/delivery',
  cancel: '/cancel',
  state: '/state',
  health: '/health',
};

export function ackSignature({ consumer, releaseId, version, appliedSeq, dispatchSeq, idempotencyKey }) {
  return [consumer, releaseId, version, appliedSeq, dispatchSeq, idempotencyKey].join('|');
}


export function consumerUrl(name) {
  const envKey = `CONSUMER_${name.toUpperCase()}_URL`;
  const override = process.env[envKey];
  if (override) return override.replace(/\/$/, '');
  return `http://127.0.0.1:${DEFAULT_PORTS[name]}`;
}

export function publisherUrl() {
  return (process.env.PUBLISHER_URL ?? `http://127.0.0.1:${DEFAULT_PORTS.publisher}`).replace(/\/$/, '');
}
