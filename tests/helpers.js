import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { startPublisher } from '../src/publisher/server.js';
import { startConsumer } from '../src/consumer/agent.js';
import { CONSUMER_NAMES, CONSUMER_PROFILES } from '../src/shared/protocol.js';

export async function makeTemp(prefix = 'rc-test-') {
  return mkdtemp(join(tmpdir(), prefix));
}

export async function jsonFetch(url, options = {}) {
  const res = await fetch(url, {
    method: options.method ?? 'GET',
    headers: options.body ? { 'content-type': 'application/json' } : {},
    body: options.body ? JSON.stringify(options.body) : undefined,
    signal: AbortSignal.timeout(options.timeout ?? 5000),
  });
  let data = null;
  try { data = await res.json(); } catch { /* ignore */ }
  return { status: res.status, ok: res.ok, data };
}

export async function waitFor(fn, { timeoutMs = 8000, intervalMs = 60, label = 'condition' } = {}) {
  const deadline = Date.now() + timeoutMs;
  let lastErr = null;
  while (Date.now() < deadline) {
    try {
      const value = await fn();
      if (value) return value;
    } catch (err) {
      lastErr = err;
    }
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  throw new Error(`timed out waiting for ${label}${lastErr ? `: ${lastErr.message}` : ''}`);
}

/**
 * Boot a publisher + three consumers on ephemeral ports with manual sweeps so
 * tests control timing. Returns helpers to drive the protocol.
 */
export async function bootStack({ policy, schemas = {} } = {}) {
  const root = await makeTemp();
  const consumerUrls = {};
  let publisher = await startPublisher({
    port: 0,
    dataDir: join(root, 'publisher'),
    consumerUrls,
    autoPoll: false,
    policy,
  });
  let base = `http://127.0.0.1:${publisher.port}`;

  const consumers = {};
  for (const name of CONSUMER_NAMES) {
    consumers[name] = await startConsumer({
      name,
      port: 0,
      dataDir: join(root, name),
      publisherUrl: base,
      supportedSchema: schemas[name] ?? CONSUMER_PROFILES[name].schema,
    });
    consumerUrls[name] = `http://127.0.0.1:${consumers[name].port}`;
  }

  async function createRelease(body) {
    const res = await jsonFetch(`${base}/api/releases`, { method: 'POST', body });
    if (!res.ok) throw new Error(`create failed: ${res.data?.error}`);
    return res.data.release;
  }

  async function publish(releaseId) {
    const res = await jsonFetch(`${base}/api/releases/${releaseId}/publish`, { method: 'POST' });
    if (!res.ok) throw new Error(`publish failed: ${res.data?.error}`);
    return res.data.release;
  }

  async function sweep(times = 3) {
    for (let i = 0; i < times; i += 1) {
      await publisher.sweep();
      await new Promise((r) => setTimeout(r, 60));
    }
  }

  async function state() {
    return (await jsonFetch(`${base}/api/state`)).data;
  }

  async function close() {
    await Promise.all(Object.values(consumers).map((c) => c.close().catch(() => {})));
    await publisher.close();
    await rm(root, { recursive: true, force: true });
  }

  return {
    root,
    get publisher() { return publisher; },
    set publisher(next) { publisher = next; },
    consumers,
    consumerUrls,
    createRelease,
    publish,
    sweep,
    state,
    close,
    jsonFetch,
    consumerBase: (name) => consumerUrls[name],
    get base() { return base; },
    /** Point the harness at a restarted publisher without rebuilding consumers. */
    rebind(nextBase) {
      base = nextBase;
      return base;
    },
  };
}
