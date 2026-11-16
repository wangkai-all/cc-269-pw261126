import { createServer } from 'node:http';
import { parseArgs } from 'node:util';
import { mkdir } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { ConsumerStore } from './store.js';
import { CONSUMER_PROFILES, DEFAULT_PORTS, publisherUrl } from '../shared/protocol.js';
import { nowIso } from '../shared/util.js';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '../..');

function parseOptions() {
  const { values } = parseArgs({
    options: {
      name: { type: 'string' },
      port: { type: 'string' },
      data: { type: 'string' },
      publisher: { type: 'string' },
      schema: { type: 'string' },
    },
  });
  const name = values.name ?? process.env.CONSUMER_NAME;
  if (!name || !CONSUMER_PROFILES[name]) {
    throw new Error('pass --name alpha|beta|gamma');
  }
  const profile = CONSUMER_PROFILES[name];
  return {
    name,
    port: Number(values.port ?? process.env.PORT ?? DEFAULT_PORTS[name]),
    dataDir: resolve(values.data ?? process.env.CONSUMER_DATA_DIR ?? `${ROOT}/.data/${name}`),
    publisherUrl: (values.publisher ?? publisherUrl()).replace(/\/$/, ''),
    supportedSchema: Number(values.schema ?? process.env.CONSUMER_SCHEMA ?? profile.schema),
  };
}

function json(res, status, body) {
  const text = JSON.stringify(body);
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'content-length': Buffer.byteLength(text),
  });
  res.end(text);
}

async function readJson(req) {
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > 8 * 1024 * 1024) throw new Error('body too large');
    chunks.push(chunk);
  }
  if (!chunks.length) return {};
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}

export async function startConsumer(options) {
  const autoRetryAcks = options.autoRetryAcks !== false;
  const { name, port, dataDir, publisherUrl, supportedSchema } = options;
  await mkdir(dataDir, { recursive: true });

  const store = new ConsumerStore({ name, dataDir, supportedSchema, publisherUrl });
  await store.init();
  const startedAt = nowIso();
  const counters = { deliveries: 0, cancels: 0, acks: 0 };
  let boundPort = port;

  const server = createServer(async (req, res) => {
    const url = new URL(req.url, `http://127.0.0.1:${port}`);
    try {
      if (req.method === 'GET' && url.pathname === '/health') {
        json(res, 200, {
          ok: true, name, port: boundPort, supportedSchema, startedAt,
          currentVersion: store.state.currentVersion,
          serving: store.state.serving,
        });
        return;
      }
      if (req.method === 'GET' && url.pathname === '/state') {
        json(res, 200, store.snapshot());
        return;
      }
      if (req.method === 'POST' && url.pathname === '/delivery') {
        counters.deliveries += 1;
        const delivery = await readJson(req);
        const result = await store.receiveDelivery(delivery);
        json(res, result.status, result.body);
        return;
      }
      if (req.method === 'POST' && url.pathname === '/cancel') {
        counters.cancels += 1;
        const body = await readJson(req);
        if (!body.releaseId) { json(res, 400, { error: 'releaseId required' }); return; }
        const result = await store.cancelRelease(body.releaseId, body.reason);
        json(res, 200, { name, releaseId: body.releaseId, ...result });
        return;
      }
      if (req.method === 'POST' && url.pathname === '/retry-acks') {
        counters.acks += 1;
        await store.flushAcks();
        json(res, 200, { name, outbox: store.snapshot().outbox });
        return;
      }
      if (req.method === 'GET' && url.pathname === '/data') {
        // What this downstream can actually serve right now. A quarantined
        // release is retained for audit but never handed out.
        const snap = store.snapshot();
        const current = snap.applications.find((a) => a.releaseId === snap.currentReleaseId);
        if (!current || current.state === 'quarantined') {
          json(res, 410, {
            name,
            serving: false,
            reason: current ? 'quarantined' : 'no-applied-release',
            version: current?.version ?? null,
            versions: snap.applications.map((a) => ({ version: a.version, state: a.state })),
          });
          return;
        }
        const payload = store.state.applications.get(current.releaseId)?.payload ?? null;
        json(res, 200, {
          name,
          serving: true,
          version: current.version,
          schema: current.schema,
          appliedSeq: current.appliedSeq,
          manifest: payload?.manifest ?? null,
          rows: payload?.rows ?? [],
        });
        return;
      }
      json(res, 404, { error: 'not found' });
    } catch (err) {
      process.stderr.write(`[${name}] request failed: ${err.stack ?? err}\n`);
      json(res, 500, { error: err.message });
    }
  });

  await new Promise((resolveListen, reject) => {
    server.once('error', reject);
    server.listen(port, '127.0.0.1', resolveListen);
  });
  const actualPort = server.address().port;
  boundPort = actualPort;

  // Retry any acks left in the durable outbox (e.g. publisher was down).
  let retryTimer = null;
  if (autoRetryAcks) {
    retryTimer = setInterval(() => {
      if (store.state.outbox.size) store.flushAcks().catch(() => {});
    }, 2000);
    setImmediate(() => store.flushAcks().catch(() => {}));
  }

  const close = async () => {
    if (retryTimer) clearInterval(retryTimer);
    server.closeAllConnections?.();
    await new Promise((r) => server.close(r));
    await store.journal.close();
  };

  return { name, port: actualPort, dataDir, store, counters, server, close };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const opts = parseOptions();
  const agent = await startConsumer(opts);
  process.stdout.write(`[${agent.name}] consumer listening on ${agent.port} (data ${agent.dataDir})\n`);

  let shuttingDown = false;
  const shutdown = async (signal) => {
    if (shuttingDown) return;
    shuttingDown = true;
    process.stdout.write(`[${agent.name}] ${signal} received, closing...\n`);
    await agent.close();
    process.exit(0);
  };
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}
