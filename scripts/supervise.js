#!/usr/bin/env node
/**
 * Local process manager for the demo.
 *
 * Keeps the publisher and each consumer in separate OS processes so that
 * "stop alpha", "crash the publisher" or "restart gamma" are real process
 * events, not simulated flags. Restarts reuse the same on-disk journal, which
 * is what makes crash-recovery demonstrable.
 */
import { spawn } from 'node:child_process';
import { createServer } from 'node:http';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { open } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { CONSUMER_NAMES, DEFAULT_PORTS } from '../src/shared/protocol.js';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const RUN_DIR = join(ROOT, '.data', 'run');
const LOG_DIR = join(ROOT, '.data', 'logs');
const SUPERVISOR_PORT = Number(process.env.SUPERVISOR_PORT ?? DEFAULT_PORTS.supervisor);

const children = new Map();
const stopping = new Set();
let shuttingDown = false;

function log(line) {
  process.stdout.write(`${line}\n`);
}

function logFileFor(name) {
  return join(LOG_DIR, `${name}.log`);
}

function isRunning(entry) {
  return Boolean(entry && entry.running);
}

async function spawnService(name, script, args) {
  if (isRunning(children.get(name))) return;
  await mkdir(LOG_DIR, { recursive: true });
  const out = await open(logFileFor(name), 'a');
  const child = spawn(process.execPath, [script, ...args], {
    cwd: ROOT,
    stdio: ['ignore', out.fd, out.fd],
    env: { ...process.env },
  });
  const entry = {
    child,
    startedAt: new Date().toISOString(),
    pid: child.pid,
    script,
    args,
    running: true,
    restartOnExit: true,
    exitCode: null,
    signal: null,
  };
  children.set(name, entry);
  child.on('exit', (code, signal) => {
    out.close().catch(() => {});
    log(`[supervisor] ${name} exited code=${code} signal=${signal ?? '-'}`);
    entry.running = false;
    entry.exitCode = code;
    entry.signal = signal;
    stopping.delete(name);
    if (!shuttingDown && entry.restartOnExit) {
      setTimeout(() => startService(name).catch((err) => log(`[supervisor] restart ${name} failed: ${err.message}`)), 300);
    }
  });
  log(`[supervisor] ${name} started pid=${child.pid}`);
  await new Promise((r) => setTimeout(r, 350));
}

async function startService(name) {
  if (name === 'publisher') {
    return spawnService('publisher', join(ROOT, 'src/publisher/server.js'), []);
  }
  if (!CONSUMER_NAMES.includes(name)) throw new Error(`unknown service ${name}`);
  return spawnService(name, join(ROOT, 'src/consumer/agent.js'), ['--name', name]);
}

async function stopService(name) {
  const entry = children.get(name);
  if (!isRunning(entry)) {
    stopping.delete(name);
    return { stopped: true, alreadyStopped: true };
  }
  entry.restartOnExit = false;
  stopping.add(name);
  entry.child.kill('SIGTERM');
  const deadline = Date.now() + 4000;
  while (entry.running && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 60));
  }
  if (entry.running) {
    entry.child.kill('SIGKILL');
    log(`[supervisor] ${name} SIGKILLed after graceful shutdown timeout`);
  }
  return { stopped: true, code: entry.exitCode };
}

async function crashService(name) {
  const entry = children.get(name);
  if (!isRunning(entry)) return { crashed: false, alreadyStopped: true };
  entry.restartOnExit = false;
  entry.child.kill('SIGKILL');
  await new Promise((r) => setTimeout(r, 250));
  log(`[supervisor] ${name} SIGKILLed (simulated crash, no auto-restart)`);
  return { crashed: true, pid: entry.pid };
}

async function restartService(name) {
  const entry = children.get(name);
  const wasRunning = isRunning(entry);
  if (wasRunning) await stopService(name);
  await startService(name);
  return { restarted: true, wasRunning };
}

function status() {
  return {
    supervisor: { port: SUPERVISOR_PORT, pid: process.pid },
    services: Object.fromEntries([...children.entries()].map(([name, e]) => [name, {
      pid: e.pid,
      running: isRunning(e),
      startedAt: e.startedAt,
      exitCode: e.exitCode ?? null,
      signal: e.signal ?? null,
      logFile: logFileFor(name),
    }])),
  };
}

function json(res, status, body) {
  const text = JSON.stringify(body);
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'content-length': Buffer.byteLength(text),
    'access-control-allow-origin': '*',
    'access-control-allow-headers': 'content-type',
    'access-control-allow-methods': 'GET,POST,OPTIONS',
  });
  res.end(text);
}

async function readJson(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  if (!chunks.length) return {};
  try { return JSON.parse(Buffer.concat(chunks).toString('utf8')); } catch { return {}; }
}

const server = createServer(async (req, res) => {
  const url = new URL(req.url, `http://127.0.0.1:${SUPERVISOR_PORT}`);
  try {
    if (req.method === 'OPTIONS') {
      json(res, 204, {});
      return;
    }
    if (req.method === 'GET' && (url.pathname === '/' || url.pathname === '/status')) {
      json(res, 200, status());
      return;
    }
    const match = url.pathname.match(/^\/(start|stop|restart|crash)\/([a-z]+)$/);
    if (match && req.method === 'POST') {
      const [, action, name] = match;
      if (name !== 'publisher' && !CONSUMER_NAMES.includes(name)) {
        json(res, 400, { error: `unknown service ${name}` });
        return;
      }
      if (action === 'start') await startService(name);
      else if (action === 'stop') await stopService(name);
      else if (action === 'crash') await crashService(name);
      else if (name === 'publisher') {
        // The publisher is the caller of this endpoint. Answer before killing
        // its process, otherwise the HTTP response is severed by the restart.
        json(res, 200, { ok: true, action, name, scheduled: true });
        const entry = children.get('publisher');
        if (entry) entry.restartOnExit = true;
        setTimeout(() => {
          restartService('publisher').catch((err) => log(`[supervisor] restart publisher failed: ${err.message}`));
        }, 250);
        return;
      } else await restartService(name);
      json(res, 200, { ok: true, action, name, status: status().services[name] ?? null });
      return;
    }
    if (req.method === 'GET' && url.pathname === '/logs') {
      const name = url.searchParams.get('name');
      if (!name) { json(res, 400, { error: 'name required' }); return; }
      try {
        const text = await readFile(logFileFor(name), 'utf8');
        json(res, 200, { name, text: text.slice(-20000) });
      } catch {
        json(res, 200, { name, text: '' });
      }
      return;
    }
    if (req.method === 'POST' && url.pathname === '/quit') {
      await readJson(req);
      if (!shuttingDown) setTimeout(() => shutdown('quit'), 50);
      json(res, 200, { ok: true, alreadyStopping: shuttingDown });
      return;
    }
    json(res, 404, { error: 'not found' });
  } catch (err) {
    json(res, 500, { error: err.message });
  }
});

async function shutdown(reason) {
  if (shuttingDown) return;
  shuttingDown = true;
  log(`[supervisor] shutting down (${reason})`);
  await Promise.all([...children.keys()].map((name) => stopService(name)));
  process.exit(0);
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

await mkdir(RUN_DIR, { recursive: true });
await mkdir(LOG_DIR, { recursive: true });
await writeFile(join(RUN_DIR, 'supervisor.pid'), String(process.pid));

const servicesToStart = process.env.START_SERVICE
  ? [process.env.START_SERVICE]
  : ['publisher', ...CONSUMER_NAMES];

for (const name of servicesToStart) {
  const entry = children.get(name);
  if (entry) entry.restartOnExit = true;
  await startService(name).catch((err) => log(`[supervisor] start ${name} failed: ${err.message}`));
}

server.listen(SUPERVISOR_PORT, '127.0.0.1', () => {
  log(`[supervisor] listening on http://127.0.0.1:${SUPERVISOR_PORT}`);
  log(`[supervisor] console: http://127.0.0.1:${DEFAULT_PORTS.publisher}`);
  log('[supervisor] Ctrl+C stops everything. Demo data lives in .data/');
});

