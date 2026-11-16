#!/usr/bin/env node
/**
 * Remove all demo state (publisher + consumers + logs).
 * Refuses while services are running to avoid deleting a live journal.
 */
import { rm } from 'node:fs/promises';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const DATA = resolve(ROOT, '.data');

const HEALTH = [
  ['publisher', 4601, '/api/health'],
  ['alpha', 4611, '/health'],
  ['beta', 4612, '/health'],
  ['gamma', 4613, '/health'],
];

let running = [];
for (const [name, port, path] of HEALTH) {
  try {
    const res = await fetch(`http://127.0.0.1:${port}${path}`, { signal: AbortSignal.timeout(600) });
    if (res.ok) running.push(name);
  } catch { /* down */ }
}

if (running.length && !process.argv.includes('--force')) {
  process.stderr.write(
    `服务仍在运行：${running.join(', ')}。先停止它们（npm start 下可直接关掉 supervisor），或加 --force。\n`,
  );
  process.exit(1);
}

await rm(DATA, { recursive: true, force: true });
process.stdout.write(`已清除演示数据：${DATA}\n`);
