import { createHash } from 'node:crypto';

export function sha256(value) {
  const text = typeof value === 'string' ? value : canonicalJson(value);
  return createHash('sha256').update(text).digest('hex');
}

export function canonicalJson(value) {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  const keys = Object.keys(value).sort();
  return `{${keys.map((k) => `${JSON.stringify(k)}:${canonicalJson(value[k])}`).join(',')}}`;
}

export function nowIso() {
  return new Date().toISOString();
}

export class HttpError extends Error {
  constructor(status, message, details = null) {
    super(message);
    this.status = status;
    this.details = details;
  }
}

export function createMutex() {
  let chain = Promise.resolve();
  return function runExclusive(fn) {
    const task = chain.then(fn, fn);
    chain = task.catch(() => {});
    return task;
  };
}
