import { sha256 } from './util.js';

/**
 * A "local data product" is a small, versioned artifact: a table of rows plus
 * an explicit schema version. The publisher ships a manifest + rows in one
 * signed payload; consumers validate the schema before applying anything.
 */

export function makeProductRow(i, cohort) {
  const buckets = ['north', 'south', 'east', 'west', 'central'];
  return {
    id: `row-${String(i).padStart(4, '0')}`,
    cohort,
    bucket: buckets[i % buckets.length],
    score: Number((((i * 37) % 1000) / 10).toFixed(1)),
    active: i % 7 !== 0,
  };
}

export function buildRelease({ version, schema, cohort, rowCount = 24, notes = '' }) {
  const rows = Array.from({ length: rowCount }, (_, i) => makeProductRow(i, cohort));
  const manifest = {
    kind: 'local-data-product',
    version,
    schema,
    cohort,
    rowCount: rows.length,
    notes,
  };
  const payloadHash = sha256({ manifest, rows });
  return { manifest, rows, payloadHash };
}

export class SchemaError extends Error {
  constructor(message, details) {
    super(message);
    this.name = 'SchemaError';
    this.details = details;
  }
}

/**
 * Validate a release payload against a consumer's supported schema version.
 * Rejection is explicit and structured: the publisher shows the reason.
 */
export function validatePayload(payload, supportedSchema) {
  const problems = [];
  const manifest = payload?.manifest;
  const rows = payload?.rows;

  if (!manifest || typeof manifest !== 'object') problems.push('missing manifest');
  if (!Array.isArray(rows)) problems.push('rows must be an array');
  if (manifest?.kind !== 'local-data-product') problems.push('manifest.kind mismatch');
  if (typeof manifest?.version !== 'string' || !manifest.version) problems.push('manifest.version missing');
  if (!Number.isInteger(manifest?.schema)) problems.push('manifest.schema must be an integer');
  if (typeof manifest?.cohort !== 'string') problems.push('manifest.cohort missing');

  if (Array.isArray(rows) && Number.isInteger(manifest?.rowCount) && rows.length !== manifest.rowCount) {
    problems.push(`rowCount ${manifest.rowCount} != rows ${rows.length}`);
  }

  if (Array.isArray(rows)) {
    for (const [i, row] of rows.entries()) {
      if (!row || typeof row !== 'object') { problems.push(`row ${i} not an object`); break; }
      if (typeof row.id !== 'string') { problems.push(`row ${i} missing id`); break; }
      if (typeof row.cohort !== 'string') { problems.push(`row ${i} missing cohort`); break; }
      if (typeof row.score !== 'number') { problems.push(`row ${i} score not a number`); break; }
      if (typeof row.active !== 'boolean') { problems.push(`row ${i} active not a boolean`); break; }
    }
  }

  if (payload?.payloadHash !== sha256({ manifest: payload.manifest, rows: payload.rows })) {
    problems.push('payloadHash mismatch (payload altered in transit or storage)');
  }

  if (!problems.length && manifest.schema > supportedSchema) {
    throw new SchemaError(
      `unsupported schema v${manifest.schema}: this consumer supports up to v${supportedSchema}`,
      { requiredSchema: manifest.schema, supportedSchema, compatible: false },
    );
  }

  if (problems.length) {
    throw new SchemaError(`invalid payload: ${problems.slice(0, 3).join('; ')}`, {
      problems,
      compatible: false,
    });
  }

  return true;
}

