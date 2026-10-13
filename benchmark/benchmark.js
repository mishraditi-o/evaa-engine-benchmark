import { parseFile } from '../ingest/parser.js';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';

export async function benchmark(filePath) {
  const start = performance.now();

  const events =
    await parseFile(filePath);

  const durationMs =
    performance.now() - start;
import { ingest } from '../ingest/index.js';

  return {
    filePath,

    eventCount:
      events.length,
// Runs a full ingest into a temporary folder and returns the benchmark block.
export async function benchmark(filePath) {
  const outputDir = await fs.mkdtemp(
    path.join(os.tmpdir(), 'evaa-benchmark-')
  );

    durationMs:
      Number(durationMs.toFixed(2)),
  try {
    const result = await ingest(filePath, outputDir);

    metric:
      'parse duration'
  };
}
    return { filePath, ...result.benchmark };
  } finally {
    await fs.rm(outputDir, { recursive: true, force: true });
  }
}
