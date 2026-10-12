
import path from 'node:path';
import fs from 'node:fs/promises';

import { parseFile } from './parser.js';
import { validateEvents } from '../utils/validation.js';
import { buildMetadata } from '../utils/metadata.js';
import {
  toColumns,
  isSortedByTime,
  sortColumns,
  previewRows
} from '../utils/columns.js';
import { writeHdf5 } from '../writers/hdf5.writer.js';
import { writeParquet } from '../writers/parquet.writer.js';
import { verifyOutputs } from '../utils/output-verification.js';

const roundMs = (ms) => Number(ms.toFixed(2));
const toMB = (bytes) => Number((bytes / (1024 * 1024)).toFixed(2));

const eventsPerSecond = (count, ms) =>
  ms > 0 ? Number((count * 1000 / ms).toFixed(2)) : null;

export async function ingest(inputPath, outputDir, options = {}) {
  const totalStart = performance.now();

  if (!inputPath) throw new Error('inputPath is required');
  if (!outputDir) throw new Error('outputDir is required');

  let peakRssBytes = process.memoryUsage().rss;

  const sampleMemory = () => {
    peakRssBytes = Math.max(
      peakRssBytes,
      process.memoryUsage().rss
    );
  };

  const sampler = setInterval(sampleMemory, 10);

  try {
    const format = path.extname(inputPath).slice(1).toLowerCase();

    console.log(`[EVAA] Ingesting: ${inputPath}`);
    console.log(`[EVAA] Format: ${format}`);

    // 1. Parse input.
    sampleMemory();
    const parseStart = performance.now();
    const events = await parseFile(inputPath);
    const parseDurationMs = performance.now() - parseStart;
    sampleMemory();

    // 9. Validate input events.
    const validationStart = performance.now();
    validateEvents(events);
    const validationDurationMs = performance.now() - validationStart;

    const metadata = buildMetadata(events, format, options);

    // Convert to compact columns and sort when needed.
    let columns = toColumns(events);
    metadata.sortedInput = isSortedByTime(columns.t);

    if (!metadata.sortedInput) {
      columns = sortColumns(columns);
    }

    const eventCount = columns.n;

    await fs.mkdir(outputDir, { recursive: true });

    const hdf5Path = path.join(outputDir, 'events.h5');
    const parquetPath = path.join(outputDir, 'data.parquet');

    // 4. Measure completed HDF5 write.
    sampleMemory();
    const hdf5Start = performance.now();
    await writeHdf5(columns, hdf5Path, metadata);
    const hdf5WriteDurationMs = performance.now() - hdf5Start;
    sampleMemory();

    // 5. Measure completed Parquet write.
    sampleMemory();
    const parquetStart = performance.now();
    await writeParquet(columns, parquetPath);
    const parquetWriteDurationMs = performance.now() - parquetStart;
    sampleMemory();

    // 8. Verify file existence and sizes.
    const fileCheckStart = performance.now();

    const [hdf5Stat, parquetStat] = await Promise.all([
      fs.stat(hdf5Path),
      fs.stat(parquetPath)
    ]);

    const filesExistAndNonEmpty =
      hdf5Stat.isFile() &&
      parquetStat.isFile() &&
      hdf5Stat.size > 0 &&
      parquetStat.size > 0;

    if (!filesExistAndNonEmpty) {
      throw new Error(
        'Output verification failed: an output file is missing or empty'
      );
    }

    const fileCheckDurationMs = performance.now() - fileCheckStart;

    // 9. Read back output metadata and selected Parquet values.
    const verificationStart = performance.now();

    const outputCorrectness = await verifyOutputs({
      hdf5Path,
      parquetPath,
      columns,
      eventCount
    });

    const outputVerificationDurationMs =
      performance.now() - verificationStart;

    sampleMemory();

    // 3 and 6. Throughput and total duration.
    const totalDurationMs = performance.now() - totalStart;

    const benchmark = {
      parsing: {
        durationMs: roundMs(parseDurationMs),
        eventCount,
        eventsPerSecond: eventsPerSecond(eventCount, parseDurationMs)
      },

      hdf5Writing: {
        durationMs: roundMs(hdf5WriteDurationMs)
      },

      parquetWriting: {
        durationMs: roundMs(parquetWriteDurationMs)
      },

      totalIngestion: {
        durationMs: roundMs(totalDurationMs),
        eventsPerSecond: eventsPerSecond(eventCount, totalDurationMs)
      },

      memory: {
        metric: 'sampled process RSS; not a guaranteed instantaneous peak',
        sampledPeakBytes: peakRssBytes,
        sampledPeakMB: toMB(peakRssBytes)
      },

      outputFiles: {
        hdf5: {
          path: hdf5Path,
          bytes: hdf5Stat.size,
          MB: toMB(hdf5Stat.size)
        },
        parquet: {
          path: parquetPath,
          bytes: parquetStat.size,
          MB: toMB(parquetStat.size)
        },
        totalBytes: hdf5Stat.size + parquetStat.size,
        totalMB: toMB(hdf5Stat.size + parquetStat.size)
      },

      validation: {
        passed: true,
        durationMs: roundMs(validationDurationMs)
      },

      outputCorrectness,

      verification: {
        fileExistenceCheckMs: roundMs(fileCheckDurationMs),
        readBackCheckMs: roundMs(outputVerificationDurationMs)
      }
    };

    console.log('[EVAA] Benchmark results:');
    console.log(JSON.stringify(benchmark, null, 2));

    return {
      success: true,
      metadata,
      preview: previewRows(columns, 10),
      eventCount,
      files: [
        { kind: 'HDF5', path: hdf5Path },
        { kind: 'PARQUET', path: parquetPath }
      ],
      benchmark
    };
  } finally {
    clearInterval(sampler);
  }
}
