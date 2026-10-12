
import { stat } from 'node:fs/promises';

export function createMetrics() {
  const startedAt = performance.now();
  const startMemory = process.memoryUsage().rss;

  let peakMemoryBytes = startMemory;

  const sampleMemory = () => {
    peakMemoryBytes = Math.max(
      peakMemoryBytes,
      process.memoryUsage().rss
    );
  };

  const memoryTimer = setInterval(sampleMemory, 10);

  return {
    async finish({
      filePath,
      eventCount,
      parseDurationMs,
      hdf5Path,
      parquetPath,
      hdf5WriteDurationMs,
      parquetWriteDurationMs,
      validation
    }) {
      sampleMemory();
      clearInterval(memoryTimer);

      const totalDurationMs =
        performance.now() - startedAt;

      const [hdf5, parquet] = await Promise.all([
        stat(hdf5Path),
        stat(parquetPath)
      ]);

      const outputFilesExist =
        hdf5.isFile() && parquet.isFile();

      const durationSeconds =
        parseDurationMs / 1000;

      return {
        filePath,
        eventCount,

        parsingDurationMs:
          Number(parseDurationMs.toFixed(2)),

        eventsPerSecond:
          durationSeconds > 0
            ? Math.round(eventCount / durationSeconds)
            : null,

        hdf5WritingDurationMs:
          Number(hdf5WriteDurationMs.toFixed(2)),

        parquetWritingDurationMs:
          Number(parquetWriteDurationMs.toFixed(2)),

        totalIngestionDurationMs:
          Number(totalDurationMs.toFixed(2)),

        memory: {
          startRssBytes: startMemory,
          sampledPeakRssBytes: peakMemoryBytes,
          sampledPeakRssMB:
            Number((peakMemoryBytes / 1024 / 1024).toFixed(2))
        },

        outputFiles: {
          hdf5: {
            path: hdf5Path,
            sizeBytes: hdf5.size
          },
          parquet: {
            path: parquetPath,
            sizeBytes: parquet.size
          }
        },

        validation: {
          ...validation,
          outputFilesExist
        }
      };
    }
  };
}
