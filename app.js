import express from 'express';
import multer from 'multer';
import crypto from 'node:crypto';
import os from 'node:os';
import path from 'node:path';
import fs from 'node:fs/promises';

import { ingest } from './index.js';

const app = express();

app.use(express.json());

// Upload directory
const uploadDir = path.join(os.tmpdir(), 'evaa-upload');

// Ensure the upload directory exists before accepting uploads
await fs.mkdir(uploadDir, { recursive: true });

// Configure Multer to preserve the original file extension
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, uploadDir);
  },

  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname).toLowerCase();
    const uniqueName = `${crypto.randomUUID()}${ext}`;
    cb(null, uniqueName);
  }
});

const upload = multer({
  storage,
  limits: {
    fileSize: 1024 * 1024 * 1024 // 1 GB maximum
  }
});

// Health-check endpoint
app.get('/api/status', (req, res) => {
  res.json({
    status: 'success',
    message: 'EVAA Engine is running'
  });
});

// Event-data ingestion endpoint
app.post('/api/ingest', upload.single('file'), async (req, res) => {
  let outputDir;

  try {
    if (!req.file) {
      return res.status(400).json({
        success: false,
        error: 'No file uploaded. Please upload a CSV or TXT event-data file.'
      });
    }

    const originalName = req.file.originalname;
    const inputPath = req.file.path;

    console.log(`[EVAA] Uploaded file: ${originalName}`);
    console.log(`[EVAA] Temporary path: ${inputPath}`);

    // Create an output directory for this ingestion
    outputDir = await fs.mkdtemp(
      path.join(os.tmpdir(), 'evaa-output-')
    );

    // Process the uploaded event data
    const result = await ingest(inputPath, outputDir);

    console.log('[EVAA] Ingestion completed successfully');

    return res.json({
      ...result,
      originalFileName: originalName
    });
  } catch (error) {
    console.error('[EVAA] Ingestion failed:', error);

    return res.status(500).json({
      success: false,
      error: error.message || 'Event-data ingestion failed'
    });
  } finally {
    // Remove the temporary uploaded input file
    if (req.file?.path) {
      await fs.unlink(req.file.path).catch((error) => {
        console.error(
          '[EVAA] Could not remove temporary upload:',
          error.message
        );
      });
    }
  }
});

// Handle Multer upload errors, including oversized files
app.use((error, req, res, next) => {
  if (error instanceof multer.MulterError) {
    const statusCode =
      error.code === 'LIMIT_FILE_SIZE' ? 413 : 400;

    return res.status(statusCode).json({
      success: false,
      error:
        error.code === 'LIMIT_FILE_SIZE'
          ? 'File exceeds the maximum upload size of 1 GB.'
          : error.message
    });
  }

  console.error('[EVAA] Server error:', error);

  return res.status(500).json({
    success: false,
    error: error.message || 'Internal server error'
  });
});

export default app;
