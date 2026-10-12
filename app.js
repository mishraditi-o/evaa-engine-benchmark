import express from 'express';
import multer from 'multer';
import os from 'node:os';
import path from 'node:path';
import fs from 'node:fs/promises';

import { ingest } from './index.js';

const app = express();

app.use(express.json());

const uploadDir = path.join(os.tmpdir(), 'evaa-upload');

await fs.mkdir(uploadDir, { recursive: true }).catch(() => {});

const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, uploadDir);
  },
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname).toLowerCase();
    cb(null, `${crypto.randomUUID()}${ext}`);
  }
});

const upload = multer({ storage });
});

/*
 * Health check
 */
app.get('/api/status', (req, res) => {
  res.json({
    status: 'success',
    message:
      'EVAA Engine is running'
  });
});

/*
 * Upload + ingest
 */
app.post(
  '/api/ingest',
  upload.single('file'),
  async (req, res) => {
    try {
      if (!req.file) {
        return res.status(400).json({
          success: false,
          error: 'No file uploaded'
        });
      }

      const outputDir =
        await fs.mkdtemp(
          path.join(
            os.tmpdir(),
            'evaa-output-'
          )
        );

      const result =
        await ingest(
          req.file.path,
          outputDir
        );

      return res.json(result);

    } catch (error) {
      console.error(error);

      return res.status(500).json({
        success: false,
        error: error.message
      });
    }
  }
);

export default app;
