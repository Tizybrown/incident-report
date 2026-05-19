require('dotenv').config();

const express = require('express');
const { S3Client, ListObjectsV2Command } = require('@aws-sdk/client-s3');
const multer = require('multer');
const multerS3 = require('multer-s3');
const Redis = require('ioredis');
const { v4: uuidv4 } = require('uuid');
const path = require('path');
const fs = require('fs');

const app = express();
app.use(express.json());

// ── CORS ───────────────────────────────────────────────────────────────────────
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

app.use(express.static('public'));

// ── Constants ──────────────────────────────────────────────────────────────────
const VALID_LGAS    = ['All_LGAs', 'Rep_Incidence', 'Moba', 'Oye', 'Ikole', 'Ido_Osi', 'Ilejemeje'];
const FALLBACK_FILE = path.join(__dirname, 'incidents.json');

// Extensions accepted regardless of MIME type (covers AVI ambiguity on Windows)
const ALLOWED_VIDEO_EXTS = new Set([
  '.mp4', '.avi', '.mov', '.mkv', '.wmv', '.flv',
  '.webm', '.m4v', '.3gp', '.3g2', '.ts', '.mts', '.m2ts',
]);
const ALLOWED_IMAGE_EXTS = new Set(['.jpg', '.jpeg', '.png']);
const ALLOWED_DOC_EXTS   = new Set(['.pdf']);

// ── DigitalOcean Spaces (S3-compatible) ────────────────────────────────────────
const s3 = new S3Client({
  endpoint:         process.env.DO_SPACES_ENDPOINT,
  region:           process.env.DO_SPACES_REGION,
  credentials: {
    accessKeyId:     process.env.DO_SPACES_KEY,
    secretAccessKey: process.env.DO_SPACES_SECRET,
  },
  forcePathStyle:   true,
  signatureVersion: 's3',
});

console.log('[Spaces] S3Client configured →', {
  endpoint: process.env.DO_SPACES_ENDPOINT,
  region:   process.env.DO_SPACES_REGION,
  bucket:   process.env.DO_SPACES_BUCKET,
});

// ── Valkey / Redis — TLS required for DO managed databases ─────────────────────
const redis = new Redis({
  host:     process.env.VALKEY_HOST,
  port:     parseInt(process.env.VALKEY_PORT, 10),
  username: process.env.VALKEY_USERNAME,
  password: process.env.VALKEY_PASSWORD,
  tls:      { rejectUnauthorized: false },
  lazyConnect:   true,
  retryStrategy: (times) => Math.min(times * 200, 2000),
});

redis.on('connect', () => console.log('[Valkey] connected'));
redis.on('error',   (err) => console.error('[Valkey] error:', err.message));

// ── Local JSON fallback helpers ─────────────────────────────────────────────────
function readFallback() {
  try {
    return JSON.parse(fs.readFileSync(FALLBACK_FILE, 'utf8'));
  } catch {
    return [];
  }
}

function writeFallback(incident) {
  const list = readFallback();
  list.unshift(incident);
  fs.writeFileSync(FALLBACK_FILE, JSON.stringify(list, null, 2));
  console.log('[fallback] wrote incident to incidents.json — total records:', list.length);
}

// ── Normalize incident — backfill "ward" on records saved before ward feature ────
function normalizeIncident(d) {
  if (!d) return d;
  if (d.ward === undefined) d.ward = 'Unknown Ward';
  return d;
}

// ── Resolve the canonical file type from MIME + extension ─────────────────────
// Returns 'video' | 'image' | 'document' | null
function resolveFileType(mime, originalname) {
  const ext = path.extname(originalname).toLowerCase();
  if (
    mime.startsWith('video/') ||
    mime === 'application/octet-stream' ||
    mime === 'application/x-troff-msvideo' ||
    ALLOWED_VIDEO_EXTS.has(ext)
  ) return 'video';
  if (mime.startsWith('image/') || ALLOWED_IMAGE_EXTS.has(ext)) return 'image';
  if (mime === 'application/pdf' || ALLOWED_DOC_EXTS.has(ext)) return 'document';
  return null;
}

// ── multer-s3 — streams directly to Spaces, nothing touches local disk ─────────
const upload = multer({
  storage: multerS3({
    s3,
    bucket:      process.env.DO_SPACES_BUCKET,
    acl:         'public-read',
    // AVI files may arrive as application/octet-stream on Windows; force correct
    // Content-Type so the object is playable when downloaded from Spaces.
    contentType(req, file, cb) {
      const ext = path.extname(file.originalname).toLowerCase();
      if (ext === '.avi') return cb(null, 'video/x-msvideo');
      multerS3.AUTO_CONTENT_TYPE(req, file, cb);
    },
    partSize:  10 * 1024 * 1024,  // 10 MB per multipart part
    queueSize: 10,                  // 10 concurrent parts per file
    key(req, file, cb) {
      console.log('[multer-s3] streaming file to Spaces →', file.originalname, '| mime:', file.mimetype);
      const lga = req.body.lga;
      if (!VALID_LGAS.includes(lga)) {
        return cb(new Error(`Invalid LGA. Must be one of: ${VALID_LGAS.join(', ')}`));
      }
      const safeName   = path.basename(file.originalname).replace(/[^a-zA-Z0-9._-]/g, '_');
      const fileType   = resolveFileType(file.mimetype, file.originalname);
      const typeFolder = fileType === 'video' ? 'videos' : fileType === 'image' ? 'images' : 'documents';
      let key;
      if (lga === 'All_LGAs') {
        key = `incident_reports/All_LGAs/${typeFolder}/${Date.now()}_${safeName}`;
      } else {
        const ward     = (req.body.ward || '').trim();
        // collapse spaces and slashes to underscores so ward never creates extra path segments
        const safeWard   = ward.replace(/[\s/\\]+/g, '_').replace(/_+/g, '_').replace(/^_|_$/g, '');
        const wardFolder = safeWard || 'no_ward';
        key = `incident_reports/${lga}/${wardFolder}/${typeFolder}/${Date.now()}_${safeName}`;
      }
      console.log('[multer-s3] destination key →', key);
      cb(null, key);
    },
  }),
  fileFilter(_req, file, cb) {
    const type = resolveFileType(file.mimetype, file.originalname);
    if (type) {
      console.log(`[fileFilter] accepted — "${file.originalname}" (mime: ${file.mimetype}, type: ${type})`);
      cb(null, true);
    } else {
      console.warn(`[fileFilter] rejected — "${file.originalname}" (mime: ${file.mimetype})`);
      cb(new Error(`File type not allowed: ${file.mimetype} / ${path.extname(file.originalname) || 'no ext'}`));
    }
  },
});

// ── Helper: public Spaces URL ───────────────────────────────────────────────────
function spacesUrl(key) {
  return `https://${process.env.DO_SPACES_BUCKET}.${process.env.DO_SPACES_REGION}.digitaloceanspaces.com/${key}`;
}

// ── Helper: scan all incident:* keys from Valkey ───────────────────────────────
async function getAllIncidentKeys() {
  const keys = [];
  let cursor = '0';
  do {
    const [next, found] = await redis.scan(cursor, 'MATCH', 'incident:*', 'COUNT', 100);
    cursor = next;
    keys.push(...found);
  } while (cursor !== '0');
  return keys;
}

// ── GET /health ────────────────────────────────────────────────────────────────
app.get('/health', (_req, res) => res.json({ status: 'ok' }));

// ── GET /test-spaces ────────────────────────────────────────────────────────────
app.get('/test-spaces', async (req, res) => {
  console.log('[test-spaces] testing Spaces connection …');
  try {
    const data = await s3.send(new ListObjectsV2Command({
      Bucket:  process.env.DO_SPACES_BUCKET,
      MaxKeys: 5,
    }));
    console.log('[test-spaces] success — object count:', data.KeyCount);
    res.json({
      success:     true,
      bucket:      process.env.DO_SPACES_BUCKET,
      region:      process.env.DO_SPACES_REGION,
      endpoint:    process.env.DO_SPACES_ENDPOINT,
      objectCount: data.KeyCount,
    });
  } catch (err) {
    console.error('[test-spaces] failed:', err.message);
    res.status(500).json({
      success:  false,
      error:    err.message,
      code:     err.Code || err.code || null,
      bucket:   process.env.DO_SPACES_BUCKET,
      endpoint: process.env.DO_SPACES_ENDPOINT,
    });
  }
});

// ── POST /upload ────────────────────────────────────────────────────────────────
// Accepts: multipart/form-data
//   fields: lga (text), fileSizes (JSON array of byte counts), videos (files, up to 100)
app.post('/upload', (req, res) => {
  console.log('[upload] request received — starting batch upload');

  // Safety net: if the handler somehow never sends a response, force one after 10 min
  const responseTimeout = setTimeout(() => {
    if (!res.headersSent) {
      console.error('[upload] response timeout — forcing error response');
      res.status(500).json({ success: false, error: 'Server took too long to respond' });
    }
  }, 10 * 60 * 1000);

  upload.array('videos', 100)(req, res, async (err) => {
    if (err) {
      clearTimeout(responseTimeout);
      if (err instanceof multer.MulterError) {
        console.error('[upload] MulterError:', err.field, err.message);
        return res.status(400).json({ success: false, error: `Upload error: ${err.message}` });
      }
      console.error('[upload] multer/Spaces error:', err.message, err.stack || '');
      return res.status(400).json({ success: false, error: err.message });
    }

    if (!req.files || req.files.length === 0) {
      clearTimeout(responseTimeout);
      console.error('[upload] no files in request');
      return res.status(400).json({ success: false, error: 'No files received' });
    }

    const lga  = req.body.lga;
    const ward = (req.body.ward || '').trim();
    const now  = new Date().toISOString();

    console.log(`[upload] lga → "${lga}" | ward → "${ward || '(none — stored under no_ward)'}" | files → ${req.files.length}`);

    // Client-provided sizes are the authoritative source — multer-s3 can report 0
    // when AUTO_CONTENT_TYPE modifies the stream before byte-counting completes.
    let clientSizes = [];
    try { clientSizes = JSON.parse(req.body.fileSizes || '[]'); } catch {}

    const results = [];

    try {
      for (let i = 0; i < req.files.length; i++) {
        const file     = req.files[i];
        const url      = spacesUrl(file.key);
        const id       = uuidv4();
        const size     = (file.size > 0) ? file.size : (clientSizes[i] || 0);
        const fileMime = file.mimetype || '';
        const fileType = resolveFileType(fileMime, file.originalname);

        console.log(`[upload] file ${i + 1}/${req.files.length} complete → ${url} (${size} bytes, type: ${fileType})`);

        const meta = {
          id,
          lga,
          ward,
          fileType: fileType || 'video',
          filename:    file.originalname,
          filepath:    url,
          uploaded_at: now,
          file_size:   String(size),
        };

        console.log(`[upload] saving to Valkey — incident:${id}`);
        try {
          await redis.hset(`incident:${id}`, meta);
          console.log(`[upload] Valkey save successful — ${file.originalname}`);
        } catch (dbErr) {
          console.error(`[upload] Valkey unavailable for ${file.originalname}, writing to incidents.json:`, dbErr.message);
          writeFallback(meta);
        }

        results.push({ success: true, url, filename: file.originalname, size });
      }
    } catch (processingErr) {
      clearTimeout(responseTimeout);
      console.error('[upload] error processing uploaded files:', processingErr.message, processingErr.stack || '');
      if (!res.headersSent) {
        return res.status(500).json({ success: false, error: processingErr.message });
      }
      return;
    }

    clearTimeout(responseTimeout);
    console.log(`[upload] batch complete — ${results.length} file(s) processed, sending response`);
    res.json({ success: true, uploaded: results.length, results });
  });
});

// ── GET /incidents ──────────────────────────────────────────────────────────────
app.get('/incidents', async (req, res) => {
  try {
    const keys = await getAllIncidentKeys();
    if (!keys.length) return res.json(readFallback().map(normalizeIncident));

    const pipeline = redis.pipeline();
    keys.forEach((k) => pipeline.hgetall(k));
    const results = await pipeline.exec();

    const incidents = results
      .map(([err, data]) => (err || !data ? null : normalizeIncident(data)))
      .filter(Boolean)
      .sort((a, b) => new Date(b.uploaded_at) - new Date(a.uploaded_at));

    res.json(incidents);
  } catch (err) {
    console.warn('[GET /incidents] Valkey unavailable, reading from incidents.json:', err.message);
    res.json(readFallback().map(normalizeIncident));
  }
});

// ── GET /incidents/:lga ─────────────────────────────────────────────────────────
app.get('/incidents/:lga', async (req, res) => {
  const { lga } = req.params;
  if (!VALID_LGAS.includes(lga)) {
    return res.status(400).json({ error: `Invalid LGA. Must be one of: ${VALID_LGAS.join(', ')}` });
  }

  try {
    const keys = await getAllIncidentKeys();
    if (!keys.length) return res.json(readFallback().map(normalizeIncident).filter(i => i.lga === lga));

    const pipeline = redis.pipeline();
    keys.forEach((k) => pipeline.hgetall(k));
    const results = await pipeline.exec();

    const incidents = results
      .map(([err, data]) => (err || !data ? null : normalizeIncident(data)))
      .filter((d) => d && d.lga === lga)
      .sort((a, b) => new Date(b.uploaded_at) - new Date(a.uploaded_at));

    res.json(incidents);
  } catch (err) {
    console.warn(`[GET /incidents/${lga}] Valkey unavailable, reading from incidents.json:`, err.message);
    res.json(readFallback().map(normalizeIncident).filter(i => i.lga === lga));
  }
});

// ── GET /incidents/:lga/:ward ───────────────────────────────────────────────────
app.get('/incidents/:lga/:ward', async (req, res) => {
  const { lga } = req.params;
  const ward = decodeURIComponent(req.params.ward).trim();

  if (!VALID_LGAS.includes(lga)) {
    return res.status(400).json({ error: `Invalid LGA. Must be one of: ${VALID_LGAS.join(', ')}` });
  }

  try {
    const keys = await getAllIncidentKeys();
    if (!keys.length) return res.json(readFallback().map(normalizeIncident).filter(i => i.lga === lga && i.ward === ward));

    const pipeline = redis.pipeline();
    keys.forEach((k) => pipeline.hgetall(k));
    const results = await pipeline.exec();

    const incidents = results
      .map(([err, data]) => (err || !data ? null : normalizeIncident(data)))
      .filter((d) => d && d.lga === lga && d.ward === ward)
      .sort((a, b) => new Date(b.uploaded_at) - new Date(a.uploaded_at));

    res.json(incidents);
  } catch (err) {
    console.warn(`[GET /incidents/${lga}/${ward}] Valkey unavailable, reading from incidents.json:`, err.message);
    res.json(readFallback().map(normalizeIncident).filter(i => i.lga === lga && i.ward === ward));
  }
});

// ── GET /debug/incidents ────────────────────────────────────────────────────────
app.get('/debug/incidents', async (req, res) => {
  try {
    const keys = await getAllIncidentKeys();
    if (!keys.length) {
      const fallback = readFallback().slice(0, 10);
      return res.json({ source: 'fallback', total: fallback.length, incidents: fallback });
    }

    const pipeline = redis.pipeline();
    keys.forEach((k) => pipeline.hgetall(k));
    const results = await pipeline.exec();

    const incidents = results
      .map(([err, data]) => (err || !data ? null : data))
      .filter(Boolean)
      .sort((a, b) => new Date(b.uploaded_at) - new Date(a.uploaded_at))
      .slice(0, 10);

    res.json({ source: 'valkey', total_keys: keys.length, showing: incidents.length, incidents });
  } catch (err) {
    const fallback = readFallback().slice(0, 10);
    res.json({ source: 'fallback', error: err.message, total: fallback.length, incidents: fallback });
  }
});

// ── Global error handler (catches anything Express itself throws) ───────────────
app.use((err, req, res, _next) => {
  console.error('[express] unhandled error:', err.message, err.stack || '');
  if (!res.headersSent) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── Start ───────────────────────────────────────────────────────────────────────
const PORT   = parseInt(process.env.PORT || '3000', 10);
const server = app.listen(PORT, () => {
  console.log(`Incident Report server → http://localhost:${PORT}`);
});

// 6-hour timeouts to accommodate very large video uploads
const SIX_HOURS = 6 * 60 * 60 * 1000;
server.setTimeout(SIX_HOURS);
server.timeout          = SIX_HOURS;
server.keepAliveTimeout = SIX_HOURS;
server.headersTimeout   = SIX_HOURS + 1000;  // must be > keepAliveTimeout
