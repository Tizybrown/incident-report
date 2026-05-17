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
const VALID_LGAS    = ['Rep_Incidence', 'Moba', 'Oye', 'Ikole', 'Ido_Osi', 'Ilejemeje'];
const FALLBACK_FILE = path.join(__dirname, 'incidents.json');

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
  list.unshift(incident); // newest first
  fs.writeFileSync(FALLBACK_FILE, JSON.stringify(list, null, 2));
  console.log('[fallback] wrote incident to incidents.json — total records:', list.length);
}

// ── multer-s3 — streams directly to Spaces, nothing touches local disk ─────────
const upload = multer({
  storage: multerS3({
    s3,
    bucket:      process.env.DO_SPACES_BUCKET,
    acl:         'public-read',
    contentType: multerS3.AUTO_CONTENT_TYPE,
    key(req, file, cb) {
      console.log('[multer-s3] streaming file to Spaces →', file.originalname);
      const lga = req.body.lga;
      if (!VALID_LGAS.includes(lga)) {
        return cb(new Error(`Invalid LGA. Must be one of: ${VALID_LGAS.join(', ')}`));
      }
      const safeName = path.basename(file.originalname).replace(/[^a-zA-Z0-9._-]/g, '_');
      const key = `incident_reports/${lga}/${Date.now()}_${safeName}`;
      console.log('[multer-s3] destination key →', key);
      cb(null, key);
    },
  }),
  limits:     { fileSize: 500 * 1024 * 1024 }, // 500 MB
  fileFilter(_req, file, cb) {
    if (!file.mimetype.startsWith('video/')) {
      return cb(new Error('Only video files are allowed'));
    }
    cb(null, true);
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
app.post('/upload', (req, res) => {
  console.log('[upload] request received — starting upload');

  upload.single('video')(req, res, async (err) => {
    if (err) {
      console.error('[upload] multer/Spaces error:', err.message);
      return res.status(400).json({ success: false, error: err.message });
    }
    if (!req.file) {
      console.error('[upload] no file in request');
      return res.status(400).json({ success: false, error: 'No video file received' });
    }

    const url = spacesUrl(req.file.key);
    console.log('[upload] Spaces upload complete →', url);

    const lga = req.body.lga;
    const id  = uuidv4();
    const now = new Date().toISOString();

    const meta = {
      id,
      lga,
      filename:    req.file.originalname,
      filepath:    url,
      uploaded_at: now,
      file_size:   String(req.file.size),
    };

    console.log('[upload] saving metadata to Valkey — key: incident:' + id);
    try {
      await redis.hset(`incident:${id}`, meta);
      console.log('[upload] Valkey save successful');
    } catch (dbErr) {
      console.error('[upload] Valkey unavailable, falling back to incidents.json:', dbErr.message);
      writeFallback(meta);
    }

    console.log('[upload] sending success response to client');
    res.json({ success: true, url, lga, filename: req.file.originalname });
  });
});

// ── GET /incidents ──────────────────────────────────────────────────────────────
app.get('/incidents', async (req, res) => {
  try {
    const keys = await getAllIncidentKeys();
    if (!keys.length) {
      // Valkey is reachable but empty — still merge in any fallback records
      const fallback = readFallback();
      return res.json(fallback);
    }

    const pipeline = redis.pipeline();
    keys.forEach((k) => pipeline.hgetall(k));
    const results = await pipeline.exec();

    const incidents = results
      .map(([err, data]) => (err || !data ? null : data))
      .filter(Boolean)
      .sort((a, b) => new Date(b.uploaded_at) - new Date(a.uploaded_at));

    res.json(incidents);
  } catch (err) {
    console.warn('[GET /incidents] Valkey unavailable, reading from incidents.json:', err.message);
    res.json(readFallback());
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
    if (!keys.length) {
      const fallback = readFallback().filter(i => i.lga === lga);
      return res.json(fallback);
    }

    const pipeline = redis.pipeline();
    keys.forEach((k) => pipeline.hgetall(k));
    const results = await pipeline.exec();

    const incidents = results
      .map(([err, data]) => (err || !data ? null : data))
      .filter((d) => d && d.lga === lga)
      .sort((a, b) => new Date(b.uploaded_at) - new Date(a.uploaded_at));

    res.json(incidents);
  } catch (err) {
    console.warn(`[GET /incidents/${lga}] Valkey unavailable, reading from incidents.json:`, err.message);
    res.json(readFallback().filter(i => i.lga === lga));
  }
});

// ── Start ───────────────────────────────────────────────────────────────────────
const PORT = parseInt(process.env.PORT || '3000', 10);
app.listen(PORT, () => {
  console.log(`Incident Report server → http://localhost:${PORT}`);
});
