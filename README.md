# Ekiti North Senatorial District — Incident Report Portal

Video incident reporting system for the 5 LGAs of Ekiti North Senatorial District, Ekiti State, Nigeria.

- Videos stream directly to **DigitalOcean Spaces** (no local disk writes)
- Metadata stored in **Valkey** (DigitalOcean managed Redis-compatible database, TLS)
- Plain HTML/CSS/JS frontend — no build step

---

## Folder structure in DigitalOcean Spaces

```
incident_reports/
├── Moba/
├── Oye/
├── Ikole/
├── Ido_Osi/
└── Gbonyin/
```

Each file is stored as: `incident_reports/<LGA>/<timestamp>_<originalfilename>`

Public URL format: `https://<bucket>.<region>.digitaloceanspaces.com/<key>`

---

## Setting up DigitalOcean Spaces

1. Log in to [cloud.digitalocean.com](https://cloud.digitalocean.com)
2. **Spaces** → **Create a Space**
   - Region: choose closest to Nigeria (`nyc3` works well)
   - **File Listing**: Restricted (private by default; files are individually `public-read`)
   - Note the **Space name** and **region**
3. **API** → **Spaces Keys** → **Generate New Key**
   - Copy the **Access Key** and **Secret Key** (the secret is shown only once)

---

## Local setup

### 1. Install dependencies

```bash
npm install
```

### 2. Configure environment

```bash
cp .env.example .env
```

Fill in your DigitalOcean Spaces credentials (Valkey values are pre-filled):

```env
DO_SPACES_KEY=your_access_key_id
DO_SPACES_SECRET=your_secret_access_key
DO_SPACES_ENDPOINT=https://nyc3.digitaloceanspaces.com
DO_SPACES_BUCKET=your-space-name
DO_SPACES_REGION=nyc3
```

### 3. Run the server

```bash
npm run dev    # development (auto-restart)
npm start      # production
```

Open **http://localhost:3000**

---

## Deploying to Railway

1. Push your code to GitHub (make sure `.env` is in `.gitignore`)
2. Go to [railway.app](https://railway.app) → **New Project** → **Deploy from GitHub repo**
3. Add all environment variables in **Settings → Variables** (same as `.env`)
4. Railway auto-detects Node.js and runs `npm start`

---

## API reference

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/upload` | Upload a video (multipart: `lga` field first, then `video` file) |
| `GET`  | `/incidents` | All incidents, sorted newest-first |
| `GET`  | `/incidents/:lga` | Incidents for one LGA (`Moba`, `Oye`, `Ikole`, `Ido_Osi`, `Gbonyin`) |

### Upload request example (curl)

```bash
curl -X POST http://localhost:3000/upload \
  -F "lga=Moba" \
  -F "video=@/path/to/incident.mp4"
```

### Upload response

```json
{ "success": true, "url": "https://bucket.nyc3.digitaloceanspaces.com/incident_reports/Moba/...", "lga": "Moba", "filename": "incident.mp4" }
```

---

## Valkey data schema

Each upload creates one hash key:

| Key pattern | Type | Fields |
|-------------|------|--------|
| `incident:<uuid>` | Hash | `lga`, `filename`, `filepath`, `uploaded_at`, `file_size` |

- `filepath` — full public Spaces URL
- `file_size` — size in bytes (stored as string)
- `uploaded_at` — ISO 8601 timestamp
