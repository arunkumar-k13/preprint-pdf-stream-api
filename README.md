# Preprint PDF Streaming API

A high-performance, lightweight FastAPI microservice designed for:
1. **Querying Preprint DOIs by Repository** from MongoDB with date-range filtering.
2. **On-the-Fly PDF Streaming** directly into client memory (`application/pdf`) with **zero disk storage overhead**.

---

## ⚡ Highlights

* **Zero Disk Persistence:** PDFs are streamed on-the-fly directly across HTTP into client RAM. No storage on disk or S3 is needed.
* **TLS & Bot-Protection Impersonation:** Employs `curl_cffi` Chrome 124 TLS fingerprint impersonation to reliably stream from protected repositories (e.g. ChemRxiv, medRxiv, bioRxiv).
* **Multi-Repository Routing:** Supports `arxiv`, `biorxiv`, `medrxiv`, `chemrxiv`, `psyarxiv`, and `socarxiv`.
* **Date Range Filtering:** Supports filtering DOIs by submission date range (`from_date` and `to_date` in `YYYY-MM-DD` format).
* **Self-Healing Fallbacks:** Automatically inspects MongoDB metadata and disambiguates manuscript versioning and prefixes (e.g. disambiguating `10.1101/` between bioRxiv and medRxiv).

---

## 📂 Project Structure

```text
preprint-pdf-stream-api/
├── main.py              # FastAPI application & route definitions
├── pdf_resolver.py      # Upstream PDF resolution & in-memory streaming engine
├── test_client.py       # End-to-end integration demo script
├── requirements.txt     # Python package dependencies
├── Dockerfile           # Production container build specification
├── .dockerignore        # Docker build ignore rules
├── .gitignore           # Git ignore rules for virtualenvs, caches, & secrets
├── .env.example         # Template for environment configuration
└── README.md            # Service documentation
```

---

## ⚙️ Configuration & Environment Variables

Configure the service using environment variables or a `.env` file (copy from `.env.example`):

| Variable | Default Value | Description |
| :--- | :--- | :--- |
| `MONGO_URI` | `mongodb://127.0.0.1:27017/` | Connection string to your MongoDB cluster |
| `MONGO_DB_NAME` | `preprint_data` | Target database containing preprint collections |
| `HOST` | `0.0.0.0` | Network interface to bind the API server |
| `PORT` | `8000` | Port for incoming HTTP requests |
| `WORKERS` | `4` | Number of Uvicorn worker processes (production) |
| `UPSTREAM_TIMEOUT`| `40` | Timeout in seconds for upstream repository downloads |

---

## 🚀 Quick Start (Local Development)

### 1. Set Up Environment
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows:
.\venv\Scripts\Activate.ps1
# Linux / macOS:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Run Locally
```bash
python main.py
```

The interactive Swagger UI documentation is available at:
👉 **`http://localhost:8000/docs`**

---

## 🐳 Docker Deployment

### 1. Build Container Image
```bash
docker build -t preprint-pdf-stream-api .
```

### 2. Run Container
```bash
docker run -d \
  --name preprint-api \
  -p 8000:8000 \
  -e MONGO_URI="mongodb://your-mongo-host:27017/" \
  -e MONGO_DB_NAME="preprint_data" \
  preprint-pdf-stream-api
```

---

## 🐧 Linux VM / Systemd Production Setup

To run as a persistent system service in production:

1. Create service file `/etc/systemd/system/preprint-pdf-api.service`:
```ini
[Unit]
Description=Preprint PDF Streaming API
After=network.target

[Service]
User=appuser
WorkingDirectory=/opt/preprint-pdf-stream-api
EnvironmentFile=/opt/preprint-pdf-stream-api/.env
ExecStart=/opt/preprint-pdf-stream-api/venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4
Restart=always

[Install]
WantedBy=multi-user.target
```

2. Enable and start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable preprint-pdf-api
sudo systemctl start preprint-pdf-api
```

---

## 📡 API Reference

### 1. Health & Info
```http
GET /
```
Returns service health status and storage mode.

---

### 2. List Supported Repositories & Document Counts
```http
GET /api/v1/preprints/repos
```
Returns list of supported repositories and document counts stored in MongoDB.

**Example Response:**
```json
{
  "status": "success",
  "repositories": [
    {"repository": "arxiv", "collection_name": "arXiv_raw", "total_documents": 1046131},
    {"repository": "biorxiv", "collection_name": "bioRxiv_raw", "total_documents": 186006},
    {"repository": "medrxiv", "collection_name": "medRxiv_raw", "total_documents": 55823},
    {"repository": "chemrxiv", "collection_name": "ChemRxiv_raw", "total_documents": 37769},
    {"repository": "psyarxiv", "collection_name": "PsyArXiv_raw", "total_documents": 43985},
    {"repository": "socarxiv", "collection_name": "SocArXiv_raw", "total_documents": 19353}
  ]
}
```

---

### 3. Get DOIs by Repository (with Date Filtering)
```http
GET /api/v1/preprints/dois?repo=biorxiv&from_date=2023-01-01&to_date=2023-12-31&limit=50&skip=0
```

**Query Parameters:**
* `repo` *(required)*: Repository dropdown selection (`arxiv`, `biorxiv`, `medrxiv`, `chemrxiv`, `psyarxiv`, `socarxiv`).
* `from_date` *(optional)*: Start submission date in `YYYY-MM-DD` format (e.g. `2023-01-01`).
* `to_date` *(optional)*: End submission date in `YYYY-MM-DD` format (e.g. `2023-12-31`).
* `limit` *(optional)*: Number of records to return (default: `100`, max: `10000`).
* `skip` *(optional)*: Number of records to skip for pagination (default: `0`).

**Example Response:**
```json
{
  "status": "success",
  "repository": "biorxiv",
  "from_date": "2023-01-01",
  "to_date": "2023-12-31",
  "count": 50,
  "skip": 0,
  "limit": 50,
  "data": [
    {
      "doi": "10.1101/2023.01.15.524100",
      "title": "Neural mechanisms of spatial navigation...",
      "submission_date": "2023-01-15",
      "version": 1,
      "pdf_url": "/api/v1/preprints/pdf?doi=10.1101/2023.01.15.524100"
    }
  ]
}
```

---

### 4. Stream PDF in Memory (Zero-Disk Storage)
```http
GET /api/v1/preprints/pdf?doi=10.1101/2021.03.26.21254381
```

**Query Parameters:**
* `doi` *(required)*: Preprint DOI to resolve and stream.
* `repo` *(optional)*: Explicitly specify repository (`arxiv`, `biorxiv`, `medrxiv`, `chemrxiv`, `psyarxiv`, `socarxiv`).
* `download` *(optional)*: Set to `true` to force direct attachment download (`Content-Disposition: attachment`), or `false` (default) for inline stream.

* **Status:** `200 OK`
* **Content-Type:** `application/pdf`
* **Custom Headers:**
  * `X-Preprint-Repo`: Identified repository (e.g., `medrxiv`).
  * `X-Processing-Time-Ms`: Retrieval time in milliseconds (e.g., `2559`).
  * `X-PDF-Size-Bytes`: Byte size of the in-memory PDF (e.g., `56886`).
  * `Content-Disposition`: `inline; filename="10.1101_2021.03.26.21254381_v2.pdf"`

---

## 💻 Client Integration (Python Example)

```python
import io
import requests
from pypdf import PdfReader

BASE_URL = "http://localhost:8000"

# Step 1: Query DOIs for a specific date range
dois_resp = requests.get(
    f"{BASE_URL}/api/v1/preprints/dois",
    params={"repo": "biorxiv", "from_date": "2023-01-01", "limit": 10}
)
items = dois_resp.json().get("data", [])

# Step 2: Stream PDF directly into memory buffer
target_doi = items[0]["doi"]
pdf_resp = requests.get(
    f"{BASE_URL}/api/v1/preprints/pdf",
    params={"doi": target_doi},
    stream=True
)

if pdf_resp.status_code == 200:
    # Load raw binary stream into memory (0 bytes persisted to disk!)
    pdf_buffer = io.BytesIO(pdf_resp.content)
    
    # Process with your internal parser
    reader = PdfReader(pdf_buffer)
    print(f"DOI: {target_doi} | Total Pages: {len(reader.pages)}")
```

---

## 🧪 Verification & Testing

To run the end-to-end integration demo:
```bash
python test_client.py
```
