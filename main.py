"""
main.py

Preprint PDF Streaming & DOI Provider API
A zero-storage microservice that supplies preprint DOIs and streams PDFs on-the-fly.
"""

import os
import io
import time
import threading
from contextlib import asynccontextmanager
from datetime import datetime
from enum import Enum
from typing import Optional, List
from fastapi import FastAPI, Query, HTTPException, Response
from fastapi.responses import StreamingResponse, JSONResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from pdf_resolver import (
    stream_pdf_bytes,
    get_mongo_db,
    COLLECTIONS_MAP
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager.
    Initializes background database indexes for high-speed date range filtering without blocking startup.
    """
    def _build_indexes():
        db = get_mongo_db()
        if db is not None:
            for coll_name in COLLECTIONS_MAP.values():
                try:
                    db[coll_name].create_index([("submission_date", 1)], background=True)
                except Exception:
                    pass
    threading.Thread(target=_build_indexes, daemon=True).start()
    yield

app = FastAPI(
    title="Preprint PDF",
    description="API for streaming preprint PDFs and querying repository DOIs.",
    version="1.0.0",
    lifespan=lifespan,
    swagger_ui_parameters={"syntaxHighlight": False}
)

# Enable CORS for internal services
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class RepoName(str, Enum):
    arxiv = "arxiv"
    biorxiv = "biorxiv"
    medrxiv = "medrxiv"
    chemrxiv = "chemrxiv"
    psyarxiv = "psyarxiv"
    socarxiv = "socarxiv"

class RepoSummary(BaseModel):
    repository: str
    collection_name: str
    total_documents: int

@app.get("/", tags=["Health & Info"])
def root():
    return {
        "service": "Preprint PDF Streaming API",
        "version": "1.0.0",
        "storage_mode": "in-memory (zero-disk-overhead)",
        "docs_url": "/docs",
        "viewer_url": "/viewer",
        "status": "healthy"
    }

@app.get("/viewer", response_class=HTMLResponse, tags=["Health & Info"])
def pdf_viewer():
    """Lightweight direct browser viewer to preview and test preprint PDFs without Swagger UI memory overhead."""
    return """<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Preprint PDF Direct Viewer</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; max-width: 680px; margin: 40px auto; padding: 20px; line-height: 1.6; color: #202124; background: #fff; }
        h1 { font-size: 24px; color: #1a73e8; margin-bottom: 8px; }
        p.subtitle { color: #5f6368; margin-top: 0; margin-bottom: 24px; font-size: 14px; }
        .card { background: #f8f9fa; border: 1px solid #dadce0; border-radius: 8px; padding: 24px; box-shadow: 0 1px 3px rgba(0,0,0,0.05); }
        label { display: block; font-weight: 600; margin-bottom: 8px; font-size: 14px; }
        .input-group { display: flex; gap: 8px; margin-bottom: 16px; }
        input[type="text"] { flex: 1; padding: 10px 14px; font-size: 15px; border: 1px solid #dadce0; border-radius: 6px; outline: none; }
        input[type="text"]:focus { border-color: #1a73e8; box-shadow: 0 0 0 2px rgba(26,115,232,0.2); }
        button { background: #1a73e8; color: white; border: none; padding: 10px 20px; font-size: 15px; font-weight: 500; border-radius: 6px; cursor: pointer; white-space: nowrap; transition: background 0.15s; }
        button:hover { background: #1557b0; }
        .samples { font-size: 13px; color: #5f6368; }
        .samples strong { color: #3c4043; }
        .sample-item { display: inline-block; background: #e8f0fe; color: #1a73e8; padding: 4px 10px; border-radius: 12px; margin: 4px 4px 4px 0; text-decoration: none; cursor: pointer; font-family: monospace; font-size: 12px; }
        .sample-item:hover { background: #d2e3fc; }
        .info-box { margin-top: 24px; padding: 16px; background: #e6f4ea; border-radius: 6px; font-size: 13px; color: #137333; }
    </style>
</head>
<body>
    <h1>📄 Preprint PDF Direct Stream Viewer</h1>
    <p class="subtitle">Stream preprint PDFs on-the-fly directly into Chrome's native viewer (zero disk storage, zero Swagger UI memory freeze).</p>
    
    <div class="card">
        <label for="doi">Preprint DOI:</label>
        <div class="input-group">
            <input type="text" id="doi" value="10.1101/2021.03.26.21254381" placeholder="Enter DOI (e.g. 10.1101/...)">
            <button onclick="openPdf()">Open PDF in New Tab ↗</button>
        </div>
        <div class="samples">
            <strong>Click a sample DOI to test:</strong><br>
            <span class="sample-item" onclick="setAndOpen('10.1101/2021.03.26.21254381')">medRxiv (10.1101/2021.03.26.21254381)</span>
            <span class="sample-item" onclick="setAndOpen('10.26434/chemrxiv.15011281/v1')">ChemRxiv (10.26434/chemrxiv.15011281/v1)</span>
            <span class="sample-item" onclick="setAndOpen('10.31235/osf.io/dptb7_v1')">SocArXiv (10.31235/osf.io/dptb7_v1)</span>
        </div>
    </div>

    <div class="info-box">
        ✓ <strong>Production Ready:</strong> In Python or internal tools, call <code>requests.get('/api/v1/preprints/pdf?doi=...')</code> to receive the raw bytes in memory with zero disk overhead.
    </div>

    <script>
        function setAndOpen(doi) {
            document.getElementById('doi').value = doi;
            openPdf();
        }
        function openPdf() {
            var doi = document.getElementById('doi').value.trim();
            if (doi) {
                window.open('/api/v1/preprints/pdf?doi=' + encodeURIComponent(doi), '_blank');
            }
        }
    </script>
</body>
</html>"""

@app.get("/api/v1/preprints/repos", tags=["Metadata & DOIs"])
def list_repositories():
    """Returns all supported repositories and document counts in MongoDB."""
    db = get_mongo_db()
    results = []
    
    for repo, coll_name in COLLECTIONS_MAP.items():
        count = 0
        if db is not None:
            try:
                count = db[coll_name].estimated_document_count()
            except Exception:
                pass
        results.append({
            "repository": repo,
            "collection_name": coll_name,
            "total_documents": count
        })
    return {"status": "success", "repositories": results}

@app.get("/api/v1/preprints/dois", tags=["Metadata & DOIs"])
def get_dois(
    repo: RepoName = Query(..., description="Repository name"),
    from_date: Optional[str] = Query(None, description="Optional start submission date (YYYY-MM-DD), e.g. 2023-01-01"),
    to_date: Optional[str] = Query(None, description="Optional end submission date (YYYY-MM-DD), e.g. 2023-12-31"),
    limit: int = Query(100, ge=1, le=10000, description="Number of DOIs to fetch (default: 100)"),
    skip: int = Query(0, ge=0, description="Number of records to skip for pagination (default: 0)")
):
    """
    Returns a paginated list of preprint DOIs for the specified repository,
    with optional submission date filtering (from_date / to_date).
    """
    clean_repo = repo.value if isinstance(repo, RepoName) else str(repo).lower().strip()
    if clean_repo not in COLLECTIONS_MAP:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported repo '{repo}'. Supported: {list(COLLECTIONS_MAP.keys())}"
        )

    db = get_mongo_db()
    if db is None:
        raise HTTPException(status_code=503, detail="Database connection currently unavailable.")

    coll = db[COLLECTIONS_MAP[clean_repo]]
    
    # Automatically filter out documents without a valid DOI
    query = {"doi": {"$exists": True, "$ne": None}}

    # Date range filtering on submission_date (YYYY-MM-DD)
    date_filter = {}
    if from_date:
        clean_from = from_date.strip()
        try:
            datetime.strptime(clean_from, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid from_date '{from_date}'. Expected format: YYYY-MM-DD (e.g. 2023-01-15)"
            )
        date_filter["$gte"] = clean_from

    if to_date:
        clean_to = to_date.strip()
        try:
            datetime.strptime(clean_to, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid to_date '{to_date}'. Expected format: YYYY-MM-DD (e.g. 2023-12-31)"
            )
        date_filter["$lte"] = clean_to

    if date_filter:
        query["submission_date"] = date_filter

    projection = {
        "doi": 1,
        "_id": 1,
        "title": 1,
        "version": 1,
        "submission_date": 1
    }

    cursor = coll.find(query, projection).skip(skip).limit(limit)
    
    items = []
    for doc in cursor:
        doi = doc.get("doi") or str(doc.get("_id"))
        items.append({
            "doi": doi,
            "title": doc.get("title", ""),
            "submission_date": doc.get("submission_date") or None,
            "version": doc.get("version", 1),
            "pdf_url": f"/api/v1/preprints/pdf?doi={doi}"
        })

    response_data = {
        "status": "success",
        "repository": clean_repo,
        "count": len(items),
        "skip": skip,
        "limit": limit
    }
    if from_date:
        response_data["from_date"] = from_date.strip()
    if to_date:
        response_data["to_date"] = to_date.strip()
    response_data["data"] = items

    return response_data

@app.get(
    "/api/v1/preprints/pdf",
    tags=["PDF Streaming"],
    response_class=StreamingResponse,
    responses={
        200: {
            "content": {
                "application/pdf": {
                    "schema": {
                        "type": "string",
                        "format": "binary"
                    }
                }
            },
            "description": "Raw binary stream of the preprint PDF."
        }
    }
)
def stream_pdf(
    doi: str = Query(..., description="Preprint DOI (e.g. 10.1101/2024.01.15.575631 or 10.26434/chemrxiv-2024-xyz)"),
    repo: Optional[RepoName] = Query(None, description="Optional: explicitly specify repository if known")
):
    """
    Streams the PDF binary directly into the response without saving to disk.

    ### 💡 Quick Browser Testing Links:
    - [Test medRxiv PDF (10.1101/2021.03.26.21254381)](/api/v1/preprints/pdf?doi=10.1101/2021.03.26.21254381)
    - [Test ChemRxiv PDF (10.26434/chemrxiv.15011281/v1)](/api/v1/preprints/pdf?doi=10.26434/chemrxiv.15011281/v1)
    - [Interactive Web Tester Page](/viewer)
    """
    start_time = time.time()
    clean_repo = repo.value if isinstance(repo, RepoName) else (repo.lower().strip() if repo else None)
    try:
        pdf_bytes, filename, detected_repo = stream_pdf_bytes(doi=doi, repo=clean_repo)
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=f"PDF validation error: {str(ve)}")
    except RuntimeError as re:
        raise HTTPException(status_code=502, detail=f"Upstream retrieval failed: {str(re)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error streaming PDF: {str(e)}")

    elapsed_ms = int((time.time() - start_time) * 1000)

    # Return stream directly from memory buffer
    headers = {
        "Content-Disposition": f'inline; filename="{filename}"',
        "X-Preprint-Repo": detected_repo,
        "X-Processing-Time-Ms": str(elapsed_ms),
        "X-PDF-Size-Bytes": str(len(pdf_bytes)),
    }

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers=headers
    )

if __name__ == "__main__":
    import uvicorn
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    reload = os.getenv("RELOAD", "true").lower() in ("true", "1", "yes")
    uvicorn.run("main:app", host=host, port=port, reload=reload)
