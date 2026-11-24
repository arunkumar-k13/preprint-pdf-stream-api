"""
main.py

Preprint PDF Streaming & DOI Provider API
A zero-storage microservice that supplies preprint DOIs and streams PDFs on-the-fly.
"""

import os
import io
import time
import threading
import urllib.parse
from contextlib import asynccontextmanager
from datetime import datetime
from enum import Enum
from typing import Optional, List
from fastapi import FastAPI, Query, HTTPException, Response, Request
from fastapi.openapi.docs import get_swagger_ui_html
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
    docs_url=None,
    lifespan=lifespan,
    swagger_ui_parameters={"syntaxHighlight": False}
)

@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html():
    html_resp = get_swagger_ui_html(
        openapi_url="/openapi.json",
        title="Preprint PDF - Swagger UI",
        swagger_ui_parameters={"syntaxHighlight": False}
    )
    custom_script = """
    <script>
    // 1. Intercept "Execute" click on the PDF endpoint to trigger direct download without opening a new tab
    document.addEventListener('click', function(e) {
        var btn = e.target.closest('button.execute');
        if (!btn) return;
        var op = btn.closest('[id*="stream_pdf"], [data-path*="/api/v1/preprints/pdf"]');
        if (!op) return;

        var input = op.querySelector('input');
        var val = input ? input.value.trim() : '';
        if (val) {
            var dl = document.createElement('a');
            dl.href = '/api/v1/preprints/pdf?doi=' + encodeURIComponent(val) + '&download=true';
            dl.download = '';
            document.body.appendChild(dl);
            dl.click();
            document.body.removeChild(dl);
        }
    }, true);

    // 2. Refine the native Execute button label seamlessly without breaking button group layout
    setInterval(function() {
        var op = document.querySelector('[id*="stream_pdf"], [data-path*="/api/v1/preprints/pdf"]');
        if (op) {
            var execBtn = op.querySelector('button.execute');
            if (execBtn && execBtn.innerText === 'Execute') {
                execBtn.innerText = 'Execute & Download PDF ⬇';
            }
        }
    }, 300);
    </script>
    """
    body = html_resp.body.decode("utf-8").replace("</body>", custom_script + "</body>")
    return HTMLResponse(content=body, status_code=200)

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
        "status": "healthy"
    }

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
    request: Request,
    doi: str = Query(..., description="Preprint DOI (e.g. 10.1101/2024.01.15.575631 or 10.26434/chemrxiv-2024-xyz)"),
    repo: Optional[RepoName] = Query(None, description="Optional: explicitly specify repository if known"),
    download: bool = Query(False, include_in_schema=False),
    raw: bool = Query(False, include_in_schema=False)
):
    """
    Streams the PDF binary directly into the response without saving to disk.
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

    # Detect in-page AJAX fetch from Swagger UI
    referer = request.headers.get("referer", "")
    sec_fetch_dest = request.headers.get("sec-fetch-dest", "")
    sec_fetch_mode = request.headers.get("sec-fetch-mode", "")

    # In modern browsers, when navigating directly in a tab (address bar or window.open):
    # sec_fetch_dest is 'document' and sec_fetch_mode is 'navigate'.
    # In Swagger UI's internal AJAX fetch, sec_fetch_dest is 'empty' and referer contains '/docs'.
    is_direct_tab = (sec_fetch_dest == "document") or (sec_fetch_mode == "navigate")
    is_swagger_ajax = ("/docs" in referer) and (not is_direct_tab) and (not raw) and (not download)

    if is_swagger_ajax:
        # Prevent Swagger UI from buffering multi-megabyte binary in browser memory (which crashes Chrome)
        host = request.headers.get("host", "127.0.0.1:8000")
        download_url = f"{request.url.scheme}://{host}/api/v1/preprints/pdf?doi={urllib.parse.quote(doi)}&download=true"
        view_url = f"{request.url.scheme}://{host}/api/v1/preprints/pdf?doi={urllib.parse.quote(doi)}&raw=true"
        return JSONResponse(
            content={
                "status": "success",
                "doi": doi,
                "repository": detected_repo,
                "filename": filename,
                "size_bytes": len(pdf_bytes),
                "processing_time_ms": elapsed_ms,
                "download_url": download_url,
                "view_url": view_url,
                "message": "Direct download started. You can also click download_url above to re-download."
            }
        )

    # Return stream directly from memory buffer
    disposition = "attachment" if download else "inline"
    headers = {
        "Content-Disposition": f'{disposition}; filename="{filename}"',
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
