"""
pdf_resolver.py

Module responsible for resolving a DOI (or preprint identifier) to its live PDF binary stream.
Supports:
- arXiv
- bioRxiv
- medRxiv
- ChemRxiv
- PsyArXiv / SocArXiv (OSF preprints)

Zero disk storage: Streams directly into memory using curl_cffi Chrome TLS impersonation.
"""

import os
import io
import re
import time
import logging
from typing import Optional, Tuple
from curl_cffi import requests
import pymongo

logger = logging.getLogger("pdf_resolver")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017/")
DB_NAME = os.getenv("MONGO_DB_NAME", "preprint_data")
DEFAULT_TIMEOUT = int(os.getenv("UPSTREAM_TIMEOUT", "40"))

COLLECTIONS_MAP = {
    "arxiv": "arXiv_raw",
    "biorxiv": "bioRxiv_raw",
    "medrxiv": "medRxiv_raw",
    "chemrxiv": "ChemRxiv_raw",
    "psyarxiv": "PsyArXiv_raw",
    "socarxiv": "SocArXiv_raw"
}

_mongo_client = None

def get_mongo_db():
    global _mongo_client
    if _mongo_client is None:
        try:
            _mongo_client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
            _mongo_client.admin.command('ping')
        except Exception as e:
            logger.warning(f"MongoDB connection failed: {e}. Will operate in fallback resolution mode.")
            return None
    return _mongo_client[DB_NAME]

def detect_repo_from_doi(doi: str) -> Optional[str]:
    """Infers preprint repository from DOI prefix or patterns."""
    clean_doi = doi.strip().lower()
    
    if "arxiv" in clean_doi or clean_doi.startswith("10.48550/"):
        return "arxiv"
    if "chemrxiv" in clean_doi or clean_doi.startswith("10.26434/"):
        return "chemrxiv"
    if "10.31234/" in clean_doi or "psyarxiv" in clean_doi:
        return "psyarxiv"
    if "10.31235/" in clean_doi or "socarxiv" in clean_doi:
        return "socarxiv"
    if "10.1101/" in clean_doi:
        # 10.1101 can be either bioRxiv or medRxiv; will be disambiguated via DB or trial
        return "biorxiv"
    return None

def find_doc_in_mongo(doi: str, repo: Optional[str] = None) -> Tuple[Optional[str], Optional[dict]]:
    """Searches MongoDB for the preprint document in the specific repository only."""
    db = get_mongo_db()
    if db is None:
        return repo, None

    clean_doi = doi.strip()
    
    # 1. If repo explicitly specified, check that collection
    if repo and repo in COLLECTIONS_MAP:
        try:
            coll = db[COLLECTIONS_MAP[repo]]
            doc = coll.find_one({"$or": [{"doi": clean_doi}, {"_id": clean_doi}]})
            if doc:
                return repo, doc
        except Exception as e:
            logger.warning(f"Error querying {repo}: {e}")

    # 2. If 10.1101 prefix, check medRxiv and bioRxiv directly
    if "10.1101/" in clean_doi:
        for r_name in ("medrxiv", "biorxiv"):
            try:
                coll = db[COLLECTIONS_MAP[r_name]]
                doc = coll.find_one({"$or": [{"doi": clean_doi}, {"_id": clean_doi}]})
                if doc:
                    return r_name, doc
            except Exception as e:
                logger.warning(f"Error querying {r_name}: {e}")

    # 3. Infer repository from DOI pattern
    target_repo = detect_repo_from_doi(clean_doi)
    if target_repo and target_repo in COLLECTIONS_MAP:
        try:
            coll = db[COLLECTIONS_MAP[target_repo]]
            doc = coll.find_one({"$or": [{"doi": clean_doi}, {"_id": clean_doi}]})
            if doc:
                return target_repo, doc
        except Exception as e:
            logger.warning(f"Error querying {target_repo}: {e}")

    return target_repo, None

def resolve_pdf_url(doi: str, repo: Optional[str] = None, doc: Optional[dict] = None) -> Tuple[str, str]:
    """
    Constructs the target PDF URL and suggested safe filename.
    Returns (pdf_url, filename).
    """
    clean_doi = doi.strip()
    doc = doc or {}
    
    if repo == "arxiv":
        arxiv_id = doc.get("arxiv_id")
        if not arxiv_id:
            arxiv_id = clean_doi.split("/")[-1].replace("arxiv.", "").replace("arXiv.", "")
        safe_id = re.sub(r'[\\/*?:"<>|]', '_', arxiv_id).strip()
        return f"https://arxiv.org/pdf/{arxiv_id}.pdf", f"{safe_id}.pdf"

    elif repo in ("biorxiv", "medrxiv"):
        version = doc.get("version") or 1
        safe_doi = re.sub(r'[\\/*?:"<>|]', '_', clean_doi).strip()
        # bioRxiv / medRxiv PDF endpoint format:
        return f"https://www.{repo}.org/content/{clean_doi}v{version}.full.pdf", f"{safe_doi}_v{version}.pdf"

    elif repo == "chemrxiv":
        safe_doi = re.sub(r'[\\/*?:"<>|]', '_', clean_doi).strip()
        return f"https://chemrxiv.org/doi/pdf/{clean_doi}", f"{safe_doi}.pdf"

    elif repo in ("psyarxiv", "socarxiv"):
        safe_doi = re.sub(r'[\\/*?:"<>|]', '_', clean_doi).strip()
        osf_id = doc.get("osf_id")
        if not osf_id and "osf.io/" in clean_doi:
            osf_id = clean_doi.split("osf.io/")[-1]
        elif not osf_id and doc.get("id"):
            osf_id = str(doc.get("id")).split("osf.io/")[-1]
            
        if osf_id:
            clean_id = re.sub(r'_v\d+$', '', osf_id).strip()
            return f"https://osf.io/{clean_id}/download", f"{safe_doi}.pdf"
        elif doc.get("pdf_url"):
            return doc.get("pdf_url"), f"{safe_doi}.pdf"
        return f"https://doi.org/{clean_doi}", f"{safe_doi}.pdf"

    # Default fallback
    safe_doi = re.sub(r'[\\/*?:"<>|]', '_', clean_doi).strip()
    return f"https://doi.org/{clean_doi}", f"{safe_doi}.pdf"

def stream_pdf_bytes(doi: str, repo: Optional[str] = None, timeout: int = DEFAULT_TIMEOUT) -> Tuple[bytes, str, str]:
    """
    Fetches the PDF into memory buffer without saving to disk.
    Returns (pdf_bytes, filename, detected_repo).
    Raises ValueError / RuntimeError on failure.
    """
    clean_doi = doi.strip()
    
    # 1. Disambiguate repo and get metadata from MongoDB if available
    detected_repo, doc = find_doc_in_mongo(clean_doi, repo)
    if not detected_repo:
        detected_repo = detect_repo_from_doi(clean_doi) or "biorxiv"

    # 2. Get target PDF URL
    pdf_url, filename = resolve_pdf_url(clean_doi, detected_repo, doc)

    # 3. Fetch with Chrome TLS impersonation (bypasses Cloudflare & Bot checks)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/pdf,*/*"
    }

    for attempt in range(2):
        try:
            resp = requests.get(
                pdf_url,
                impersonate="chrome124",
                allow_redirects=True,
                timeout=timeout
            )
            if resp.status_code == 200:
                break
            elif resp.status_code in (403, 429) and attempt == 0:
                time.sleep(1.5)
        except Exception as e:
            if attempt == 1:
                raise RuntimeError(f"Network error contacting upstream {pdf_url}: {e}")
            time.sleep(1)

    # If bioRxiv fails with 404 on 10.1101, try medRxiv as fallback
    if resp.status_code == 404 and detected_repo == "biorxiv" and "10.1101/" in clean_doi:
        alt_repo = "medrxiv"
        pdf_url, filename = resolve_pdf_url(clean_doi, alt_repo, doc)
        resp = requests.get(pdf_url, headers=headers, impersonate="chrome124", allow_redirects=True, timeout=timeout)
        if resp.status_code == 200:
            detected_repo = alt_repo

    if resp.status_code != 200:
        raise RuntimeError(f"Upstream server returned HTTP {resp.status_code} for {pdf_url}")

    content = resp.content
    if len(content) < 500:
        raise ValueError(f"Content too short ({len(content)} bytes) to be a valid PDF manuscript.")

    # Validate %PDF magic header
    if not content.startswith(b"%PDF"):
        # Sometimes html landing page is returned instead of direct PDF
        raise ValueError("Upstream response did not begin with %PDF header (likely redirected to landing page).")

    return content, filename, detected_repo
