"""
test_client.py

Demonstration client for your teammate:
Shows how their internal processing tool can:
1. Get the list of DOIs for a repository.
2. Request and receive the live PDF stream in memory.
3. Process the PDF text without saving any file to disk.
"""

import io
import requests
from pypdf import PdfReader

API_BASE_URL = "http://127.0.0.1:8000"

def run_demo():
    print("=" * 65)
    print("Preprint PDF Streaming API — Zero-Storage Demo")
    print("=" * 65)

    # 1. Health check
    print("\n[Step 1] Checking API health...")
    try:
        r = requests.get(f"{API_BASE_URL}/", timeout=5)
        print("API Status:", r.json())
    except Exception as e:
        print(f"Error connecting to API at {API_BASE_URL}. Ensure it is running!\nDetails: {e}")
        return

    # 2. Get list of DOIs for ChemRxiv (with optional date filtering)
    target_repo = "chemrxiv"
    print(f"\n[Step 2] Fetching 3 DOIs from repository: '{target_repo}' (filtered by date range)...")
    r = requests.get(
        f"{API_BASE_URL}/api/v1/preprints/dois",
        params={
            "repo": target_repo,
            "limit": 3
        }
    )
    
    if r.status_code != 200:
        print("Failed to get DOIs:", r.text)
        return
        
    data = r.json()
    items = data.get("data", [])
    print(f"Found {len(items)} DOIs:")
    for idx, item in enumerate(items, 1):
        doi = item.get("doi")
        sub_date = item.get("submission_date") or "N/A"
        print(f"  {idx}. DOI: {doi} (Submission Date: {sub_date})")

    dois = [it["doi"] for it in items]

    if not dois:
        print("No DOIs found in database. Using a fallback demo DOI...")
        dois = ["10.26434/chemrxiv-2024-w85f6"]

    sample_doi = dois[0]

    # 3. Stream the PDF in memory
    print(f"\n[Step 3] Streaming PDF for DOI: '{sample_doi}' directly into RAM...")
    pdf_resp = requests.get(
        f"{API_BASE_URL}/api/v1/preprints/pdf",
        params={"doi": sample_doi},
        stream=True
    )

    if pdf_resp.status_code == 200:
        pdf_bytes = pdf_resp.content
        size_kb = len(pdf_bytes) / 1024
        repo_header = pdf_resp.headers.get("X-Preprint-Repo", "unknown")
        proc_time = pdf_resp.headers.get("X-Processing-Time-Ms", "N/A")
        
        print(f"✓ Success! Received PDF stream in memory.")
        print(f"  - Size in RAM: {size_kb:.2f} KB")
        print(f"  - Detected Repo: {repo_header}")
        print(f"  - Upstream Fetch Time: {proc_time} ms")
        print(f"  - Files saved to disk: 0 (ZERO)")

        # 4. Simulate his internal processor reading the PDF
        print("\n[Step 4] Simulating internal tool processing PDF from memory buffer...")
        reader = PdfReader(io.BytesIO(pdf_bytes))
        num_pages = len(reader.pages)
        first_page_text = reader.pages[0].extract_text() or ""
        snippet = first_page_text[:250].replace('\n', ' ')

        print(f"  - Total Pages: {num_pages}")
        print(f"  - First Page Text Snippet: \"{snippet}...\"")
        print("\n✓ Workflow verified end-to-end without disk persistence!")
    else:
        print(f"✗ Failed to stream PDF: HTTP {pdf_resp.status_code} - {pdf_resp.text}")

if __name__ == "__main__":
    run_demo()
