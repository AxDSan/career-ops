#!/usr/bin/env python3
"""
Autonomous Job Scanner for CareerOps
Fetches jobs from Greenhouse, Ashby, and Lever APIs, filters, deduplicates,
and populates batch-input.tsv for evaluation.
"""

import json
import re
import sys
import uuid
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
import yaml

CAREER_OPS_DIR = Path("/root/.hermes/projects/career-ops")
BATCH_DIR = CAREER_OPS_DIR / "batch"
DATA_DIR = CAREER_OPS_DIR / "data"

INPUT_FILE = BATCH_DIR / "batch-input.tsv"
STATE_FILE = BATCH_DIR / "batch-state.tsv"
PIPELINE_FILE = DATA_DIR / "pipeline.md"
SCAN_HISTORY_FILE = DATA_DIR / "scan-history.tsv"
PORTALS_FILE = CAREER_OPS_DIR / "portals.yml"


def log(message: str):
    print(f"[{datetime.now().isoformat()}] {message}")


def load_portals():
    if not PORTALS_FILE.exists():
        log("❌ portals.yml not found")
        return None
    with open(PORTALS_FILE) as f:
        return yaml.safe_load(f)


def load_seen_urls():
    seen = set()
    if SCAN_HISTORY_FILE.exists():
        with open(SCAN_HISTORY_FILE) as f:
            next(f, None)  # skip header
            for line in f:
                parts = line.strip().split("\t")
                if parts:
                    seen.add(parts[0])
    # Also check pipeline.md for exact URLs
    if PIPELINE_FILE.exists():
        text = PIPELINE_FILE.read_text()
        for match in re.finditer(r"https?://[^\s|)\]]+", text):
            seen.add(match.group(0).rstrip(").,;"))
    # And applications.md company+role combinations
    return seen


def title_matches(title: str, filters: dict) -> bool:
    t = title.lower()
    # Must have at least one positive
    positives = filters.get("positive", [])
    if positives and not any(p.lower() in t for p in positives):
        return False
    # Must have zero negatives
    negatives = filters.get("negative", [])
    if any(n.lower() in t for n in negatives):
        return False
    return True


def slugify_company(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def fetch_greenhouse_jobs(board_token: str) -> list:
    url = f"https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs"
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        jobs = []
        for job in data.get("jobs", []):
            jobs.append({
                "title": job.get("title", "Unknown"),
                "url": job.get("absolute_url", ""),
                "company": data.get("name", board_token),
                "location": job.get("location", {}).get("name", ""),
            })
        return jobs
    except Exception as e:
        log(f"⚠️ Greenhouse API error for {board_token}: {e}")
        return []


def fetch_ashby_jobs(company_slug: str) -> list:
    """Ashby uses a public GraphQL endpoint for job boards."""
    url = "https://jobs.ashbyhq.com/api/non-user-graphql?operationName=ApiJobBoardWithTeams"
    payload = {
        "operationName": "ApiJobBoardWithTeams",
        "variables": {"organizationHosted CareersPageName": company_slug},
        "query": """
        query ApiJobBoardWithTeams($organizationHostedCareersPageName: String!) {
          jobBoard: jobBoardWithTeams(organizationHostedCareersPageName: $organizationHostedCareersPageName) {
            teams {
              id
              name
              jobs {
                id
                title
                employmentType
                locationId
                departmentId
                teamId
                isListed
                __typename
              }
              __typename
            }
            __typename
          }
        }
        """
    }
    try:
        resp = requests.post(url, json=payload, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        jobs = []
        teams = data.get("data", {}).get("jobBoard", {}).get("teams", [])
        for team in teams:
            for job in team.get("jobs", []):
                if not job.get("isListed", True):
                    continue
                job_id = job.get("id", "")
                jobs.append({
                    "title": job.get("title", "Unknown"),
                    "url": f"https://jobs.ashbyhq.com/{company_slug}/{job_id}",
                    "company": company_slug.replace("-", " ").title(),
                    "location": "",
                })
        return jobs
    except Exception as e:
        log(f"⚠️ Ashby API error for {company_slug}: {e}")
        return []


def fetch_lever_jobs(company_slug: str) -> list:
    """Lever has a public JSON endpoint."""
    url = f"https://api.lever.co/v0/postings/{company_slug}?mode=json"
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        jobs = []
        for job in data:
            jobs.append({
                "title": job.get("text", "Unknown"),
                "url": job.get("hostedUrl", job.get("applyUrl", "")),
                "company": company_slug.replace("-", " ").title(),
                "location": job.get("categories", {}).get("location", ""),
            })
        return jobs
    except Exception as e:
        log(f"⚠️ Lever API error for {company_slug}: {e}")
        return []


def extract_board_token(careers_url: str) -> str | None:
    """Extract Greenhouse board token from URL like https://job-boards.greenhouse.io/companyname"""
    parsed = urlparse(careers_url)
    parts = parsed.path.strip("/").split("/")
    if "greenhouse.io" in parsed.netloc and len(parts) >= 1:
        return parts[0]
    return None


def extract_ashby_slug(careers_url: str) -> str | None:
    parsed = urlparse(careers_url)
    parts = parsed.path.strip("/").split("/")
    if "jobs.ashbyhq.com" in parsed.netloc and len(parts) >= 1:
        return parts[0]
    return None


def extract_lever_slug(careers_url: str) -> str | None:
    parsed = urlparse(careers_url)
    parts = parsed.path.strip("/").split("/")
    if "jobs.lever.co" in parsed.netloc and len(parts) >= 1:
        return parts[0]
    return None


def run_scan():
    log("🚀 Starting autonomous job scan")

    portals = load_portals()
    if not portals:
        return {"status": "error", "reason": "missing portals.yml"}

    filters = portals.get("title_filter", {})
    seen_urls = load_seen_urls()
    today = datetime.now().strftime("%Y-%m-%d")

    all_candidates = []

    # === Level 1 & 2: Tracked companies (APIs) ===
    for company in portals.get("tracked_companies", []):
        if not company.get("enabled", True):
            continue

        name = company.get("name", "Unknown")
        careers_url = company.get("careers_url", "")
        api_url = company.get("api", "")
        scan_method = company.get("scan_method", "api")

        jobs = []

        # Try explicit API first
        if api_url and "greenhouse" in api_url:
            # Extract board token from API URL
            parsed = urlparse(api_url)
            parts = parsed.path.strip("/").split("/")
            if len(parts) >= 3:
                board_token = parts[2]
                jobs = fetch_greenhouse_jobs(board_token)

        # Fallback to URL-based extraction
        if not jobs and careers_url:
            if "greenhouse.io" in careers_url:
                token = extract_board_token(careers_url)
                if token:
                    jobs = fetch_greenhouse_jobs(token)
            elif "jobs.ashbyhq.com" in careers_url:
                slug = extract_ashby_slug(careers_url)
                if slug:
                    jobs = fetch_ashby_jobs(slug)
            elif "jobs.lever.co" in careers_url:
                slug = extract_lever_slug(careers_url)
                if slug:
                    jobs = fetch_lever_jobs(slug)

        if jobs:
            log(f"  ✅ {name}: {len(jobs)} jobs found")
        else:
            log(f"  ⚠️ {name}: no jobs found")

        for job in jobs:
            job["source"] = name
            all_candidates.append(job)

    # === Level 3: WebSearch queries ===
    # Try to use duckduckgo-search if available
    try:
        from duckduckgo_search import DDGS
        have_ddgs = True
    except ImportError:
        have_ddgs = False
        log("  ⚠️ duckduckgo-search not available, skipping web search queries")

    if have_ddgs:
        for query_config in portals.get("search_queries", []):
            if not query_config.get("enabled", True):
                continue
            qname = query_config.get("name", "unknown")
            query = query_config.get("query", "")
            if not query:
                continue

            try:
                with DDGS() as ddgs:
                    results = list(ddgs.text(query, max_results=10))
            except Exception as e:
                log(f"  ⚠️ DDGS error for {qname}: {e}")
                continue

            log(f"  🔍 {qname}: {len(results)} web results")

            for r in results:
                title = r.get("title", "")
                href = r.get("href", "")
                if not href or not href.startswith("http"):
                    continue

                # Try to extract company from title
                company = "Unknown"
                m = re.search(r"(.+?)(?:\s*[@|—–-]\s*|\s+at\s+)(.+?)$", title)
                if m:
                    job_title = m.group(1).strip()
                    company = m.group(2).strip()
                else:
                    job_title = title

                all_candidates.append({
                    "title": job_title,
                    "url": href,
                    "company": company,
                    "location": "",
                    "source": qname,
                })

    # === Filter & deduplicate ===
    filtered = []
    skipped_title = 0
    skipped_dup = 0

    for job in all_candidates:
        if not job.get("url"):
            continue
        if job["url"] in seen_urls:
            skipped_dup += 1
            continue
        if not title_matches(job["title"], filters):
            skipped_title += 1
            continue
        filtered.append(job)
        seen_urls.add(job["url"])

    log(f"📊 Total candidates: {len(all_candidates)}")
    log(f"📊 Filtered out (title): {skipped_title}")
    log(f"📊 Duplicates skipped: {skipped_dup}")
    log(f"📊 New matches: {len(filtered)}")

    if not filtered:
        return {"status": "success", "new_jobs": 0}

    # === Persist results ===

    # 1. batch-input.tsv
    INPUT_FILE.parent.mkdir(exist_ok=True)
    input_exists = INPUT_FILE.exists() and INPUT_FILE.stat().st_size > 0
    with open(INPUT_FILE, "a") as f:
        if not input_exists:
            f.write("id\turl\tcompany\ttitle\n")
        for job in filtered:
            job_id = f"job_{uuid.uuid4().hex[:12]}"
            f.write(f"{job_id}\t{job['url']}\t{job['company']}\t{job['title']}\n")

    # 2. pipeline.md
    pipeline_text = PIPELINE_FILE.read_text() if PIPELINE_FILE.exists() else "# Pipeline — Pending Job Offers\n\n## Pendientes\n\n## Procesadas\n\n"
    pending_section = "## Pendientes\n"
    insert_idx = pipeline_text.find(pending_section)
    if insert_idx != -1:
        insert_pos = insert_idx + len(pending_section)
        new_lines = ""
        for job in filtered:
            new_lines += f"\n- [ ] {job['url']} | {job['company']} | {job['title']}"
        pipeline_text = pipeline_text[:insert_pos] + new_lines + pipeline_text[insert_pos:]
        PIPELINE_FILE.write_text(pipeline_text)

    # 3. scan-history.tsv
    history_exists = SCAN_HISTORY_FILE.exists() and SCAN_HISTORY_FILE.stat().st_size > 0
    with open(SCAN_HISTORY_FILE, "a") as f:
        if not history_exists:
            f.write("url\tfirst_seen\tportal\ttitle\tcompany\tstatus\n")
        for job in filtered:
            source = job.get("source", "unknown")
            f.write(f"{job['url']}\t{today}\t{source}\t{job['title']}\t{job['company']}\tadded\n")

    log(f"✅ Added {len(filtered)} jobs to batch-input.tsv and pipeline.md")
    return {"status": "success", "new_jobs": len(filtered)}


if __name__ == "__main__":
    result = run_scan()
    print(json.dumps(result, indent=2))
    sys.exit(0 if result.get("new_jobs", 0) >= 0 else 1)
