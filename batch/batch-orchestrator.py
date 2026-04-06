#!/usr/bin/env python3
"""
career-ops batch orchestrator for Hermes Agent
Uses delegate_task via hermes chat -q subprocesses for safe parallel execution
"""
import argparse
import asyncio
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_DIR = SCRIPT_DIR.parent
BATCH_DIR = SCRIPT_DIR
INPUT_FILE = BATCH_DIR / "batch-input.tsv"
STATE_FILE = BATCH_DIR / "batch-state.tsv"
PROMPT_FILE = BATCH_DIR / "batch-prompt.md"
LOGS_DIR = BATCH_DIR / "logs"
TRACKER_DIR = BATCH_DIR / "tracker-additions"
REPORTS_DIR = PROJECT_DIR / "reports"
APPLICATIONS_FILE = PROJECT_DIR / "data" / "applications.md"
LOCK_FILE = BATCH_DIR / "batch-runner.pid"

DEFAULT_PARALLEL = 1
DEFAULT_MAX_RETRIES = 2


def log(msg):
    print(f"[{datetime.now().isoformat()}] {msg}")


def acquire_lock():
    if LOCK_FILE.exists():
        old_pid = LOCK_FILE.read_text().strip()
        try:
            os.kill(int(old_pid), 0)
            log(f"ERROR: Another batch runner is running (PID {old_pid})")
            log(f"If stale, remove {LOCK_FILE}")
            sys.exit(1)
        except (ProcessLookupError, ValueError):
            log(f"WARN: Stale lock file (PID {old_pid}). Removing.")
            LOCK_FILE.unlink()
    LOCK_FILE.write_text(str(os.getpid()))


def release_lock():
    LOCK_FILE.unlink(missing_ok=True)


def init_state():
    if not STATE_FILE.exists():
        STATE_FILE.write_text("id\turl\tstatus\tstarted_at\tcompleted_at\treport_num\tscore\terror\tretries\n")


def read_state():
    init_state()
    rows = []
    with open(STATE_FILE) as f:
        header = next(f).strip().split("\t")
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split("\t")
            row = dict(zip(header, parts))
            rows.append(row)
    return rows


def get_status(state_rows, offer_id):
    for row in state_rows:
        if row["id"] == str(offer_id):
            return row.get("status", "none")
    return "none"


def get_retries(state_rows, offer_id):
    for row in state_rows:
        if row["id"] == str(offer_id):
            return int(row.get("retries", "0") or 0)
    return 0


def next_report_num():
    max_num = 0
    if REPORTS_DIR.exists():
        for f in REPORTS_DIR.glob("*.md"):
            basename = f.name
            num_str = basename.split("-")[0]
            try:
                num = int(num_str)
                max_num = max(max_num, num)
            except ValueError:
                pass

    state_rows = read_state()
    for row in state_rows:
        rnum = row.get("report_num", "")
        if rnum and rnum != "-":
            try:
                max_num = max(max_num, int(rnum))
            except ValueError:
                pass
    return f"{max_num + 1:03d}"


def update_state(offer_id, url, status, started_at, completed_at, report_num, score, error, retries):
    init_state()
    rows = read_state()
    found = False
    for row in rows:
        if row["id"] == str(offer_id):
            row["status"] = status
            row["started_at"] = started_at
            row["completed_at"] = completed_at
            row["report_num"] = report_num
            row["score"] = score
            row["error"] = error
            row["retries"] = str(retries)
            found = True
            break

    if not found:
        rows.append({
            "id": str(offer_id),
            "url": url,
            "status": status,
            "started_at": started_at,
            "completed_at": completed_at,
            "report_num": report_num,
            "score": score,
            "error": error,
            "retries": str(retries),
        })

    with open(STATE_FILE, "w") as f:
        f.write("id\turl\tstatus\tstarted_at\tcompleted_at\treport_num\tscore\terror\tretries\n")
        for row in rows:
            f.write(f"{row['id']}\t{row['url']}\t{row['status']}\t{row['started_at']}\t{row['completed_at']}\t{row['report_num']}\t{row['score']}\t{row['error']}\t{row['retries']}\n")


def read_input_offers():
    offers = []
    if not INPUT_FILE.exists():
        return offers
    with open(INPUT_FILE) as f:
        header = next(f).strip().split("\t")
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split("\t")
            row = dict(zip(header, parts))
            if row.get("id") and row.get("url"):
                offers.append(row)
    return offers


def build_worker_prompt(url, jd_file, report_num, date, offer_id, company="", title=""):
    prompt_template = PROMPT_FILE.read_text()
    # Replace placeholders
    prompt = prompt_template.replace("{{URL}}", url)
    prompt = prompt.replace("{{JD_FILE}}", jd_file)
    prompt = prompt.replace("{{REPORT_NUM}}", report_num)
    prompt = prompt.replace("{{DATE}}", date)
    prompt = prompt.replace("{{ID}}", str(offer_id))
    prompt = prompt.replace("{{COMPANY}}", company)
    prompt = prompt.replace("{{TITLE}}", title)

    # Prepend job description content if file exists
    jd_content = ""
    jd_path = Path(jd_file)
    if jd_path.exists():
        jd_content = jd_path.read_text()
    else:
        jd_content = "[JD file not found - obtain from URL]"

    full_prompt = f"""{prompt}

---

## JD CONTENT FOR THIS OFFER

{jd_content}

---

Execute the full pipeline now. Return ONLY the JSON output specified in Paso 6 at the end of your response.
"""
    return full_prompt


async def run_hermes_worker(prompt, log_file):
    """Spawn a Hermes agent via hermes chat -q --quiet"""
    cmd = [
        "hermes", "chat", "-q", prompt,
        "--quiet",
        "--toolsets", "terminal,file,web",
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=str(PROJECT_DIR),
    )
    stdout, stderr = await proc.communicate()

    stdout_text = stdout.decode("utf-8", errors="replace")
    stderr_text = stderr.decode("utf-8", errors="replace")

    with open(log_file, "w") as f:
        f.write("=== STDOUT ===\n")
        f.write(stdout_text)
        f.write("\n=== STDERR ===\n")
        f.write(stderr_text)
        f.write(f"\n=== EXIT CODE ===\n{proc.returncode}\n")

    return proc.returncode, stdout_text, stderr_text


def extract_json_from_output(text):
    """Extract the final JSON object from Hermes output"""
    # Try to find JSON block
    patterns = [
        r"```json\s*(.*?)\s*```",
        r"```\s*(\{.*?)\s*```",
    ]
    for pat in patterns:
        matches = re.findall(pat, text, re.DOTALL)
        if matches:
            try:
                return json.loads(matches[-1])
            except json.JSONDecodeError:
                continue

    # Try to find raw JSON object at end of text
    try:
        # Look for last occurrence of {"status": ... }
        match = re.search(r'\{[\s\S]*"status"[\s\S]*\}', text)
        if match:
            return json.loads(match.group(0))
    except json.JSONDecodeError:
        pass

    return None


async def process_offer(offer, date, parallel_semaphore):
    offer_id = offer["id"]
    url = offer["url"]
    source = offer.get("source", "")
    notes = offer.get("notes", "")

    report_num = next_report_num()
    started_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    retries = get_retries(read_state(), offer_id)
    jd_file = f"/tmp/batch-jd-{offer_id}.txt"

    log(f"--- Processing offer #{offer_id}: {url} (report {report_num}, attempt {retries + 1})")
    update_state(offer_id, url, "processing", started_at, "-", report_num, "-", "-", retries)

    # Build prompt
    company = offer.get("company", "")
    title = offer.get("title", "")
    prompt = build_worker_prompt(url, jd_file, report_num, date, offer_id, company, title)
    log_file = LOGS_DIR / f"{report_num}-{offer_id}.log"

    async with parallel_semaphore:
        exit_code, stdout, stderr = await run_hermes_worker(prompt, str(log_file))

    completed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    result_json = extract_json_from_output(stdout)

    if result_json and result_json.get("status") == "completed":
        score = result_json.get("score", "-")
        update_state(offer_id, url, "completed", started_at, completed_at, report_num, str(score), "-", retries)
        log(f"    ✅ Completed (score: {score}, report: {report_num})")
        return True
    else:
        retries += 1
        error_msg = "Worker failed"
        if result_json and result_json.get("error"):
            error_msg = result_json["error"][:200]
        elif stderr.strip():
            error_msg = stderr.strip()[:200]
        elif stdout.strip():
            error_msg = stdout.strip()[-200:]
        update_state(offer_id, url, "failed", started_at, completed_at, report_num, "-", error_msg, retries)
        log(f"    ❌ Failed (attempt {retries}, exit code {exit_code})")
        return False


def merge_tracker():
    log("=== Merging tracker additions ===")
    merge_script = PROJECT_DIR / "merge-tracker.mjs"
    if merge_script.exists():
        subprocess.run(["node", str(merge_script)], cwd=PROJECT_DIR)
    else:
        log("WARN: merge-tracker.mjs not found")

    log("=== Verifying pipeline integrity ===")
    verify_script = PROJECT_DIR / "verify-pipeline.mjs"
    if verify_script.exists():
        result = subprocess.run(["node", str(verify_script)], cwd=PROJECT_DIR, capture_output=True, text=True)
        if result.returncode != 0:
            log("⚠️  Verification found issues")
        else:
            log("✅ Pipeline verified")
    else:
        log("WARN: verify-pipeline.mjs not found")


def print_summary():
    log("=== Batch Summary ===")
    rows = read_state()
    if not rows:
        log("No state found.")
        return

    total = len(rows)
    completed = sum(1 for r in rows if r.get("status") == "completed")
    failed = sum(1 for r in rows if r.get("status") == "failed")
    pending = total - completed - failed

    scored = [r for r in rows if r.get("status") == "completed" and r.get("score") not in ("", "-", None)]
    if scored:
        scores = []
        for r in scored:
            try:
                scores.append(float(r["score"]))
            except (ValueError, TypeError):
                pass
        avg = sum(scores) / len(scores) if scores else 0
        log(f"Total: {total} | Completed: {completed} | Failed: {failed} | Pending: {pending}")
        log(f"Average score: {avg:.1f}/5 ({len(scores)} scored)")
    else:
        log(f"Total: {total} | Completed: {completed} | Failed: {failed} | Pending: {pending}")


def main():
    parser = argparse.ArgumentParser(description="career-ops batch orchestrator for Hermes Agent")
    parser.add_argument("--parallel", type=int, default=DEFAULT_PARALLEL, help="Number of parallel workers")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be processed")
    parser.add_argument("--retry-failed", action="store_true", help="Only retry failed offers")
    parser.add_argument("--start-from", type=int, default=0, help="Start from offer ID N")
    parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES, help="Max retries per offer")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of pending offers to process (0 = unlimited)")
    args = parser.parse_args()

    if not INPUT_FILE.exists():
        log(f"ERROR: {INPUT_FILE} not found. Add offers first.")
        sys.exit(1)

    if not PROMPT_FILE.exists():
        log(f"ERROR: {PROMPT_FILE} not found.")
        sys.exit(1)

    if not args.dry_run:
        acquire_lock()

    os.makedirs(LOGS_DIR, exist_ok=True)
    os.makedirs(TRACKER_DIR, exist_ok=True)
    os.makedirs(REPORTS_DIR, exist_ok=True)

    offers = read_input_offers()
    state_rows = read_state()
    date = datetime.now().strftime("%Y-%m-%d")

    total_input = len(offers)
    if total_input == 0:
        log("No offers in input file.")
        if not args.dry_run:
            release_lock()
        sys.exit(0)

    # Filter pending offers
    pending = []
    for offer in offers:
        offer_id = offer["id"]
        # Only apply start_from to numeric IDs
        if args.start_from > 0:
            try:
                if int(offer_id) < args.start_from:
                    continue
            except ValueError:
                pass

        status = get_status(state_rows, offer_id)
        retries = get_retries(state_rows, offer_id)

        if args.retry_failed:
            if status != "failed":
                continue
            if retries >= args.max_retries:
                log(f"SKIP #{offer_id}: max retries ({args.max_retries}) reached")
                continue
        else:
            if status == "completed":
                continue
            if status == "failed" and retries >= args.max_retries:
                log(f"SKIP #{offer_id}: failed and max retries reached")
                continue

        pending.append(offer)

    if args.limit > 0:
        pending = pending[:args.limit]

    pending_count = len(pending)
    limit_str = f" (limited to {args.limit})" if args.limit > 0 else ""
    log(f"=== career-ops batch orchestrator ===")
    log(f"Parallel: {args.parallel} | Max retries: {args.max_retries}")
    log(f"Input: {total_input} offers | Pending: {pending_count}{limit_str}")

    if pending_count == 0:
        log("No offers to process.")
        print_summary()
        if not args.dry_run:
            release_lock()
        sys.exit(0)

    if args.dry_run:
        log("=== DRY RUN ===")
        for offer in pending:
            status = get_status(state_rows, offer["id"])
            log(f"  #{offer['id']}: {offer['url']} (status: {status})")
        log(f"Would process {pending_count} offers")
        sys.exit(0)

    # Process with asyncio + semaphore for parallel control
    semaphore = asyncio.Semaphore(args.parallel)

    async def run_all():
        tasks = [process_offer(offer, date, semaphore) for offer in pending]
        await asyncio.gather(*tasks)

    asyncio.run(run_all())

    merge_tracker()
    print_summary()

    if not args.dry_run:
        release_lock()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("Interrupted by user")
        release_lock()
        sys.exit(1)
    except Exception as e:
        log(f"ERROR: {e}")
        release_lock()
        sys.exit(1)
