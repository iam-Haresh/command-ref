"""
GitLab Pipeline Image Reporter
================================
For every project in a GitLab instance, finds the most recent pipeline
triggered from a feature/* branch, then reports the Docker image used
by jobs whose names contain 'build', 'test', or 'publish'.

Projects are processed concurrently using a thread pool. Each thread
owns its own GitLab client (python-gitlab's requests.Session is not
thread-safe when shared across threads).

Output: console table + pipeline_image_report.csv

Requirements:
    pip install python-gitlab tabulate

Environment variables (required):
    GITLAB_URL    - e.g. https://gitlab.example.com
    GITLAB_TOKEN  - personal/project access token with read_api scope
"""

import os
import csv
import sys
import logging
import threading
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import gitlab
from tabulate import tabulate

# ── Configuration ─────────────────────────────────────────────────────────────

GITLAB_URL   = os.environ.get("GITLAB_URL", "https://gitlab.com")
GITLAB_TOKEN = os.environ.get("GITLAB_TOKEN", "")

# Number of projects processed in parallel
THREAD_COUNT = 10

# How many recent pipelines to scan per project when looking for feature/* ones
PIPELINE_SCAN_LIMIT = 100

# Job name keywords → report column  (case-insensitive substring match)
JOB_CATEGORIES = {
    "build":   "build_image",
    "test":    "test_image",
    "publish": "publish_image",
}

OUTPUT_CSV = "pipeline_image_report.csv"

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  [%(threadName)s]  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Thread-local GitLab client ────────────────────────────────────────────────
# Each worker thread gets its own gitlab.Gitlab instance so HTTP sessions
# are never shared across threads.

_thread_local = threading.local()


def get_thread_gl() -> gitlab.Gitlab:
    """Return (or lazily create) the per-thread GitLab client."""
    if not hasattr(_thread_local, "gl"):
        _thread_local.gl = gitlab.Gitlab(GITLAB_URL, private_token=GITLAB_TOKEN)
    return _thread_local.gl


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_image_name(job_detail) -> str:
    """
    Extract the Docker image name from a fully-fetched job object.
    The 'image' attribute may be:
        - a dict  → {"name": "python:3.11", "entrypoint": [...]}
        - a str   → "python:3.11"
        - missing → job inherits from pipeline-level image (not exposed via API)
    """
    raw = getattr(job_detail, "image", None)
    if not raw:
        return "inherited / not set"
    if isinstance(raw, dict):
        return raw.get("name", str(raw))
    return str(raw)


def classify_job(job_name: str) -> Optional[str]:
    """Return the category key if the job name matches a keyword, else None."""
    lower = job_name.lower()
    for keyword in JOB_CATEGORIES:
        if keyword in lower:
            return keyword
    return None


# ── Progress counter (thread-safe) ────────────────────────────────────────────

class _Counter:
    """Simple thread-safe integer counter."""
    def __init__(self):
        self._lock  = threading.Lock()
        self._value = 0

    def increment(self) -> int:
        with self._lock:
            self._value += 1
            return self._value

    @property
    def value(self) -> int:
        with self._lock:
            return self._value


# ── Core logic ────────────────────────────────────────────────────────────────

def process_project(project_id: int, counter: _Counter, total: int) -> Optional[dict]:
    """
    Worker function — runs inside a thread-pool thread.

    Uses the thread-local GitLab client so each thread has its own
    HTTP session (python-gitlab / requests is not thread-safe when shared).

    Returns a report row dict, or None if no matching feature/* pipeline
    or relevant jobs were found for this project.
    """
    gl           = get_thread_gl()
    done         = counter.increment()
    project_name = f"id={project_id}"           # fallback label before fetch

    try:
        project      = gl.projects.get(project_id)
        project_name = project.path_with_namespace
    except gitlab.exceptions.GitlabGetError as exc:
        log.warning("[%d/%d] Cannot fetch project %s: %s", done, total, project_id, exc)
        return None

    log.info("[%d/%d] Processing: %s", done, total, project_name)

    # ── Step 1: find the most recent pipeline on a feature/* branch ──────────
    latest_feature_pipeline = None
    try:
        for pipeline in project.pipelines.list(
            per_page=PIPELINE_SCAN_LIMIT,
            order_by="id",
            sort="desc",
            iterator=False,
        ):
            ref = getattr(pipeline, "ref", "") or ""
            if ref.startswith("feature/"):
                latest_feature_pipeline = pipeline
                break
    except gitlab.exceptions.GitlabListError as exc:
        log.warning("  ↳ Cannot list pipelines for %s: %s", project_name, exc)
        return None

    if not latest_feature_pipeline:
        log.debug("  ↳ No feature/* pipeline found — skipping %s", project_name)
        return None

    pipeline_ref = latest_feature_pipeline.ref
    pipeline_id  = latest_feature_pipeline.id
    log.info("  ↳ Found pipeline #%s on ref=%s", pipeline_id, pipeline_ref)

    # ── Step 2: collect jobs matching build / test / publish ─────────────────
    row = {
        "project_url":   project.web_url,
        "project_name":  project_name,
        "branch":        pipeline_ref,
        "pipeline_id":   pipeline_id,
        "build_image":   "",
        "test_image":    "",
        "publish_image": "",
    }

    matched_any = False
    try:
        pipeline_jobs = latest_feature_pipeline.jobs.list(all=True)
    except gitlab.exceptions.GitlabListError as exc:
        log.warning("  ↳ Cannot list jobs: %s", exc)
        return None

    for job in pipeline_jobs:
        category = classify_job(job.name)
        if category is None:
            continue

        col = JOB_CATEGORIES[category]
        if row[col]:            # already filled by an earlier matching job
            continue

        # Full job detail required — the pipeline job list doesn't include 'image'
        try:
            job_detail = project.jobs.get(job.id)
            image_name = get_image_name(job_detail)
        except gitlab.exceptions.GitlabGetError as exc:
            log.warning("    ↳ Cannot fetch job %s (%s): %s", job.id, job.name, exc)
            image_name = "error fetching"

        log.info(
            "    job %-30s  category=%-8s  image=%s",
            job.name, category, image_name,
        )
        row[col]    = image_name
        matched_any = True

    return row if matched_any else None


# ── Orchestrator ──────────────────────────────────────────────────────────────

def run():
    if not GITLAB_TOKEN:
        log.error("GITLAB_TOKEN environment variable is not set. Exiting.")
        sys.exit(1)

    # Auth check via a dedicated client (main thread only)
    log.info("Connecting to GitLab: %s", GITLAB_URL)
    gl_main = gitlab.Gitlab(GITLAB_URL, private_token=GITLAB_TOKEN)
    try:
        gl_main.auth()
        log.info("Authenticated as: %s", gl_main.users.get_current().username)
    except Exception as exc:
        log.error("Authentication failed: %s", exc)
        sys.exit(1)

    # Collect all project IDs first (iterator keeps the main-thread session tidy)
    log.info("Fetching project list …")
    try:
        project_ids = [p.id for p in gl_main.projects.list(all=True, iterator=True)]
    except gitlab.exceptions.GitlabListError as exc:
        log.error("Failed to list projects: %s", exc)
        sys.exit(1)

    total = len(project_ids)
    log.info("Total projects found: %d  |  Thread pool size: %d", total, THREAD_COUNT)

    # ── Thread pool ───────────────────────────────────────────────────────────
    counter = _Counter()
    report  : list[dict] = []
    skipped = 0

    with ThreadPoolExecutor(max_workers=THREAD_COUNT, thread_name_prefix="gl-worker") as pool:
        # Submit all project IDs at once; the pool caps concurrency at THREAD_COUNT
        futures = {
            pool.submit(process_project, pid, counter, total): pid
            for pid in project_ids
        }

        for future in as_completed(futures):
            pid = futures[future]
            try:
                result = future.result()
                if result:
                    report.append(result)
                else:
                    skipped += 1
            except Exception as exc:
                log.error("Unhandled error for project id=%s: %s", pid, exc)
                skipped += 1

    # ── Output ────────────────────────────────────────────────────────────────
    if not report:
        log.warning("No matching projects found.")
        return

    # Sort by project name for a stable, readable output
    report.sort(key=lambda r: r["project_name"])

    table_cols = [
        ("Project URL",    "project_url"),
        ("Branch",         "branch"),
        ("Pipeline ID",    "pipeline_id"),
        ("Build Image",    "build_image"),
        ("Test Image",     "test_image"),
        ("Publish Image",  "publish_image"),
    ]

    table_data = [[row[k] for _, k in table_cols] for row in report]
    headers    = [h for h, _ in table_cols]

    print("\n" + tabulate(table_data, headers=headers, tablefmt="rounded_outline"))
    print(f"\n  Total projects   : {total}")
    print(f"  Matched projects : {len(report)}")
    print(f"  Skipped projects : {skipped}")
    print(f"  Thread pool size : {THREAD_COUNT}")

    # CSV
    csv_fields = [k for _, k in table_cols]
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=csv_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(report)

    log.info("CSV saved → %s", OUTPUT_CSV)


if __name__ == "__main__":
    run()
