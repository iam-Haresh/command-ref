"""
GitLab Pipeline Image Report
----------------------------
For all non-archived projects in GitLab:
  - Finds the latest pipeline triggered from a feature/* branch
  - Looks for jobs containing: build, test, publish (in job name)
  - Extracts the Docker image used by parsing job trace logs
  - Outputs a CSV report

Usage:
    python gitlab_image_report.py

Requirements:
    pip install python-gitlab
"""

import re
import csv
import sys
import time
import logging
from datetime import datetime

import gitlab

# ── Config ────────────────────────────────────────────────────────────────────
GITLAB_URL   = "https://gitlab.example.com"
TOKEN        = "your_private_token"
BRANCH_PREFIX = "feature/"
JOB_KEYWORDS  = ["build", "test", "publish"]   # order matters for matching priority
OUTPUT_FILE   = f"gitlab_image_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

# How many pipelines to scan per project before giving up on finding a feature/* one
MAX_PIPELINES_TO_SCAN = 100

# Delay between trace fetches to avoid hammering the API (seconds)
TRACE_FETCH_DELAY = 0.2
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)


IMAGE_RE = re.compile(
    r"Using Docker executor with image\s+(.+?)\s*\.\.\.",
    re.IGNORECASE
)


def extract_image_from_trace(trace: str) -> str:
    """Parse docker image name from GitLab runner trace log."""
    match = IMAGE_RE.search(trace)
    return match.group(1).strip() if match else None


def get_latest_feature_pipeline(project):
    """
    Scan recent pipelines for this project and return the most recent one
    whose ref starts with 'feature/'. Returns None if not found.
    """
    try:
        count = 0
        for pl in project.pipelines.list(
            iterator=True,
            per_page=50,
            order_by="id",
            sort="desc"
        ):
            if pl.ref and pl.ref.startswith(BRANCH_PREFIX):
                return pl
            count += 1
            if count >= MAX_PIPELINES_TO_SCAN:
                break
    except gitlab.exceptions.GitlabListError as e:
        log.warning(f"  Could not list pipelines: {e}")
    return None


def get_matched_keyword(job_name: str) -> str | None:
    """Return the first matching keyword found in a job name, or None."""
    name_lower = job_name.lower()
    for kw in JOB_KEYWORDS:
        if kw in name_lower:
            return kw
    return None


def fetch_job_images(project, pipeline) -> dict:
    """
    For a given pipeline, iterate jobs and extract images for
    build/test/publish jobs by parsing their trace logs.
    Returns dict: { "build": "<image>", "test": "<image>", "publish": "<image>" }
    """
    images = {kw: None for kw in JOB_KEYWORDS}

    try:
        # Get the full pipeline object (list returns lazy objects)
        full_pipeline = project.pipelines.get(pipeline.id)
        jobs = full_pipeline.jobs.list(all=True)
    except gitlab.exceptions.GitlabGetError as e:
        log.warning(f"  Could not fetch pipeline jobs: {e}")
        return images

    for job in jobs:
        keyword = get_matched_keyword(job.name)
        if not keyword:
            continue

        # Skip if we already captured an image for this keyword
        if images[keyword] is not None:
            log.debug(f"  Skipping {job.name} — already have image for '{keyword}'")
            continue

        log.info(f"  Fetching trace for job: [{keyword}] {job.name} (id={job.id})")

        try:
            raw_trace = project.jobs.get(job.id).trace()
            trace_text = (
                raw_trace.decode("utf-8", errors="ignore")
                if isinstance(raw_trace, bytes)
                else raw_trace
            )
            image = extract_image_from_trace(trace_text)
            images[keyword] = image if image else "shell/custom runner"
            time.sleep(TRACE_FETCH_DELAY)

        except gitlab.exceptions.GitlabGetError as e:
            log.warning(f"  Could not fetch trace for job {job.name}: {e}")
            images[keyword] = "trace fetch error"

    # Fill missing keywords with N/A
    for kw in JOB_KEYWORDS:
        if images[kw] is None:
            images[kw] = "N/A"

    return images


def main():
    log.info(f"Connecting to {GITLAB_URL}")
    gl = gitlab.Gitlab(GITLAB_URL, private_token=TOKEN)

    try:
        gl.auth()
        log.info(f"Authenticated as: {gl.users.get_current().username}")
    except Exception as e:
        log.error(f"Authentication failed: {e}")
        sys.exit(1)

    log.info("Fetching all non-archived projects...")
    projects = gl.projects.list(archived=False, iterator=True, per_page=100)

    report_rows = []
    total = 0
    skipped = 0

    for project in projects:
        total += 1
        log.info(f"[{total}] {project.name_with_namespace}")

        pipeline = get_latest_feature_pipeline(project)
        if not pipeline:
            log.info(f"  ↳ No feature/* pipeline found — skipping")
            skipped += 1
            continue

        log.info(f"  ↳ Pipeline #{pipeline.id}  branch={pipeline.ref}  status={pipeline.status}")

        images = fetch_job_images(project, pipeline)

        report_rows.append({
            "project_url":   project.web_url,
            "project_name":  project.name_with_namespace,
            "branch":        pipeline.ref,
            "pipeline_id":   pipeline.id,
            "pipeline_status": pipeline.status,
            "build_image":   images["build"],
            "test_image":    images["test"],
            "publish_image": images["publish"],
        })

        log.info(
            f"  ↳ build={images['build']} | "
            f"test={images['test']} | "
            f"publish={images['publish']}"
        )

    # ── Write CSV ─────────────────────────────────────────────────────────────
    fieldnames = [
        "project_name",
        "project_url",
        "branch",
        "pipeline_id",
        "pipeline_status",
        "build_image",
        "test_image",
        "publish_image",
    ]

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(report_rows)

    log.info("")
    log.info("=" * 60)
    log.info(f"Report saved to  : {OUTPUT_FILE}")
    log.info(f"Total projects   : {total}")
    log.info(f"With feature/*   : {len(report_rows)}")
    log.info(f"Skipped (no match): {skipped}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
