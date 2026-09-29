#!/usr/bin/env python3
"""
Automated JLCParts Crawl Orchestrator
------------------------------------
Orchestrates parallel GitHub Actions runs in rounds to crawl LCSC component
extras and images in batches (~10 minutes per round), downloads artifacts,
safely merges databases without data loss, and logs failure lists until
the entire target scope is crawled.

Rule: Strictly no emojis in code, comments, or outputs.
"""

import argparse
import datetime
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
LOCAL_CACHE_DB = BASE_DIR / "database" / "jlcparts" / "cache.sqlite3"
PARTSHELF_DB = BASE_DIR / "PartShelf" / "data" / "libraries" / "jlcparts.db"
STATE_FILE = BASE_DIR / "database" / "jlcparts" / "crawler_orchestrator_state.json"
TEMP_DOWNLOAD_DIR = BASE_DIR / "database" / "jlcparts" / "temp_orchestrator_downloads"


def log(msg: str):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted = f"[{timestamp}] {msg}"
    print(formatted, flush=True)


def get_db_stats(db_path: Path):
    if not db_path.exists():
        return {}
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM jlc_components")
    total_jlc = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM jlc_components WHERE stock > 0")
    total_stock = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM lcsc_components WHERE image IS NOT NULL AND image != ''")
    total_images = cur.fetchone()[0]
    cur.execute("""
        SELECT count(*)
        FROM jlc_components j
        LEFT JOIN lcsc_components l ON l.lcsc = j.lcsc
        WHERE j.stock > 0 AND (l.lcsc IS NULL OR l.image IS NULL OR l.image = '' OR l.attributes = '{}')
    """)
    stock_missing = cur.fetchone()[0]
    cur.execute("""
        SELECT count(*)
        FROM jlc_components j
        LEFT JOIN lcsc_components l ON l.lcsc = j.lcsc
        WHERE (l.lcsc IS NULL OR l.image IS NULL OR l.image = '' OR l.attributes = '{}')
    """)
    all_missing = cur.fetchone()[0]
    conn.close()
    return {
        "total_jlc": total_jlc,
        "total_stock": total_stock,
        "total_images": total_images,
        "stock_missing": stock_missing,
        "all_missing": all_missing,
    }


def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            log(f"Warning: Failed to read state file: {e}")
    return {
        "round": 1,
        "offset": 0,
        "stock_only": True,
        "completed_runs": [],
        "total_images_added": 0,
    }


def save_state(state: dict):
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
    except Exception as e:
        log(f"Warning: Failed to save state file: {e}")


def merge_action_db(action_db_path: Path, target_db_path: Path, label: str):
    if not target_db_path.exists():
        log(f"Target DB does not exist: {target_db_path}")
        return 0, 0
    t0 = time.time()
    conn = sqlite3.connect(target_db_path)
    cur = conn.cursor()

    cur.execute("SELECT count(*) FROM jlc_components")
    pre_jlc = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM lcsc_components WHERE image IS NOT NULL AND image != ''")
    pre_images = cur.fetchone()[0]

    cur.execute("ATTACH DATABASE ? AS action_db", (str(action_db_path),))

    cur.execute("""
    INSERT OR REPLACE INTO jlc_components (
        lcsc, fetched_at, present, sync_seen, category, subcategory, mfr, package, joints,
        manufacturer, library_type, preferred, last_on_stock, description, datasheet,
        stock, price, attributes, rohs, eccn, assembly, assembly_process, assembly_mode,
        website_component_id, attrition
    )
    SELECT
        lcsc, fetched_at, present, sync_seen, category, subcategory, mfr, package, joints,
        manufacturer, library_type, preferred, last_on_stock, description, datasheet,
        stock, price, attributes, rohs, eccn, assembly, assembly_process, assembly_mode,
        website_component_id, attrition
    FROM action_db.jlc_components
    """)

    cur.execute("""
    INSERT INTO lcsc_components (lcsc, fetched_at, manufacturer, attributes, image, url_slug)
    SELECT lcsc, fetched_at, manufacturer, attributes, image, url_slug
    FROM action_db.lcsc_components
    WHERE true
    ON CONFLICT(lcsc) DO UPDATE SET
        fetched_at = excluded.fetched_at,
        manufacturer = COALESCE(NULLIF(excluded.manufacturer, ''), lcsc_components.manufacturer),
        attributes = CASE 
            WHEN excluded.attributes IS NOT NULL AND excluded.attributes != '{}' THEN excluded.attributes 
            ELSE lcsc_components.attributes 
        END,
        image = CASE 
            WHEN excluded.image IS NOT NULL AND excluded.image != '' THEN excluded.image 
            ELSE lcsc_components.image 
        END,
        url_slug = CASE 
            WHEN excluded.url_slug IS NOT NULL AND excluded.url_slug != '' THEN excluded.url_slug 
            ELSE lcsc_components.url_slug 
        END
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_jlc_mfr ON jlc_components(mfr);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jlc_package ON jlc_components(package);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jlc_category ON jlc_components(category, subcategory);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jlc_stock ON jlc_components(stock);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_lcsc_comp_lcsc ON lcsc_components(lcsc);")
    conn.commit()

    cur.execute("DETACH DATABASE action_db")

    cur.execute("SELECT count(*) FROM jlc_components")
    post_jlc = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM lcsc_components WHERE image IS NOT NULL AND image != ''")
    post_images = cur.fetchone()[0]
    conn.close()

    elapsed = time.time() - t0
    added_jlc = post_jlc - pre_jlc
    added_images = post_images - pre_images
    log(f"Merged into {label} in {elapsed:.2f}s: +{added_images:,} images, +{added_jlc:,} components (Total images: {post_images:,})")
    return added_images, added_jlc


def get_active_runs(repo: str, branch: str) -> list:
    cmd = [
        "gh", "run", "list",
        "--repo", repo,
        "--workflow", "update_components.yaml",
        "--branch", branch,
        "--json", "databaseId,status,conclusion,createdAt,url",
        "--limit", "10",
    ]
    res = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if res.returncode != 0:
        log(f"Warning: Failed to list runs: {res.stderr.strip()}")
        return []
    try:
        runs = json.loads(res.stdout)
        active = [r for r in runs if r.get("status") in ("in_progress", "queued")]
        return active
    except Exception as e:
        log(f"Warning: Failed to parse runs JSON: {e}")
        return []


def trigger_workflow_run(repo: str, branch: str, workflow: str, offset: int, limit: int,
                         concurrency: int, max_seconds: int, stock_only: bool) -> str:
    cmd = [
        "gh", "workflow", "run", workflow,
        "--repo", repo,
        "--ref", branch,
        "-f", f"batch_offset={offset}",
        "-f", f"fetch_limit={limit}",
        "-f", f"concurrency={concurrency}",
        "-f", f"stock_only={str(stock_only).lower()}",
        "-f", f"max_seconds={max_seconds}",
    ]
    res = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if res.returncode != 0:
        log(f"Error triggering run for offset {offset}: {res.stderr.strip()}")
        return ""
    output = res.stdout.strip()
    log(f"Triggered workflow run for offset {offset} (limit={limit}): {output}")
    return output


def wait_for_runs(repo: str, run_ids: list, poll_interval: int = 30) -> dict:
    log(f"Monitoring {len(run_ids)} run(s): {run_ids} (polling every {poll_interval}s)...")
    pending = set(run_ids)
    results = {}
    start_wait = time.time()

    while pending:
        time.sleep(poll_interval)
        elapsed = int(time.time() - start_wait)
        for run_id in list(pending):
            cmd = ["gh", "run", "view", str(run_id), "--repo", repo, "--json", "status,conclusion"]
            res = subprocess.run(cmd, capture_output=True, text=True, check=False)
            if res.returncode != 0:
                continue
            try:
                info = json.loads(res.stdout)
                status = info.get("status")
                conclusion = info.get("conclusion")
                if status == "completed":
                    log(f"Run {run_id} completed: conclusion={conclusion} (elapsed {elapsed}s)")
                    results[run_id] = conclusion
                    pending.remove(run_id)
                else:
                    log(f"Run {run_id}: status={status} (elapsed {elapsed}s)")
            except Exception:
                pass

    return results


def process_run_artifacts(repo: str, run_id: int) -> int:
    run_dir = TEMP_DOWNLOAD_DIR / f"run_{run_id}"
    if run_dir.exists():
        shutil.rmtree(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)

    log(f"Downloading artifacts for run {run_id}...")
    cmd = ["gh", "run", "download", str(run_id), "--repo", repo, "--dir", str(run_dir)]
    res = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if res.returncode != 0:
        log(f"Warning: Artifact download failed for run {run_id}: {res.stderr.strip()}")
        return 0

    cache_files = list(run_dir.glob("**/cache.sqlite3"))
    if not cache_files:
        log(f"No cache.sqlite3 found in artifacts for run {run_id}")
        return 0

    downloaded_cache = cache_files[0]
    log(f"Found cache database: {downloaded_cache} ({downloaded_cache.stat().st_size:,} bytes)")

    added_part, _ = merge_action_db(downloaded_cache, PARTSHELF_DB, "PartShelf jlcparts.db")
    merge_action_db(downloaded_cache, LOCAL_CACHE_DB, "jlcparts/cache.sqlite3")

    failed_files = list(run_dir.glob("**/failed_components*.json"))
    for f in failed_files:
        try:
            with open(f, "r", encoding="utf-8") as fp:
                data = json.load(fp)
            total = data.get("total_requested", 0)
            failed = data.get("total_failed", 0)
            log(f"Artifact report [{f.name}]: requested {total}, failed {failed}")
        except Exception:
            pass

    try:
        shutil.rmtree(run_dir)
    except Exception:
        pass

    return added_part


def run_orchestrator(args):
    log("==================================================")
    log("JLCParts Automated Crawl Orchestrator Starting")
    log("==================================================")
    log(f"Repository:   {args.repo}")
    log(f"Branch:       {args.branch}")
    log(f"Batch size:   {args.batch_size} components (~10 min/Action)")
    log(f"Concurrency:  {args.concurrency} workers per Action")
    log(f"Parallel:     {args.parallel_runs} concurrent Actions per round")
    log(f"Max seconds:  {args.max_seconds}s per Action")
    log(f"Stock only:   {args.stock_only}")

    state = load_state()
    current_round = state.get("round", 1)
    current_offset = state.get("offset", 0)
    stock_only = state.get("stock_only", args.stock_only)

    while True:
        stats = get_db_stats(PARTSHELF_DB)
        log("--------------------------------------------------")
        log(f"Current DB Stats: {stats.get('total_jlc', 0):,} components, {stats.get('total_images', 0):,} with images")
        log(f"Missing (in-stock): {stats.get('stock_missing', 0):,} | Missing (total): {stats.get('all_missing', 0):,}")
        log(f"Current State: Round {current_round}, Next Offset: {current_offset}, stock_only: {stock_only}")
        log("--------------------------------------------------")

        if stock_only and stats.get("stock_missing", 0) == 0:
            log("All in-stock components have been crawled successfully!")
            if not args.auto_continue_all:
                log("Finished crawling in-stock components. Exiting orchestrator.")
                break
            log("Switching stock_only to False to crawl remaining out-of-stock components...")
            stock_only = False
            current_offset = 0

        if not stock_only and stats.get("all_missing", 0) == 0:
            log("All components in the entire database have been crawled! Complete.")
            break

        active_runs = get_active_runs(args.repo, args.branch)
        target_run_ids = []

        if active_runs:
            log(f"Detected {len(active_runs)} already active run(s) on GitHub. Adopting them for Round {current_round}:")
            for r in active_runs:
                run_id = r["databaseId"]
                log(f"  - Run {run_id} ({r['status']}): {r['url']}")
                target_run_ids.append(run_id)
        else:
            log(f"Launching Round {current_round}: {args.parallel_runs} parallel Actions...")
            for i in range(args.parallel_runs):
                offset = current_offset + (i * args.batch_size)
                trigger_workflow_run(
                    repo=args.repo,
                    branch=args.branch,
                    workflow=args.workflow,
                    offset=offset,
                    limit=args.batch_size,
                    concurrency=args.concurrency,
                    max_seconds=args.max_seconds,
                    stock_only=stock_only
                )
                time.sleep(3)

            time.sleep(15)
            active_runs = get_active_runs(args.repo, args.branch)
            target_run_ids = [r["databaseId"] for r in active_runs]
            log(f"Active run IDs in this round: {target_run_ids}")

        if not target_run_ids:
            log("Warning: No run IDs detected. Retrying in 30s...")
            time.sleep(30)
            continue

        results = wait_for_runs(args.repo, target_run_ids, poll_interval=args.poll_interval)
        log(f"Round {current_round} execution finished: {results}")

        round_added_images = 0
        for run_id in target_run_ids:
            if results.get(run_id) == "success":
                added = process_run_artifacts(args.repo, run_id)
                round_added_images += added
                if run_id not in state["completed_runs"]:
                    state["completed_runs"].append(run_id)
            else:
                log(f"Skipping artifact download for non-successful run {run_id} (conclusion: {results.get(run_id)})")

        log(f"Round {current_round} Complete! Newly added images in this round: +{round_added_images:,}")

        current_offset += (args.parallel_runs * args.batch_size)
        current_round += 1

        state["round"] = current_round
        state["offset"] = current_offset
        state["stock_only"] = stock_only
        state["total_images_added"] = state.get("total_images_added", 0) + round_added_images
        save_state(state)

        if args.single_round:
            log("Single round mode finished. Exiting.")
            break

        log("Cooling down 10s before launching next round...")
        time.sleep(10)


def main():
    parser = argparse.ArgumentParser(description="Automated JLCParts Crawl Orchestrator")
    parser.add_argument("--repo", default="baiyunquan/jlcparts", help="GitHub repo")
    parser.add_argument("--branch", default="feat/lcsc-open-api-crawler", help="Git branch")
    parser.add_argument("--workflow", default="update_components.yaml", help="Workflow filename")
    parser.add_argument("--batch-size", type=int, default=8000, help="Components per Action (~10 min)")
    parser.add_argument("--parallel-runs", type=int, default=4, help="Concurrent Actions per round")
    parser.add_argument("--concurrency", type=int, default=6, help="Workers per Action")
    parser.add_argument("--max-seconds", type=int, default=720, help="Max runtime per Action (seconds)")
    parser.add_argument("--stock-only", action="store_true", default=True, help="Prioritize in-stock")
    parser.add_argument("--poll-interval", type=int, default=30, help="Polling interval (seconds)")
    parser.add_argument("--auto-continue-all", action="store_true", default=False, help="Continue to out-of-stock")
    parser.add_argument("--single-round", action="store_true", default=False, help="Run only one round and exit")

    args = parser.parse_args()
    try:
        run_orchestrator(args)
    except KeyboardInterrupt:
        log("Orchestrator interrupted by user (Ctrl+C). Progress state is preserved.")


if __name__ == "__main__":
    main()
