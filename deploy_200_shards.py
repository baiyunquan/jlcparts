#!/usr/bin/env python3
"""
Automated deployment and streaming merge manager for 200-shard matrix crawling.
Manages GitHub Actions matrix dispatch, artifact streaming, image extraction,
and high-speed database delta ingestion.
"""

import argparse
import gzip
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import time
import zipfile

REPO_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = REPO_DIR.parent.parent

CACHE_SQLITE_PATH = REPO_DIR / "cache.sqlite3"
PARTSHELF_DB_PATH = PROJECT_ROOT / "PartShelf" / "data" / "libraries" / "jlcparts.db"
PARTSHELF_IMAGES_DIR = PROJECT_ROOT / "PartShelf" / "static" / "images" / "parts"
STATE_FILE = REPO_DIR / "crawler_200_state.json"
FAILED_CONSOLIDATED_FILE = REPO_DIR / "consolidated_failed_components.json"

WORKFLOW_FILE = "crawl_all_matrix.yaml"
BRANCH_NAME = "feat/lcsc-open-api-crawler"

def run_cmd(cmd, cwd=REPO_DIR, check=True):
    print(f">> Executing: {' '.join(cmd) if isinstance(cmd, list) else cmd}")
    res = subprocess.run(
        cmd,
        cwd=cwd,
        shell=isinstance(cmd, str),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace"
    )
    if check and res.returncode != 0:
        print(f"Command failed (code {res.returncode}):\n{res.stderr}", file=sys.stderr)
        raise subprocess.CalledProcessError(res.returncode, cmd, res.stdout, res.stderr)
    return res

def load_state():
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {
        "run_id": None,
        "run_url": None,
        "started_at": None,
        "merged_shards": [],
        "total_components_merged": 0,
        "total_images_extracted": 0,
        "total_failures": 0,
    }

def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)

def ingest_delta(components, db_paths):
    if not components:
        return 0
    now = int(time.time())
    records = []
    for c in components:
        lcsc = c.get("lcsc")
        if not lcsc:
            continue
        mfr = c.get("manufacturer") or ""
        attrs = json.dumps(c.get("attributes") or {}, ensure_ascii=False)
        img = c.get("image") or ""
        url_slug = c.get("url_slug") or ""
        records.append((lcsc, now, mfr, attrs, img, url_slug))

    for db_path in db_paths:
        if not Path(db_path).exists():
            continue
        try:
            conn = sqlite3.connect(db_path)
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.execute("PRAGMA journal_mode = WAL")
            conn.executemany("""
                INSERT INTO lcsc_components (lcsc, fetched_at, manufacturer, attributes, image, url_slug)
                VALUES (?, ?, ?, ?, ?, ?)
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
            """, records)
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Warning: Failed to ingest into {db_path}: {e}")

    return len(records)

def process_shard_artifact(zip_path, target_images_dir, db_paths):
    extract_tmp = zip_path.parent / f"tmp_{zip_path.stem}"
    extract_tmp.mkdir(parents=True, exist_ok=True)
    comp_count = 0
    img_count = 0
    failed_count = 0

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_tmp)

        delta_gz = extract_tmp / "crawled_components.json.gz"
        delta_json = extract_tmp / "crawled_components.json"
        items = []
        if delta_gz.exists():
            try:
                with gzip.open(delta_gz, "rt", encoding="utf-8") as f:
                    items = json.load(f)
            except Exception as e:
                print(f"Warning: Error reading {delta_gz}: {e}")
        elif delta_json.exists():
            try:
                with open(delta_json, "r", encoding="utf-8") as f:
                    items = json.load(f)
            except Exception as e:
                print(f"Warning: Error reading {delta_json}: {e}")

        if items:
            comp_count = ingest_delta(items, db_paths)

        images_tar = extract_tmp / "images.tar.gz"
        if images_tar.exists() and images_tar.stat().st_size > 50:
            target_images_dir.mkdir(parents=True, exist_ok=True)
            try:
                with tarfile.open(images_tar, "r:gz") as tf:
                    members = tf.getmembers()
                    tf.extractall(target_images_dir)
                    img_count = len(members)
            except Exception as e:
                print(f"Warning: Error extracting {images_tar}: {e}")

        failed_file = extract_tmp / "failed_components.json"
        if failed_file.exists():
            try:
                with open(failed_file, "r", encoding="utf-8") as f:
                    fdata = json.load(f)
                    failed_count = len(fdata.get("failed_items", []))
            except Exception:
                pass

    finally:
        shutil.rmtree(extract_tmp, ignore_errors=True)

    return comp_count, img_count, failed_count

def deploy_workflow(concurrency=6, jitter_min=0.1, jitter_max=0.3, shard_size=5500):
    print("Preparing repository files for commit and push...")

    # Ensure queue exists
    queue_file = REPO_DIR / "data" / "lcsc_queue.txt.gz"
    if not queue_file.exists():
        print(f"Queue file missing: {queue_file}. Generating queue...")
        conn = sqlite3.connect(CACHE_SQLITE_PATH)
        cur = conn.execute("""
            SELECT j.lcsc 
            FROM jlc_components j 
            LEFT JOIN lcsc_components l ON l.lcsc = j.lcsc 
            ORDER BY (j.stock > 0) DESC, j.stock DESC, j.preferred DESC, COALESCE(l.fetched_at, 0) ASC, j.lcsc ASC
        """)
        queue_file.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(queue_file, "wt", encoding="utf-8") as f:
            for row in cur:
                f.write(f"C{row[0]}\n")
        conn.close()
        print("Generated queue successfully.")

    # Git add and commit
    run_cmd(["git", "add", "data/lcsc_queue.txt.gz", "scripts/crawl_shard.py",
             ".github/workflows/crawl_all_matrix.yaml", ".github/workflows/update_components.yaml",
             "jlcparts/ui.py", ".gitignore", "deploy_200_shards.py"])

    # Check diff
    diff_check = run_cmd(["git", "status", "--porcelain"], check=False)
    if diff_check.stdout.strip():
        print("Creating git commit...")
        run_cmd(["git", "commit", "-m", "feat: deploy 200-shard matrix crawler with slim artifact output"])
    else:
        print("Working tree already committed.")

    print("Pushing to remote origin...")
    run_cmd(["git", "push", "origin", BRANCH_NAME])

    print("Triggering GitHub Actions matrix workflow (200 shards)...")
    run_cmd([
        "gh", "workflow", "run", WORKFLOW_FILE,
        "--ref", BRANCH_NAME,
        "-f", f"concurrency={concurrency}",
        "-f", f"jitter_min={jitter_min}",
        "-f", f"jitter_max={jitter_max}",
        "-f", f"shard_size={shard_size}"
    ])

    print("Waiting 6 seconds for GitHub to register workflow run...")
    time.sleep(6)

    list_res = run_cmd([
        "gh", "run", "list",
        "--workflow", WORKFLOW_FILE,
        "--limit", "1",
        "--json", "databaseId,url,status,conclusion,createdAt"
    ])

    runs = json.loads(list_res.stdout)
    if not runs:
        print("Could not retrieve active run from GitHub. Please check https://github.com/baiyunquan/jlcparts/actions")
        return

    active_run = runs[0]
    run_id = str(active_run["databaseId"])
    run_url = active_run["url"]

    print("----------------------------------------------------------------------")
    print(f"Workflow triggered successfully!")
    print(f"Run ID:    {run_id}")
    print(f"Run URL:   {run_url}")
    print(f"Status:    {active_run.get('status')}")
    print(f"Shards:    200 matrix jobs (5,500 components per shard)")
    print(f"Artifacts: Slim output (images.tar.gz + crawled_components.json.gz)")
    print("----------------------------------------------------------------------")

    state = {
        "run_id": run_id,
        "run_url": run_url,
        "started_at": active_run.get("createdAt") or time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "merged_shards": [],
        "total_components_merged": 0,
        "total_images_extracted": 0,
        "total_failures": 0,
    }
    save_state(state)
    print("State initialized in crawler_200_state.json")

def sync_artifacts():
    state = load_state()
    run_id = state.get("run_id")
    if not run_id:
        print("No active run_id found in state. Run with --action deploy first.")
        return

    print(f"Checking artifacts for Run ID {run_id}...")
    artifacts_res = run_cmd([
        "gh", "api", f"/repos/baiyunquan/jlcparts/actions/runs/{run_id}/artifacts",
        "--jq", ".artifacts[] | {id: .id, name: .name, size_in_bytes: .size_in_bytes, expired: .expired}"
    ], check=False)

    if artifacts_res.returncode != 0 or not artifacts_res.stdout.strip():
        print("No artifacts available yet. The jobs are likely still running.")
        return

    artifacts = []
    for line in artifacts_res.stdout.strip().splitlines():
        if line.strip():
            try:
                artifacts.append(json.loads(line))
            except Exception:
                pass

    print(f"Found {len(artifacts)} total uploaded artifacts on GitHub.")
    merged_shards = set(state.get("merged_shards", []))
    dl_dir = REPO_DIR / "temp_shard_downloads"
    dl_dir.mkdir(parents=True, exist_ok=True)

    db_paths = [CACHE_SQLITE_PATH, PARTSHELF_DB_PATH]

    new_merges = 0
    for art in artifacts:
        name = art.get("name", "")
        if not name.startswith("shard_"):
            continue
        try:
            shard_idx = int(name.replace("shard_", ""))
        except ValueError:
            continue

        if shard_idx in merged_shards:
            continue

        print(f"Downloading artifact {name} (ID: {art['id']})...")
        zip_path = dl_dir / f"{name}.zip"
        run_cmd(["gh", "api", f"/repos/baiyunquan/jlcparts/actions/artifacts/{art['id']}/zip",
                 ">", str(zip_path)], shell=True, check=False)

        if not zip_path.exists() or zip_path.stat().st_size == 0:
            # Fallback to gh run download
            run_cmd(["gh", "run", "download", str(run_id), "-n", name, "-D", str(dl_dir / name)], check=False)
            extracted_sub = dl_dir / name
            if extracted_sub.exists():
                c_cnt, i_cnt, f_cnt = 0, 0, 0
                delta_gz = extracted_sub / "crawled_components.json.gz"
                if delta_gz.exists():
                    try:
                        with gzip.open(delta_gz, "rt", encoding="utf-8") as f:
                            c_cnt = ingest_delta(json.load(f), db_paths)
                    except Exception:
                        pass
                tar_p = extracted_sub / "images.tar.gz"
                if tar_p.exists() and tar_p.stat().st_size > 50:
                    PARTSHELF_IMAGES_DIR.mkdir(parents=True, exist_ok=True)
                    try:
                        with tarfile.open(tar_p, "r:gz") as tf:
                            tf.extractall(PARTSHELF_IMAGES_DIR)
                            i_cnt = len(tf.getmembers())
                    except Exception:
                        pass
                shutil.rmtree(extracted_sub, ignore_errors=True)
                merged_shards.add(shard_idx)
                state["total_components_merged"] += c_cnt
                state["total_images_extracted"] += i_cnt
                new_merges += 1
                print(f"Merged Shard {shard_idx}: {c_cnt} components, {i_cnt} images.")
            continue

        c_cnt, i_cnt, f_cnt = process_shard_artifact(zip_path, PARTSHELF_IMAGES_DIR, db_paths)
        zip_path.unlink(missing_ok=True)

        merged_shards.add(shard_idx)
        state["total_components_merged"] += c_cnt
        state["total_images_extracted"] += i_cnt
        state["total_failures"] += f_cnt
        new_merges += 1
        print(f"Merged Shard {shard_idx}: {c_cnt} components, {i_cnt} images, {f_cnt} failed.")

    state["merged_shards"] = sorted(list(merged_shards))
    save_state(state)
    print(f"Sync complete: {new_merges} new shards merged. Total merged: {len(merged_shards)}/200 shards.")

def print_status():
    state = load_state()
    run_id = state.get("run_id")
    print("----------------------------------------------------------------------")
    print(f"Active Run ID:            {run_id or 'None'}")
    print(f"Run URL:                  {state.get('run_url') or 'None'}")
    print(f"Started At:               {state.get('started_at') or 'None'}")
    print(f"Merged Shards:            {len(state.get('merged_shards', []))}/200")
    print(f"Total Components Merged:  {state.get('total_components_merged', 0)}")
    print(f"Total Images Extracted:   {state.get('total_images_extracted', 0)}")
    print(f"Total Failures Tracked:   {state.get('total_failures', 0)}")
    print("----------------------------------------------------------------------")

def main():
    parser = argparse.ArgumentParser(description="200-shard matrix crawler deployer and merger.")
    parser.add_argument("--action", choices=["deploy", "sync", "status"], default="deploy",
                        help="Action to perform: deploy, sync, or status")
    parser.add_argument("--concurrency", type=int, default=6,
                        help="Concurrent requests per worker")
    parser.add_argument("--jitter-min", type=float, default=0.1,
                        help="Minimum jitter delay")
    parser.add_argument("--jitter-max", type=float, default=0.3,
                        help="Maximum jitter delay")
    parser.add_argument("--shard-size", type=int, default=5500,
                        help="Components per shard")

    args = parser.parse_args()

    if args.action == "deploy":
        deploy_workflow(
            concurrency=args.concurrency,
            jitter_min=args.jitter_min,
            jitter_max=args.jitter_max,
            shard_size=args.shard_size
        )
    elif args.action == "sync":
        sync_artifacts()
    elif args.action == "status":
        print_status()

if __name__ == "__main__":
    main()
