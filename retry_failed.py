#!/usr/bin/env python3
"""
Local Retry and Failure Compensator for JLCParts / PartShelf.
Analyzes missing and failed components, cleans/categorizes the failure list,
and performs high-resilience local catch-up downloads and database updates.
"""

import argparse
import json
import os
from pathlib import Path
import random
import re
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

REPO_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = REPO_DIR.parent.parent

sys.path.insert(0, str(REPO_DIR))

from jlcparts.partLib import getLcscExtraNew
from jlcparts.lcsc import RateLimitError

PARTSHELF_DB_PATH = PROJECT_ROOT / "PartShelf" / "data" / "libraries" / "jlcparts.db"
CACHE_SQLITE_PATH = REPO_DIR / "cache.sqlite3"
PARTSHELF_IMAGES_DIR = PROJECT_ROOT / "PartShelf" / "static" / "images" / "parts"
CONSOLIDATED_FAILED_FILE = REPO_DIR / "consolidated_failed_components.json"
RETRY_SUMMARY_FILE = REPO_DIR / "retry_execution_summary.json"

_thread_local = threading.local()

def _get_thread_session():
    if not hasattr(_thread_local, "session"):
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=[500, 502, 503, 504],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(
            pool_connections=12,
            pool_maxsize=12,
            max_retries=retry_strategy,
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        _thread_local.session = session
    return _thread_local.session

def sanitize_filename(filename: str) -> str:
    if not filename:
        return ""
    return re.sub(r'[\\/:*?"<>|]', "_", filename)

def fetch_single_component(lcsc_code, jitter_range=(0.1, 0.25), images_dir=PARTSHELF_IMAGES_DIR):
    if jitter_range and jitter_range[1] > 0:
        time.sleep(random.uniform(jitter_range[0], jitter_range[1]))
    try:
        session = _get_thread_session()
        extra = getLcscExtraNew(lcsc_code, session=session)
        if images_dir and extra and extra.get("images"):
            target_dir = Path(images_dir)
            target_dir.mkdir(parents=True, exist_ok=True)
            for img in extra.get("images", []):
                small_url = img.get("small")
                raw_filename = img.get("filename")
                if small_url and raw_filename:
                    clean_filename = sanitize_filename(raw_filename)
                    img["filename"] = clean_filename
                    img_path = target_dir / clean_filename
                    if not img_path.exists():
                        try:
                            r = session.get(small_url, timeout=5)
                            if r.status_code == 200:
                                with open(img_path, "wb") as f:
                                    f.write(r.content)
                        except Exception:
                            pass
        return (lcsc_code, extra, None)
    except RateLimitError as e:
        return (lcsc_code, None, f"RateLimitError: {e}")
    except Exception as e:
        return (lcsc_code, None, f"{e}")

def get_missing_components(db_path, stock_only=False, limit=0):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    stock_filter = "AND j.stock > 0" if stock_only else ""
    limit_clause = f"LIMIT {int(limit)}" if limit > 0 else ""
    query = f"""
        SELECT j.lcsc, j.stock, j.mfr, j.category
        FROM jlc_components j
        LEFT JOIN lcsc_components l ON l.lcsc = j.lcsc
        WHERE l.lcsc IS NULL
        {stock_filter}
        ORDER BY (j.stock > 0) DESC, j.stock DESC, j.preferred DESC, j.lcsc ASC
        {limit_clause}
    """
    rows = cur.execute(query).fetchall()
    conn.close()
    return rows

def analyze_and_export_failures():
    print("----------------------------------------------------------------------")
    print("Phase 1: Failure Classification & Inventory Analysis")
    print("----------------------------------------------------------------------")

    raw_data = []
    if CONSOLIDATED_FAILED_FILE.exists():
        try:
            with open(CONSOLIDATED_FAILED_FILE, "r", encoding="utf-8") as f:
                raw_data = json.load(f)
        except Exception as e:
            print(f"Warning loading failure log: {e}")

    discontinued = []
    rate_limited = []
    other = []

    seen = set()
    for item in raw_data:
        code = item.get("lcsc")
        if not code or code in seen:
            continue
        seen.add(code)
        err = str(item.get("error", ""))
        if "Not found" in err:
            discontinued.append(item)
        elif "RateLimitError" in err or "403" in err or "429" in err:
            rate_limited.append(item)
        else:
            other.append(item)

    print(f"Total Unique Logged Failures:      {len(seen)}")
    print(f"  - Genuine Discontinued/Removed: {len(discontinued)}")
    print(f"  - Rate Limit / 403 / 429 Drops: {len(rate_limited)}")
    print(f"  - Other Network / Timeout:      {len(other)}")

    conn = sqlite3.connect(PARTSHELF_DB_PATH)
    total_jlc = conn.execute("SELECT count(*) FROM jlc_components").fetchone()[0]
    total_lcsc = conn.execute("SELECT count(*) FROM lcsc_components").fetchone()[0]
    missing_stock = conn.execute("SELECT count(*) FROM jlc_components j LEFT JOIN lcsc_components l ON l.lcsc = j.lcsc WHERE l.lcsc IS NULL AND j.stock > 0").fetchone()[0]
    missing_nostock = conn.execute("SELECT count(*) FROM jlc_components j LEFT JOIN lcsc_components l ON l.lcsc = j.lcsc WHERE l.lcsc IS NULL AND (j.stock == 0 OR j.stock IS NULL)").fetchone()[0]
    conn.close()

    print(f"\nDatabase Coverage Metrics:")
    print(f"  - Total Catalog Components:     {total_jlc}")
    print(f"  - Enriched with LCSC Details:   {total_lcsc} ({(total_lcsc/total_jlc)*100:.2f}%)")
    print(f"  - Missing In-Stock (Stock > 0): {missing_stock} (Highest Priority)")
    print(f"  - Missing Zero-Stock:           {missing_nostock} (Legacy/Discontinued)")

    # Save detailed categorized reports
    with open(REPO_DIR / "discontinued_components_list.json", "w", encoding="utf-8") as f:
        json.dump(discontinued, f, indent=2, ensure_ascii=False)
    with open(REPO_DIR / "rate_limited_retry_candidates.json", "w", encoding="utf-8") as f:
        json.dump(rate_limited, f, indent=2, ensure_ascii=False)

    print(f"\nCategorized lists saved to:")
    print(f"  - discontinued_components_list.json ({len(discontinued)} items)")
    print(f"  - rate_limited_retry_candidates.json ({len(rate_limited)} items)")
    print("----------------------------------------------------------------------\n")

    return missing_stock, missing_nostock

def batch_upsert(records, db_paths=[PARTSHELF_DB_PATH, CACHE_SQLITE_PATH]):
    if not records:
        return
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
            print(f"Warning: Failed batch upsert into {db_path}: {e}")

def run_retry_download(stock_only=False, limit=0, concurrency=6, jitter_min=0.1, jitter_max=0.25):
    print("----------------------------------------------------------------------")
    print("Phase 2: Local Catch-Up Download and Database Enrichment")
    print(f"Parameters: stock_only={stock_only}, limit={limit or 'All'}, concurrency={concurrency}")
    print("----------------------------------------------------------------------")

    raw_candidates = get_missing_components(PARTSHELF_DB_PATH, stock_only=stock_only, limit=limit)
    total = len(raw_candidates)
    print(f"Loaded {total} target components for retry processing.")

    if total == 0:
        print("No missing components match the given filter. All set!")
        return

    codes = [f"C{r[0]}" for r in raw_candidates]

    success_enriched = 0
    discontinued_count = 0
    rate_limited_count = 0
    records_buffer = []

    t0 = time.time()
    jitter = (jitter_min, jitter_max)

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {
            executor.submit(fetch_single_component, code, jitter, PARTSHELF_IMAGES_DIR): code
            for code in codes
        }

        for i, future in enumerate(as_completed(futures)):
            code = futures[future]
            try:
                code_res, extra, err = future.result()
            except Exception as e:
                err = f"Worker exception: {e}"
                extra = None
                code_res = code

            lcsc_clean = str(code_res).strip()
            lcsc_num = int(lcsc_clean[1:]) if lcsc_clean.upper().startswith("C") and lcsc_clean[1:].isdigit() else int(lcsc_clean)
            now = int(time.time())

            if err:
                if "RateLimitError" in str(err):
                    rate_limited_count += 1
                continue

            if extra:
                mfr_name = ""
                if isinstance(extra.get("manufacturer"), dict):
                    mfr_name = extra.get("manufacturer", {}).get("name", "")
                elif isinstance(extra.get("manufacturer"), str):
                    mfr_name = extra.get("manufacturer")

                raw_img = extra.get("images", [{}])[0].get("filename", "") if extra.get("images") else ""
                clean_img = sanitize_filename(raw_img)
                url_slug = extra.get("url", "").rsplit("/", 1)[-1].replace(".html", "") if extra.get("url") else ""

                records_buffer.append((
                    lcsc_num, now, mfr_name,
                    json.dumps(extra.get("attributes", {}), ensure_ascii=False),
                    clean_img, url_slug
                ))
                success_enriched += 1
            else:
                # Genuinely discontinued / not found: record empty placeholder to avoid perpetual retry
                records_buffer.append((lcsc_num, now, "", "{}", "", ""))
                discontinued_count += 1

            if len(records_buffer) >= 100:
                batch_upsert(records_buffer)
                records_buffer.clear()

            if (i + 1) % 50 == 0 or (i + 1) == total:
                elapsed = max(0.01, time.time() - t0)
                rate = (i + 1) / elapsed
                pct = ((i + 1) / total) * 100
                print(f"[{i + 1}/{total}] ({pct:.1f}%) | Enriched: {success_enriched} | Discontinued: {discontinued_count} | 403 Rate Limit: {rate_limited_count} | {rate:.1f} req/s")

    if records_buffer:
        batch_upsert(records_buffer)
        records_buffer.clear()

    elapsed = max(0.01, time.time() - t0)
    print("\n----------------------------------------------------------------------")
    print(f"Retry Run Complete in {elapsed:.1f}s")
    print(f"  - Newly Enriched with Full Metadata: {success_enriched}")
    print(f"  - Confirmed Discontinued/Empty:      {discontinued_count}")
    print(f"  - Rate Limit Encounters:             {rate_limited_count}")
    print("----------------------------------------------------------------------")

    summary = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_attempted": total,
        "newly_enriched": success_enriched,
        "confirmed_discontinued": discontinued_count,
        "rate_limited_encounters": rate_limited_count,
        "elapsed_seconds": round(elapsed, 1),
    }
    with open(RETRY_SUMMARY_FILE, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

def main():
    parser = argparse.ArgumentParser(description="Retry and complete missing/failed LCSC components.")
    parser.add_argument("--stock-only", action="store_true", default=False,
                        help="Only process components with stock > 0 (highest priority)")
    parser.add_argument("--limit", type=int, default=0,
                        help="Limit the number of components to retry (0 for all)")
    parser.add_argument("--concurrency", type=int, default=6,
                        help="Worker threads (default: 6)")
    parser.add_argument("--jitter-min", type=float, default=0.1,
                        help="Minimum delay jitter in seconds")
    parser.add_argument("--jitter-max", type=float, default=0.25,
                        help="Maximum delay jitter in seconds")
    parser.add_argument("--report-only", action="store_true", default=False,
                        help="Only analyze and generate failure lists without crawling")

    args = parser.parse_args()

    analyze_and_export_failures()

    if not args.report_only:
        run_retry_download(
            stock_only=args.stock_only,
            limit=args.limit,
            concurrency=args.concurrency,
            jitter_min=args.jitter_min,
            jitter_max=args.jitter_max
        )

if __name__ == "__main__":
    main()
