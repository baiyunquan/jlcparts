#!/usr/bin/env python3
import argparse
import gzip
import json
import os
from pathlib import Path
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# Ensure project root is in sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from jlcparts.partLib import getLcscExtraNew
from jlcparts.lcsc import RateLimitError

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
            pool_connections=10,
            pool_maxsize=10,
            max_retries=retry_strategy,
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        _thread_local.session = session
    return _thread_local.session

def fetch_single_component(lcsc_code, jitter_range=(0.1, 0.3), images_dir=None):
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
                filename = img.get("filename")
                if small_url and filename:
                    img_path = target_dir / filename
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

def main():
    parser = argparse.ArgumentParser(description="Crawl LCSC component data for a specific shard.")
    parser.add_argument("--queue", type=str, default="data/lcsc_queue.txt.gz",
                        help="Path to gzipped LCSC component queue file")
    parser.add_argument("--shard", type=int, required=True,
                        help="Shard index (e.g. 0 to 199)")
    parser.add_argument("--shard-size", type=int, default=5500,
                        help="Number of components per shard")
    parser.add_argument("--concurrency", type=int, default=6,
                        help="Worker concurrency")
    parser.add_argument("--jitter-min", type=float, default=0.1,
                        help="Minimum delay jitter in seconds")
    parser.add_argument("--jitter-max", type=float, default=0.3,
                        help="Maximum delay jitter in seconds")
    parser.add_argument("--images-dir", type=str, default="downloaded_images",
                        help="Directory to store thumbnail images")
    parser.add_argument("--crawled-output", type=str, default="output_data/crawled_components.json.gz",
                        help="Path to output crawled component delta JSON (.gz)")
    parser.add_argument("--failed-log", type=str, default="output_data/failed_components.json",
                        help="Path to output failed components summary JSON")

    args = parser.parse_args()

    start_idx = args.shard * args.shard_size
    end_idx = start_idx + args.shard_size

    queue_path = Path(args.queue)
    if not queue_path.exists():
        print(f"Error: Queue file {queue_path} does not exist", file=sys.stderr)
        sys.exit(1)

    print(f"Starting Shard {args.shard}: lines [{start_idx} .. {end_idx}) from {queue_path}")
    t0 = time.time()

    components = []
    if queue_path.name.endswith(".gz"):
        with gzip.open(queue_path, "rt", encoding="utf-8") as f:
            for idx, line in enumerate(f):
                if idx >= end_idx:
                    break
                if idx >= start_idx:
                    components.append(line.strip())
    else:
        with open(queue_path, "r", encoding="utf-8") as f:
            for idx, line in enumerate(f):
                if idx >= end_idx:
                    break
                if idx >= start_idx:
                    components.append(line.strip())

    total = len(components)
    print(f"Loaded {total} component codes for Shard {args.shard}")
    if total == 0:
        print(f"Shard {args.shard} has no components (out of range). Exiting.")
        sys.exit(0)

    if args.images_dir:
        Path(args.images_dir).mkdir(parents=True, exist_ok=True)

    success_count = 0
    failed_items = []
    crawled_components = []
    consecutive_rate_limits = 0

    jitter = (args.jitter_min, args.jitter_max)

    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = {
            executor.submit(fetch_single_component, code, jitter, args.images_dir): code
            for code in components
        }

        for i, future in enumerate(as_completed(futures)):
            code = futures[future]
            try:
                code_res, extra, err = future.result()
            except Exception as e:
                err = f"Worker exception: {e}"
                extra = None
                code_res = code

            if err:
                if "RateLimitError" in str(err):
                    consecutive_rate_limits += 1
                    print(f"Rate limit hit on {code}: {err} (consecutive: {consecutive_rate_limits})")
                    if consecutive_rate_limits >= 15:
                        print("CRITICAL: Exceeded 15 consecutive rate limits. Backing off 30s...")
                        time.sleep(30)
                        consecutive_rate_limits = 0
                else:
                    consecutive_rate_limits = 0
                failed_items.append({"lcsc": code_res, "error": str(err)})
                continue

            consecutive_rate_limits = 0

            # If component has valid extra data
            if extra:
                mfr_name = ""
                if isinstance(extra.get("manufacturer"), dict):
                    mfr_name = extra.get("manufacturer", {}).get("name", "")
                elif isinstance(extra.get("manufacturer"), str):
                    mfr_name = extra.get("manufacturer")

                img_name = extra.get("images", [{}])[0].get("filename", "") if extra.get("images") else ""
                url_slug = extra.get("url", "").rsplit("/", 1)[-1].replace(".html", "") if extra.get("url") else ""

                lcsc_clean = str(code_res).strip()
                if lcsc_clean.upper().startswith("C") and lcsc_clean[1:].isdigit():
                    lcsc_num = int(lcsc_clean[1:])
                elif lcsc_clean.isdigit():
                    lcsc_num = int(lcsc_clean)
                else:
                    lcsc_num = code_res

                crawled_components.append({
                    "lcsc": lcsc_num,
                    "manufacturer": mfr_name,
                    "attributes": extra.get("attributes", {}),
                    "image": img_name,
                    "url_slug": url_slug,
                    "images": extra.get("images", [])
                })
                success_count += 1
            else:
                # Component was not found / discontinued on LCSC
                failed_items.append({"lcsc": code_res, "error": "Not found or discontinued"})

            if (i + 1) % 100 == 0 or (i + 1) == total:
                elapsed = max(0.01, time.time() - t0)
                rate = (i + 1) / elapsed
                pct = ((i + 1) / total) * 100
                print(f"[{i + 1}/{total}] ({pct:.1f}%) | Success: {success_count} | Failed: {len(failed_items)} | {rate:.1f} req/s")

    elapsed = max(0.01, time.time() - t0)
    print(f"Finished Shard {args.shard}: {success_count} succeeded, {len(failed_items)} failed/discontinued in {elapsed:.1f}s")

    # Output crawled components delta
    if args.crawled_output and crawled_components:
        out_p = Path(args.crawled_output)
        out_p.parent.mkdir(parents=True, exist_ok=True)
        if out_p.name.endswith(".gz"):
            with gzip.open(out_p, "wt", encoding="utf-8") as f:
                json.dump(crawled_components, f, ensure_ascii=False)
        else:
            with open(out_p, "w", encoding="utf-8") as f:
                json.dump(crawled_components, f, indent=2, ensure_ascii=False)
        print(f"Saved {len(crawled_components)} crawled components to {out_p}")

    # Output failed components log
    if args.failed_log:
        log_p = Path(args.failed_log)
        log_p.parent.mkdir(parents=True, exist_ok=True)
        summary = {
            "shard": args.shard,
            "shard_size": args.shard_size,
            "total_requested": total,
            "success_count": success_count,
            "failed_count": len(failed_items),
            "elapsed_seconds": round(elapsed, 1),
            "failed_items": failed_items,
        }
        with open(log_p, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        print(f"Saved failed summary to {log_p}")

        txt_p = log_p.with_suffix(".txt")
        with open(txt_p, "w", encoding="utf-8") as f:
            for item in failed_items:
                f.write(f"{item['lcsc']}\t{item['error']}\n")
        print(f"Saved failed list to {txt_p}")

if __name__ == "__main__":
    main()
