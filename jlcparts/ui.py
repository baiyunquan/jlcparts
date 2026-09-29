from multiprocessing import Pool
import json
import os
import random
from pathlib import Path
import threading
import time

import click
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from jlcparts.datatables import buildtables, normalizeAttribute
from jlcparts.lcsc import pullPreferredComponents, RateLimitError
from jlcparts.partLib import (PartLibrary, PartLibraryDb, getLcscExtraNew,
                              loadJlcTable, loadJlcTableLazy, parsePrice)
from jlcparts.sourceDb import SourceDb, migrateCache
from jlcparts.webdb import buildwebdb

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

def fetchLcscData(lcsc, jitter_range=(0.1, 0.3), images_dir=None):
    if jitter_range and jitter_range[1] > 0:
        time.sleep(random.uniform(jitter_range[0], jitter_range[1]))
    try:
        session = _get_thread_session()
        extra = getLcscExtraNew(lcsc, session=session)
        if images_dir and extra and extra.get("images"):
            target_dir = Path(images_dir)
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
        return (lcsc, extra, None)
    except RateLimitError as e:
        return (lcsc, None, f"RateLimitError: {e}")
    except Exception as e:
        return (lcsc, None, f"{type(e).__name__}: {e}")

def refreshExtraData(db, missing, age, limit=20000, concurrency=6, stock_only=True,
                     offset=0, max_seconds=None, jitter_range=(0.1, 0.3),
                     failed_log_path="failed_components.json",
                     images_dir=None, crawled_output_path=None):
    from concurrent.futures import ThreadPoolExecutor, as_completed

    if images_dir:
        Path(images_dir).mkdir(parents=True, exist_ok=True)

    target_components = list(missing)
    if limit > 0:
        needed = max(0, limit - len(target_components))
        if needed > 0:
            extra_needed = db.getMissingExtra(needed, stock_only=stock_only, offset=offset)
            target_components.extend(extra_needed)
    else:
        target_components.extend(db.getMissingExtra(1500000, stock_only=stock_only, offset=offset))

    if age > 0:
        age_count = min(age, max(0, limit - len(target_components))) if limit > 0 else age
        if age_count > 0:
            print(f"{age_count} components will be aged and thus refreshed")
            target_components.extend(db.getNOldest(age_count))

    # Deduplicate while preserving order
    seen = set()
    deduped = []
    for item in target_components:
        if item not in seen:
            seen.add(item)
            deduped.append(item)

    if limit > 0:
        final_list = deduped[:limit]
    else:
        final_list = deduped

    if not final_list:
        print("No missing LCSC extra data to refresh.")
        return

    print(f"Refreshing extra data and images for {len(final_list)} components (offset={offset}, limit={limit}, concurrency={concurrency}, jitter={jitter_range}s, stock_only={stock_only})...")
    start_time = time.time()
    success_count = 0
    skipped_count = 0
    failed_items = []
    crawled_components = []
    consecutive_rate_limits = 0

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {executor.submit(fetchLcscData, lcsc, jitter_range, images_dir): lcsc for lcsc in final_list}
        total = len(final_list)
        for i, future in enumerate(as_completed(futures)):
            if max_seconds is not None and (time.time() - start_time) >= max_seconds:
                print(f"  Reached max_seconds ({max_seconds}s). Stopping early to preserve progress and save database...")
                executor.shutdown(wait=False, cancel_futures=True)
                break

            lcsc, extra, error = future.result()
            if error is not None:
                skipped_count += 1
                failed_items.append({
                    "lcsc": lcsc,
                    "error": str(error),
                    "timestamp": int(time.time()),
                })
                if "RateLimitError" in str(error):
                    consecutive_rate_limits += 1
                    if consecutive_rate_limits >= 4:
                        print(f"  [RateLimit Warning] Multiple rate limits detected, cooling down for 15s...")
                        time.sleep(15)
                        consecutive_rate_limits = 0
                else:
                    consecutive_rate_limits = 0

                if i % 50 == 0 or i == total - 1:
                    print(f"  [{i+1}/{total}] ({((i+1)/total*100):.1f}%) {lcsc} failed/skipped: {error}")
                continue

            consecutive_rate_limits = 0
            success_count += 1
            db.updateExtra(lcsc, extra)

            if extra:
                mfr_name = ""
                if isinstance(extra.get("manufacturer"), dict):
                    mfr_name = extra.get("manufacturer", {}).get("name", "")
                elif isinstance(extra.get("manufacturer"), str):
                    mfr_name = extra.get("manufacturer")
                img_name = extra.get("images", [{}])[0].get("filename", "") if extra.get("images") else ""
                url_slug = extra.get("url", "").rsplit("/", 1)[-1].replace(".html", "") if extra.get("url") else ""

                crawled_components.append({
                    "lcsc": lcsc,
                    "manufacturer": mfr_name,
                    "attributes": extra.get("attributes", {}),
                    "image": img_name,
                    "url_slug": url_slug,
                    "images": extra.get("images", [])
                })

            if i % 50 == 0 or i == total - 1:
                elapsed = max(0.01, time.time() - start_time)
                rate = (i + 1) / elapsed
                has_img = bool(extra and extra.get("images"))
                img_info = f", image: {len(extra.get('images', []))}" if has_img else ", no image"
                print(f"  [{i+1}/{total}] ({((i+1)/total*100):.1f}%) {lcsc} fetched ({rate:.1f} req/s{img_info})")

    elapsed = max(0.01, time.time() - start_time)
    print(f"Completed refresh of {len(final_list)} components in {elapsed:.1f}s ({success_count} updated, {skipped_count} failed/skipped).")

    if crawled_output_path and crawled_components:
        try:
            p = Path(crawled_output_path)
            p.parent.mkdir(parents=True, exist_ok=True)
            if p.name.endswith(".gz"):
                import gzip
                with gzip.open(p, "wt", encoding="utf-8") as fp:
                    json.dump(crawled_components, fp, ensure_ascii=False)
            else:
                with open(p, "w", encoding="utf-8") as fp:
                    json.dump(crawled_components, fp, indent=2, ensure_ascii=False)
            print(f"Saved {len(crawled_components)} crawled components to {crawled_output_path}")
        except Exception as e:
            print(f"Warning: Failed to write {crawled_output_path}: {e}")

    if failed_log_path:
        failed_summary = {
            "total_requested": len(final_list),
            "total_success": success_count,
            "total_failed": len(failed_items),
            "offset": offset,
            "limit": limit,
            "failed_items": failed_items,
        }
        try:
            with open(failed_log_path, "w", encoding="utf-8") as f:
                json.dump(failed_summary, f, indent=2, ensure_ascii=False)
            print(f"Saved failed components summary to {failed_log_path} ({len(failed_items)} items)")

            txt_path = Path(failed_log_path).with_suffix(".txt")
            with open(txt_path, "w", encoding="utf-8") as f:
                for item in failed_items:
                    f.write(f"{item['lcsc']}\t{item['error']}\n")
            print(f"Saved failed components list to {txt_path}")
        except Exception as e:
            print(f"Warning: Failed to write failed_components log: {e}")

def apiComponentToDbComponent(component):
    from .jlcpcb import normalizeComponent

    c = normalizeComponent(component)
    return {
        "lcsc": c["lcscPart"],
        "category": c["firstCategory"],
        "subcategory": c["secondCategory"],
        "mfr": c["mfrPart"],
        "package": c["package"],
        "joints": int(c["solderJoint"]),
        "manufacturer": c["manufacturer"],
        "basic": c["libraryType"].lower() == "base",
        "description": c["description"],
        "datasheet": c["datasheet"],
        "stock": int(c["stock"]),
        "price": parsePrice(c["price"]),
        "jlc_extra": c["jlcExtra"],
        "jlc_raw": component,
    }

@click.command()
@click.argument("source", type=click.Path(dir_okay=False, exists=True))
@click.argument("db", type=click.Path(dir_okay=False, writable=True))
@click.option("--age", type=int, default=0,
    help="Automatically discard n oldest components and fetch them again")
@click.option("--limit", type=int, default=10000,
    help="Limit number of newly added components")
@click.option("--partial", is_flag=True,
    help="Do not remove DB components missing from SOURCE")
@click.option("--skip", type=int, default=0,
    help="Skip this many rows from SOURCE before importing")
def getLibrary(source, db, age, limit, partial, skip):
    """
    Download library inside OUTPUT (JSON format) based on SOURCE (csv table
    provided by JLC PCB).

    You can specify previously downloaded library as a cache to save requests to
    fetch LCSC extra data.
    """
    OLD = 0
    REFRESHED = 1

    db = PartLibraryDb(db)
    missing = set()
    total = 0
    skipped = 0
    with db.startTransaction():
        if not partial:
            db.resetFlag(value=OLD)
        with open(source, newline="") as f:
            jlcTable = loadJlcTableLazy(f)
            for component in jlcTable:
                if skipped < skip:
                    skipped += 1
                    continue
                total += 1
                if db.exists(component["lcsc"]):
                    db.updateJlcPart(component, flag=None if partial else REFRESHED)
                else:
                    component["extra"] = {}
                    db.addComponent(component, flag=None if partial else REFRESHED)
                    missing.add(component["lcsc"])
        if skipped != 0:
            print(f"Skipped {skipped} components")
        print(f"New {len(missing)} components out of {total} total")
        refreshExtraData(db, missing, age, limit)
        if not partial:
            db.removeWithFlag(value=OLD)
    # Temporary work-around for space-related issues in CI - simply don't rebuild the DB
    # db.vacuum()

@click.command()
@click.argument("db", type=click.Path(dir_okay=False, writable=True))
@click.option("--checkpoint", type=click.Path(dir_okay=False), default=None,
    help="Read/write a checkpoint JSON for resumable fetches")
@click.option("--max-seconds", type=int, default=None,
    help="Stop after roughly this many seconds and save the checkpoint")
@click.option("--age", type=int, default=0,
    help="Automatically discard n oldest components and fetch them again")
@click.option("--limit", type=int, default=20000,
    help="Limit number of newly added LCSC extra records")
@click.option("--offset", type=int, default=0,
    help="Offset in component queue to start fetching from")
@click.option("--retries", type=int, default=10,
    help="Retry failed JLCPCB API pages this many times")
@click.option("--retry-delay", type=int, default=5,
    help="Wait this many seconds between JLCPCB API retries")
@click.option("--concurrency", type=int, default=6,
    help="Number of concurrent workers for LCSC requests")
@click.option("--jitter-min", type=float, default=0.1,
    help="Minimum jitter delay in seconds")
@click.option("--jitter-max", type=float, default=0.3,
    help="Maximum jitter delay in seconds")
@click.option("--stock-only", is_flag=True, default=False,
    help="Prioritize and only fetch components with stock > 0")
@click.option("--failed-log", type=str, default="failed_components.json",
    help="Path to save failed components log")
@click.option("--verbose", is_flag=True,
    help="Be verbose")
def fetchDb(db, checkpoint, max_seconds, age, limit, offset, retries, retry_delay, concurrency, jitter_min, jitter_max, stock_only, failed_log, verbose):
    """
    Fetch JLC PCB component data directly into DB.
    """
    from .jlcpcb import (
        createComponentInterface,
        enrichComponentsFromWebsite,
        loadCheckpoint,
        writeCheckpoint,
    )

    if max_seconds is not None and checkpoint is None:
        raise RuntimeError("max-seconds requires a checkpoint so the fetch can resume")

    start_overall = time.monotonic()
    OLD = 0
    REFRESHED = 1

    lib = SourceDb(db)
    checkpointState = loadCheckpoint(checkpoint)
    count = int(checkpointState.get("count", 0))
    done = False
    missing = set()

    if checkpointState.get("done"):
        if checkpoint and os.path.exists(checkpoint):
            os.remove(checkpoint)
        return

    if not checkpointState:
        with lib.startTransaction():
            lib.resetFlag(value=OLD)

    try:
        interf = createComponentInterface(lastKey=checkpointState.get("lastKey"))
    except RuntimeError as e:
        if "Missing JLCPCB OpenAPI credential" in str(e):
            print("JLCPCB OpenAPI credentials (JLCPCB_APP_ID) not configured.")
            print("Skipping catalog pagination from JLC OpenAPI and proceeding directly to refresh LCSC extra data and images...")
            interf = None
        else:
            raise

    if interf is not None:
        start = time.monotonic()
        while True:
            if max_seconds is not None and time.monotonic() - start >= max_seconds:
                writeCheckpoint(checkpoint, db, interf.lastPage, count, False)
                break

            for i in range(retries):
                try:
                    page = interf.getPage()
                    break
                except Exception as e:
                    if i == retries - 1:
                        raise e from None
                    time.sleep(retry_delay)
            if page is None:
                with lib.startTransaction():
                    lib.removeWithFlag(value=OLD)
                if checkpoint and os.path.exists(checkpoint):
                    os.remove(checkpoint)
                done = True
                break

            page = enrichComponentsFromWebsite(page)

            with lib.startTransaction():
                for apiComponent in page:
                    isNew = not lib.exists(apiComponent["componentCode"])
                    lib.updateJlcPayload(apiComponent, flag=REFRESHED)
                    if isNew:
                        missing.add(apiComponent["componentCode"])

            count += len(page)
            if verbose:
                print(f"Fetched {count}")
            writeCheckpoint(checkpoint, db, interf.lastPage, count, False)

    remaining_seconds = None
    if max_seconds is not None:
        elapsed = time.monotonic() - start_overall
        remaining_seconds = max(60, int(max_seconds - elapsed))

    refreshExtraData(
        lib, missing, age, limit,
        concurrency=concurrency,
        stock_only=stock_only,
        offset=offset,
        max_seconds=remaining_seconds,
        jitter_range=(jitter_min, jitter_max),
        failed_log_path=failed_log
    )
    if verbose:
        print("Fetch complete" if (done or interf is None) else "Fetch checkpointed")


@click.command()
@click.argument("db", type=click.Path(dir_okay=False, writable=True))
@click.option("--limit", type=int, default=20000,
    help="Limit number of component extras/images to fetch")
@click.option("--offset", type=int, default=0,
    help="Offset in component queue to start fetching from")
@click.option("--concurrency", type=int, default=6,
    help="Number of concurrent workers for LCSC requests")
@click.option("--jitter-min", type=float, default=0.1,
    help="Minimum jitter delay in seconds")
@click.option("--jitter-max", type=float, default=0.3,
    help="Maximum jitter delay in seconds")
@click.option("--stock-only", is_flag=True, default=False,
    help="Only fetch components with stock > 0")
@click.option("--max-seconds", type=int, default=None,
    help="Maximum runtime in seconds")
@click.option("--failed-log", type=str, default="failed_components.json",
    help="Path to save failed components log")
@click.option("--images-dir", type=str, default=None,
    help="Directory to save downloaded thumbnail images")
@click.option("--crawled-output", type=str, default=None,
    help="Path to save crawled components json/json.gz delta")
def fetchextra(db, limit, offset, concurrency, jitter_min, jitter_max, stock_only, max_seconds, failed_log, images_dir, crawled_output):
    """
    Fetch LCSC extra details and preview images for components in DB.
    """
    lib = SourceDb(db)
    refreshExtraData(
        lib, [], age=0, limit=limit,
        concurrency=concurrency,
        stock_only=stock_only,
        offset=offset,
        max_seconds=max_seconds,
        jitter_range=(jitter_min, jitter_max),
        failed_log_path=failed_log,
        images_dir=images_dir,
        crawled_output_path=crawled_output
    )



@click.command()
@click.argument("db", type=click.Path(dir_okay=False, writable=True))
def updatePreferred(db):
    """
    Download list of preferred components from JLC PCB and mark them into the DB.
    """
    preferred = pullPreferredComponents()
    lib = SourceDb(db)
    lib.setPreferred(preferred)


@click.command()
@click.argument("source", type=click.Path(dir_okay=False, exists=True))
@click.argument("output", type=click.Path(dir_okay=False), required=False)
def migratecache(source, output):
    """
    Migrate a legacy cache.sqlite3 into the compact source-db-v2 format.
    """
    migrateCache(source, output)


@click.command()
@click.argument("libraryFilename")
def listcategories(libraryfilename):
    """
    Print all categories from library specified by LIBRARYFILENAMEto standard
    output
    """
    lib = PartLibrary(libraryfilename)
    for c, subcats in lib.categories().items():
        print(f"{c}:")
        for s in subcats:
            print(f"  {s}")

@click.command()
@click.argument("libraryFilename")
def listattributes(libraryfilename):
    """
    Print all keys in the extra["attributes"] arguments from library specified by
    LIBRARYFILENAME to standard output
    """
    keys = set()
    lib = PartLibrary(libraryfilename)
    for subcats in lib.lib.values():
        for parts in subcats.values():
            for data in parts.values():
                if "extra" not in data:
                    continue
                extra = data["extra"]
                attr = extra.get("attributes", {})
                if not isinstance(attr, list):
                    for k in extra.get("attributes", {}).keys():
                        keys.add(k)
    for k in keys:
        print(k)

@click.command()
@click.argument("lcsc_code")
def fetchDetails(lcsc_code):
    """
    Fetch LCSC extra information for a given LCSC code
    """
    print(getLcscExtraNew(lcsc_code))

@click.command()
@click.argument("filename", type=click.Path(writable=True))
@click.option("--verbose", is_flag=True,
    help="Be verbose")
@click.option("--limit", type=int, default=None,
    help="Fetch at most this many components")
@click.option("--checkpoint", type=click.Path(dir_okay=False), default=None,
    help="Read/write a checkpoint JSON for resumable fetches")
@click.option("--max-seconds", type=int, default=None,
    help="Stop after roughly this many seconds and save the checkpoint")
def fetchTable(filename, verbose, limit, checkpoint, max_seconds):
    """
    Fetch JLC PCB component table
    """
    from .jlcpcb import pullComponentTable

    def report(count: int) -> None:
        if (verbose):
            print(f"Fetched {count}")

    pullComponentTable(filename, report, limit=limit, checkpoint=checkpoint,
                       maxSeconds=max_seconds)

@click.command()
@click.argument("lcsc")
def testComponent(lcsc):
    """
    Tests parsing attributes of given component
    """
    extra = getLcscExtraNew(lcsc)["attributes"]

    extra.pop("url", None)
    extra.pop("images", None)
    extra.pop("prices", None)
    extra.pop("datasheet", None)
    extra.pop("id", None)
    extra.pop("manufacturer", None)
    extra.pop("number", None)
    extra.pop("title", None)
    extra.pop("quantity", None)
    for i in range(10):
        extra.pop(f"quantity{i}", None)
    normalized = dict(normalizeAttribute(key, val) for key, val in extra.items())
    print(json.dumps(normalized, indent=4))


@click.group()
def cli():
    pass

cli.add_command(getLibrary)
cli.add_command(listcategories)
cli.add_command(listattributes)
cli.add_command(buildtables)
cli.add_command(buildwebdb)
cli.add_command(updatePreferred)
cli.add_command(migratecache)
cli.add_command(fetchDetails)
cli.add_command(fetchDb)
cli.add_command(fetchextra)
cli.add_command(fetchTable)
cli.add_command(testComponent)

if __name__ == "__main__":
    cli()
