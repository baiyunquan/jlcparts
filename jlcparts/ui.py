from multiprocessing import Pool
import json
import os
import threading
import time

import click
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from jlcparts.datatables import buildtables, normalizeAttribute
from jlcparts.lcsc import pullPreferredComponents
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

def fetchLcscData(lcsc):
    try:
        session = _get_thread_session()
        extra = getLcscExtraNew(lcsc, session=session)
        return (lcsc, extra, None)
    except Exception as e:
        return (lcsc, None, f"{type(e).__name__}: {e}")

def refreshExtraData(db, missing, age, limit, concurrency=24, stock_only=False, max_seconds=None):
    from concurrent.futures import ThreadPoolExecutor, as_completed

    missing = set(missing)
    if limit > 0:
        needed = max(0, limit - len(missing))
        if needed > 0:
            missing.update(db.getMissingExtra(needed, stock_only=stock_only))
    else:
        # limit <= 0 means crawl all missing components
        missing.update(db.getMissingExtra(1500000, stock_only=stock_only))

    if age > 0:
        ageCount = min(age, max(0, limit - len(missing))) if limit > 0 else age
        if ageCount > 0:
            print(f"{ageCount} components will be aged and thus refreshed")
            missing = missing.union(db.getNOldest(ageCount))

    if limit > 0:
        missing = list(missing)[:limit]
    else:
        missing = list(missing)

    if not missing:
        print("No missing LCSC extra data to refresh.")
        return

    print(f"Refreshing extra data and images for {len(missing)} components (concurrency={concurrency})...")
    start_time = time.time()
    success_count = 0
    skipped_count = 0

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {executor.submit(fetchLcscData, lcsc): lcsc for lcsc in missing}
        total = len(missing)
        for i, future in enumerate(as_completed(futures)):
            if max_seconds is not None and (time.time() - start_time) >= max_seconds:
                print(f"  Reached max_seconds ({max_seconds}s). Stopping early to preserve progress and save database...")
                executor.shutdown(wait=False, cancel_futures=True)
                break

            lcsc, extra, error = future.result()
            if error is not None:
                skipped_count += 1
                if i % 100 == 0 or i == total - 1:
                    print(f"  [{i+1}/{total}] ({((i+1)/total*100):.1f}%) {lcsc} skipped: {error}")
                continue

            success_count += 1
            db.updateExtra(lcsc, extra)
            if i % 100 == 0 or i == total - 1:
                elapsed = max(0.01, time.time() - start_time)
                rate = (i + 1) / elapsed
                has_img = bool(extra and extra.get("images"))
                img_info = f", image: {len(extra.get('images', []))}" if has_img else ", no image"
                print(f"  [{i+1}/{total}] ({((i+1)/total*100):.1f}%) {lcsc} fetched ({rate:.1f} req/s{img_info})")

    elapsed = max(0.01, time.time() - start_time)
    print(f"Completed refresh of {len(missing)} components in {elapsed:.1f}s ({success_count} updated, {skipped_count} skipped).")

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
@click.option("--limit", type=int, default=10000,
    help="Limit number of newly added LCSC extra records")
@click.option("--retries", type=int, default=10,
    help="Retry failed JLCPCB API pages this many times")
@click.option("--retry-delay", type=int, default=5,
    help="Wait this many seconds between JLCPCB API retries")
@click.option("--concurrency", type=int, default=24,
    help="Number of concurrent workers for LCSC requests")
@click.option("--stock-only", is_flag=True, default=False,
    help="Prioritize and only fetch components with stock > 0")
@click.option("--verbose", is_flag=True,
    help="Be verbose")
def fetchDb(db, checkpoint, max_seconds, age, limit, retries, retry_delay, concurrency, stock_only, verbose):
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

    refreshExtraData(lib, missing, age, limit, concurrency=concurrency, stock_only=stock_only, max_seconds=remaining_seconds)
    if verbose:
        print("Fetch complete" if (done or interf is None) else "Fetch checkpointed")


@click.command()
@click.argument("db", type=click.Path(dir_okay=False, writable=True))
@click.option("--limit", type=int, default=1000,
    help="Limit number of component extras/images to fetch")
@click.option("--concurrency", type=int, default=24,
    help="Number of concurrent workers for LCSC requests")
@click.option("--stock-only", is_flag=True, default=False,
    help="Only fetch components with stock > 0")
@click.option("--max-seconds", type=int, default=None,
    help="Maximum runtime in seconds")
def fetchextra(db, limit, concurrency, stock_only, max_seconds):
    """
    Fetch LCSC extra details and preview images for components in DB.
    """
    lib = SourceDb(db)
    missing = lib.getMissingExtra(limit, stock_only=stock_only)
    refreshExtraData(lib, missing, age=0, limit=limit, concurrency=concurrency, stock_only=stock_only, max_seconds=max_seconds)



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
