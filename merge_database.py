"""
Merges downloaded GitHub Actions crawl databases into:
1. database/jlcparts/cache.sqlite3
2. PartShelf/data/libraries/jlcparts.db

Supports passing specific database files or searching in downloads_*/
"""

import sys
import json
import sqlite3
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
LOCAL_CACHE_DB = BASE_DIR / "database" / "jlcparts" / "cache.sqlite3"
PARTSHELF_DB = BASE_DIR / "PartShelf" / "data" / "libraries" / "jlcparts.db"


def merge_into_db(action_db_path: Path, target_db_path: Path, label: str):
    print(f"\n==========================================")
    print(f"Merging {action_db_path.name} into: {label}")
    print(f"==========================================")
    
    if not target_db_path.exists():
        raise FileNotFoundError(f"Target DB not found: {target_db_path}")
    
    t0 = time.time()
    conn = sqlite3.connect(target_db_path)
    cur = conn.cursor()

    # Pre-merge counts
    cur.execute("SELECT count(*) FROM jlc_components")
    pre_jlc = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM lcsc_components WHERE image IS NOT NULL AND image != ''")
    pre_images = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM lcsc_components")
    pre_lcsc = cur.fetchone()[0]
    print(f"Pre-merge: {pre_jlc:,} jlc_components, {pre_lcsc:,} lcsc_components ({pre_images:,} with images)")

    # Attach the action db
    cur.execute("ATTACH DATABASE ? AS action_db", (str(action_db_path),))

    # 1. Merge jlc_components (INSERT OR REPLACE to update stock, price, new items)
    print("Merging jlc_components...")
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
    conn.commit()

    # 2. Merge lcsc_components preserving existing images
    print("Merging lcsc_components with image preservation...")
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
    conn.commit()

    # 3. Ensure all required indexes exist
    print("Verifying / creating indexes...")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jlc_mfr ON jlc_components(mfr);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jlc_package ON jlc_components(package);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jlc_category ON jlc_components(category, subcategory);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jlc_stock ON jlc_components(stock);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_lcsc_comp_lcsc ON lcsc_components(lcsc);")
    conn.commit()

    # Detach action db
    cur.execute("DETACH DATABASE action_db")

    # Post-merge counts
    cur.execute("SELECT count(*) FROM jlc_components")
    post_jlc = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM lcsc_components WHERE image IS NOT NULL AND image != ''")
    post_images = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM lcsc_components")
    post_lcsc = cur.fetchone()[0]
    conn.close()

    elapsed = time.time() - t0
    print(f"Post-merge in {elapsed:.2f}s:")
    print(f"  jlc_components:  {post_jlc:,} (+{post_jlc - pre_jlc:,})")
    print(f"  lcsc_components: {post_lcsc:,} (+{post_lcsc - pre_lcsc:,})")
    print(f"  Images total:    {post_images:,} (+{post_images - pre_images:,})")


def check_failed_logs(search_dir: Path):
    failed_files = list(search_dir.glob("**/failed_components*.json"))
    if not failed_files:
        return
    print("\n--- Failed Components Artifact Inspection ---")
    for f in failed_files:
        try:
            with open(f, "r", encoding="utf-8") as fp:
                data = json.load(fp)
            total = data.get("total_requested", 0)
            failed = data.get("total_failed", 0)
            print(f"Log: {f.name} in {f.parent.name}")
            print(f"  Requested: {total}, Failed: {failed}")
            items = data.get("failed_items", [])
            if items:
                print("  Sample failures:")
                for item in items[:5]:
                    print(f"    - {item.get('lcsc')}: {item.get('error')}")
        except Exception as e:
            print(f"  Could not read {f}: {e}")


def main():
    db_paths = []
    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            p = Path(arg)
            if p.is_file():
                db_paths.append(p)
            elif p.is_dir():
                db_paths.extend(p.glob("**/*.sqlite*"))
    else:
        # Auto-search in downloads_*
        downloads_base = BASE_DIR / "database" / "jlcparts"
        for p in downloads_base.glob("downloads_*/**/*.sqlite*"):
            if p.is_file():
                db_paths.append(p)

    if not db_paths:
        print("No source database files found to merge.")
        print(f"Usage: python merge_database.py [path_to_cache.sqlite3 ...]")
        return

    print(f"Found {len(db_paths)} database(s) to merge:")
    for p in db_paths:
        print(f"  - {p}")

    for db_path in db_paths:
        merge_into_db(db_path, PARTSHELF_DB, "PartShelf jlcparts.db")
        merge_into_db(db_path, LOCAL_CACHE_DB, "jlcparts/cache.sqlite3")

    check_failed_logs(BASE_DIR / "database" / "jlcparts")
    print("\nAll database merges completed successfully!")


if __name__ == "__main__":
    main()
