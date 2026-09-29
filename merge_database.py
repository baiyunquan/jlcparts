"""
Merges the new GitHub Actions crawl results (run 36502617816) into:
1. database/jlcparts/cache.sqlite3
2. PartShelf/data/libraries/jlcparts.db
"""

import sqlite3
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
ACTION_DB = BASE_DIR / "database" / "jlcparts" / "downloads_run_36502617816" / "jlcparts_cache_database" / "cache.sqlite3"
LOCAL_CACHE_DB = BASE_DIR / "database" / "jlcparts" / "cache.sqlite3"
PARTSHELF_DB = BASE_DIR / "PartShelf" / "data" / "libraries" / "jlcparts.db"


def merge_into_db(target_db_path: Path, label: str):
    print(f"\n==========================================")
    print(f"Merging into: {label} ({target_db_path})")
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
    cur.execute("ATTACH DATABASE ? AS action_db", (str(ACTION_DB),))

    # 1. Merge jlc_components (INSERT OR REPLACE to update stock, price, new items)
    print("Merging jlc_components (1,099,000 rows)...")
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
        attributes = COALESCE(NULLIF(excluded.attributes, ''), lcsc_components.attributes),
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


def main():
    print(f"Action DB source: {ACTION_DB}")
    if not ACTION_DB.exists():
        print(f"Action DB does not exist at {ACTION_DB}")
        return

    # Merge into PartShelf
    merge_into_db(PARTSHELF_DB, "PartShelf jlcparts.db")

    # Merge into local repository cache
    merge_into_db(LOCAL_CACHE_DB, "jlcparts/cache.sqlite3")

    print("\nAll database merges completed successfully!")


if __name__ == "__main__":
    main()
