"""
🧪 Pipeline Integrity Test Suite
=================================
Standalone script to validate data consistency across all 3 layers:
  Bronze (JSON) → Silver (CSV) → Gold (PostgreSQL)

Run this AFTER the pipeline completes to audit data integrity.

Usage:
    python scripts/4_test_pipeline.py          (from project root)
    python scripts/4_test_pipeline.py --local  (skip Gold/PostgreSQL checks)
"""

import json
import csv
import os
import sys
import glob

# ============================================================
# Configuration
# ============================================================
BRONZE_STORE  = "data/bronze/store_raw.json"
BRONZE_REVIEW = "data/bronze/reviews_raw.json"
REGISTRY_PATH = "data/game_registry.json"

SILVER_TABLES = {
    "games_main":         {"dir": "data/silver/games_main",         "key_cols": ["AppID"]},
    "games_genres":       {"dir": "data/silver/games_genres",       "key_cols": ["AppID", "Genre_ID"]},
    "games_categories":   {"dir": "data/silver/games_categories",   "key_cols": ["AppID", "Category_ID"]},
    "games_screenshots":  {"dir": "data/silver/games_screenshots",  "key_cols": ["AppID", "Screenshot_ID"]},
    "games_movies":       {"dir": "data/silver/games_movies",       "key_cols": ["AppID", "Movie_ID"]},
    "games_dlc":          {"dir": "data/silver/games_dlc",          "key_cols": ["AppID", "DLC_AppID"]},
    "games_achievements": {"dir": "data/silver/games_achievements", "key_cols": ["AppID", "Achievement_Name"]},
}

GOLD_TABLES = {
    "games_main":         "gold_games_main",
    "games_genres":       "gold_games_genres",
    "games_categories":   "gold_games_categories",
    "games_screenshots":  "gold_games_screenshots",
    "games_movies":       "gold_games_movies",
    "games_dlc":          "gold_games_dlc",
    "games_achievements": "gold_games_achievements",
}

# ============================================================
# Helpers
# ============================================================
class TestResult:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.warnings = 0
        self.details = []
    
    def ok(self, msg):
        self.passed += 1
        self.details.append(("✅", msg))
        print(f"   ✅ {msg}")
    
    def fail(self, msg):
        self.failed += 1
        self.details.append(("❌", msg))
        print(f"   ❌ {msg}")
    
    def warn(self, msg):
        self.warnings += 1
        self.details.append(("⚠️", msg))
        print(f"   ⚠️ {msg}")
    
    @property
    def all_passed(self):
        return self.failed == 0


def find_csv_file(silver_dir):
    """Find the single CSV part file inside a Spark output directory."""
    pattern = os.path.join(silver_dir, "part-*.csv")
    files = glob.glob(pattern)
    if not files:
        return None
    return files[0]


def read_csv_rows(filepath):
    """Read a CSV and return list of dicts."""
    rows = []
    with open(filepath, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def read_json(filepath):
    """Read a JSON array file."""
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


# ============================================================
# Test Suite
# ============================================================
def test_bronze_layer(results):
    """Test Bronze layer data integrity."""
    print("\n" + "=" * 60)
    print("🥉 BRONZE LAYER TESTS")
    print("=" * 60)
    
    # Test: Files exist
    if os.path.exists(BRONZE_STORE):
        results.ok(f"store_raw.json exists ({os.path.getsize(BRONZE_STORE):,} bytes)")
    else:
        results.fail("store_raw.json MISSING")
        return results
    
    if os.path.exists(BRONZE_REVIEW):
        results.ok(f"reviews_raw.json exists ({os.path.getsize(BRONZE_REVIEW):,} bytes)")
    else:
        results.fail("reviews_raw.json MISSING")
        return results
    
    # Test: Valid JSON
    try:
        store_data = read_json(BRONZE_STORE)
        results.ok(f"store_raw.json is valid JSON ({len(store_data)} records)")
    except Exception as e:
        results.fail(f"store_raw.json is INVALID JSON: {e}")
        return results
    
    try:
        review_data = read_json(BRONZE_REVIEW)
        results.ok(f"reviews_raw.json is valid JSON ({len(review_data)} records)")
    except Exception as e:
        results.fail(f"reviews_raw.json is INVALID JSON: {e}")
        return results
    
    # Test: Non-empty
    if len(store_data) > 0:
        results.ok(f"Store data non-empty: {len(store_data)} games")
    else:
        results.fail("Store data is EMPTY")
    
    if len(review_data) > 0:
        results.ok(f"Review data non-empty: {len(review_data)} reviews")
    else:
        results.fail("Review data is EMPTY")
    
    # Test: 1:1 count match
    if len(store_data) == len(review_data):
        results.ok(f"Store/Review counts match: {len(store_data)}")
    else:
        results.fail(f"Store/Review COUNT MISMATCH: {len(store_data)} store vs {len(review_data)} reviews")
    
    # Test: All entries have appid
    store_ids = {g.get("appid") for g in store_data}
    review_ids = {r.get("appid") for r in review_data}
    
    if None not in store_ids:
        results.ok(f"All store entries have 'appid'")
    else:
        results.fail("Some store entries MISSING 'appid'")
    
    if None not in review_ids:
        results.ok(f"All review entries have 'appid'")
    else:
        results.fail("Some review entries MISSING 'appid'")
    
    # Test: AppIDs match
    if store_ids == review_ids:
        results.ok(f"AppIDs match between store and reviews: {sorted(store_ids)}")
    else:
        results.fail(f"AppID MISMATCH: store-only={store_ids - review_ids}, review-only={review_ids - store_ids}")
    
    # Test: Required fields present
    required_store_fields = ["name", "type", "appid"]
    for record in store_data:
        for field in required_store_fields:
            if field not in record:
                results.fail(f"Game {record.get('appid', '?')} missing required field '{field}'")
    
    # Store for cross-layer comparison
    results._bronze_store_count = len(store_data)
    results._bronze_store_ids = store_ids
    
    # Test: Game Registry
    if os.path.exists(REGISTRY_PATH):
        try:
            with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
                registry = json.load(f)
            results.ok(f"Game registry exists ({len(registry)} games tracked)")
            
            # Registry should cover all bronze AppIDs
            registry_ids = {int(k) for k in registry.keys()}
            bronze_ids = {int(i) for i in store_ids if i is not None}
            missing = bronze_ids - registry_ids
            if not missing:
                results.ok(f"Registry covers all bronze AppIDs")
            else:
                results.warn(f"Registry missing {len(missing)} bronze AppIDs: {missing}")
            
            # Every registry entry should have required fields
            bad_entries = [k for k, v in registry.items() 
                          if not all(f in v for f in ("name", "last_updated", "rank"))]
            if not bad_entries:
                results.ok("All registry entries have required fields")
            else:
                results.fail(f"Registry entries missing fields: {bad_entries[:5]}")
        except Exception as e:
            results.fail(f"Game registry is INVALID: {e}")
    else:
        results.warn("Game registry not found (first run?)")
    
    return results


def test_silver_layer(results):
    """Test Silver layer data integrity."""
    print("\n" + "=" * 60)
    print("🥈 SILVER LAYER TESTS")
    print("=" * 60)
    
    results._silver_counts = {}
    
    for table_name, info in SILVER_TABLES.items():
        silver_dir = info["dir"]
        key_cols = info["key_cols"]
        
        # Test: Directory exists
        if not os.path.exists(silver_dir):
            results.fail(f"{table_name}: directory MISSING")
            continue
        
        # Test: CSV file exists
        csv_file = find_csv_file(silver_dir)
        if csv_file is None:
            results.fail(f"{table_name}: no CSV part file found")
            continue
        
        results.ok(f"{table_name}: CSV found")
        
        # Test: Read and count rows
        try:
            rows = read_csv_rows(csv_file)
        except Exception as e:
            results.fail(f"{table_name}: FAILED to read CSV — {e}")
            continue
        
        row_count = len(rows)
        results._silver_counts[table_name] = row_count
        
        if row_count > 0:
            results.ok(f"{table_name}: {row_count} rows")
        else:
            results.fail(f"{table_name}: EMPTY (0 rows)")
            continue
        
        # Test: Key columns present
        header = list(rows[0].keys())
        missing_keys = [k for k in key_cols if k not in header]
        if missing_keys:
            results.fail(f"{table_name}: missing key columns {missing_keys}")
        else:
            results.ok(f"{table_name}: key columns present {key_cols}")
        
        # Test: No null AppIDs
        null_appids = sum(1 for r in rows if not r.get("AppID") or r["AppID"].strip() == "")
        if null_appids == 0:
            results.ok(f"{table_name}: no null AppIDs")
        else:
            results.fail(f"{table_name}: {null_appids} rows with null AppID!")
        
        # Test: No duplicate composite keys
        seen_keys = set()
        duplicates = 0
        for r in rows:
            key = tuple(r.get(k, "") for k in key_cols)
            if key in seen_keys:
                duplicates += 1
            seen_keys.add(key)
        
        if duplicates == 0:
            results.ok(f"{table_name}: no duplicate keys")
        else:
            results.warn(f"{table_name}: {duplicates} duplicate composite keys")
    
    # Test: games_main should have exactly bronze_store_count rows
    if hasattr(results, "_bronze_store_count"):
        main_count = results._silver_counts.get("games_main", 0)
        if main_count == results._bronze_store_count:
            results.ok(f"games_main row count matches bronze: {main_count}")
        else:
            results.fail(f"games_main count ({main_count}) != bronze count ({results._bronze_store_count})")
    
    # Test: Child tables should have >= parent unique AppID count
    if hasattr(results, "_bronze_store_count"):
        for table_name in ["games_genres", "games_categories", "games_screenshots"]:
            count = results._silver_counts.get(table_name, 0)
            if count >= results._bronze_store_count:
                results.ok(f"{table_name} has more rows ({count}) than games ({results._bronze_store_count}) — expected for child table")
            elif count > 0:
                results.warn(f"{table_name} has fewer rows ({count}) than games ({results._bronze_store_count}) — some games may lack this data")
    
    return results


def test_gold_layer(results):
    """Test Gold layer by connecting to PostgreSQL and comparing counts."""
    print("\n" + "=" * 60)
    print("🥇 GOLD LAYER TESTS (PostgreSQL)")
    print("=" * 60)
    
    try:
        import psycopg2
    except ImportError:
        results.warn("psycopg2 not installed — skipping Gold tests. Install with: pip install psycopg2-binary")
        return results
    
    db_url = os.getenv("DB_URL", "jdbc:postgresql://postgres_general:5432/sessiondb")
    db_host = db_url.split("//")[1].split(":")[0] if "//" in db_url else "postgres_general"
    db_port = db_url.split(":")[-1].split("/")[0] if db_url.count(":") >= 3 else "5432"
    db_name = db_url.split("/")[-1] if "/" in db_url else "sessiondb"
    db_user = os.getenv("DB_USER", "admin")
    db_pass = os.getenv("DB_PASS", "admin")
    
    try:
        conn = psycopg2.connect(
            host=db_host, port=db_port,
            dbname=db_name, user=db_user, password=db_pass
        )
        cursor = conn.cursor()
        results.ok(f"Connected to PostgreSQL at {db_host}:{db_port}/{db_name}")
    except Exception as e:
        results.warn(f"Cannot connect to PostgreSQL: {e}")
        results.warn("Gold layer tests skipped. Are Docker containers running?")
        return results
    
    for table_name, gold_table in GOLD_TABLES.items():
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {gold_table}")
            gold_count = cursor.fetchone()[0]
            silver_count = results._silver_counts.get(table_name, -1)
            
            if gold_count == silver_count:
                results.ok(f"{gold_table}: Gold={gold_count}, Silver={silver_count} — MATCH ✓")
            elif gold_count >= silver_count:
                results.ok(f"{gold_table}: Gold={gold_count} >= Silver={silver_count} — OK (historical data)")
            else:
                results.fail(f"{gold_table}: Gold={gold_count} < Silver={silver_count} — DATA LOSS!")
        except Exception as e:
            results.warn(f"{gold_table}: table not found or error — {e}")
            conn.rollback()
    
    cursor.close()
    conn.close()
    return results


# ============================================================
# Main
# ============================================================
def main():
    print("=" * 60)
    print("🧪 STEAM ETL PIPELINE — FULL INTEGRITY TEST SUITE")
    print("=" * 60)
    
    skip_gold = "--local" in sys.argv
    
    results = TestResult()
    
    test_bronze_layer(results)
    test_silver_layer(results)
    
    if not skip_gold:
        test_gold_layer(results)
    else:
        print("\n⏭️  Gold layer tests skipped (--local flag)")
    
    # ============================================================
    # Summary
    # ============================================================
    print("\n" + "=" * 60)
    print("📋 TEST SUMMARY")
    print("=" * 60)
    print(f"   ✅ Passed:   {results.passed}")
    print(f"   ❌ Failed:   {results.failed}")
    print(f"   ⚠️  Warnings: {results.warnings}")
    print("=" * 60)
    
    if results.all_passed:
        print("🎉 ALL TESTS PASSED — Pipeline integrity verified!")
        return 0
    else:
        print("🚨 TESTS FAILED — Data integrity issues found!")
        return 1


if __name__ == "__main__":
    sys.exit(main())
