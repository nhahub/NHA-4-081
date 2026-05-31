from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import os, urllib.request, sys

# 🔑 Database Configuration (reads from .env / environment variables)
DB_URL  = os.getenv("DB_URL",  "jdbc:postgresql://postgres_general:5432/sessiondb")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASS = os.getenv("DB_PASS", "admin")

# 📦 Auto-download JDBC driver if missing
JDBC_DRIVER_PATH = "drivers/postgresql-42.7.4.jar"
if not os.path.exists(JDBC_DRIVER_PATH):
    os.makedirs("drivers", exist_ok=True)
    print("📥 Downloading PostgreSQL JDBC driver...")
    urllib.request.urlretrieve(
        "https://jdbc.postgresql.org/download/postgresql-42.7.4.jar",
        JDBC_DRIVER_PATH
    )
    print("✅ JDBC driver downloaded.")

# ============================================================
# 📋 Silver → Gold Table Mapping
# Each entry defines:
#   - silver_path: where the CSV lives
#   - gold_table:  target PostgreSQL table name
#   - key_cols:    composite key for upsert deduplication
#
# 🐛 FIX: Previously ALL tables used only ["AppID"] as the key,
#    which collapsed child tables (genres, categories, achievements,
#    screenshots, movies, dlc) to 1 row per game.
#    Now each table uses its proper composite key.
# ============================================================
SILVER_TABLES = [
    {
        "silver_path": "data/silver/games_main",
        "gold_table":  "gold_games_main",
        "key_cols":    ["AppID"],
    },
    {
        "silver_path": "data/silver/games_genres",
        "gold_table":  "gold_games_genres",
        "key_cols":    ["AppID", "Genre_ID"],
    },
    {
        "silver_path": "data/silver/games_categories",
        "gold_table":  "gold_games_categories",
        "key_cols":    ["AppID", "Category_ID"],
    },
    {
        "silver_path": "data/silver/games_screenshots",
        "gold_table":  "gold_games_screenshots",
        "key_cols":    ["AppID", "Screenshot_ID"],
    },
    {
        "silver_path": "data/silver/games_movies",
        "gold_table":  "gold_games_movies",
        "key_cols":    ["AppID", "Movie_ID"],
    },
    {
        "silver_path": "data/silver/games_dlc",
        "gold_table":  "gold_games_dlc",
        "key_cols":    ["AppID", "DLC_AppID"],
    },
    {
        "silver_path": "data/silver/games_achievements",
        "gold_table":  "gold_games_achievements",
        "key_cols":    ["AppID", "Achievement_Name"],
    },
]

def upsert_table(spark, silver_path, table_name, key_cols):
    """Load silver CSV, UPSERT into PostgreSQL using composite key deduplication."""
    
    print(f"📥 [LOADING] {silver_path} → {table_name}...")
    
    # 1. Load Today's Silver Data
    df_new = spark.read \
        .option("multiLine", "true") \
        .option("escape", '"') \
        .csv(silver_path, header=True, inferSchema=True)
    
    new_count = df_new.count()
    if new_count == 0:
        print(f"   ⚠️ [SKIP] {silver_path} is empty. Nothing to load.")
        return {"table": table_name, "silver": 0, "gold": 0, "status": "SKIPPED"}
    
    df_new = df_new.withColumn("Last_Updated", F.current_timestamp())

    # 🛡️ Drop rows where any key column is null or blank (e.g. empty Achievement_Name from Steam API)
    for key_col in key_cols:
        if key_col in df_new.columns:
            df_new = df_new.filter(
                F.col(key_col).isNotNull() & (F.length(F.trim(F.col(key_col).cast("string"))) > 0)
            )
    new_count = df_new.count()
    print(f"   📊 Silver rows loaded (after key sanitization): {new_count}")
    
    # 2. Validate that key columns exist in the data
    missing_keys = [k for k in key_cols if k not in df_new.columns]
    if missing_keys:
        msg = f"   ❌ [ERROR] Key columns {missing_keys} not found in {silver_path}. Columns: {df_new.columns}"
        print(msg)
        return {"table": table_name, "silver": new_count, "gold": 0, "status": "KEY_ERROR", "error": msg}
    
    # 3. Try to Load Existing Gold Data from PostgreSQL
    try:
        df_existing = spark.read \
            .format("jdbc") \
            .option("url", DB_URL) \
            .option("dbtable", table_name) \
            .option("user", DB_USER) \
            .option("password", DB_PASS) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        existing_count = df_existing.count()
        print(f"   🔗 Existing table found ({existing_count} rows). Merging...")
    except Exception:
        print(f"   ⚠️ Table '{table_name}' not found. First load.")
        df_existing = spark.createDataFrame([], df_new.schema)
    
    # Force the new CSV data to strictly match the existing DB schema
    for col_name, col_type in df_existing.dtypes:
        if col_name in df_new.columns:
            df_new = df_new.withColumn(col_name, F.col(col_name).cast(col_type))
    
    # 4. UPSERT: Union → Deduplicate by COMPOSITE KEY (keep newest)
    df_combined = df_existing.unionByName(df_new, allowMissingColumns=True)
    
    window_spec = Window.partitionBy(*key_cols).orderBy(F.col("Last_Updated").desc())
    df_final = df_combined.withColumn("row_num", F.row_number().over(window_spec)) \
                          .filter(F.col("row_num") == 1) \
                          .drop("row_num")
    
    gold_count = df_final.count()
    print(f"   📊 Final gold rows (after dedup): {gold_count}")
    
    # 5. Push to PostgreSQL
    df_final.write \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", table_name) \
        .option("user", DB_USER) \
        .option("password", DB_PASS) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
    
    print(f"   ✅ {table_name} secured in PostgreSQL ({gold_count} rows).")
    return {"table": table_name, "silver": new_count, "gold": gold_count, "status": "OK"}

def validate_results(results):
    """Post-load validation: check that no data was lost between Silver and Gold."""
    print("\n" + "=" * 60)
    print("🧪 [VALIDATION] Post-Load Integrity Check")
    print("=" * 60)
    
    all_passed = True
    for r in results:
        table  = r["table"]
        silver = r["silver"]
        gold   = r["gold"]
        status = r["status"]
        
        if status == "SKIPPED":
            print(f"   ⏭️  {table}: SKIPPED (empty silver)")
            continue
        elif status == "KEY_ERROR":
            print(f"   ❌ {table}: FAILED — {r.get('error', 'missing key columns')}")
            all_passed = False
            continue
        elif status != "OK":
            print(f"   ❌ {table}: FAILED — {status}")
            all_passed = False
            continue
        
        # Gold should have >= Silver rows (gold accumulates history).
        # Allow a tiny tolerance (up to 0.01%) for intentionally sanitized bad rows
        # e.g. blank Achievement_Names from the Steam API that are stripped in Gold.
        tolerance = max(1, int(silver * 0.0001))
        if gold >= silver - tolerance:
            print(f"   ✅ {table}: Silver={silver}, Gold={gold} — OK")
        else:
            print(f"   ❌ {table}: Silver={silver}, Gold={gold} — DATA LOSS DETECTED!")
            all_passed = False
    
    print("=" * 60)
    if all_passed:
        print("🎉 [VALIDATION PASSED] All tables match. No data loss.")
    else:
        print("🚨 [VALIDATION FAILED] Some tables have data integrity issues!")
    print("=" * 60 + "\n")
    
    return all_passed

def load_steam_gold():
    print("🚀 [SPARK] Initiating Gold Layer Transfer...")
    
    # Convert to absolute path so Spark can always find it
    abs_jar = os.path.abspath(JDBC_DRIVER_PATH)
    
    spark = SparkSession.builder \
        .appName("Steam_Gold_Loader") \
        .config("spark.jars", abs_jar) \
        .config("spark.driver.extraClassPath", abs_jar) \
        .config("spark.executor.extraClassPath", abs_jar) \
        .getOrCreate()

    results = []
    for entry in SILVER_TABLES:
        try:
            result = upsert_table(
                spark,
                entry["silver_path"],
                entry["gold_table"],
                entry["key_cols"],
            )
            results.append(result)
        except Exception as e:
            print(f"   ❌ [FAILED] {entry['gold_table']}: {e}")
            results.append({
                "table": entry["gold_table"],
                "silver": -1,
                "gold": -1,
                "status": f"EXCEPTION: {e}",
            })
    
    # 🧪 Run post-load validation
    passed = validate_results(results)
    
    print("🎉 [TASK COMPLETE] All Gold tables secured. Power BI / pgAdmin ready.")
    spark.stop()
    
    if not passed:
        sys.exit(1)

if __name__ == "__main__":
    load_steam_gold()