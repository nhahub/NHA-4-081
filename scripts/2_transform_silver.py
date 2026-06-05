from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, BooleanType, FloatType, StringType, StructType
import re
import os, sys, urllib.request

# ============================================================
# ️ Safe Struct Field Extractor
# Some games return requirements as "" instead of {"minimum": ...}
# Spark infers the column as STRING, so .minimum crashes.
# This checks the schema first and returns None if it's not a struct.
# ============================================================
def safe_struct_field(df, col_name, field_name, alias_name, transform_udf=None):
    """Safely extract a nested field from a column that may be STRING or STRUCT."""
    try:
        col_type = df.schema[col_name].dataType
        if isinstance(col_type, StructType) and field_name in col_type.names:
            expr = F.col(f"{col_name}.{field_name}")
            if transform_udf:
                expr = transform_udf(expr)
            return expr.alias(alias_name)
    except Exception:
        pass
    return F.lit(None).cast(StringType()).alias(alias_name)

# ============================================================
#  HTML Stripper UDF - Strips all HTML tags from descriptions
# ============================================================
def strip_html(text):
    if text is None:
        return None
    clean = re.sub(r'<[^>]+>', ' ', text)       # Remove HTML tags
    clean = re.sub(r'\s+', ' ', clean).strip()   # Collapse whitespace
    return clean

def transform_silver_layer():
    print("🚀 [TASK START] Igniting PySpark Engine...")
    
    #  Auto-download JDBC driver if missing (Needed for Jupiter JVM initialization)
    JDBC_DRIVER_PATH = "drivers/postgresql-42.7.4.jar"
    if not os.path.exists(JDBC_DRIVER_PATH):
        os.makedirs("drivers", exist_ok=True)
        print("📥 Downloading PostgreSQL JDBC driver...")
        urllib.request.urlretrieve(
            "https://jdbc.postgresql.org/download/postgresql-42.7.4.jar",
            JDBC_DRIVER_PATH
        )
    
    abs_jar = os.path.abspath(JDBC_DRIVER_PATH)
    
    # 1. Initialize Spark
    spark = SparkSession.builder \
        .appName("Steam_Medallion_Silver") \
        .config("spark.jars", abs_jar) \
        .config("spark.driver.extraClassPath", abs_jar) \
        .config("spark.executor.extraClassPath", abs_jar) \
        .getOrCreate()

    # Register HTML stripper as a Spark UDF
    strip_html_udf = F.udf(strip_html, StringType())

    from pyspark.sql.window import Window

    # 2. Load ALL Bronze batch files via wildcard — PySpark handles Big Data, Python doesn't
    df_store_raw   = spark.read.option("multiline", "true").json("data/bronze/store_raw_*.json") \
                          .withColumn("_src_file", F.input_file_name())
    df_reviews_raw = spark.read.option("multiline", "true").json("data/bronze/reviews_raw_*.json") \
                          .withColumn("_src_file", F.input_file_name())

    # Deduplicate: keep the most recently fetched record per AppID
    # File names contain timestamps (store_raw_YYYYMMDD_HHMMSS.json), so lexicographic desc = newest first
    store_w   = Window.partitionBy("appid").orderBy(F.col("_src_file").desc())
    reviews_w = Window.partitionBy("appid").orderBy(F.col("_src_file").desc())

    df_store   = df_store_raw.withColumn("_rn", F.row_number().over(store_w)) \
                             .filter(F.col("_rn") == 1).drop("_rn", "_src_file")
    df_reviews = df_reviews_raw.withColumn("_rn", F.row_number().over(reviews_w)) \
                               .filter(F.col("_rn") == 1).drop("_rn", "_src_file")

    #  Bronze Input Validation
    store_count   = df_store.count()
    reviews_count = df_reviews.count()
    print(f"📊 [CHECK] Bronze input: {store_count} store records (deduplicated), {reviews_count} review records")

    if store_count == 0:
        print("❌ [FATAL] No store_raw_*.json files found in data/bronze/! Run extract first.")
        spark.stop()
        sys.exit(1)
    if reviews_count == 0:
        print("❌ [FATAL] No reviews_raw_*.json files found in data/bronze/! Run extract first.")
        spark.stop()
        sys.exit(1)

    # Cast appid to Integer on both sides so the join types match
    df_store = df_store.withColumn("appid", F.col("appid").cast(IntegerType()))
    df_reviews = df_reviews.withColumn("appid", F.col("appid").cast(IntegerType()))

    print("🔗 [PROCESS] Joining Store Data with Review Data...")
    df_joined = df_store.join(df_reviews, on="appid", how="left")

    # ============================================================
    #  TABLE 1: games_main.csv — The Master Fact Table (~40 cols)
    # ============================================================
    print("🪄 [PROCESS] Forging games_main.csv...")
    
    df_main = df_joined.select(
        F.col("appid").alias("AppID"),
        F.col("name").alias("Name"),
        F.col("type").alias("Type"),
        F.col("is_free").cast(BooleanType()).alias("Is_Free"),
        F.coalesce(F.col("required_age").cast(IntegerType()), F.lit(0)).alias("Required_Age"),
        
        #  SPELL: Strip HTML from description fields
        strip_html_udf(F.col("short_description")).alias("Short_Description"),
        strip_html_udf(F.col("detailed_description")).alias("Detailed_Description"),
        strip_html_udf(F.col("about_the_game")).alias("About_The_Game"),
        strip_html_udf(F.col("supported_languages")).alias("Supported_Languages"),
        
        # Press reviews (some games have critic quotes with newlines)
        strip_html_udf(F.col("reviews")).alias("Press_Reviews"),
        
        #  Website
        F.col("website").alias("Website"),
        
        # ️ ALL the Images
        F.col("header_image").alias("Header_Image"),
        F.col("capsule_image").alias("Capsule_Image"),
        F.col("capsule_imagev5").alias("Capsule_Image_V5"),
        F.col("background").alias("Background_Image"),
        F.col("background_raw").alias("Background_Raw"),
        
        #  Pricing (Full breakdown)
        (F.coalesce(F.col("price_overview.final"), F.lit(0)) / 100).cast(FloatType()).alias("Price_USD"),
        F.col("price_overview.final_formatted").alias("Price_Formatted"),
        F.col("price_overview.currency").alias("Currency"),
        (F.coalesce(F.col("price_overview.initial"), F.lit(0)) / 100).cast(FloatType()).alias("Initial_Price_USD"),
        F.coalesce(F.col("price_overview.discount_percent"), F.lit(0)).alias("Discount_Percent"),
        
        # ️ Platforms
        F.coalesce(F.col("platforms.windows"), F.lit(False)).alias("Is_Windows"),
        F.coalesce(F.col("platforms.mac"), F.lit(False)).alias("Is_Mac"),
        F.coalesce(F.col("platforms.linux"), F.lit(False)).alias("Is_Linux"),
        
        # ‍ Developers & Publishers (Array → comma-separated)
        F.concat_ws(", ", F.col("developers")).alias("Developers"),
        F.concat_ws(", ", F.col("publishers")).alias("Publishers"),
        
        #  System Requirements (HTML stripped)
        # Some games return requirements as "" instead of a struct,
        # so safe_struct_field() checks the schema before dot-access.
        safe_struct_field(df_joined, "pc_requirements", "minimum", "PC_Requirements_Min", strip_html_udf),
        safe_struct_field(df_joined, "pc_requirements", "recommended", "PC_Requirements_Rec", strip_html_udf),
        safe_struct_field(df_joined, "mac_requirements", "minimum", "Mac_Requirements_Min", strip_html_udf),
        safe_struct_field(df_joined, "mac_requirements", "recommended", "Mac_Requirements_Rec", strip_html_udf),
        safe_struct_field(df_joined, "linux_requirements", "minimum", "Linux_Requirements_Min", strip_html_udf),
        safe_struct_field(df_joined, "linux_requirements", "recommended", "Linux_Requirements_Rec", strip_html_udf),
        
        #  Release Date
        F.col("release_date.date").alias("Release_Date"),
        F.coalesce(F.col("release_date.coming_soon"), F.lit(False)).alias("Coming_Soon"),
        
        #  Legal (often has <br> and newlines)
        strip_html_udf(F.col("legal_notice")).alias("Legal_Notice"),

        #  Support Info
        F.col("support_info.url").alias("Support_URL"),
        F.col("support_info.email").alias("Support_Email"),
        
        # ⭐ Metacritic
        F.col("metacritic.score").alias("Metacritic_Score"),
        F.col("metacritic.url").alias("Metacritic_URL"),
        
        #  Recommendations & Achievements totals
        F.coalesce(F.col("recommendations.total"), F.lit(0)).alias("Recommendations_Total"),
        F.coalesce(F.col("achievements.total"), F.lit(0)).alias("Achievements_Total"),
        
        #  Content Descriptors
        strip_html_udf(F.col("content_descriptors.notes")).alias("Content_Descriptor_Notes"),
        
        #  Reviews from reviews_raw.json (joined data)
        F.coalesce(F.col("total_positive"), F.lit(0)).alias("Positive_Reviews"),
        F.coalesce(F.col("total_negative"), F.lit(0)).alias("Negative_Reviews"),
        F.col("review_score").alias("Review_Score"),
        F.col("review_score_desc").alias("Review_Score_Desc"),
        F.coalesce(F.col("total_reviews"), F.lit(0)).alias("Total_Reviews"),
        
        #  Live Player Count
        F.coalesce(F.col("live_peak_players"), F.lit(0)).alias("Current_Players")
    )
    df_main = df_main.dropDuplicates(["AppID"])

    # ============================================================
    #  Column Logic Checks (warnings only — never block the pipeline)
    # Uses filter() on the already-computed DataFrame — zero extra Spark jobs.
    # ============================================================
    logic_warnings = []

    neg_price = df_main.filter(F.col("Price_USD") < 0).count()
    if neg_price > 0:
        logic_warnings.append(f"⚠️  {neg_price} rows have negative Price_USD")

    bad_discount = df_main.filter(
        (F.col("Discount_Percent") < 0) | (F.col("Discount_Percent") > 100)
    ).count()
    if bad_discount > 0:
        logic_warnings.append(f"⚠️  {bad_discount} rows have Discount_Percent outside 0-100")

    bad_reviews = df_main.filter(
        F.col("Positive_Reviews") > F.col("Total_Reviews")
    ).count()
    if bad_reviews > 0:
        logic_warnings.append(f"⚠️  {bad_reviews} rows where Positive_Reviews > Total_Reviews")

    neg_recommendations = df_main.filter(F.col("Recommendations_Total") < 0).count()
    if neg_recommendations > 0:
        logic_warnings.append(f"⚠️  {neg_recommendations} rows have negative Recommendations_Total")

    bad_review_score = df_main.filter(
        F.col("Review_Score").isNotNull() &
        ((F.col("Review_Score") < 0) | (F.col("Review_Score") > 9))
    ).count()
    if bad_review_score > 0:
        logic_warnings.append(f"⚠️  {bad_review_score} rows have Review_Score outside 0-9")

    if logic_warnings:
        print("\n🔍 [LOGIC CHECKS] Column anomalies detected in games_main:")
        for w in logic_warnings:
            print(f"   {w}")
    else:
        print("🔍 [LOGIC CHECKS] All column constraints satisfied ✅")

    df_main.coalesce(1).write.option("escape", '"').csv("data/silver/games_main", header=True, mode="overwrite")
    print("✅ [SAVED] games_main.csv")

    # ============================================================
    #  TABLE 2: games_genres.csv — Exploded genres
    # ============================================================
    print("🪄 [PROCESS] Forging games_genres.csv...")
    
    df_genres = df_store.select(
        F.col("appid").alias("AppID"),
        F.explode_outer(F.col("genres")).alias("genre")
    ).select(
        "AppID",
        F.col("genre.id").alias("Genre_ID"),
        F.col("genre.description").alias("Genre_Name")
    ).filter(F.col("Genre_ID").isNotNull()).dropDuplicates(["AppID", "Genre_ID"])
    
    df_genres.coalesce(1).write.option("escape", '"').csv("data/silver/games_genres", header=True, mode="overwrite")
    print("✅ [SAVED] games_genres.csv")

    # ============================================================
    # ️ TABLE 3: games_categories.csv — Exploded categories
    # ============================================================
    print("🪄 [PROCESS] Forging games_categories.csv...")
    
    df_categories = df_store.select(
        F.col("appid").alias("AppID"),
        F.explode_outer(F.col("categories")).alias("cat")
    ).select(
        "AppID",
        F.col("cat.id").alias("Category_ID"),
        F.col("cat.description").alias("Category_Name")
    ).filter(F.col("Category_ID").isNotNull()).dropDuplicates(["AppID", "Category_ID"])
    
    df_categories.coalesce(1).write.option("escape", '"').csv("data/silver/games_categories", header=True, mode="overwrite")
    print("✅ [SAVED] games_categories.csv")

    # ============================================================
    #  TABLE 4: games_screenshots.csv — All screenshot URLs
    # ============================================================
    print("🪄 [PROCESS] Forging games_screenshots.csv...")
    
    df_screenshots = df_store.select(
        F.col("appid").alias("AppID"),
        F.explode_outer(F.col("screenshots")).alias("ss")
    ).select(
        "AppID",
        F.col("ss.id").alias("Screenshot_ID"),
        F.col("ss.path_thumbnail").alias("Thumbnail_URL"),
        F.col("ss.path_full").alias("Full_URL")
    ).filter(F.col("Screenshot_ID").isNotNull()).dropDuplicates(["AppID", "Screenshot_ID"])
    
    df_screenshots.coalesce(1).write.option("escape", '"').csv("data/silver/games_screenshots", header=True, mode="overwrite")
    print("✅ [SAVED] games_screenshots.csv")

    # ============================================================
    #  TABLE 5: games_movies.csv — All trailer/video data
    # ============================================================
    print("🪄 [PROCESS] Forging games_movies.csv...")
    
    df_movies = df_store.select(
        F.col("appid").alias("AppID"),
        F.explode_outer(F.col("movies")).alias("mov")
    ).select(
        "AppID",
        F.col("mov.id").alias("Movie_ID"),
        F.col("mov.name").alias("Movie_Name"),
        F.col("mov.thumbnail").alias("Thumbnail"),
        F.col("mov.highlight").alias("Highlight")
    ).filter(F.col("Movie_ID").isNotNull()).dropDuplicates(["AppID", "Movie_ID"])
    
    df_movies.coalesce(1).write.option("escape", '"').csv("data/silver/games_movies", header=True, mode="overwrite")
    print("✅ [SAVED] games_movies.csv")

    # ============================================================
    #  TABLE 6: games_dlc.csv — DLC AppIDs
    # ============================================================
    print("🪄 [PROCESS] Forging games_dlc.csv...")
    
    df_dlc = df_store.select(
        F.col("appid").alias("AppID"),
        F.explode_outer(F.col("dlc")).alias("DLC_AppID")
    ).filter(F.col("DLC_AppID").isNotNull()).dropDuplicates(["AppID", "DLC_AppID"])
    
    df_dlc.coalesce(1).write.option("escape", '"').csv("data/silver/games_dlc", header=True, mode="overwrite")
    print("✅ [SAVED] games_dlc.csv")

    # ============================================================
    #  TABLE 7: games_achievements.csv — Highlighted achievements
    # ============================================================
    print("🪄 [PROCESS] Forging games_achievements.csv...")
    
    df_achievements = df_store.select(
        F.col("appid").alias("AppID"),
        F.explode_outer(F.col("achievements.highlighted")).alias("ach")
    ).select(
        "AppID",
        F.col("ach.name").alias("Achievement_Name"),
        F.col("ach.path").alias("Achievement_Icon")
    ).filter(F.col("Achievement_Name").isNotNull() & (F.length(F.trim(F.col("Achievement_Name"))) > 0)).dropDuplicates(["AppID", "Achievement_Name"])
    
    df_achievements.coalesce(1).write.option("escape", '"').csv("data/silver/games_achievements", header=True, mode="overwrite")
    print("✅ [SAVED] games_achievements.csv")

    # ============================================================
    #  Post-Transform Validation
    # ============================================================
    print("\n" + "=" * 60)
    print("🧪 [VALIDATION] Post-Transform Integrity Check")
    print("=" * 60)
    
    # Re-read the written CSVs to validate what actually got saved
    silver_tables = {
        "games_main":         {"path": "data/silver/games_main",         "key": "AppID", "min_rows": store_count},
        "games_genres":       {"path": "data/silver/games_genres",       "key": "Genre_ID"},
        "games_categories":   {"path": "data/silver/games_categories",   "key": "Category_ID"},
        "games_screenshots":  {"path": "data/silver/games_screenshots",  "key": "Screenshot_ID"},
        "games_movies":       {"path": "data/silver/games_movies",       "key": "Movie_ID"},
        "games_dlc":          {"path": "data/silver/games_dlc",          "key": "DLC_AppID"},
        # Achievement_Name is optional display text — Gold sanitizes blank names before UPSERT
        "games_achievements": {"path": "data/silver/games_achievements", "key": "AppID"},
    }
    
    all_passed = True
    for name, info in silver_tables.items():
        try:
            df_check = spark.read.option("multiLine", "true").option("escape", '"').csv(
                info["path"], header=True, inferSchema=True
            )
            row_count = df_check.count()
            null_appids = df_check.filter(F.col("AppID").isNull()).count()
            null_keys = df_check.filter(F.col(info["key"]).isNull()).count()
            
            issues = []
            if row_count == 0:
                issues.append("EMPTY TABLE")
            if null_appids > 0:
                issues.append(f"{null_appids} null AppIDs")
            if null_keys > 0:
                issues.append(f"{null_keys} null {info['key']}")
            if "min_rows" in info and row_count < info["min_rows"]:
                issues.append(f"expected >= {info['min_rows']} rows, got {row_count}")
            
            if issues:
                print(f"   ❌ {name}: {row_count} rows — {', '.join(issues)}")
                all_passed = False
            else:
                print(f"   ✅ {name}: {row_count} rows")
        except Exception as e:
            print(f"   ❌ {name}: FAILED TO READ — {e}")
            all_passed = False
    
    print("=" * 60)
    if all_passed:
        print("🎉 [VALIDATION PASSED] All 7 Silver tables verified.\n")
    else:
        print("🚨 [VALIDATION FAILED] Some tables have issues! Check above.\n")
    
    # ============================================================
    print(f"🎉 [TASK COMPLETE] All 7 Silver tables forged in data/silver/")
    spark.stop()
    
    if not all_passed:
        sys.exit(1)

if __name__ == "__main__":
    transform_silver_layer()