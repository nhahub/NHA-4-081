import os
import pickle
import json
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score, accuracy_score
from sklearn.preprocessing import LabelEncoder

print("🔄 Step 1: Connecting to Postgres and downloading data...")
engine = create_engine("postgresql://myuser:mypassword@localhost:5432/steam_db")

# Fetch main games and genres[cite: 1]
query_main = """
    SELECT m.*, g."Genre_Name" AS genre 
    FROM gold_games_main m 
    LEFT JOIN gold_games_genres g ON m."AppID" = g."AppID"
"""
df = pd.read_sql(query_main, engine)
print(f"✅ Loaded {len(df):,} rows from main games table.")

# 1. Fetch and aggregate DLC counts per AppID[cite: 1]
print("📥 Fetching and processing DLC data...")
query_dlc = 'SELECT "AppID", COUNT("DLC_AppID") AS dlc_count FROM gold_games_dlc GROUP BY "AppID"'
df_dlc = pd.read_sql(query_dlc, engine)

# 2. Fetch and aggregate Category counts per AppID[cite: 1]
print("📥 Fetching and processing Categories data...")
query_cats = 'SELECT "AppID", COUNT("Category_ID") AS category_count FROM gold_games_categories GROUP BY "AppID"'
df_cats = pd.read_sql(query_cats, engine)

# Merge new features into the main dataframe[cite: 1]
df = df.merge(df_dlc, on="AppID", how="left")
df = df.merge(df_cats, on="AppID", how="left")

# Fill missing counts with 0 (for games without DLCs or categories)[cite: 1]
df["dlc_count"] = df["dlc_count"].fillna(0).astype(int)
df["category_count"] = df["category_count"].fillna(0).astype(int)

print("\n🔄 Step 2: Mapping columns to script names...")
mapping = {
    "Price_USD": "price",
    "Discount_Percent": "discount_pct",
    "Achievements_Total": "achievement_count",
    "Recommendations_Total": "recommendations",
    "Total_Reviews": "num_reviews",
    "Review_Score": "review_score"
}
df = df.rename(columns=mapping)

# Drop missing target rows[cite: 1]
df = df.dropna(subset=["review_score"]).reset_index(drop=True)

print("\n🔄 Step 3: Resolving features and fixing dates...")
# Convert features safely to numeric including our 2 new columns[cite: 1]
numeric_cols = ["price", "discount_pct", "achievement_count", "recommendations", "num_reviews", "dlc_count", "category_count"]
for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

df["Release_Date"] = pd.to_datetime(df["Release_Date"], errors="coerce")
use_random_split = df["Release_Date"].isna().all()

if not use_random_split:
    print("📅 Dates parsed successfully! Engineering date features...")
    df["release_month"] = df["Release_Date"].dt.month.fillna(1).astype(int)
    df["release_year"] = df["Release_Date"].dt.year.fillna(2025).astype(int)
    df["days_since_release"] = (pd.Timestamp.now() - df["Release_Date"]).dt.days.fillna(0)
    features = numeric_cols + ["release_month", "release_year", "days_since_release", "genre_enc"]
else:
    print("⚠️ Warning: Release_Date format unreadable. Falling back to a random train/test split.")
    features = numeric_cols + ["genre_enc"]

print("\n🔄 Step 4: Encoding Categorical Data...")
df["genre"] = df["genre"].fillna("Unknown").astype(str)
le_genre = LabelEncoder()
df["genre_enc"] = le_genre.fit_transform(df["genre"])

# Set up target columns[cite: 1]
X = df[features]
y_reg = df["review_score"]

# Create Steam Review Buckets[cite: 1]
def to_bucket(score):
    if score >= 95: return 6
    elif score >= 80: return 5
    elif score >= 70: return 4
    elif score >= 40: return 3
    elif score >= 20: return 2
    elif score >= 10: return 1
    else: return 0

y_clf = df["review_score"].apply(to_bucket)

print("\n🔄 Step 5: Splitting Data into Train & Test validation sets...")
if not use_random_split:
    df["Release_Date_Fill"] = df["Release_Date"].fillna(pd.Timestamp("2000-01-01"))
    df = df.sort_values("Release_Date_Fill").reset_index(drop=True)
    split_idx = int(len(df) * 0.8)
    
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train_reg, y_test_reg = y_reg.iloc[:split_idx], y_reg.iloc[split_idx:]
    y_train_clf, y_test_clf = y_clf.iloc[:split_idx], y_clf.iloc[split_idx:]
else:
    X_train, X_test, y_train_reg, y_test_reg = train_test_split(X, y_reg, test_size=0.2, random_state=42)
    X_train, X_test, y_train_clf, y_test_clf = train_test_split(X, y_clf, test_size=0.2, random_state=42)

print(f"📊 Training Matrix: {X_train.shape[0]:,} rows | Testing Matrix: {X_test.shape[0]:,} rows")

print("\n🚀 Step 6: Training Regressor with New Hyperparameters...")
# New tuned parameters applied here[cite: 1]
reg = lgb.LGBMRegressor(
    n_estimators=500,        
    learning_rate=0.02,
    num_leaves=31,
    random_state=42
)

reg.fit(
    X_train, y_train_reg,
    eval_set=[(X_test, y_test_reg)],
    callbacks=[lgb.log_evaluation(period=50)]
)

# Test evaluation[cite: 1]
preds = reg.predict(X_test)
print(f"📉 Regressor MAE Loss: {mean_absolute_error(y_test_reg, preds):.2f}")
print(f"📈 Regressor R2 Accuracy Score: {r2_score(y_test_reg, preds):.3f}")

print("\n💾 Saving Model Artifacts...")
os.makedirs("./models", exist_ok=True)
artifact = {
    "regressor": reg,
    "genre_encoder": le_genre,
    "features": features
}
# Saved as rating_predictor_v3.pkl to preserve the v2 asset[cite: 1]
with open("./models/rating_predictor_v3.pkl", "wb") as f:
    pickle.dump(artifact, f)

print("🎉 DONE! New version model compiled and saved as rating_predictor_v3.pkl[cite: 1]")