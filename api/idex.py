import os
import pickle
from datetime import date, datetime

import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

MODEL_PATH = os.path.join(os.path.dirname(__file__), "model", "rating_predictor_v3.pkl")

with open(MODEL_PATH, "rb") as f:
    _artifact = pickle.load(f)

REGRESSOR = _artifact["regressor"]
GENRE_ENCODER = _artifact["genre_encoder"]
FEATURES = _artifact["features"]
GENRES = sorted(GENRE_ENCODER.classes_.tolist())
IMPORTANCES = {
    feat: int(val)
    for feat, val in zip(FEATURES, REGRESSOR.feature_importances_.tolist())
}

app = FastAPI(title="Steam Review Score Predictor")

# Handles cross-origin security limits securely so your Vercel site can call your Railway container
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def to_bucket(score: float):
    if score >= 95:
        return 6, "Overwhelmingly Positive"
    elif score >= 80:
        return 5, "Very Positive"
    elif score >= 70:
        return 4, "Positive"
    elif score >= 40:
        return 3, "Mixed"
    elif score >= 20:
        return 2, "Negative"
    elif score >= 10:
        return 1, "Very Negative"
    else:
        return 0, "Overwhelmingly Negative"


class PredictRequest(BaseModel):
    price: float = Field(..., ge=0, le=500)
    discount_pct: float = Field(0, ge=0, le=100)
    achievement_count: int = Field(0, ge=0, le=10000)
    recommendations: int = Field(0, ge=0, le=50_000_000)
    num_reviews: int = Field(0, ge=0, le=50_000_000)
    dlc_count: int = Field(0, ge=0, le=5000)
    category_count: int = Field(0, ge=0, le=100)
    release_date: str = Field(..., description="YYYY-MM-DD")
    genre: str


@app.get("/api/meta")
def meta():
    return {
        "genres": GENRES,
        "feature_importance": IMPORTANCES,
        "features": FEATURES,
    }


@app.post("/api/predict")
def predict(req: PredictRequest):
    if req.genre not in GENRES:
        raise HTTPException(status_code=400, detail=f"Unknown genre '{req.genre}'")

    try:
        rel_date = datetime.strptime(req.release_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="release_date must be YYYY-MM-DD")

    days_since = (date.today() - rel_date).days
    genre_enc = int(GENRE_ENCODER.transform([req.genre])[0])

    row = {
        "price": req.price,
        "discount_pct": req.discount_pct,
        "achievement_count": req.achievement_count,
        "recommendations": req.recommendations,
        "num_reviews": req.num_reviews,
        "dlc_count": req.dlc_count,
        "category_count": req.category_count,
        "release_month": rel_date.month,
        "release_year": rel_date.year,
        "days_since_release": days_since,
        "genre_enc": genre_enc,
    }
    X = pd.DataFrame([row])[FEATURES]
    raw_pred = float(REGRESSOR.predict(X)[0])
    clamped = max(0.0, min(100.0, raw_pred))
    bucket_id, bucket_label = to_bucket(clamped)

    return {
        "review_score_raw": round(raw_pred, 2),
        "review_score": round(clamped, 2),
        "bucket_id": bucket_id,
        "bucket_label": bucket_label,
        "inputs_used": row,
        "feature_importance": IMPORTANCES,
    }


@app.get("/api/health")
def health():
    return {"status": "ok", "model_features": FEATURES}
