import os
from fastapi import FastAPI, HTTPException
from app.inference import Predictor
from app.schemas import PredictionRequest, PredictionResponse

app = FastAPI(title="FleetPulse predictive maintenance", version="0.1.0")
predictor: Predictor | None = None
try:
    predictor = Predictor(os.getenv("MODEL_ARTIFACT_DIR", "artifacts"))
except Exception:
    predictor = None

@app.get("/health")
def health() -> dict: return {"status": "ok", "modelLoaded": predictor is not None}

@app.post("/api/v1/predict/maintenance", response_model=PredictionResponse)
def predict(request: PredictionRequest) -> dict:
    if predictor is None: raise HTTPException(status_code=503, detail="model artifact is not loaded")
    return predictor.predict(request.vehicleId, request.features.model_dump())
