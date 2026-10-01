from pathlib import Path
import json
import joblib
import pandas as pd
from app.features.engineering import FEATURE_COLUMNS


class Predictor:
    def __init__(self, artifact_dir: str = "artifacts"):
        path = Path(artifact_dir); self.model = joblib.load(path / "model.joblib"); self.metadata = json.loads((path / "metadata.json").read_text())
    def predict(self, vehicle_id: str, features: dict) -> dict:
        frame = pd.DataFrame([{column: features.get(column) for column in FEATURE_COLUMNS}]); score = float(self.model.predict_proba(frame)[0, 1]); return {"vehicleId": vehicle_id, "riskScore": score, "prediction": "MAINTENANCE_RISK" if score >= 0.5 else "NORMAL", "horizonDays": 7, "modelVersion": self.metadata["modelVersion"], "featureVersion": self.metadata["featureVersion"], "topRiskFactors": list(FEATURE_COLUMNS[:3])}
