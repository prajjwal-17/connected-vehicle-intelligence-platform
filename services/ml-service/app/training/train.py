from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import joblib
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, f1_score, precision_score, recall_score, roc_auc_score
from sklearn.pipeline import make_pipeline
from app.features.engineering import FEATURE_COLUMNS, build_features, chronological_split, load_events


def metrics(model, x: pd.DataFrame, y: pd.Series) -> dict:
    if len(y) == 0 or y.nunique() < 2: return {"supported": False, "reason": "test split has fewer than two classes"}
    scores = model.predict_proba(x)[:, 1]; predictions = (scores >= 0.5).astype(int)
    return {"supported": True, "precision": precision_score(y, predictions, zero_division=0), "recall": recall_score(y, predictions, zero_division=0), "f1": f1_score(y, predictions, zero_division=0), "roc_auc": roc_auc_score(y, scores), "pr_auc": average_precision_score(y, scores)}


def train(input_path: str, artifact_dir: str) -> dict:
    dataset = build_features(load_events(input_path)); train_set, validation_set, test_set = chronological_split(dataset)
    x_train = train_set[FEATURE_COLUMNS]; y_train = train_set["maintenance_required_within_7_days"]
    if y_train.nunique() < 2: raise ValueError("training split must contain both maintenance classes; add synthetic maintenance ground truth")
    baseline = make_pipeline(SimpleImputer(strategy="median", keep_empty_features=True), LogisticRegression(max_iter=500, class_weight="balanced", random_state=42))
    improved = make_pipeline(SimpleImputer(strategy="median", keep_empty_features=True), RandomForestClassifier(n_estimators=100, max_depth=6, random_state=42, class_weight="balanced"))
    baseline.fit(x_train, y_train); improved.fit(x_train, y_train); x_test = test_set[FEATURE_COLUMNS]; y_test = test_set["maintenance_required_within_7_days"]
    metadata = {"modelVersion": "v1", "featureVersion": "telemetry-7d-v1", "trainingTimestamp": datetime.now(timezone.utc).isoformat(), "rows": len(dataset), "features": FEATURE_COLUMNS, "target": "maintenance_required_within_7_days", "chronologicalSplit": {"train": len(train_set), "validation": len(validation_set), "test": len(test_set)}, "baseline": metrics(baseline, x_test, y_test), "improved": metrics(improved, x_test, y_test), "leakageChecks": ["features use events at or before prediction_timestamp", "labels inspect only the following seven days", "splits are chronological"]}
    output = Path(artifact_dir); output.mkdir(parents=True, exist_ok=True); joblib.dump(improved, output / "model.joblib"); (output / "metadata.json").write_text(json.dumps(metadata, indent=2)); return metadata


if __name__ == "__main__":
    parser = argparse.ArgumentParser(); parser.add_argument("--input", default="data/telemetry.jsonl"); parser.add_argument("--artifacts", default="artifacts"); args = parser.parse_args(); print(json.dumps(train(args.input, args.artifacts), indent=2))
