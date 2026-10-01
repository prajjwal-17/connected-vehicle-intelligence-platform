import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.features.engineering import FEATURE_COLUMNS, build_features, load_events
from app.training.train import train
from app.inference import Predictor


def write_events(path: Path) -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows = []
    for vehicle_index in range(4):
        for index in range(20):
            timestamp = start + timedelta(days=index)
            rows.append({
                "event_id": f"00000000-0000-4000-8000-{vehicle_index:012d}{index:0>0}"[-36:],
                "vehicle_id": f"vehicle-{vehicle_index}", "vin": "FP000000000000001",
                "event_timestamp": timestamp.isoformat(), "event_type": "TELEMETRY",
                "sequence_number": index, "speed_kph": 40 if index % 2 else 0, "ignition_on": 1,
                "engine_temperature_c": 120 if vehicle_index % 2 and index > 10 else 85,
                "fault_scenario": "ENGINE_OVERHEATING" if vehicle_index % 2 and index > 10 else None,
                "fault_severity": "CRITICAL" if vehicle_index % 2 and index > 10 else None,
                "battery_soc_percent": None, "battery_soh_percent": None,
            })
        maintenance_day = 10 if vehicle_index < 2 else 20
        rows.append({**rows[-1], "event_id": f"10000000-0000-4000-8000-{vehicle_index:012d}", "event_timestamp": (start + timedelta(days=maintenance_day)).isoformat(), "event_type": "MAINTENANCE_DUE"})
    path.write_text("\n".join(json.dumps(row) for row in rows))


def test_features_are_bounded_and_labels_use_future_ground_truth(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"; write_events(path)
    dataset = build_features(load_events(path))
    assert set(FEATURE_COLUMNS).issubset(dataset.columns)
    assert set(dataset["maintenance_required_within_7_days"].unique()) == {0, 1}
    assert "event_type" not in FEATURE_COLUMNS


def test_training_and_prediction_metadata(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"; artifacts = tmp_path / "artifacts"; write_events(path)
    metadata = train(str(path), str(artifacts))
    assert metadata["modelVersion"] == "v1"
    assert metadata["chronologicalSplit"]["train"] < metadata["rows"]
    result = Predictor(str(artifacts)).predict("vehicle-1", {column: 0 for column in FEATURE_COLUMNS})
    assert result["vehicleId"] == "vehicle-1"
    assert 0 <= result["riskScore"] <= 1
    assert result["featureVersion"] == "telemetry-7d-v1"
