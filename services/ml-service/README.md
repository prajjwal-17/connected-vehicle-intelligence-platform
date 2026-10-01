# FleetPulse predictive maintenance service

This Python service trains and serves a baseline Logistic Regression model and an improved Random Forest model over synthetic FleetPulse telemetry. It does not control vehicles or perform safety-critical actions; its output is an estimate of maintenance risk within seven days.

## Pipeline

The historical consumer stores the shared telemetry contract in ClickHouse. Export raw rows to JSONL, then run `python -m app.training.train --input data/telemetry.jsonl --artifacts artifacts`. Features use only telemetry at or before each prediction timestamp. The label is `1` when a simulator `MAINTENANCE_DUE` event occurs in the following seven days; it is not random. Data is split chronologically 60/20/20 into train, validation, and test. Future maintenance/fault/telemetry values are never features.

Features include seven-day fault and critical-fault counts, engine temperature mean/max/slope, harsh-braking count, idle minutes, distance proxy, battery SOC mean, latest SOH, SOH change, and anomaly count. Missing battery/engine values are imputed in the model pipeline.

Run the API with `uvicorn app.api:app --host 0.0.0.0 --port 8000`. The endpoint is `POST /api/v1/predict/maintenance`; `/health` reports whether an artifact is loaded. Responses include `modelVersion`, `featureVersion`, a seven-day horizon, and feature names associated with the estimate. Feature associations are not causal explanations.

Install locally with `pip install -r requirements.txt`. Model artifacts and datasets are local outputs and should not be committed.
