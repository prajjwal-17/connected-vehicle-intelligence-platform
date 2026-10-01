from __future__ import annotations

from datetime import timedelta
from pathlib import Path
import json
import pandas as pd

FEATURE_COLUMNS = [
    "harsh_braking_count_7d", "fault_count_7d", "engine_temp_avg_7d", "engine_temp_max_7d",
    "engine_temp_slope", "idle_minutes_7d", "distance_km_7d", "battery_soc_avg_7d",
    "battery_soh_latest", "battery_soh_change", "critical_fault_count_7d", "anomaly_count_7d",
]


def load_events(path: str | Path) -> pd.DataFrame:
    rows = [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame["event_timestamp"] = pd.to_datetime(frame["event_timestamp"], utc=True)
    return frame.sort_values(["vehicle_id", "event_timestamp", "event_id"])


def build_features(events: pd.DataFrame, window_days: int = 7) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame(columns=["vehicle_id", "prediction_timestamp", *FEATURE_COLUMNS, "maintenance_required_within_7_days"])
    output: list[dict] = []
    window = timedelta(days=window_days)
    for vehicle_id, group in events.groupby("vehicle_id", sort=False):
        group = group.sort_values("event_timestamp").reset_index(drop=True)
        maintenance_times = group.loc[group["event_type"] == "MAINTENANCE_DUE", "event_timestamp"].tolist()
        for index, row in group.iterrows():
            timestamp = row["event_timestamp"]
            history = group[(group["event_timestamp"] <= timestamp) & (group["event_timestamp"] > timestamp - window)]
            temps = pd.to_numeric(history["engine_temperature_c"], errors="coerce").dropna()
            speeds = pd.to_numeric(history["speed_kph"], errors="coerce").fillna(0)
            faults = history[history["fault_scenario"].notna()]
            battery_soc = pd.to_numeric(history["battery_soc_percent"], errors="coerce").dropna()
            battery_soh = pd.to_numeric(history["battery_soh_percent"], errors="coerce").dropna()
            prior_temp = pd.to_numeric(group.loc[:index, "engine_temperature_c"], errors="coerce").dropna()
            slope = float((prior_temp.iloc[-1] - prior_temp.iloc[0]) / max((timestamp - group.loc[prior_temp.index[0], "event_timestamp"]).total_seconds(), 1)) if len(prior_temp) > 1 else 0.0
            future_label = int(any(timestamp < due <= timestamp + window for due in maintenance_times))
            output.append({
                "vehicle_id": vehicle_id, "prediction_timestamp": timestamp.isoformat(),
                "harsh_braking_count_7d": float(((speeds.shift(1) - speeds) / (1 / 60) > 25).sum()),
                "fault_count_7d": float(len(faults)), "engine_temp_avg_7d": float(temps.mean()) if len(temps) else 0.0,
                "engine_temp_max_7d": float(temps.max()) if len(temps) else 0.0, "engine_temp_slope": slope,
                "idle_minutes_7d": float(((history["ignition_on"] == 1) & (speeds <= 1)).sum()),
                "distance_km_7d": float(speeds.sum() / 60), "battery_soc_avg_7d": float(battery_soc.mean()) if len(battery_soc) else None,
                "battery_soh_latest": float(battery_soh.iloc[-1]) if len(battery_soh) else None,
                "battery_soh_change": float(battery_soh.iloc[-1] - battery_soh.iloc[0]) if len(battery_soh) > 1 else 0.0,
                "critical_fault_count_7d": float((faults["fault_severity"] == "CRITICAL").sum()),
                "anomaly_count_7d": float((faults["fault_scenario"].notna()).sum()),
                "maintenance_required_within_7_days": future_label,
            })
    return pd.DataFrame(output)


def chronological_split(dataset: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    ordered = dataset.sort_values("prediction_timestamp").reset_index(drop=True)
    first = max(int(len(ordered) * 0.6), 1); second = max(int(len(ordered) * 0.8), first + 1)
    return ordered.iloc[:first], ordered.iloc[first:second], ordered.iloc[second:]
