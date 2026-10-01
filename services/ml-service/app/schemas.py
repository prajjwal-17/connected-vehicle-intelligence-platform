from pydantic import BaseModel, Field


class MaintenanceFeatures(BaseModel):
    harsh_braking_count_7d: float = Field(ge=0)
    fault_count_7d: float = Field(ge=0)
    engine_temp_avg_7d: float = Field(ge=-40)
    engine_temp_max_7d: float = Field(ge=-40)
    engine_temp_slope: float
    idle_minutes_7d: float = Field(ge=0)
    distance_km_7d: float = Field(ge=0)
    battery_soc_avg_7d: float | None = Field(default=None, ge=0, le=100)
    battery_soh_latest: float | None = Field(default=None, ge=0, le=100)
    battery_soh_change: float | None = None
    critical_fault_count_7d: float = Field(ge=0)
    anomaly_count_7d: float = Field(ge=0)


class PredictionRequest(BaseModel):
    vehicleId: str = Field(min_length=1)
    features: MaintenanceFeatures


class PredictionResponse(BaseModel):
    vehicleId: str
    riskScore: float
    prediction: str
    horizonDays: int
    modelVersion: str
    featureVersion: str
    topRiskFactors: list[str]
