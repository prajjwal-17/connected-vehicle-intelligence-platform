export type MaintenanceRisk = {
  vehicleId: string;
  riskScore: number;
  prediction: string;
  horizonDays: number;
  modelVersion: string;
  featureVersion: string;
  topRiskFactors: string[];
};

export class MlClient {
  constructor(private readonly baseUrl: string) {}

  async maintenanceRisk(
    vehicleId: string,
    summary: {
      events: number;
      averageEngineTemperatureC: number | null;
      latestBatterySoh: number | null;
    },
  ): Promise<MaintenanceRisk> {
    if (!summary.events)
      throw Object.assign(new Error('No telemetry is available for this vehicle'), {
        statusCode: 503,
      });
    const response = await fetch(`${this.baseUrl}/api/v1/predict/maintenance`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        vehicleId,
        features: {
          harsh_braking_count_7d: 0,
          fault_count_7d: 0,
          engine_temp_avg_7d: summary.averageEngineTemperatureC ?? 0,
          engine_temp_max_7d: summary.averageEngineTemperatureC ?? 0,
          engine_temp_slope: 0,
          idle_minutes_7d: 0,
          distance_km_7d: 0,
          battery_soc_avg_7d: null,
          battery_soh_latest: summary.latestBatterySoh,
          battery_soh_change: null,
          critical_fault_count_7d: 0,
          anomaly_count_7d: 0,
        },
      }),
    });
    if (!response.ok)
      throw Object.assign(new Error(`ML service unavailable (${response.status})`), {
        statusCode: 503,
      });
    return response.json() as Promise<MaintenanceRisk>;
  }
}
