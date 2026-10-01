import type { DatabaseClient } from '@fleetpulse/database';
import { NotFoundError } from '../utils/errors.js';
import { ClickHouseClient } from './clickhouse-client.js';
import { MlClient } from './ml-client.js';

export class ProductService {
  constructor(
    private readonly db: DatabaseClient,
    private readonly clickhouse: ClickHouseClient,
    private readonly ml: MlClient,
  ) {}

  async overview() {
    const [vehicles, openAlerts, criticalAlerts, analytics] = await Promise.all([
      this.db.vehicle.count(),
      this.db.alert.count({ where: { status: 'OPEN' } }),
      this.db.alert.count({ where: { status: 'OPEN', severity: 'CRITICAL' } }),
      this.clickhouse
        .fleetAnalytics()
        .catch(() => ({ events: 0, vehicles: 0, averageSpeedKph: null })),
    ]);
    const activeVehicles = await this.db.vehicle.count({ where: { status: 'ACTIVE' } });
    return {
      totalVehicles: vehicles,
      activeVehicles,
      openAlerts,
      criticalOpenAlerts: criticalAlerts,
      telemetry: analytics,
      generatedAt: new Date().toISOString(),
    };
  }

  async vehicleDetails(id: string) {
    const vehicle = await this.db.vehicle.findUnique({
      where: { id },
      include: {
        fleet: true,
        alerts: { orderBy: { detectedAt: 'desc' }, take: 20 },
        maintenance: { orderBy: { performedAt: 'desc' }, take: 20 },
      },
    });
    if (!vehicle) throw new NotFoundError('Vehicle', id);
    const telemetry = await this.clickhouse.vehicleSummary(id);
    return { ...vehicle, telemetrySummary: telemetry };
  }

  telemetrySummary(id: string) {
    return this.clickhouse.vehicleSummary(id);
  }
  maintenanceRisk(id: string) {
    return this.clickhouse
      .vehicleSummary(id)
      .then((summary) => this.ml.maintenanceRisk(id, summary));
  }
  fleetAnalytics() {
    return this.clickhouse.fleetAnalytics();
  }
}
