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

  async overview(tenantId?: string) {
    const vehicleWhere = tenantId ? { fleet: { tenantId } } : undefined;
    const vehiclesForAnalytics = tenantId
      ? await this.db.vehicle.findMany({ where: vehicleWhere, select: { id: true, vin: true } })
      : undefined;
    const vehicleIds = vehiclesForAnalytics?.map((vehicle) => vehicle.id);
    const vins = vehiclesForAnalytics?.map((vehicle) => vehicle.vin);
    const [vehicles, activeVehicles, openAlerts, criticalAlerts, analytics] = await Promise.all([
      this.db.vehicle.count({ where: vehicleWhere }),
      this.db.vehicle.count({
        where: { status: 'ACTIVE', ...(tenantId ? { fleet: { tenantId } } : {}) },
      }),
      this.db.alert.count({
        where: { status: 'OPEN', ...(tenantId ? { vehicle: { fleet: { tenantId } } } : {}) },
      }),
      this.db.alert.count({
        where: {
          status: 'OPEN',
          severity: 'CRITICAL',
          ...(tenantId ? { vehicle: { fleet: { tenantId } } } : {}),
        },
      }),
      this.clickhouse
        .fleetAnalytics(7, vehicleIds, vins)
        .catch(() => ({ events: 0, vehicles: 0, averageSpeedKph: null })),
    ]);
    return {
      totalVehicles: vehicles,
      activeVehicles,
      openAlerts,
      criticalOpenAlerts: criticalAlerts,
      telemetry: analytics,
      generatedAt: new Date().toISOString(),
    };
  }

  async vehicleDetails(id: string, tenantId?: string) {
    const vehicle = await this.db.vehicle.findFirst({
      where: { id, ...(tenantId ? { fleet: { tenantId } } : {}) },
      include: {
        fleet: true,
        alerts: { orderBy: { detectedAt: 'desc' }, take: 20 },
        maintenance: { orderBy: { performedAt: 'desc' }, take: 20 },
      },
    });
    if (!vehicle) throw new NotFoundError('Vehicle', id);
    const telemetry = await this.clickhouse.vehicleSummary(id, vehicle.vin);
    return { ...vehicle, telemetrySummary: telemetry };
  }

  async telemetrySummary(id: string, tenantId?: string) {
    const vehicle = await this.requireVehicle(id, tenantId);
    return this.clickhouse.vehicleSummary(id, vehicle.vin);
  }
  async maintenanceRisk(id: string, tenantId?: string) {
    const vehicle = await this.requireVehicle(id, tenantId);
    return this.clickhouse
      .vehicleSummary(id, vehicle.vin)
      .then((summary) => this.ml.maintenanceRisk(id, summary));
  }
  async fleetAnalytics(tenantId?: string) {
    const vehicles = tenantId
      ? await this.db.vehicle.findMany({
          where: { fleet: { tenantId } },
          select: { id: true, vin: true },
        })
      : undefined;
    return this.clickhouse.fleetAnalytics(
      7,
      vehicles?.map((vehicle) => vehicle.id),
      vehicles?.map((vehicle) => vehicle.vin),
    );
  }

  private async requireVehicle(id: string, tenantId?: string) {
    const vehicle = await this.db.vehicle.findFirst({
      where: { id, ...(tenantId ? { fleet: { tenantId } } : {}) },
      select: { id: true, vin: true },
    });
    if (!vehicle) throw new NotFoundError('Vehicle', id);
    return vehicle;
  }
}
