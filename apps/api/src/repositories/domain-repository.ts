import type { DatabaseClient } from '@fleetpulse/database';
import type {
  AlertSeverity,
  AlertStatus,
  Prisma,
  PowertrainType,
  VehicleStatus,
} from '@prisma/client';

export class DomainRepository {
  constructor(private readonly db: DatabaseClient) {}

  listFleets(tenantId: string | undefined, cursor: string | undefined, limit: number) {
    return this.db.fleet.findMany({
      where: { ...(tenantId ? { tenantId } : {}), ...(cursor ? { id: { gt: cursor } } : {}) },
      orderBy: { id: 'asc' },
      take: limit + 1,
    });
  }

  getFleet(id: string, tenantId: string | undefined) {
    return this.db.fleet.findFirst({ where: { id, ...(tenantId ? { tenantId } : {}) } });
  }

  listVehicles(
    tenantId: string | undefined,
    fleetId: string | undefined,
    powertrain: PowertrainType | undefined,
    status: VehicleStatus | undefined,
    oem: string | undefined,
    cursor: string | undefined,
    limit: number,
  ) {
    const where: Prisma.VehicleWhereInput = {
      ...(tenantId ? { fleet: { tenantId } } : {}),
      ...(fleetId ? { fleetId } : {}),
      ...(powertrain ? { powertrainType: powertrain } : {}),
      ...(status ? { status } : {}),
      ...(oem ? { oem } : {}),
      ...(cursor ? { id: { gt: cursor } } : {}),
    };
    return this.db.vehicle.findMany({ where, orderBy: { id: 'asc' }, take: limit + 1 });
  }

  getVehicle(id: string, tenantId: string | undefined) {
    return this.db.vehicle.findFirst({
      where: { id, ...(tenantId ? { fleet: { tenantId } } : {}) },
    });
  }

  listAlerts(
    tenantId: string | undefined,
    vehicleId: string | undefined,
    status: AlertStatus | undefined,
    severity: AlertSeverity | undefined,
    alertType: string | undefined,
    cursor: string | undefined,
    limit: number,
  ) {
    const where: Prisma.AlertWhereInput = {
      ...(tenantId ? { vehicle: { fleet: { tenantId } } } : {}),
      ...(vehicleId ? { vehicleId } : {}),
      ...(status ? { status } : {}),
      ...(severity ? { severity } : {}),
      ...(alertType ? { alertType } : {}),
      ...(cursor ? { id: { gt: cursor } } : {}),
    };
    return this.db.alert.findMany({ where, orderBy: { id: 'asc' }, take: limit + 1 });
  }

  listMaintenance(
    tenantId: string | undefined,
    vehicleId: string | undefined,
    cursor: string | undefined,
    limit: number,
  ) {
    const where: Prisma.MaintenanceRecordWhereInput = {
      ...(tenantId ? { vehicle: { fleet: { tenantId } } } : {}),
      ...(vehicleId ? { vehicleId } : {}),
      ...(cursor ? { id: { gt: cursor } } : {}),
    };
    return this.db.maintenanceRecord.findMany({ where, orderBy: { id: 'asc' }, take: limit + 1 });
  }
}
