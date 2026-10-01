import type { DatabaseClient } from '@fleetpulse/database';
import type { AlertStatus, Prisma } from '@prisma/client';

export class DomainRepository {
  constructor(private readonly db: DatabaseClient) {}

  listFleets(cursor: string | undefined, limit: number) {
    return this.db.fleet.findMany({
      where: cursor ? { id: { gt: cursor } } : undefined,
      orderBy: { id: 'asc' },
      take: limit + 1,
    });
  }

  getFleet(id: string) {
    return this.db.fleet.findUnique({ where: { id } });
  }

  listVehicles(fleetId: string | undefined, cursor: string | undefined, limit: number) {
    const where: Prisma.VehicleWhereInput = {
      ...(fleetId ? { fleetId } : {}),
      ...(cursor ? { id: { gt: cursor } } : {}),
    };
    return this.db.vehicle.findMany({ where, orderBy: { id: 'asc' }, take: limit + 1 });
  }

  getVehicle(id: string) {
    return this.db.vehicle.findUnique({ where: { id } });
  }

  listAlerts(
    vehicleId: string | undefined,
    status: AlertStatus | undefined,
    cursor: string | undefined,
    limit: number,
  ) {
    const where: Prisma.AlertWhereInput = {
      ...(vehicleId ? { vehicleId } : {}),
      ...(status ? { status } : {}),
      ...(cursor ? { id: { gt: cursor } } : {}),
    };
    return this.db.alert.findMany({ where, orderBy: { id: 'asc' }, take: limit + 1 });
  }

  listMaintenance(vehicleId: string | undefined, cursor: string | undefined, limit: number) {
    const where: Prisma.MaintenanceRecordWhereInput = {
      ...(vehicleId ? { vehicleId } : {}),
      ...(cursor ? { id: { gt: cursor } } : {}),
    };
    return this.db.maintenanceRecord.findMany({ where, orderBy: { id: 'asc' }, take: limit + 1 });
  }
}
