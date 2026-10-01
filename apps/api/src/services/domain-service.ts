import type { AlertStatus } from '@prisma/client';
import { DomainRepository } from '../repositories/domain-repository.js';
import { NotFoundError } from '../utils/errors.js';
import { pageResult } from '../utils/pagination.js';

export class DomainService {
  constructor(private readonly repository: DomainRepository) {}

  async listFleets(cursor: string | undefined, limit: number) {
    const fleets = await this.repository.listFleets(cursor, limit);
    return pageResult(fleets, limit, (fleet) => fleet.id);
  }

  async getFleet(id: string) {
    const fleet = await this.repository.getFleet(id);
    if (!fleet) throw new NotFoundError('Fleet', id);
    return fleet;
  }

  async listVehicles(fleetId: string | undefined, cursor: string | undefined, limit: number) {
    const vehicles = await this.repository.listVehicles(fleetId, cursor, limit);
    return pageResult(vehicles, limit, (vehicle) => vehicle.id);
  }

  async getVehicle(id: string) {
    const vehicle = await this.repository.getVehicle(id);
    if (!vehicle) throw new NotFoundError('Vehicle', id);
    return vehicle;
  }

  async listAlerts(
    vehicleId: string | undefined,
    status: AlertStatus | undefined,
    cursor: string | undefined,
    limit: number,
  ) {
    const alerts = await this.repository.listAlerts(vehicleId, status, cursor, limit);
    return pageResult(alerts, limit, (alert) => alert.id);
  }

  async listMaintenance(vehicleId: string | undefined, cursor: string | undefined, limit: number) {
    const records = await this.repository.listMaintenance(vehicleId, cursor, limit);
    return pageResult(records, limit, (record) => record.id);
  }
}
