import type { AlertSeverity, AlertStatus, PowertrainType, VehicleStatus } from '@prisma/client';
import { DomainRepository } from '../repositories/domain-repository.js';
import { NotFoundError } from '../utils/errors.js';
import { pageResult } from '../utils/pagination.js';

export class DomainService {
  constructor(private readonly repository: DomainRepository) {}

  async listFleets(tenantId: string | undefined, cursor: string | undefined, limit: number) {
    const fleets = await this.repository.listFleets(tenantId, cursor, limit);
    return pageResult(fleets, limit, (fleet) => fleet.id);
  }

  async getFleet(id: string, tenantId: string | undefined) {
    const fleet = await this.repository.getFleet(id, tenantId);
    if (!fleet) throw new NotFoundError('Fleet', id);
    return fleet;
  }

  async listVehicles(
    tenantId: string | undefined,
    fleetId: string | undefined,
    powertrain: PowertrainType | undefined,
    status: VehicleStatus | undefined,
    oem: string | undefined,
    cursor: string | undefined,
    limit: number,
  ) {
    const vehicles = await this.repository.listVehicles(
      tenantId,
      fleetId,
      powertrain,
      status,
      oem,
      cursor,
      limit,
    );
    return pageResult(vehicles, limit, (vehicle) => vehicle.id);
  }

  async getVehicle(id: string, tenantId: string | undefined) {
    const vehicle = await this.repository.getVehicle(id, tenantId);
    if (!vehicle) throw new NotFoundError('Vehicle', id);
    return vehicle;
  }

  async listAlerts(
    tenantId: string | undefined,
    vehicleId: string | undefined,
    status: AlertStatus | undefined,
    severity: AlertSeverity | undefined,
    alertType: string | undefined,
    cursor: string | undefined,
    limit: number,
  ) {
    const alerts = await this.repository.listAlerts(
      tenantId,
      vehicleId,
      status,
      severity,
      alertType,
      cursor,
      limit,
    );
    return pageResult(alerts, limit, (alert) => alert.id);
  }

  async listMaintenance(
    tenantId: string | undefined,
    vehicleId: string | undefined,
    cursor: string | undefined,
    limit: number,
  ) {
    const records = await this.repository.listMaintenance(tenantId, vehicleId, cursor, limit);
    return pageResult(records, limit, (record) => record.id);
  }
}
