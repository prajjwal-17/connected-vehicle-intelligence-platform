import { ProductService } from '../services/product-service.js';
import { DomainService } from '../services/domain-service.js';

export const toolNames = [
  'getFleetOverview',
  'getVehicle',
  'getVehicleAlerts',
  'getOpenAlerts',
  'getMaintenanceRisk',
  'getFleetAnalytics',
  'getVehicleTelemetrySummary',
] as const;
export type ToolName = (typeof toolNames)[number];
type VehicleIdInput = { vehicleId: string };

export function createTools(domain: DomainService, product: ProductService) {
  return {
    getFleetOverview: () => product.overview(),
    getVehicle: (input: VehicleIdInput) => domain.getVehicle(input.vehicleId),
    getVehicleAlerts: (input: VehicleIdInput) =>
      domain.listAlerts(input.vehicleId, undefined, undefined, undefined, undefined, 20),
    getOpenAlerts: () => domain.listAlerts(undefined, 'OPEN', undefined, undefined, undefined, 20),
    getMaintenanceRisk: (input: VehicleIdInput) => product.maintenanceRisk(input.vehicleId),
    getFleetAnalytics: () => product.fleetAnalytics(),
    getVehicleTelemetrySummary: (input: VehicleIdInput) =>
      product.telemetrySummary(input.vehicleId),
  };
}
