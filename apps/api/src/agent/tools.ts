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
    getFleetOverview: (tenantId?: string) => product.overview(tenantId),
    getVehicle: (input: VehicleIdInput, tenantId?: string) =>
      domain.getVehicle(input.vehicleId, tenantId),
    getVehicleAlerts: (input: VehicleIdInput, tenantId?: string) =>
      domain.listAlerts(tenantId, input.vehicleId, undefined, undefined, undefined, undefined, 20),
    getOpenAlerts: (tenantId?: string) =>
      domain.listAlerts(tenantId, undefined, 'OPEN', undefined, undefined, undefined, 20),
    getMaintenanceRisk: (input: VehicleIdInput, tenantId?: string) =>
      product.maintenanceRisk(input.vehicleId, tenantId),
    getFleetAnalytics: (tenantId?: string) => product.fleetAnalytics(tenantId),
    getVehicleTelemetrySummary: (input: VehicleIdInput, tenantId?: string) =>
      product.telemetrySummary(input.vehicleId, tenantId),
  };
}
