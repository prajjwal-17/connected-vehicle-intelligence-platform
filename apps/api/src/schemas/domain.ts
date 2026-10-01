import { z } from 'zod';
import { DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE } from '../utils/pagination.js';

const pagination = {
  cursor: z.string().uuid().optional(),
  limit: z.coerce.number().int().min(1).max(MAX_PAGE_SIZE).default(DEFAULT_PAGE_SIZE),
};

export const idParamsSchema = z.object({ id: z.string().uuid() });
export const fleetListQuerySchema = z.object(pagination);
export const vehicleListQuerySchema = z.object({
  ...pagination,
  fleetId: z.string().uuid().optional(),
});
export const alertListQuerySchema = z.object({
  ...pagination,
  vehicleId: z.string().uuid().optional(),
  status: z.enum(['OPEN', 'ACKNOWLEDGED', 'RESOLVED']).optional(),
});
export const maintenanceListQuerySchema = z.object({
  ...pagination,
  vehicleId: z.string().uuid().optional(),
});

export type FleetListQuery = z.infer<typeof fleetListQuerySchema>;
export type VehicleListQuery = z.infer<typeof vehicleListQuerySchema>;
export type AlertListQuery = z.infer<typeof alertListQuerySchema>;
export type MaintenanceListQuery = z.infer<typeof maintenanceListQuerySchema>;
