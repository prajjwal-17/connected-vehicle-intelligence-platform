import type { FastifyInstance } from 'fastify';
import { DomainService } from '../services/domain-service.js';
import {
  alertListQuerySchema,
  fleetListQuerySchema,
  idParamsSchema,
  maintenanceListQuerySchema,
  vehicleListQuerySchema,
} from '../schemas/domain.js';

export function registerDomainRoutes(app: FastifyInstance, service: DomainService) {
  app.get('/api/v1/fleets', async (request) => {
    const query = fleetListQuerySchema.parse(request.query);
    return service.listFleets(query.cursor, query.limit);
  });

  app.get('/api/v1/fleets/:id', async (request) => {
    const { id } = idParamsSchema.parse(request.params);
    return { data: await service.getFleet(id) };
  });

  app.get('/api/v1/vehicles', async (request) => {
    const query = vehicleListQuerySchema.parse(request.query);
    return service.listVehicles(
      query.fleetId,
      query.powertrain,
      query.status,
      query.oem,
      query.cursor,
      query.limit,
    );
  });

  app.get('/api/v1/vehicles/:id', async (request) => {
    const { id } = idParamsSchema.parse(request.params);
    return { data: await service.getVehicle(id) };
  });

  app.get('/api/v1/alerts', async (request) => {
    const query = alertListQuerySchema.parse(request.query);
    return service.listAlerts(
      query.vehicleId,
      query.status,
      query.severity,
      query.alertType,
      query.cursor,
      query.limit,
    );
  });

  app.get('/api/v1/maintenance', async (request) => {
    const query = maintenanceListQuerySchema.parse(request.query);
    return service.listMaintenance(query.vehicleId, query.cursor, query.limit);
  });
}
