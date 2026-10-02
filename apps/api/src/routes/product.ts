import type { FastifyInstance } from 'fastify';
import { idParamsSchema } from '../schemas/domain.js';
import { ProductService } from '../services/product-service.js';
import { AgentService } from '../agent/agent-service.js';
import { tenantIdFromRequest } from '../utils/tenant.js';

export function registerProductRoutes(
  app: FastifyInstance,
  product: ProductService,
  agent: AgentService,
) {
  app.get('/api/v1/dashboard/overview', async (request) => ({
    data: await product.overview(tenantIdFromRequest(request)),
  }));
  app.get('/api/v1/analytics/summary', async (request) => ({
    data: await product.fleetAnalytics(tenantIdFromRequest(request)),
  }));
  app.get('/api/v1/vehicles/:id/telemetry/summary', async (request) => {
    const { id } = idParamsSchema.parse(request.params);
    return { data: await product.telemetrySummary(id, tenantIdFromRequest(request)) };
  });
  app.get('/api/v1/vehicles/:id/maintenance-risk', async (request) => {
    const { id } = idParamsSchema.parse(request.params);
    return { data: await product.maintenanceRisk(id, tenantIdFromRequest(request)) };
  });
  app.post('/api/v1/agent/query', async (request) => {
    const body = request.body as { question?: string };
    if (!body?.question) throw new Error('question is required');
    return { data: await agent.answer(body.question, request.id, tenantIdFromRequest(request)) };
  });
}
