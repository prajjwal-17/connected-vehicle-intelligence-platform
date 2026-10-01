import type { FastifyInstance } from 'fastify';
import { idParamsSchema } from '../schemas/domain.js';
import { ProductService } from '../services/product-service.js';
import { AgentService } from '../agent/agent-service.js';

export function registerProductRoutes(
  app: FastifyInstance,
  product: ProductService,
  agent: AgentService,
) {
  app.get('/api/v1/dashboard/overview', async () => ({ data: await product.overview() }));
  app.get('/api/v1/analytics/summary', async () => ({ data: await product.fleetAnalytics() }));
  app.get('/api/v1/vehicles/:id/telemetry/summary', async (request) => {
    const { id } = idParamsSchema.parse(request.params);
    return { data: await product.telemetrySummary(id) };
  });
  app.get('/api/v1/vehicles/:id/maintenance-risk', async (request) => {
    const { id } = idParamsSchema.parse(request.params);
    return { data: await product.maintenanceRisk(id) };
  });
  app.post('/api/v1/agent/query', async (request) => {
    const body = request.body as { question?: string };
    if (!body?.question) throw new Error('question is required');
    return { data: await agent.answer(body.question, request.id) };
  });
}
