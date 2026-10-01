import Fastify, { type FastifyInstance } from 'fastify';
import type { AppConfig } from '@fleetpulse/config';
import type { DatabaseClient } from '@fleetpulse/database';
import { ZodError } from 'zod';
import { prisma } from '@fleetpulse/database';
import { registerDomainRoutes } from './routes/domain.js';
import { DomainRepository } from './repositories/domain-repository.js';
import { DomainService } from './services/domain-service.js';
import { ClickHouseClient } from './services/clickhouse-client.js';
import { MlClient } from './services/ml-client.js';
import { ProductService } from './services/product-service.js';
import { AgentService } from './agent/agent-service.js';
import { registerProductRoutes } from './routes/product.js';

export function buildApp(config: AppConfig, database: DatabaseClient = prisma): FastifyInstance {
  const app = Fastify({
    logger: { level: config.LOG_LEVEL },
    requestIdHeader: 'x-request-id',
    genReqId: (request) => request.headers['x-request-id']?.toString() ?? crypto.randomUUID(),
  });

  app.setErrorHandler((error, request, reply) => {
    request.log.error({ err: error }, 'request failed');
    if (error instanceof ZodError) {
      return reply.status(400).send({
        error: {
          code: 'VALIDATION_ERROR',
          message: 'Request validation failed',
          details: error.flatten(),
          requestId: request.id,
        },
      });
    }
    const statusCode =
      typeof error === 'object' &&
      error !== null &&
      'statusCode' in error &&
      typeof error.statusCode === 'number'
        ? error.statusCode
        : 500;
    const message =
      statusCode === 404 && error instanceof Error ? error.message : 'Internal Server Error';
    return reply.status(statusCode).send({
      error: {
        code: statusCode === 404 ? 'NOT_FOUND' : 'INTERNAL_SERVER_ERROR',
        message,
        requestId: request.id,
      },
    });
  });

  app.get('/health', async (request) => ({
    status: 'ok',
    service: 'fleetpulse-api',
    requestId: request.id,
    timestamp: new Date().toISOString(),
  }));

  const service = new DomainService(new DomainRepository(database));
  registerDomainRoutes(app, service);
  const product = new ProductService(
    database,
    new ClickHouseClient({
      url: config.CLICKHOUSE_URL,
      database: config.CLICKHOUSE_DB,
      user: config.CLICKHOUSE_USER,
      password: config.CLICKHOUSE_PASSWORD,
    }),
    new MlClient(config.ML_SERVICE_URL),
  );
  registerProductRoutes(app, product, new AgentService(service, product));

  return app;
}
