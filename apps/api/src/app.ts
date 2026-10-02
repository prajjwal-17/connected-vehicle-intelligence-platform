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
  const requestCounts = new Map<string, { window: number; count: number }>();
  const metrics = { requests: 0, errors: 0, totalDurationMs: 0 };
  const app = Fastify({
    logger: { level: config.LOG_LEVEL },
    bodyLimit: 1024 * 1024,
    requestIdHeader: 'x-request-id',
    genReqId: (request) => request.headers['x-request-id']?.toString() ?? crypto.randomUUID(),
  });

  app.addHook('onRequest', async (request, reply) => {
    const origin = request.headers.origin;
    const allowedOrigins = config.CORS_ORIGINS.split(',').map((value) => value.trim());
    if (origin && !allowedOrigins.includes(origin))
      return reply.status(403).send({
        error: { code: 'CORS_REJECTED', message: 'Origin is not allowed', requestId: request.id },
      });
    const key = request.ip;
    const now = Date.now();
    const current = requestCounts.get(key);
    if (!current || now - current.window >= 60_000)
      requestCounts.set(key, { window: now, count: 1 });
    else if (++current.count > config.RATE_LIMIT_PER_MINUTE)
      return reply.status(429).send({
        error: { code: 'RATE_LIMITED', message: 'Too many requests', requestId: request.id },
      });
    if (config.AUTH_MODE === 'header' && request.url.startsWith('/api/')) {
      const role = request.headers['x-user-role'];
      if (
        !request.headers['x-tenant-id'] ||
        !request.headers['x-user-id'] ||
        !['ADMIN', 'FLEET_MANAGER', 'VIEWER'].includes(String(role))
      )
        return reply.status(401).send({
          error: {
            code: 'UNAUTHENTICATED',
            message: 'Authentication context required',
            requestId: request.id,
          },
        });
    }
    metrics.requests++;
    (request as typeof request & { startedAt?: number }).startedAt = performance.now();
  });

  app.addHook('onResponse', async (request, reply) => {
    const startedAt = (request as typeof request & { startedAt?: number }).startedAt;
    if (startedAt !== undefined) metrics.totalDurationMs += performance.now() - startedAt;
    if (reply.statusCode >= 400) metrics.errors++;
  });

  app.addHook('onSend', async (request, reply) => {
    reply.header('x-content-type-options', 'nosniff');
    reply.header('x-frame-options', 'DENY');
    reply.header('referrer-policy', 'no-referrer');
    reply.header('permissions-policy', 'geolocation=(), microphone=(), camera=()');
    const origin = request.headers.origin;
    if (
      origin &&
      config.CORS_ORIGINS.split(',')
        .map((value) => value.trim())
        .includes(origin)
    )
      reply.header('access-control-allow-origin', origin);
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

  app.get('/ready', async (request, reply) => {
    try {
      await database.$queryRaw`SELECT 1`;
      return {
        status: 'ready',
        service: 'fleetpulse-api',
        requestId: request.id,
        timestamp: new Date().toISOString(),
      };
    } catch {
      return reply.status(503).send({
        error: { code: 'NOT_READY', message: 'Database is unavailable', requestId: request.id },
      });
    }
  });

  app.get(
    '/metrics',
    async () =>
      `# HELP fleetpulse_http_requests_total HTTP requests\n# TYPE fleetpulse_http_requests_total counter\nfleetpulse_http_requests_total ${metrics.requests}\n# HELP fleetpulse_http_errors_total HTTP responses with status >= 400\n# TYPE fleetpulse_http_errors_total counter\nfleetpulse_http_errors_total ${metrics.errors}\n# HELP fleetpulse_http_duration_ms_total Total request duration in milliseconds\n# TYPE fleetpulse_http_duration_ms_total counter\nfleetpulse_http_duration_ms_total ${metrics.totalDurationMs.toFixed(2)}\n`,
  );

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
