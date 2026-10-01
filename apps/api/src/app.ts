import Fastify, { type FastifyInstance } from 'fastify';
import type { AppConfig } from '@fleetpulse/config';

export function buildApp(config: AppConfig): FastifyInstance {
  const app = Fastify({
    logger: { level: config.LOG_LEVEL },
    requestIdHeader: 'x-request-id',
    genReqId: (request) => request.headers['x-request-id']?.toString() ?? crypto.randomUUID(),
  });

  app.setErrorHandler((error, request, reply) => {
    request.log.error({ err: error }, 'request failed');
    const statusCode =
      typeof error === 'object' &&
      error !== null &&
      'statusCode' in error &&
      typeof error.statusCode === 'number'
        ? error.statusCode
        : 500;
    return reply.status(statusCode).send({
      error: 'Internal Server Error',
      requestId: request.id,
    });
  });

  app.get('/health', async (request) => ({
    status: 'ok',
    service: 'fleetpulse-api',
    requestId: request.id,
    timestamp: new Date().toISOString(),
  }));

  return app;
}
