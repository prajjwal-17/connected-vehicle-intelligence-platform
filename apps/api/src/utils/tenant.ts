import type { FastifyRequest } from 'fastify';
import { z } from 'zod';

export function tenantIdFromRequest(request: FastifyRequest): string | undefined {
  const value = request.headers['x-tenant-id'];
  return value ? z.string().uuid().parse(String(value)) : undefined;
}
