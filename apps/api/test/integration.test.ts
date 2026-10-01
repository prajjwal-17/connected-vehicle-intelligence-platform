import { beforeAll, afterAll, describe, expect, it } from 'vitest';
import { loadConfig } from '@fleetpulse/config';
import { prisma } from '@fleetpulse/database';
import { buildApp } from '../src/app.js';

const runIntegration = process.env.RUN_DB_INTEGRATION === 'true';

describe.skipIf(!runIntegration)('PostgreSQL-backed API', () => {
  const config = loadConfig({
    ...process.env,
    NODE_ENV: 'test',
    LOG_LEVEL: 'silent',
    DATABASE_URL:
      process.env.DATABASE_URL ?? 'postgresql://fleetpulse:fleetpulse@127.0.0.1:55432/fleetpulse',
    REDIS_URL: process.env.REDIS_URL ?? 'redis://127.0.0.1:6379',
    KAFKA_BROKERS: process.env.KAFKA_BROKERS ?? '127.0.0.1:9092',
  });
  const app = buildApp(config, prisma);

  beforeAll(async () => {
    await prisma.$connect();
    await app.ready();
  });

  afterAll(async () => {
    await app.close();
    await prisma.$disconnect();
  });

  it('reads seeded fleets and vehicles from PostgreSQL', async () => {
    const fleets = await app.inject({ method: 'GET', url: '/api/v1/fleets?limit=5' });
    const vehicles = await app.inject({ method: 'GET', url: '/api/v1/vehicles?limit=5' });

    expect(fleets.statusCode).toBe(200);
    expect(fleets.json().data.length).toBeGreaterThan(0);
    expect(vehicles.statusCode).toBe(200);
    expect(vehicles.json().data.length).toBeGreaterThan(0);
  });

  it('reads alerts and maintenance records', async () => {
    const alerts = await app.inject({ method: 'GET', url: '/api/v1/alerts?status=OPEN' });
    const maintenance = await app.inject({ method: 'GET', url: '/api/v1/maintenance' });

    expect(alerts.statusCode).toBe(200);
    expect(alerts.json().data.length).toBeGreaterThan(0);
    expect(maintenance.statusCode).toBe(200);
    expect(maintenance.json().data.length).toBeGreaterThan(0);
  });
});
