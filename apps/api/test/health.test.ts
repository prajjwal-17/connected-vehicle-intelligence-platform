import { describe, expect, it } from 'vitest';
import { loadConfig } from '@fleetpulse/config';
import { buildApp } from '../src/app.js';

const config = loadConfig({
  NODE_ENV: 'test',
  PORT: '3000',
  LOG_LEVEL: 'silent',
  DATABASE_URL: 'postgresql://fleetpulse:fleetpulse@localhost:55432/fleetpulse',
  REDIS_URL: 'redis://localhost:6379',
  KAFKA_BROKERS: 'localhost:9092',
});

describe('GET /health', () => {
  it('returns a structured liveness response', async () => {
    const app = buildApp(config);
    const response = await app.inject({ method: 'GET', url: '/health' });
    const body = response.json();

    expect(response.statusCode).toBe(200);
    expect(body.status).toBe('ok');
    expect(body.service).toBe('fleetpulse-api');
    expect(body.requestId).toBeTruthy();
    await app.close();
  });
});
