import { describe, expect, it } from 'vitest';
import { loadConfig } from '../src/index.js';

const validEnv = {
  NODE_ENV: 'test',
  PORT: '3000',
  LOG_LEVEL: 'info',
  DATABASE_URL: 'postgresql://fleetpulse:fleetpulse@localhost:55432/fleetpulse',
  REDIS_URL: 'redis://localhost:6379',
  KAFKA_BROKERS: 'localhost:9092',
};

describe('configuration', () => {
  it('parses valid environment values', () => {
    expect(loadConfig(validEnv).PORT).toBe(3000);
  });

  it('fails fast when a required value is absent', () => {
    const invalidEnv = { ...validEnv };
    delete invalidEnv.DATABASE_URL;
    expect(() => loadConfig(invalidEnv)).toThrow();
  });
});
