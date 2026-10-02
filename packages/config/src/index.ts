import { z } from 'zod';

export const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  PORT: z.coerce.number().int().positive().default(3000),
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent']).default('info'),
  DATABASE_URL: z.string().url(),
  REDIS_URL: z.string().url(),
  KAFKA_BROKERS: z.string().min(1),
  CLICKHOUSE_URL: z.string().url().default('http://localhost:8123'),
  CLICKHOUSE_DB: z.string().min(1).default('fleetpulse'),
  CLICKHOUSE_USER: z.string().min(1).default('fleetpulse'),
  CLICKHOUSE_PASSWORD: z.string().default('fleetpulse'),
  ML_SERVICE_URL: z.string().url().default('http://localhost:8000'),
  AUTH_MODE: z.enum(['disabled', 'header']).default('disabled'),
  CORS_ORIGINS: z.string().default('http://localhost:3001'),
  RATE_LIMIT_PER_MINUTE: z.coerce.number().int().positive().default(120),
});

export type AppConfig = z.infer<typeof envSchema>;

export function loadConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  return envSchema.parse(env);
}
