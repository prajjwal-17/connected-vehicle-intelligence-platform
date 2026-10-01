import { z } from 'zod';

const schema = z.object({
  KAFKA_BROKERS: z.string().min(1).default('localhost:9092'),
  KAFKA_TOPIC: z.string().min(1).default('vehicle.telemetry.v1'),
  HISTORICAL_ANALYTICS_GROUP: z.string().min(1).default('fleetpulse-historical-analytics'),
  KAFKA_FROM_BEGINNING: z.coerce.boolean().default(false),
  CLICKHOUSE_URL: z.string().url().default('http://localhost:8123'),
  CLICKHOUSE_DB: z.string().min(1).default('fleetpulse'),
  CLICKHOUSE_USER: z.string().default('fleetpulse'),
  CLICKHOUSE_PASSWORD: z.string().default('fleetpulse'),
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent']).default('info'),
});
export type HistoricalConfig = {
  brokers: string[];
  topic: string;
  group: string;
  fromBeginning: boolean;
  url: string;
  database: string;
  user: string;
  password: string;
  logLevel: z.infer<typeof schema>['LOG_LEVEL'];
};
export function loadHistoricalConfig(env: NodeJS.ProcessEnv = process.env): HistoricalConfig {
  const p = schema.parse(env);
  return {
    brokers: p.KAFKA_BROKERS.split(',').map((v) => v.trim()),
    topic: p.KAFKA_TOPIC,
    group: p.HISTORICAL_ANALYTICS_GROUP,
    fromBeginning: p.KAFKA_FROM_BEGINNING,
    url: p.CLICKHOUSE_URL,
    database: p.CLICKHOUSE_DB,
    user: p.CLICKHOUSE_USER,
    password: p.CLICKHOUSE_PASSWORD,
    logLevel: p.LOG_LEVEL,
  };
}
