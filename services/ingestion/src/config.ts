import { z } from 'zod';

const configSchema = z.object({
  KAFKA_BROKERS: z.string().min(1).default('localhost:9092'),
  KAFKA_TOPIC: z.string().min(1).default('vehicle.telemetry.v1'),
  KAFKA_CLIENT_ID: z.string().min(1).default('fleetpulse-ingestion'),
  KAFKA_GROUP_ID: z.string().min(1).default('fleetpulse-ingestion'),
  KAFKA_PARTITIONS: z.coerce.number().int().positive().default(6),
  KAFKA_REPLICATION_FACTOR: z.coerce.number().int().positive().default(1),
  KAFKA_RETENTION_MS: z.coerce.number().int().positive().default(604_800_000),
  KAFKA_FROM_BEGINNING: z.coerce.boolean().default(false),
  REDIS_URL: z.string().url().default('redis://localhost:6379'),
  IDEMPOTENCY_TTL_SECONDS: z.coerce.number().int().positive().default(604_800),
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent']).default('info'),
});

export type IngestionConfig = {
  kafkaBrokers: string[];
  kafkaTopic: string;
  kafkaClientId: string;
  kafkaGroupId: string;
  kafkaPartitions: number;
  kafkaReplicationFactor: number;
  kafkaRetentionMs: number;
  kafkaFromBeginning: boolean;
  redisUrl: string;
  idempotencyTtlSeconds: number;
  logLevel: z.infer<typeof configSchema>['LOG_LEVEL'];
};

export function loadIngestionConfig(env: NodeJS.ProcessEnv = process.env): IngestionConfig {
  const parsed = configSchema.parse(env);
  return {
    kafkaBrokers: parsed.KAFKA_BROKERS.split(',').map((broker) => broker.trim()),
    kafkaTopic: parsed.KAFKA_TOPIC,
    kafkaClientId: parsed.KAFKA_CLIENT_ID,
    kafkaGroupId: parsed.KAFKA_GROUP_ID,
    kafkaPartitions: parsed.KAFKA_PARTITIONS,
    kafkaReplicationFactor: parsed.KAFKA_REPLICATION_FACTOR,
    kafkaRetentionMs: parsed.KAFKA_RETENTION_MS,
    kafkaFromBeginning: parsed.KAFKA_FROM_BEGINNING,
    redisUrl: parsed.REDIS_URL,
    idempotencyTtlSeconds: parsed.IDEMPOTENCY_TTL_SECONDS,
    logLevel: parsed.LOG_LEVEL,
  };
}

export function kafkaConfigFromSimulator(config: {
  kafkaBrokers: string[];
  kafkaTopic: string;
  kafkaClientId: string;
  kafkaPartitions: number;
  kafkaReplicationFactor: number;
  kafkaRetentionMs: number;
}) {
  return {
    kafkaBrokers: config.kafkaBrokers,
    kafkaTopic: config.kafkaTopic,
    kafkaClientId: config.kafkaClientId,
    kafkaPartitions: config.kafkaPartitions,
    kafkaReplicationFactor: config.kafkaReplicationFactor,
    kafkaRetentionMs: config.kafkaRetentionMs,
  };
}
