import { z } from 'zod';

const booleanFromEnv = z.preprocess((value) => {
  if (typeof value === 'string') return value.toLowerCase() === 'true';
  return value;
}, z.boolean());

const envSchema = z.object({
  VEHICLE_COUNT: z.coerce.number().int().min(1).max(1_000_000).default(100),
  SIMULATION_INTERVAL_MS: z.coerce.number().int().positive().default(100),
  EVENTS_PER_SECOND: z.coerce.number().positive().default(100),
  TIME_ACCELERATION: z.coerce.number().positive().default(1),
  BURST_ENABLED: booleanFromEnv.default(false),
  BURST_MULTIPLIER: z.coerce.number().positive().default(3),
  BURST_DURATION_MS: z.coerce.number().int().nonnegative().default(5_000),
  DUPLICATE_EVENT_RATE: z.coerce.number().min(0).max(1).default(0),
  OUT_OF_ORDER_RATE: z.coerce.number().min(0).max(1).default(0),
  MAX_EVENT_DELAY_MS: z.coerce.number().int().nonnegative().default(5_000),
  OUTPUT_MODE: z.enum(['stdout', 'jsonl', 'memory', 'kafka']).default('stdout'),
  OUTPUT_FILE: z.string().min(1).default('./tmp/telemetry.jsonl'),
  REGION: z.enum(['north-india', 'us-west', 'europe']).default('north-india'),
  SIMULATION_DURATION_SECONDS: z.coerce.number().nonnegative().default(10),
  SEED: z.coerce.number().int().default(42),
  MAX_BUFFER_SIZE: z.coerce.number().int().positive().max(100_000).default(1_000),
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent']).default('info'),
  KAFKA_BROKERS: z.string().min(1).default('localhost:9092'),
  KAFKA_TOPIC: z.string().min(1).default('vehicle.telemetry.v1'),
  KAFKA_CLIENT_ID: z.string().min(1).default('fleetpulse-simulator'),
  KAFKA_PARTITIONS: z.coerce.number().int().positive().default(6),
  KAFKA_REPLICATION_FACTOR: z.coerce.number().int().positive().default(1),
  KAFKA_RETENTION_MS: z.coerce.number().int().positive().default(604_800_000),
});

export type SimulatorConfig = {
  vehicleCount: number;
  simulationIntervalMs: number;
  eventsPerSecond: number;
  timeAcceleration: number;
  burstEnabled: boolean;
  burstMultiplier: number;
  burstDurationMs: number;
  duplicateEventRate: number;
  outOfOrderRate: number;
  maxEventDelayMs: number;
  outputMode: 'stdout' | 'jsonl' | 'memory' | 'kafka';
  outputFile: string;
  region: 'north-india' | 'us-west' | 'europe';
  durationSeconds: number;
  seed: number;
  maxBufferSize: number;
  logLevel: SimulatorLogLevel;
  kafkaBrokers: string[];
  kafkaTopic: string;
  kafkaClientId: string;
  kafkaPartitions: number;
  kafkaReplicationFactor: number;
  kafkaRetentionMs: number;
};

export type SimulatorLogLevel = z.infer<typeof envSchema>['LOG_LEVEL'];

export function loadSimulatorConfig(env: NodeJS.ProcessEnv = process.env): SimulatorConfig {
  const parsed = envSchema.parse(env);
  return {
    vehicleCount: parsed.VEHICLE_COUNT,
    simulationIntervalMs: parsed.SIMULATION_INTERVAL_MS,
    eventsPerSecond: parsed.EVENTS_PER_SECOND,
    timeAcceleration: parsed.TIME_ACCELERATION,
    burstEnabled: parsed.BURST_ENABLED,
    burstMultiplier: parsed.BURST_MULTIPLIER,
    burstDurationMs: parsed.BURST_DURATION_MS,
    duplicateEventRate: parsed.DUPLICATE_EVENT_RATE,
    outOfOrderRate: parsed.OUT_OF_ORDER_RATE,
    maxEventDelayMs: parsed.MAX_EVENT_DELAY_MS,
    outputMode: parsed.OUTPUT_MODE,
    outputFile: parsed.OUTPUT_FILE,
    region: parsed.REGION,
    durationSeconds: parsed.SIMULATION_DURATION_SECONDS,
    seed: parsed.SEED,
    maxBufferSize: parsed.MAX_BUFFER_SIZE,
    logLevel: parsed.LOG_LEVEL,
    kafkaBrokers: parsed.KAFKA_BROKERS.split(',').map((broker) => broker.trim()),
    kafkaTopic: parsed.KAFKA_TOPIC,
    kafkaClientId: parsed.KAFKA_CLIENT_ID,
    kafkaPartitions: parsed.KAFKA_PARTITIONS,
    kafkaReplicationFactor: parsed.KAFKA_REPLICATION_FACTOR,
    kafkaRetentionMs: parsed.KAFKA_RETENTION_MS,
  };
}
