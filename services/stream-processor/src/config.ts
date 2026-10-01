import { z } from 'zod';

const schema = z.object({
  KAFKA_BROKERS: z.string().min(1).default('localhost:9092'),
  KAFKA_TOPIC: z.string().min(1).default('vehicle.telemetry.v1'),
  STREAM_PROCESSOR_GROUP: z.string().min(1).default('fleetpulse-stream-processor'),
  KAFKA_FROM_BEGINNING: z.coerce.boolean().default(false),
  REDIS_URL: z.string().url().default('redis://localhost:6379'),
  DATABASE_URL: z
    .string()
    .url()
    .default('postgresql://fleetpulse:fleetpulse@localhost:55432/fleetpulse'),
  MAX_OUT_OF_ORDER_MS: z.coerce.number().int().nonnegative().default(30_000),
  ENGINE_TEMP_THRESHOLD_C: z.coerce.number().default(110),
  ENGINE_TEMP_WINDOW_SECONDS: z.coerce.number().positive().default(30),
  RAPID_TEMP_RISE_THRESHOLD: z.coerce.number().positive().default(0.5),
  HARSH_BRAKING_THRESHOLD: z.coerce.number().positive().default(25),
  IDLE_THRESHOLD_SECONDS: z.coerce.number().positive().default(300),
  LOW_BATTERY_THRESHOLD_PERCENT: z.coerce.number().min(0).max(100).default(20),
  LOW_BATTERY_SOH_THRESHOLD_PERCENT: z.coerce.number().min(0).max(100).default(70),
  ALERT_COOLDOWN_SECONDS: z.coerce.number().positive().default(300),
  STATE_TTL_SECONDS: z.coerce.number().positive().default(900),
  ALERT_RESOLUTION_NORMAL_EVENTS: z.coerce.number().int().positive().default(3),
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent']).default('info'),
});

export type StreamProcessorConfig = {
  kafkaBrokers: string[];
  kafkaTopic: string;
  groupId: string;
  fromBeginning: boolean;
  redisUrl: string;
  databaseUrl: string;
  maxOutOfOrderMs: number;
  engineTempThresholdC: number;
  engineTempWindowSeconds: number;
  rapidTempRiseThreshold: number;
  harshBrakingThreshold: number;
  idleThresholdSeconds: number;
  lowBatteryThresholdPercent: number;
  lowBatterySohThresholdPercent: number;
  alertCooldownSeconds: number;
  stateTtlSeconds: number;
  normalEventsForResolution: number;
  logLevel: z.infer<typeof schema>['LOG_LEVEL'];
};

export function loadStreamProcessorConfig(
  env: NodeJS.ProcessEnv = process.env,
): StreamProcessorConfig {
  const p = schema.parse(env);
  return {
    kafkaBrokers: p.KAFKA_BROKERS.split(',').map((v) => v.trim()),
    kafkaTopic: p.KAFKA_TOPIC,
    groupId: p.STREAM_PROCESSOR_GROUP,
    fromBeginning: p.KAFKA_FROM_BEGINNING,
    redisUrl: p.REDIS_URL,
    databaseUrl: p.DATABASE_URL,
    maxOutOfOrderMs: p.MAX_OUT_OF_ORDER_MS,
    engineTempThresholdC: p.ENGINE_TEMP_THRESHOLD_C,
    engineTempWindowSeconds: p.ENGINE_TEMP_WINDOW_SECONDS,
    rapidTempRiseThreshold: p.RAPID_TEMP_RISE_THRESHOLD,
    harshBrakingThreshold: p.HARSH_BRAKING_THRESHOLD,
    idleThresholdSeconds: p.IDLE_THRESHOLD_SECONDS,
    lowBatteryThresholdPercent: p.LOW_BATTERY_THRESHOLD_PERCENT,
    lowBatterySohThresholdPercent: p.LOW_BATTERY_SOH_THRESHOLD_PERCENT,
    alertCooldownSeconds: p.ALERT_COOLDOWN_SECONDS,
    stateTtlSeconds: p.STATE_TTL_SECONDS,
    normalEventsForResolution: p.ALERT_RESOLUTION_NORMAL_EVENTS,
    logLevel: p.LOG_LEVEL,
  };
}
