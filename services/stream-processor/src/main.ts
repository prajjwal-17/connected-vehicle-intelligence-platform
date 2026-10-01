import pino from 'pino';
import { PrismaClient } from '@prisma/client';
import { loadStreamProcessorConfig } from './config.js';
import { AlertManager } from './alerts.js';
import { StreamEventProcessor } from './processor.js';
import { StreamProcessorConsumer } from './consumer.js';

const config = loadStreamProcessorConfig();
const logger = pino({ level: config.logLevel, base: { service: 'fleetpulse-stream-processor' } });
const db = new PrismaClient({ datasources: { db: { url: config.databaseUrl } } });
const alerts = new AlertManager(db, config.redisUrl, config.alertCooldownSeconds, logger);
const processor = new StreamEventProcessor(config, alerts, logger);
const consumer = new StreamProcessorConsumer(config, processor, logger);

const shutdown = async (signal: string) => {
  logger.info({ signal }, 'stream processor shutdown requested');
  await consumer.stop();
  await alerts.disconnect();
  await db.$disconnect();
};
process.once('SIGINT', () => void shutdown('SIGINT'));
process.once('SIGTERM', () => void shutdown('SIGTERM'));
consumer.start().catch(async (error) => {
  logger.fatal({ err: error }, 'stream processor startup failed');
  await db.$disconnect();
  process.exitCode = 1;
});
