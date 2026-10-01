import pino from 'pino';
import { loadIngestionConfig } from './config.js';
import { TelemetryIngestionConsumer } from './consumer.js';

const config = loadIngestionConfig();
const logger = pino({ level: config.logLevel, base: { service: 'fleetpulse-ingestion' } });
const consumer = new TelemetryIngestionConsumer(config, logger, async (event) => {
  logger.debug(
    { eventId: event.eventId, vehicleId: event.vehicleId, timestamp: event.timestamp },
    'telemetry processed',
  );
});

const shutdown = async (signal: string) => {
  logger.info({ signal }, 'ingestion shutdown requested');
  await consumer.stop();
  process.exitCode = 0;
};

process.once('SIGINT', () => void shutdown('SIGINT'));
process.once('SIGTERM', () => void shutdown('SIGTERM'));

consumer.start().catch((error) => {
  logger.fatal({ err: error }, 'ingestion startup failed');
  process.exitCode = 1;
});
