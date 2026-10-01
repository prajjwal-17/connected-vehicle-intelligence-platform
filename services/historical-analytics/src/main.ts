import pino from 'pino';
import { loadHistoricalConfig } from './config.js';
import { ClickHouseStore } from './store.js';
import { HistoricalConsumer } from './consumer.js';
const config = loadHistoricalConfig();
const logger = pino({
  level: config.logLevel,
  base: { service: 'fleetpulse-historical-analytics' },
});
const store = new ClickHouseStore(config);
new HistoricalConsumer(config, store, logger).start().catch((error) => {
  logger.fatal({ err: error }, 'historical consumer stopped');
  process.exitCode = 1;
});
