import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import pino, { type Logger } from 'pino';
import { KafkaEventSink } from '@fleetpulse/ingestion';
import { loadSimulatorConfig, type SimulatorConfig } from './config/index.js';
import { SimulationEngine } from './simulation/engine.js';
import {
  BoundedEventSink,
  JsonlEventSink,
  MemoryEventSink,
  OutOfOrderSink,
  StdoutEventSink,
  type EventSink,
} from './output/sinks.js';

type CliOptions = {
  vehicles?: number;
  duration?: number;
  rate?: number;
  output?: string;
  outputMode?: 'stdout' | 'jsonl' | 'memory' | 'kafka';
  burst?: boolean;
  duplicates?: number;
  outOfOrder?: number;
  seed?: number;
};

function parseCli(argv: string[]): CliOptions {
  const options: CliOptions = {};
  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    const value = argv[index + 1];
    if (argument === '--vehicles') options.vehicles = Number(value);
    if (argument === '--duration') options.duration = Number(value);
    if (argument === '--rate') options.rate = Number(value);
    if (argument === '--output') options.output = value;
    if (argument === '--output-mode') options.outputMode = value as CliOptions['outputMode'];
    if (argument === '--duplicates') options.duplicates = Number(value);
    if (argument === '--out-of-order') options.outOfOrder = Number(value);
    if (argument === '--seed') options.seed = Number(value);
    if (argument === '--burst') options.burst = true;
  }
  return options;
}

function applyCli(config: SimulatorConfig, options: CliOptions): SimulatorConfig {
  return {
    ...config,
    vehicleCount: options.vehicles ?? config.vehicleCount,
    durationSeconds: options.duration ?? config.durationSeconds,
    eventsPerSecond: options.rate ?? config.eventsPerSecond,
    burstEnabled: options.burst ?? config.burstEnabled,
    duplicateEventRate: options.duplicates ?? config.duplicateEventRate,
    outOfOrderRate: options.outOfOrder ?? config.outOfOrderRate,
    seed: options.seed ?? config.seed,
    outputMode: options.output ? 'jsonl' : (options.outputMode ?? config.outputMode),
    outputFile: options.output ?? config.outputFile,
  };
}

async function createSink(
  config: SimulatorConfig,
  logger: Logger<never, boolean>,
): Promise<EventSink> {
  const baseSink: EventSink =
    config.outputMode === 'jsonl'
      ? await JsonlEventSink.create(config.outputFile)
      : config.outputMode === 'memory'
        ? new MemoryEventSink()
        : config.outputMode === 'kafka'
          ? new KafkaEventSink(config, logger)
          : new StdoutEventSink();
  const reorderedSink =
    config.outOfOrderRate > 0
      ? new OutOfOrderSink(baseSink, config.outOfOrderRate, config.maxEventDelayMs, Math.random)
      : baseSink;
  return new BoundedEventSink(reorderedSink, config.maxBufferSize, () =>
    logger.debug('simulator sink backpressure applied'),
  );
}

async function main() {
  const config = applyCli(loadSimulatorConfig(), parseCli(process.argv.slice(2)));
  const logger = pino({ level: config.logLevel, base: { service: 'telemetry-simulator' } });
  logger.info(
    {
      vehicleCount: config.vehicleCount,
      eventsPerSecond: config.eventsPerSecond,
      durationSeconds: config.durationSeconds,
      burstEnabled: config.burstEnabled,
      duplicateEventRate: config.duplicateEventRate,
      outOfOrderRate: config.outOfOrderRate,
      outputMode: config.outputMode,
    },
    'simulator starting',
  );

  const sink = await createSink(config, logger);
  const engine = new SimulationEngine(config, sink, logger);
  await engine.run();
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}

export { applyCli, createSink, parseCli };
