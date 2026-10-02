import type { Logger } from 'pino';
import { telemetryEventSchema, type TelemetryEvent } from '@fleetpulse/schemas';
import type { SimulatorConfig } from '../config/index.js';
import {
  advanceVehicle,
  generateTelemetryEvent,
  type TelemetryGenerationTimings,
} from '../generators/telemetry.js';
import { SeededRandom } from '../models/random.js';
import { createVehiclePopulation, type VirtualVehicle } from '../models/vehicle.js';
import type { EventSink } from '../output/sinks.js';

export type SimulationMetrics = {
  vehiclesInitialized: number;
  eventsGenerated: number;
  duplicatesGenerated: number;
  outOfOrderEvents: number;
  validationFailures: number;
  backpressureEvents: number;
  burstEvents: number;
  elapsedMs: number;
  achievedEventsPerSecond: number;
  cpuUserMs?: number;
  cpuSystemMs?: number;
  rssMb?: number;
  profiling?: {
    eventGenerationMs: number;
    schemaValidationMs: number;
    serializationMs: number;
    kafkaSendWaitMs: number;
    batchesSent: number;
    successfulSends: number;
    failedSends: number;
    averageBatchSendLatencyMs: number;
    p95BatchSendLatencyMs: number;
    inFlightPeak: number;
    queueDepthPeak: number;
  };
};

export type SimulationRunOptions = {
  durationSeconds?: number;
  maxEvents?: number;
};

export type SimulationResult = {
  metrics: SimulationMetrics;
  vehicles: VirtualVehicle[];
};

export class SimulationEngine {
  readonly vehicles: VirtualVehicle[];
  private readonly random: SeededRandom;

  constructor(
    private readonly config: SimulatorConfig,
    private readonly sink: EventSink,
    private readonly logger: Logger<never, boolean>,
  ) {
    this.random = new SeededRandom(config.seed + 101);
    this.vehicles = createVehiclePopulation(
      config.vehicleCount,
      config.seed,
      config.region,
      config.vehicleIndexOffset,
    );
    logger.info(
      { vehicleCount: this.vehicles.length, region: config.region },
      'simulator population initialized',
    );
  }

  async run(options: SimulationRunOptions = {}): Promise<SimulationResult> {
    const durationSeconds = options.durationSeconds ?? this.config.durationSeconds;
    const baseRate = this.config.eventsPerSecond;
    const burstSeconds = this.config.burstEnabled
      ? Math.min(durationSeconds, this.config.burstDurationMs / 1_000)
      : 0;
    const totalEvents =
      options.maxEvents ??
      Math.floor(baseRate * (durationSeconds + burstSeconds * (this.config.burstMultiplier - 1)));
    const metrics: SimulationMetrics = {
      vehiclesInitialized: this.vehicles.length,
      eventsGenerated: 0,
      duplicatesGenerated: 0,
      outOfOrderEvents: 0,
      validationFailures: 0,
      backpressureEvents: 0,
      burstEvents: 0,
      elapsedMs: 0,
      achievedEventsPerSecond: 0,
      profiling: {
        eventGenerationMs: 0,
        schemaValidationMs: 0,
        serializationMs: 0,
        kafkaSendWaitMs: 0,
        batchesSent: 0,
        successfulSends: 0,
        failedSends: 0,
        averageBatchSendLatencyMs: 0,
        p95BatchSendLatencyMs: 0,
        inFlightPeak: 0,
        queueDepthPeak: 0,
      },
    };
    const generationTimings: TelemetryGenerationTimings = {
      generationMs: 0,
      schemaValidationMs: 0,
    };
    const startedAt = performance.now();
    const cpuStarted = process.cpuUsage();
    let simulatedNow = Date.now();

    for (let index = 0; index < totalEvents; index += 1) {
      const inBurst =
        this.config.burstEnabled &&
        index < Math.floor(baseRate * burstSeconds * this.config.burstMultiplier);
      const effectiveRate = inBurst ? baseRate * this.config.burstMultiplier : baseRate;
      const vehicle = this.vehicles[index % this.vehicles.length];
      const deltaSeconds = Math.max(0.1, this.vehicles.length / effectiveRate);
      simulatedNow += deltaSeconds * 1_000 * this.config.timeAcceleration;
      advanceVehicle(vehicle, this.random, deltaSeconds * this.config.timeAcceleration);

      let event: TelemetryEvent;
      try {
        event = generateTelemetryEvent(vehicle, new Date(simulatedNow), generationTimings);
        const validationStarted = performance.now();
        telemetryEventSchema.parse(event);
        generationTimings.schemaValidationMs += performance.now() - validationStarted;
      } catch (error) {
        metrics.validationFailures += 1;
        this.logger.error(
          { err: error, vehicleId: vehicle.vehicleId },
          'generated telemetry failed schema validation',
        );
        continue;
      }

      await this.sink.write(event);
      metrics.eventsGenerated += 1;
      if (inBurst) metrics.burstEvents += 1;
      if (this.random.next() < this.config.duplicateEventRate) {
        await this.sink.write(event);
        metrics.duplicatesGenerated += 1;
      }
      if (this.random.next() < this.config.outOfOrderRate) metrics.outOfOrderEvents += 1;
      if (index % 1_000 === 0 && index > 0)
        await new Promise<void>((resolve) => setImmediate(resolve));
    }

    await this.sink.close();
    metrics.elapsedMs = performance.now() - startedAt;
    const cpuUsed = process.cpuUsage(cpuStarted);
    metrics.cpuUserMs = cpuUsed.user / 1_000;
    metrics.cpuSystemMs = cpuUsed.system / 1_000;
    metrics.rssMb = process.memoryUsage().rss / (1024 * 1024);
    metrics.achievedEventsPerSecond =
      metrics.eventsGenerated / Math.max(metrics.elapsedMs / 1_000, 0.001);
    const sinkMetrics = this.sink.getMetrics?.();
    const sendLatencies = [...(sinkMetrics?.sendLatencyMs ?? [])].sort((a, b) => a - b);
    const p95Index = Math.max(0, Math.ceil(sendLatencies.length * 0.95) - 1);
    metrics.profiling = {
      eventGenerationMs: generationTimings.generationMs,
      schemaValidationMs:
        generationTimings.schemaValidationMs + (sinkMetrics?.schemaValidationMs ?? 0),
      serializationMs: sinkMetrics?.serializationMs ?? 0,
      kafkaSendWaitMs: sinkMetrics?.kafkaSendWaitMs ?? 0,
      batchesSent: sinkMetrics?.batchesSent ?? 0,
      successfulSends: sinkMetrics?.successfulSends ?? 0,
      failedSends: sinkMetrics?.failedSends ?? 0,
      averageBatchSendLatencyMs:
        sendLatencies.length > 0
          ? sendLatencies.reduce((sum, value) => sum + value, 0) / sendLatencies.length
          : 0,
      p95BatchSendLatencyMs: sendLatencies[p95Index] ?? 0,
      inFlightPeak: sinkMetrics?.inFlightPeak ?? 0,
      queueDepthPeak: sinkMetrics?.queueDepthPeak ?? 0,
    };
    this.logger.info({ ...metrics }, 'simulator stopped');
    return { metrics, vehicles: this.vehicles };
  }
}
