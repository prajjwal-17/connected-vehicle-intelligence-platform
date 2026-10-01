import type { Logger } from 'pino';
import { telemetryEventSchema, type TelemetryEvent } from '@fleetpulse/schemas';
import type { SimulatorConfig } from '../config/index.js';
import { advanceVehicle, generateTelemetryEvent } from '../generators/telemetry.js';
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
    this.vehicles = createVehiclePopulation(config.vehicleCount, config.seed, config.region);
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
    };
    const startedAt = performance.now();
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
        event = generateTelemetryEvent(vehicle, new Date(simulatedNow));
        telemetryEventSchema.parse(event);
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
    metrics.achievedEventsPerSecond =
      metrics.eventsGenerated / Math.max(metrics.elapsedMs / 1_000, 0.001);
    this.logger.info({ ...metrics }, 'simulator stopped');
    return { metrics, vehicles: this.vehicles };
  }
}
