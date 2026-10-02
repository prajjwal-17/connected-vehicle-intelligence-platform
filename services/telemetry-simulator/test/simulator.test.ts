import { describe, expect, it } from 'vitest';
import pino from 'pino';
import { telemetryEventSchema } from '@fleetpulse/schemas';
import { loadSimulatorConfig } from '../src/config/index.js';
import { advanceVehicle, generateTelemetryEvent } from '../src/generators/telemetry.js';
import { SeededRandom } from '../src/models/random.js';
import { createVehiclePopulation, generateVin } from '../src/models/vehicle.js';
import { MemoryEventSink } from '../src/output/sinks.js';
import { SimulationEngine } from '../src/simulation/engine.js';

const logger = pino({ level: 'silent' });

function config(overrides: Partial<ReturnType<typeof loadSimulatorConfig>> = {}) {
  return {
    ...loadSimulatorConfig({}),
    outputMode: 'memory' as const,
    ...overrides,
  };
}

describe('vehicle model and telemetry', () => {
  it('generates unique synthetic 17-character VINs', () => {
    const vehicles = createVehiclePopulation(100, 7, 'north-india');
    expect(vehicles).toHaveLength(100);
    expect(new Set(vehicles.map((vehicle) => vehicle.vin)).size).toBe(100);
    expect(generateVin(1)).toHaveLength(17);
  });

  it('evolves motion and odometer from the prior vehicle state', () => {
    const vehicle = createVehiclePopulation(1, 7, 'north-india')[0];
    vehicle.tripState = 'DRIVING';
    vehicle.speedKph = 50;
    const before = {
      latitude: vehicle.latitude,
      longitude: vehicle.longitude,
      odometerKm: vehicle.odometerKm,
    };

    advanceVehicle(vehicle, new SeededRandom(11), 10);

    expect(vehicle.odometerKm).toBeGreaterThan(before.odometerKm);
    expect(vehicle.latitude !== before.latitude || vehicle.longitude !== before.longitude).toBe(
      true,
    );
    expect(Math.abs(vehicle.speedKph - 50)).toBeLessThan(10);
  });

  it('emits a schema-valid event with idempotency metadata', () => {
    const vehicle = createVehiclePopulation(1, 7, 'north-india')[0];
    const event = generateTelemetryEvent(vehicle, new Date('2026-10-01T12:00:00.000Z'));
    expect(telemetryEventSchema.parse(event)).toEqual(event);
    expect(event.schemaVersion).toBe('1.0');
    expect(event.eventId).toMatch(/^[0-9a-f-]{36}$/);
    expect(event.sequenceNumber).toBe(1);
  });

  it('emits explainable fault progression signals', () => {
    const vehicle = createVehiclePopulation(1, 7, 'north-india')[0];
    vehicle.tripState = 'DRIVING';
    vehicle.powertrainType = 'ICE';
    vehicle.engineTemperatureC = 90;
    advanceVehicle(vehicle, new SeededRandom(11), 20);
    const event = generateTelemetryEvent(vehicle, new Date('2026-10-01T12:00:00.000Z'));

    expect(event.eventType).toBe('FAULT');
    expect(event.fault?.scenario).toBe('ENGINE_OVERHEATING');
    expect(event.engine?.temperatureC).toBeGreaterThan(90);
    expect(event.engine?.temperatureC).toBeLessThanOrEqual(180);
  });
});

describe('simulator engine', () => {
  it('runs a 100-vehicle local simulation', async () => {
    const sink = new MemoryEventSink();
    const result = await new SimulationEngine(
      config({ vehicleCount: 100, eventsPerSecond: 50 }),
      sink,
      logger,
    ).run({ maxEvents: 100 });
    expect(result.metrics.vehiclesInitialized).toBe(100);
    expect(result.metrics.eventsGenerated).toBe(100);
    expect(sink.events).toHaveLength(100);
  });

  it('runs a 1000-vehicle local simulation with duplicates and bursts', async () => {
    const sink = new MemoryEventSink();
    const result = await new SimulationEngine(
      config({
        vehicleCount: 1_000,
        eventsPerSecond: 100,
        burstEnabled: true,
        duplicateEventRate: 1,
      }),
      sink,
      logger,
    ).run({ maxEvents: 100 });
    expect(result.metrics.vehiclesInitialized).toBe(1_000);
    expect(result.metrics.duplicatesGenerated).toBe(100);
    expect(result.metrics.burstEvents).toBeGreaterThan(0);
    expect(sink.events).toHaveLength(200);
  });

  it('initializes 100,000 vehicles without per-vehicle workers or timers', async () => {
    const sink = new MemoryEventSink();
    const result = await new SimulationEngine(config({ vehicleCount: 100_000 }), sink, logger).run({
      maxEvents: 0,
    });
    expect(result.metrics.vehiclesInitialized).toBe(100_000);
    expect(sink.events).toHaveLength(0);
  }, 15_000);
});
