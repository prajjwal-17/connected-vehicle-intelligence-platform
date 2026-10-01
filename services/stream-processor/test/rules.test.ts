import { describe, expect, it } from 'vitest';
import { randomUUID } from 'node:crypto';
import { parseTelemetryEvent } from '@fleetpulse/schemas';
import { createDetectors } from '../src/rules.js';
import { BoundedVehicleStateStore } from '../src/state.js';
import { loadStreamProcessorConfig } from '../src/config.js';

const base = (overrides: Record<string, unknown> = {}) =>
  parseTelemetryEvent({
    schemaVersion: '1.0',
    eventId: randomUUID(),
    vehicleId: 'vehicle-test',
    vin: 'FP000000000000001',
    timestamp: '2026-01-01T00:00:00.000Z',
    sequenceNumber: 1,
    eventType: 'TELEMETRY',
    vehicleState: 'DRIVING',
    location: { latitude: 28, longitude: 77 },
    motion: { speedKph: 50, heading: 90 },
    ignitionOn: true,
    tripId: null,
    engine: { rpm: 1800, temperatureC: 85 },
    fuel: { levelPercent: 50 },
    ...overrides,
  });

describe('stream processor rules', () => {
  it('detects sustained overheating and rapid temperature rise', () => {
    const config = loadStreamProcessorConfig({
      ENGINE_TEMP_THRESHOLD_C: '110',
      RAPID_TEMP_RISE_THRESHOLD: '0.5',
    });
    const store = new BoundedVehicleStateStore(120, 900_000);
    const detectors = createDetectors(config);
    const first = base({
      eventId: randomUUID(),
      timestamp: '2026-01-01T00:00:00.000Z',
      engine: { rpm: 1800, temperatureC: 112 },
    });
    const second = base({
      eventId: randomUUID(),
      timestamp: '2026-01-01T00:00:10.000Z',
      engine: { rpm: 1800, temperatureC: 118 },
    });
    store.add(first);
    const state = store.add(second);
    const detections = detectors.flatMap((detector) => detector.evaluate(second, state));
    expect(detections.map((d) => d.type)).toEqual(
      expect.arrayContaining(['ENGINE_OVERHEATING', 'RAPID_TEMPERATURE_RISE']),
    );
  });

  it('calculates harsh braking from consecutive speeds', () => {
    const config = loadStreamProcessorConfig({ HARSH_BRAKING_THRESHOLD: '2' });
    const store = new BoundedVehicleStateStore(120, 900_000);
    const detectors = createDetectors(config);
    const first = base({
      eventId: randomUUID(),
      timestamp: '2026-01-01T00:00:00.000Z',
      motion: { speedKph: 80, heading: 90 },
    });
    const second = base({
      eventId: randomUUID(),
      timestamp: '2026-01-01T00:00:05.000Z',
      motion: { speedKph: 40, heading: 90 },
    });
    store.add(first);
    const state = store.add(second);
    expect(
      detectors.find((d) => d.name === 'HARSH_BRAKING')?.evaluate(second, state)[0]?.type,
    ).toBe('HARSH_BRAKING');
  });

  it('keeps a bounded, event-time-sorted window', () => {
    const store = new BoundedVehicleStateStore(2, 900_000);
    const one = base({ eventId: randomUUID(), timestamp: '2026-01-01T00:00:02.000Z' });
    const two = base({ eventId: randomUUID(), timestamp: '2026-01-01T00:00:01.000Z' });
    const three = base({ eventId: randomUUID(), timestamp: '2026-01-01T00:00:03.000Z' });
    store.add(one);
    const state = store.add(two);
    expect(state.events.map((e) => e.timestamp)).toEqual([
      '2026-01-01T00:00:01.000Z',
      '2026-01-01T00:00:02.000Z',
    ]);
    store.add(three);
    expect(store.size()).toBe(1);
  });
});
