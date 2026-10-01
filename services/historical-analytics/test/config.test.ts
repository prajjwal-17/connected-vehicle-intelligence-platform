import { describe, expect, it } from 'vitest';
import { loadHistoricalConfig } from '../src/config.js';
import { toHistoricalRow } from '../src/store.js';

describe('historical analytics boundaries', () => {
  it('loads independent consumer defaults', () => {
    const config = loadHistoricalConfig({});
    expect(config.group).toBe('fleetpulse-historical-analytics');
    expect(config.database).toBe('fleetpulse');
  });
  it('maps the shared event contract to typed analytical columns', () => {
    const row = toHistoricalRow({
      schemaVersion: '1.0',
      eventId: '11111111-1111-4111-8111-111111111111',
      vehicleId: 'vehicle-1',
      vin: 'FP000000000000001',
      timestamp: '2026-01-01T00:00:00.000Z',
      sequenceNumber: 1,
      eventType: 'TELEMETRY',
      vehicleState: 'DRIVING',
      location: { latitude: 28, longitude: 77 },
      motion: { speedKph: 40, heading: 90 },
      ignitionOn: true,
      tripId: null,
      engine: { rpm: 1800, temperatureC: 90 },
      fuel: { levelPercent: 60 },
    });
    expect(row.event_id).toBe('11111111-1111-4111-8111-111111111111');
    expect(row.engine_temperature_c).toBe(90);
    expect(row.ignition_on).toBe(1);
  });
});
