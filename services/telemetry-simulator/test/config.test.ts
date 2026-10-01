import { describe, expect, it } from 'vitest';
import { loadSimulatorConfig } from '../src/config/index.js';

describe('simulator configuration', () => {
  it('provides safe development defaults', () => {
    const config = loadSimulatorConfig({});
    expect(config.vehicleCount).toBe(100);
    expect(config.outputMode).toBe('stdout');
  });

  it('parses boolean and rate controls from environment variables', () => {
    const config = loadSimulatorConfig({
      VEHICLE_COUNT: '1000',
      BURST_ENABLED: 'true',
      DUPLICATE_EVENT_RATE: '0.001',
      OUT_OF_ORDER_RATE: '0.01',
    });
    expect(config.vehicleCount).toBe(1000);
    expect(config.burstEnabled).toBe(true);
    expect(config.duplicateEventRate).toBe(0.001);
    expect(config.outOfOrderRate).toBe(0.01);
  });

  it('rejects unsafe population sizes and invalid rates', () => {
    expect(() => loadSimulatorConfig({ VEHICLE_COUNT: '0' })).toThrow();
    expect(() => loadSimulatorConfig({ DUPLICATE_EVENT_RATE: '2' })).toThrow();
  });
});
