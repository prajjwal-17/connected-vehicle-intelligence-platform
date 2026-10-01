import { describe, expect, it } from 'vitest';
import { loadStreamProcessorConfig } from '../src/config.js';

describe('stream processor config', () => {
  it('loads safe defaults', () => {
    const config = loadStreamProcessorConfig({});
    expect(config.groupId).toBe('fleetpulse-stream-processor');
    expect(config.maxOutOfOrderMs).toBe(30_000);
    expect(config.alertCooldownSeconds).toBe(300);
  });
  it('rejects invalid thresholds', () => {
    expect(() => loadStreamProcessorConfig({ ENGINE_TEMP_THRESHOLD_C: 'not-a-number' })).toThrow();
  });
});
