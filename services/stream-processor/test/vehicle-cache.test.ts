import { describe, expect, it } from 'vitest';
import { BoundedVehicleLookupCache } from '../src/vehicle-cache.js';

describe('bounded vehicle lookup cache', () => {
  it('avoids PostgreSQL loader calls on a cache hit', async () => {
    let queries = 0;
    const cache = new BoundedVehicleLookupCache(async () => {
      queries += 1;
      return 'vehicle-1';
    });
    await expect(cache.get('VIN-1')).resolves.toBe('vehicle-1');
    await expect(cache.get('VIN-1')).resolves.toBe('vehicle-1');
    expect(queries).toBe(1);
  });

  it('queries on a cache miss and preserves nonexistent vehicles', async () => {
    let queries = 0;
    const cache = new BoundedVehicleLookupCache(async () => {
      queries += 1;
      return undefined;
    });
    await expect(cache.get('UNKNOWN')).resolves.toBeUndefined();
    await expect(cache.get('UNKNOWN')).resolves.toBeUndefined();
    expect(queries).toBe(1);
  });

  it('expires positive and negative entries according to their TTLs', async () => {
    let now = 0;
    let queries = 0;
    const cache = new BoundedVehicleLookupCache(
      async () => {
        queries += 1;
        return queries === 1 ? 'vehicle-1' : undefined;
      },
      10,
      100,
      10,
      () => now,
    );
    await cache.get('VIN-1');
    now = 101;
    await cache.get('VIN-1');
    expect(queries).toBe(2);
  });

  it('evicts oldest entries at the configured bound', async () => {
    let queries = 0;
    const cache = new BoundedVehicleLookupCache(async (vin) => {
      queries += 1;
      return vin;
    }, 2);
    await cache.get('VIN-1');
    await cache.get('VIN-2');
    await cache.get('VIN-3');
    await cache.get('VIN-1');
    expect(cache.size).toBe(2);
    expect(queries).toBe(4);
  });

  it('does not cache PostgreSQL failures', async () => {
    let queries = 0;
    const cache = new BoundedVehicleLookupCache(async () => {
      queries += 1;
      throw new Error('database unavailable');
    });
    await expect(cache.get('VIN-1')).rejects.toThrow('database unavailable');
    await expect(cache.get('VIN-1')).rejects.toThrow('database unavailable');
    expect(queries).toBe(2);
  });

  it('keeps mappings independent for multiple vehicles', async () => {
    const cache = new BoundedVehicleLookupCache(async (vin) => `id-${vin}`);
    await expect(cache.get('VIN-A')).resolves.toBe('id-VIN-A');
    await expect(cache.get('VIN-B')).resolves.toBe('id-VIN-B');
    expect(cache.size).toBe(2);
  });

  it('batches positive, negative, and duplicate VIN lookups', async () => {
    const batches: string[][] = [];
    const cache = new BoundedVehicleLookupCache(
      async () => undefined,
      10,
      100,
      10,
      undefined,
      true,
      async (vins) => {
        batches.push(vins);
        return new Map([['KNOWN', 'vehicle-1']]);
      },
      2,
    );
    const result = await cache.getMany(['KNOWN', 'UNKNOWN', 'KNOWN', 'MISSING']);
    expect(result.get('KNOWN')).toBe('vehicle-1');
    expect(result.get('UNKNOWN')).toBeUndefined();
    expect(result.get('MISSING')).toBeUndefined();
    expect(batches).toEqual([['KNOWN', 'UNKNOWN'], ['MISSING']]);
    await cache.getMany(['KNOWN', 'UNKNOWN']);
    expect(batches).toHaveLength(2);
  });

  it('does not populate any batch entries when the batch loader fails', async () => {
    let batches = 0;
    const cache = new BoundedVehicleLookupCache(
      async () => undefined,
      10,
      100,
      10,
      undefined,
      true,
      async () => {
        batches += 1;
        throw new Error('database unavailable');
      },
    );
    await expect(cache.getMany(['VIN-1', 'VIN-2'])).rejects.toThrow('database unavailable');
    expect(cache.size).toBe(0);
    expect(batches).toBe(1);
  });
});
