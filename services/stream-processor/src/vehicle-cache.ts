export type VehicleLookupLoader = (vin: string) => Promise<string | undefined>;
export type VehicleBatchLookupLoader = (vins: string[]) => Promise<Map<string, string>>;

type CacheEntry = {
  vehicleId: string | undefined;
  expiresAt: number;
};

export class BoundedVehicleLookupCache {
  private readonly entries = new Map<string, CacheEntry>();
  private readonly uniqueVins = new Set<string>();
  private lookups = 0;
  private hits = 0;
  private misses = 0;
  private positiveHits = 0;
  private negativeHits = 0;
  private positiveMisses = 0;
  private negativeMisses = 0;
  private expirations = 0;

  constructor(
    private readonly loader: VehicleLookupLoader,
    private readonly maxEntries = 10_000,
    private readonly ttlMs = 900_000,
    private readonly negativeTtlMs = 60_000,
    private readonly now: () => number = () => Date.now(),
    private readonly collectStats = false,
    private readonly batchLoader?: VehicleBatchLookupLoader,
    private readonly batchSize = 250,
  ) {}

  async get(vin: string): Promise<string | undefined> {
    return (await this.getMany([vin])).get(vin);
  }

  async getMany(vins: string[]): Promise<Map<string, string | undefined>> {
    const uniqueVins = [...new Set(vins)];
    const result = new Map<string, string | undefined>();
    const misses: string[] = [];
    const now = this.now();

    for (const vin of uniqueVins) {
      if (this.collectStats) {
        this.lookups += 1;
        this.uniqueVins.add(vin);
      }
      const cached = this.entries.get(vin);
      if (cached && cached.expiresAt > now) {
        if (this.collectStats) {
          this.hits += 1;
          if (cached.vehicleId === undefined) this.negativeHits += 1;
          else this.positiveHits += 1;
        }
        result.set(vin, cached.vehicleId);
        continue;
      }
      if (cached) {
        if (this.collectStats) this.expirations += 1;
        this.entries.delete(vin);
      }
      misses.push(vin);
    }

    for (let index = 0; index < misses.length; index += this.batchSize) {
      const chunk = misses.slice(index, index + this.batchSize);
      const loaded = this.batchLoader
        ? await this.batchLoader(chunk)
        : new Map(
            await Promise.all(chunk.map(async (vin) => [vin, await this.loader(vin)] as const)),
          );
      for (const vin of chunk) {
        const vehicleId = loaded.get(vin);
        if (this.collectStats) {
          this.misses += 1;
          if (vehicleId === undefined) this.negativeMisses += 1;
          else this.positiveMisses += 1;
        }
        this.entries.set(vin, {
          vehicleId,
          expiresAt: now + (vehicleId === undefined ? this.negativeTtlMs : this.ttlMs),
        });
        result.set(vin, vehicleId);
      }
      this.evictIfNeeded();
    }

    return result;
  }

  get size(): number {
    return this.entries.size;
  }

  stats() {
    return {
      lookups: this.lookups,
      hits: this.hits,
      misses: this.misses,
      positiveHits: this.positiveHits,
      negativeHits: this.negativeHits,
      positiveMisses: this.positiveMisses,
      negativeMisses: this.negativeMisses,
      expirations: this.expirations,
      uniqueVins: this.uniqueVins.size,
      entries: this.entries.size,
    };
  }

  private evictIfNeeded(): void {
    while (this.entries.size > this.maxEntries) {
      const oldest = this.entries.keys().next().value;
      if (oldest === undefined) return;
      this.entries.delete(oldest);
    }
  }
}
