import { randomUUID } from 'node:crypto';
import type { Logger } from 'pino';
import { createClient, type RedisClientType } from 'redis';
import type { DatabaseClient } from '@fleetpulse/database';
import type { TelemetryEvent } from '@fleetpulse/schemas';
import type { Detection } from './rules.js';
import { BoundedVehicleLookupCache, type VehicleBatchLookupLoader } from './vehicle-cache.js';

export type AlertMetrics = {
  anomaliesDetected: number;
  alertsCreated: number;
  alertsSuppressed: number;
  alertsResolved: number;
  detectionLatencyMs: number;
  databaseWriteErrors: number;
  stateStoreErrors: number;
  redisCommands: number;
  redisLatencyMs: number;
  postgresQueries: number;
  postgresQueryMs: number;
  vehicleLookupQueries: number;
  vehicleLookupQueryMs: number;
  vehicleLookupBatches: number;
  alertResolutionQueries: number;
  alertResolutionQueryMs: number;
  postgresWrites: number;
  postgresWriteMs: number;
};

const severityMap = { INFO: 'INFO', WARNING: 'WARNING', CRITICAL: 'CRITICAL' } as const;

export class AlertManager {
  readonly metrics: AlertMetrics = {
    anomaliesDetected: 0,
    alertsCreated: 0,
    alertsSuppressed: 0,
    alertsResolved: 0,
    detectionLatencyMs: 0,
    databaseWriteErrors: 0,
    stateStoreErrors: 0,
    redisCommands: 0,
    redisLatencyMs: 0,
    postgresQueries: 0,
    postgresQueryMs: 0,
    vehicleLookupQueries: 0,
    vehicleLookupQueryMs: 0,
    vehicleLookupBatches: 0,
    alertResolutionQueries: 0,
    alertResolutionQueryMs: 0,
    postgresWrites: 0,
    postgresWriteMs: 0,
  };
  private readonly redis: RedisClientType;
  private readonly activeTypes = new Map<string, Set<string>>();
  private readonly vehicleCache: BoundedVehicleLookupCache;

  constructor(
    private readonly db: DatabaseClient,
    redisUrl: string,
    private readonly cooldownSeconds: number,
    private readonly logger: Logger<never, boolean>,
    private readonly vehicleLookupBatchSize = 250,
  ) {
    this.redis = createClient({ url: redisUrl });
    const batchLoader: VehicleBatchLookupLoader = async (vins) => {
      const started = performance.now();
      const vehicles = await this.db.vehicle.findMany({
        where: { vin: { in: vins } },
        select: { id: true, vin: true },
      });
      const elapsed = performance.now() - started;
      this.metrics.postgresQueries += 1;
      this.metrics.postgresQueryMs += elapsed;
      this.metrics.vehicleLookupQueries += 1;
      this.metrics.vehicleLookupQueryMs += elapsed;
      this.metrics.vehicleLookupBatches += 1;
      return new Map(vehicles.map((vehicle) => [vehicle.vin, vehicle.id]));
    };
    this.vehicleCache = new BoundedVehicleLookupCache(
      async (vin) => {
        const started = performance.now();
        const vehicle = await this.db.vehicle.findUnique({
          where: { vin },
          select: { id: true },
        });
        this.metrics.postgresQueries += 1;
        const elapsed = performance.now() - started;
        this.metrics.postgresQueryMs += elapsed;
        this.metrics.vehicleLookupQueries += 1;
        this.metrics.vehicleLookupQueryMs += elapsed;
        return vehicle?.id;
      },
      100_000,
      900_000,
      60_000,
      undefined,
      process.env.STREAM_PROFILE === '1',
      batchLoader,
      this.vehicleLookupBatchSize,
    );
  }

  async prefetchVehicleLookups(events: TelemetryEvent[]): Promise<void> {
    const vins = events
      .filter((event) => !this.isDatabaseVehicleId(event.vehicleId))
      .map((event) => event.vin);
    await this.vehicleCache.getMany(vins);
  }

  async connect(): Promise<void> {
    if (!this.redis.isOpen) await this.redis.connect();
  }

  private async vehicleDbId(event: TelemetryEvent): Promise<string | undefined> {
    if (this.isDatabaseVehicleId(event.vehicleId)) return event.vehicleId;
    return this.vehicleCache.get(event.vin);
  }

  private isDatabaseVehicleId(vehicleId: string): boolean {
    return /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(vehicleId);
  }

  private cooldownKey(event: TelemetryEvent, type: string): string {
    return `alert:${event.vehicleId}:${type}`;
  }

  async handle(event: TelemetryEvent, detections: Detection[]): Promise<void> {
    if (detections.length === 0) return;
    this.metrics.anomaliesDetected += detections.length;
    this.metrics.detectionLatencyMs = Math.max(0, Date.now() - Date.parse(event.timestamp));
    const vehicleId = await this.vehicleDbId(event);
    if (!vehicleId) {
      this.logger.warn(
        { vehicleId: event.vehicleId, vin: event.vin },
        'anomaly detected for vehicle not present in transactional database',
      );
      return;
    }
    await this.connect();
    for (const detection of detections) {
      const key = this.cooldownKey(event, detection.type);
      try {
        const started = performance.now();
        const claimed = await this.redis.set(key, '1', { NX: true, EX: this.cooldownSeconds });
        this.metrics.redisCommands += 1;
        this.metrics.redisLatencyMs += performance.now() - started;
        if (claimed !== 'OK') {
          this.metrics.alertsSuppressed += 1;
          continue;
        }
      } catch (error) {
        this.metrics.stateStoreErrors += 1;
        this.logger.error({ err: error, key }, 'alert cooldown state unavailable');
        throw error;
      }
      await this.createWithRetry(vehicleId, event, detection);
      const types = this.activeTypes.get(vehicleId) ?? new Set<string>();
      types.add(detection.type);
      this.activeTypes.set(vehicleId, types);
    }
  }

  private async createWithRetry(
    vehicleId: string,
    event: TelemetryEvent,
    detection: Detection,
  ): Promise<void> {
    let lastError: unknown;
    for (let attempt = 0; attempt < 3; attempt += 1) {
      try {
        const started = performance.now();
        await this.db.alert.create({
          data: {
            id: randomUUID(),
            vehicleId,
            alertType: detection.type,
            severity: severityMap[detection.severity],
            detectedAt: new Date(event.timestamp),
            metadata: {
              ...detection.metadata,
              eventId: event.eventId,
              vehicleId: event.vehicleId,
              eventTimestamp: event.timestamp,
              detectionCreatedAt: new Date().toISOString(),
            },
          },
        });
        this.metrics.postgresWrites += 1;
        this.metrics.postgresWriteMs += performance.now() - started;
        this.metrics.alertsCreated += 1;
        this.logger.info(
          { vehicleId, alertType: detection.type, severity: detection.severity },
          'stream anomaly alert created',
        );
        return;
      } catch (error) {
        lastError = error;
        await new Promise((resolve) => setTimeout(resolve, 50 * (attempt + 1)));
      }
    }
    this.metrics.databaseWriteErrors += 1;
    this.logger.error(
      { err: lastError, vehicleId, alertType: detection.type },
      'alert persistence failed after bounded retries',
    );
    throw lastError;
  }

  async resolveNormal(event: TelemetryEvent, normalTypes: string[]): Promise<void> {
    const vehicleId = await this.vehicleDbId(event);
    if (!vehicleId) return;
    const active = this.activeTypes.get(vehicleId);
    if (!active) return;
    for (const type of normalTypes) {
      if (!active.has(type)) continue;
      const started = performance.now();
      const updated = await this.db.alert.updateMany({
        where: { vehicleId, alertType: type, status: 'OPEN' },
        data: { status: 'RESOLVED', resolvedAt: new Date(event.timestamp) },
      });
      this.metrics.postgresQueries += 1;
      const elapsed = performance.now() - started;
      this.metrics.postgresQueryMs += elapsed;
      this.metrics.alertResolutionQueries += 1;
      this.metrics.alertResolutionQueryMs += elapsed;
      if (updated.count > 0) {
        this.metrics.alertsResolved += updated.count;
        this.logger.info({ vehicleId, alertType: type }, 'stream anomaly alert resolved');
      }
      active.delete(type);
    }
  }

  async disconnect(): Promise<void> {
    if (this.redis.isOpen) await this.redis.quit();
  }

  vehicleLookupProfile() {
    return this.vehicleCache.stats();
  }
}
