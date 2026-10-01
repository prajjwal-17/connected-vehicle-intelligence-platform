import { randomUUID } from 'node:crypto';
import type { Logger } from 'pino';
import { createClient, type RedisClientType } from 'redis';
import type { DatabaseClient } from '@fleetpulse/database';
import type { TelemetryEvent } from '@fleetpulse/schemas';
import type { Detection } from './rules.js';

export type AlertMetrics = {
  anomaliesDetected: number;
  alertsCreated: number;
  alertsSuppressed: number;
  alertsResolved: number;
  detectionLatencyMs: number;
  databaseWriteErrors: number;
  stateStoreErrors: number;
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
  };
  private readonly redis: RedisClientType;
  private readonly activeTypes = new Map<string, Set<string>>();

  constructor(
    private readonly db: DatabaseClient,
    redisUrl: string,
    private readonly cooldownSeconds: number,
    private readonly logger: Logger<never, boolean>,
  ) {
    this.redis = createClient({ url: redisUrl });
  }

  async connect(): Promise<void> {
    if (!this.redis.isOpen) await this.redis.connect();
  }

  private async vehicleDbId(event: TelemetryEvent): Promise<string | undefined> {
    if (
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(event.vehicleId)
    )
      return event.vehicleId;
    const vehicle = await this.db.vehicle.findUnique({
      where: { vin: event.vin },
      select: { id: true },
    });
    return vehicle?.id;
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
        const claimed = await this.redis.set(key, '1', { NX: true, EX: this.cooldownSeconds });
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
      const updated = await this.db.alert.updateMany({
        where: { vehicleId, alertType: type, status: 'OPEN' },
        data: { status: 'RESOLVED', resolvedAt: new Date(event.timestamp) },
      });
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
}
