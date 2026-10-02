import type { TelemetryEvent } from '@fleetpulse/schemas';
import type { HistoricalConfig } from './config.js';

export type HistoricalRow = {
  event_id: string;
  vehicle_id: string;
  vin: string;
  schema_version: string;
  event_timestamp: string;
  event_type: string;
  sequence_number: number;
  latitude: number;
  longitude: number;
  speed_kph: number;
  heading: number;
  engine_temperature_c: number | null;
  rpm: number | null;
  fuel_percent: number | null;
  battery_soc_percent: number | null;
  battery_soh_percent: number | null;
  ignition_on: number;
  fault_scenario: string | null;
  fault_severity: string | null;
};
export function toHistoricalRow(event: TelemetryEvent): HistoricalRow {
  return {
    event_id: event.eventId,
    vehicle_id: event.vehicleId,
    vin: event.vin,
    schema_version: event.schemaVersion,
    event_timestamp: new Date(event.timestamp).toISOString().replace('T', ' ').replace('Z', ''),
    event_type: event.eventType,
    sequence_number: event.sequenceNumber,
    latitude: event.location.latitude,
    longitude: event.location.longitude,
    speed_kph: event.motion.speedKph,
    heading: event.motion.heading,
    engine_temperature_c: event.engine?.temperatureC ?? null,
    rpm: event.engine?.rpm ?? null,
    fuel_percent: event.fuel?.levelPercent ?? null,
    battery_soc_percent: event.battery?.socPercent ?? null,
    battery_soh_percent: event.battery?.sohPercent ?? null,
    ignition_on: event.ignitionOn ? 1 : 0,
    fault_scenario: event.fault?.scenario ?? null,
    fault_severity: event.fault?.severity ?? null,
  };
}

export class ClickHouseStore {
  constructor(private readonly config: HistoricalConfig) {}
  private async request(
    query: string,
    body?: string,
    database = this.config.database,
  ): Promise<string> {
    const url = new URL(this.config.url);
    url.searchParams.set('database', database);
    url.searchParams.set('user', this.config.user);
    if (this.config.password) url.searchParams.set('password', this.config.password);
    const response = await fetch(url, {
      method: 'POST',
      body: body === undefined ? query : `${query}\n${body}`,
      headers: { 'content-type': 'text/plain' },
    });
    if (!response.ok) throw new Error(`ClickHouse ${response.status}: ${await response.text()}`);
    return response.text();
  }
  async initialize(): Promise<void> {
    const database = this.config.database.replace(/[^a-zA-Z0-9_]/g, '');
    if (!database) throw new Error('CLICKHOUSE_DB must contain an alphanumeric database name');
    await this.request(`CREATE DATABASE IF NOT EXISTS ${database}`, undefined, 'default');
    await this.request(
      `CREATE TABLE IF NOT EXISTS telemetry_events (event_id UUID, vehicle_id String, vin String, schema_version LowCardinality(String), event_timestamp DateTime64(3, 'UTC'), event_type LowCardinality(String), sequence_number UInt64, latitude Float64, longitude Float64, speed_kph Float32, heading Float32, engine_temperature_c Nullable(Float32), rpm Nullable(Float32), fuel_percent Nullable(Float32), battery_soc_percent Nullable(Float32), battery_soh_percent Nullable(Float32), ignition_on UInt8, fault_scenario Nullable(String), fault_severity Nullable(String)) ENGINE = MergeTree PARTITION BY toDate(event_timestamp) ORDER BY (vehicle_id, event_timestamp, event_id)`,
    );
  }
  async insert(event: TelemetryEvent): Promise<void> {
    await this.insertBatch([event]);
  }
  async insertBatch(events: TelemetryEvent[]): Promise<void> {
    if (events.length === 0) return;
    await this.request(
      'INSERT INTO telemetry_events FORMAT JSONEachRow',
      `${events.map((event) => JSON.stringify(toHistoricalRow(event))).join('\n')}\n`,
    );
  }
  async query<T = Record<string, unknown>>(sql: string): Promise<T[]> {
    const text = await this.request(`${sql} FORMAT JSONEachRow`);
    return text.trim()
      ? text
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line) as T)
      : [];
  }
}
