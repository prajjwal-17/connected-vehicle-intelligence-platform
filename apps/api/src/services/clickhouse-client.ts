export type TelemetrySummary = {
  events: number;
  firstEvent: string | null;
  lastEvent: string | null;
  averageSpeedKph: number | null;
  averageEngineTemperatureC: number | null;
  latestBatterySoh: number | null;
};

export class ClickHouseClient {
  constructor(
    private readonly config: { url: string; database: string; user: string; password: string },
  ) {}

  private async query<T>(sql: string): Promise<T[]> {
    const url = new URL(this.config.url);
    url.searchParams.set('database', this.config.database);
    url.searchParams.set('user', this.config.user);
    url.searchParams.set('password', this.config.password);
    const response = await fetch(url, {
      method: 'POST',
      body: `${sql} FORMAT JSONEachRow`,
      headers: { 'content-type': 'text/plain' },
    });
    if (!response.ok) throw new Error(`ClickHouse ${response.status}: ${await response.text()}`);
    const text = await response.text();
    return text.trim()
      ? text
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line) as T)
      : [];
  }

  async vehicleSummary(vehicleId: string, vin?: string): Promise<TelemetrySummary> {
    const identifiers = [vehicleId, vin]
      .filter(Boolean)
      .map((value) => `'${value!.replace(/'/g, "''")}'`);
    const filter =
      identifiers.length > 1
        ? `vehicle_id = ${identifiers[0]} OR vin = ${identifiers[1]}`
        : `vehicle_id = ${identifiers[0]}`;
    const rows = await this.query<TelemetrySummary>(
      `SELECT count() AS events, min(event_timestamp) AS firstEvent, max(event_timestamp) AS lastEvent, avg(speed_kph) AS averageSpeedKph, avg(engine_temperature_c) AS averageEngineTemperatureC, argMax(battery_soh_percent, event_timestamp) AS latestBatterySoh FROM telemetry_events WHERE ${filter}`,
    );
    return (
      rows[0] ?? {
        events: 0,
        firstEvent: null,
        lastEvent: null,
        averageSpeedKph: null,
        averageEngineTemperatureC: null,
        latestBatterySoh: null,
      }
    );
  }

  async fleetAnalytics(days = 7, vehicleIds?: string[], vins?: string[]) {
    if (vehicleIds && vehicleIds.length === 0 && vins?.length === 0)
      return { events: 0, vehicles: 0, averageSpeedKph: null };
    const from = new Date(Date.now() - days * 86400000)
      .toISOString()
      .slice(0, 19)
      .replace('T', ' ');
    const idFilter = vehicleIds?.length
      ? `vehicle_id IN (${vehicleIds.map((id) => `'${id.replace(/'/g, "''")}'`).join(',')})`
      : '';
    const vinFilter = vins?.length
      ? `vin IN (${vins.map((vin) => `'${vin.replace(/'/g, "''")}'`).join(',')})`
      : '';
    const filter =
      idFilter || vinFilter ? ` AND (${[idFilter, vinFilter].filter(Boolean).join(' OR ')})` : '';
    const rows = await this.query<{
      events: number;
      vehicles: number;
      averageSpeedKph: number | null;
    }>(
      `SELECT count() AS events, uniqExact(vehicle_id) AS vehicles, avg(speed_kph) AS averageSpeedKph FROM telemetry_events WHERE event_timestamp >= '${from}'${filter}`,
    );
    return rows[0] ?? { events: 0, vehicles: 0, averageSpeedKph: null };
  }
}
