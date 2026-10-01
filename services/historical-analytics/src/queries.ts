import type { ClickHouseStore } from './store.js';
export const analyticsQueries = {
  telemetryVolume: (from: string, to: string) =>
    `SELECT vehicle_id, count() AS events, toDate(event_timestamp) AS day FROM telemetry_events WHERE event_timestamp >= '${from}' AND event_timestamp < '${to}' GROUP BY vehicle_id, day ORDER BY day, events DESC`,
  fleetHourlyVolume: (from: string, to: string) =>
    `SELECT toStartOfHour(event_timestamp) AS hour, count() AS events FROM telemetry_events WHERE event_timestamp >= '${from}' AND event_timestamp < '${to}' GROUP BY hour ORDER BY hour`,
  vehicleUtilization: (from: string, to: string) =>
    `SELECT vehicle_id, sumIf(1, speed_kph > 1) AS driving_events, sumIf(1, ignition_on = 1 AND speed_kph <= 1) AS idle_events, sum(speed_kph) / 3600 AS distance_proxy_km FROM telemetry_events WHERE event_timestamp >= '${from}' AND event_timestamp < '${to}' GROUP BY vehicle_id`,
  temperature: (from: string, to: string) =>
    `SELECT vehicle_id, avg(engine_temperature_c) AS average_temperature_c, max(engine_temperature_c) AS max_temperature_c FROM telemetry_events WHERE event_timestamp >= '${from}' AND event_timestamp < '${to}' AND engine_temperature_c IS NOT NULL GROUP BY vehicle_id`,
  battery: (from: string, to: string) =>
    `SELECT vehicle_id, avg(battery_soc_percent) AS average_soc, min(battery_soc_percent) AS minimum_soc, argMax(battery_soh_percent, event_timestamp) AS latest_soh FROM telemetry_events WHERE event_timestamp >= '${from}' AND event_timestamp < '${to}' AND battery_soc_percent IS NOT NULL GROUP BY vehicle_id`,
  faults: (from: string, to: string) =>
    `SELECT vehicle_id, fault_scenario, count() AS faults FROM telemetry_events WHERE event_timestamp >= '${from}' AND event_timestamp < '${to}' AND fault_scenario IS NOT NULL GROUP BY vehicle_id, fault_scenario`,
};
export async function runAnalytics(store: ClickHouseStore, from: string, to: string) {
  return {
    volume: await store.query(analyticsQueries.telemetryVolume(from, to)),
    hourly: await store.query(analyticsQueries.fleetHourlyVolume(from, to)),
    utilization: await store.query(analyticsQueries.vehicleUtilization(from, to)),
    temperature: await store.query(analyticsQueries.temperature(from, to)),
    battery: await store.query(analyticsQueries.battery(from, to)),
    faults: await store.query(analyticsQueries.faults(from, to)),
  };
}
