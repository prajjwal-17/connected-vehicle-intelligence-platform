import { mkdir, writeFile } from 'node:fs/promises';
import { dirname } from 'node:path';
import { loadHistoricalConfig } from './config.js';
import { ClickHouseStore } from './store.js';
const config = loadHistoricalConfig();
const store = new ClickHouseStore(config);
const from = process.env.ANALYTICS_FROM ?? '2000-01-01 00:00:00';
const to = process.env.ANALYTICS_TO ?? '2100-01-01 00:00:00';
const output = process.env.FEATURE_INPUT ?? './data/telemetry.jsonl';
await mkdir(dirname(output), { recursive: true });
const rows = await store.query(
  `SELECT * FROM telemetry_events WHERE event_timestamp >= '${from}' AND event_timestamp < '${to}' ORDER BY vehicle_id, event_timestamp, event_id`,
);
await writeFile(
  output,
  rows.map((row) => JSON.stringify(row)).join('\n') + (rows.length ? '\n' : ''),
);
console.log(JSON.stringify({ output, rows: rows.length }));
