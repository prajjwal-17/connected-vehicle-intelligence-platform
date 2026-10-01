import { describe, expect, it, vi } from 'vitest';
import { randomUUID } from 'node:crypto';
import { Kafka } from 'kafkajs';
import pino from 'pino';
import { loadHistoricalConfig } from '../src/config.js';
import { ClickHouseStore } from '../src/store.js';
import { HistoricalConsumer } from '../src/consumer.js';

const enabled = process.env.FLEETPULSE_INTEGRATION === 'true';

describe.skipIf(!enabled)('Kafka → ClickHouse historical pipeline', () => {
  it('stores a validated shared-schema event in analytical storage', async () => {
    const base = loadHistoricalConfig();
    const config = { ...base, group: `historical-test-${Date.now()}`, fromBeginning: false };
    const store = new ClickHouseStore(config);
    await store.initialize();
    const logger = pino({ level: 'silent' });
    const consumer = new HistoricalConsumer(config, store, logger);
    const kafka = new Kafka({ clientId: `historical-test-${Date.now()}`, brokers: config.brokers });
    const producer = kafka.producer({ allowAutoTopicCreation: false });
    const eventId = randomUUID();
    const run = consumer.start();
    await new Promise((resolve) => setTimeout(resolve, 1_000));
    await producer.connect();
    await producer.send({
      topic: config.topic,
      messages: [
        {
          key: 'historical-test-vehicle',
          value: JSON.stringify({
            schemaVersion: '1.0',
            eventId,
            vehicleId: 'historical-test-vehicle',
            vin: 'FP000000000000001',
            timestamp: new Date().toISOString(),
            sequenceNumber: 1,
            eventType: 'TELEMETRY',
            vehicleState: 'DRIVING',
            location: { latitude: 28, longitude: 77 },
            motion: { speedKph: 40, heading: 90 },
            ignitionOn: true,
            tripId: null,
            engine: { rpm: 1800, temperatureC: 90 },
            fuel: { levelPercent: 60 },
          }),
        },
      ],
    });
    await vi.waitFor(
      async () => {
        const rows = await store.query<{ event_id: string }>(
          `SELECT event_id FROM telemetry_events WHERE event_id = '${eventId}'`,
        );
        expect(rows).toHaveLength(1);
      },
      { timeout: 15_000 },
    );
    await consumer.stop();
    await producer.disconnect();
    await run.catch(() => undefined);
  }, 30_000);
});
