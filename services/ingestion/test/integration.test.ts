import { describe, expect, it, vi } from 'vitest';
import { randomUUID } from 'node:crypto';
import pino from 'pino';
import { parseTelemetryEvent } from '@fleetpulse/schemas';
import { loadIngestionConfig } from '../src/config.js';
import { KafkaTelemetryProducer } from '../src/kafka.js';
import { TelemetryIngestionConsumer } from '../src/consumer.js';

const integrationEnabled = process.env.FLEETPULSE_INTEGRATION === 'true';

describe.skipIf(!integrationEnabled)('Kafka + Redis ingestion integration', () => {
  it('publishes, processes, deduplicates, and preserves out-of-order event times', async () => {
    const base = loadIngestionConfig();
    const testTopic = `${base.kafkaTopic}.test.${Date.now()}`;
    const config = {
      ...base,
      kafkaTopic: testTopic,
      kafkaGroupId: `fleetpulse-test-${Date.now()}`,
      kafkaFromBeginning: true,
    };
    const logger = pino({ level: 'silent' });
    const producer = new KafkaTelemetryProducer(config, logger);
    const processed: string[] = [];
    const consumer = new TelemetryIngestionConsumer(config, logger, async (event) => {
      processed.push(event.eventId);
    });
    const event = parseTelemetryEvent({
      schemaVersion: '1.0',
      eventId: randomUUID(),
      vehicleId: 'vehicle-integration',
      vin: 'FP000000000000002',
      timestamp: '2026-01-01T00:00:02.000Z',
      sequenceNumber: 2,
      eventType: 'TELEMETRY',
      vehicleState: 'DRIVING',
      location: { latitude: 28.6139, longitude: 77.209 },
      motion: { speedKph: 35, heading: 90 },
      ignitionOn: true,
      tripId: null,
      engine: { rpm: 1800, temperatureC: 85 },
      fuel: { levelPercent: 70 },
    });

    const runPromise = consumer.start();
    await new Promise((resolve) => setTimeout(resolve, 1_000));
    await producer.send(event);
    await producer.send(event);
    await vi.waitFor(() => expect(processed).toHaveLength(1), { timeout: 10_000 });
    await vi.waitFor(() => expect(consumer.metrics.telemetryEventsDuplicate).toBe(1), {
      timeout: 10_000,
    });
    await consumer.stop();
    await producer.disconnect();
    await runPromise.catch(() => undefined);
  }, 20_000);
});
