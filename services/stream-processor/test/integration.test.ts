import { describe, expect, it, vi } from 'vitest';
import { randomUUID } from 'node:crypto';
import { Kafka } from 'kafkajs';
import { PrismaClient } from '@prisma/client';
import pino from 'pino';
import { loadStreamProcessorConfig } from '../src/config.js';
import { AlertManager } from '../src/alerts.js';
import { StreamEventProcessor } from '../src/processor.js';
import { StreamProcessorConsumer } from '../src/consumer.js';

const enabled = process.env.FLEETPULSE_INTEGRATION === 'true';

describe.skipIf(!enabled)('Phase 4 Kafka → processor → PostgreSQL', () => {
  it('creates one cooldown-protected alert and resolves it after normal telemetry', async () => {
    const config = loadStreamProcessorConfig({
      KAFKA_FROM_BEGINNING: 'false',
      ALERT_COOLDOWN_SECONDS: '60',
      ALERT_RESOLUTION_NORMAL_EVENTS: '3',
    });
    const logger = pino({ level: 'silent' });
    const db = new PrismaClient({ datasources: { db: { url: config.databaseUrl } } });
    const vehicleId = `phase4-${Date.now()}`;
    const groupId = `phase4-test-${Date.now()}`;
    const manager = new AlertManager(db, config.redisUrl, config.alertCooldownSeconds, logger);
    const processor = new StreamEventProcessor({ ...config, groupId }, manager, logger);
    const consumer = new StreamProcessorConsumer({ ...config, groupId }, processor, logger);
    const kafka = new Kafka({
      clientId: `phase4-test-${Date.now()}`,
      brokers: config.kafkaBrokers,
    });
    const producer = kafka.producer({ allowAutoTopicCreation: false });
    const timestamp = new Date();
    const makeEvent = (eventId: string, seconds: number, temperature: number) => ({
      schemaVersion: '1.0',
      eventId,
      vehicleId,
      vin: 'FPNO0101000000000',
      timestamp: new Date(timestamp.getTime() + seconds * 1000).toISOString(),
      sequenceNumber: seconds + 1,
      eventType: 'TELEMETRY',
      vehicleState: 'DRIVING',
      location: { latitude: 28, longitude: 77 },
      motion: { speedKph: 40, heading: 90 },
      ignitionOn: true,
      tripId: null,
      engine: { rpm: 1800, temperatureC: temperature },
      fuel: { levelPercent: 60 },
    });
    const events = [makeEvent(randomUUID(), 0, 112), makeEvent(randomUUID(), 10, 118)];
    const run = consumer.start();
    await new Promise((resolve) => setTimeout(resolve, 1_000));
    await producer.connect();
    await producer.send({
      topic: config.kafkaTopic,
      messages: events.map((event) => ({ key: vehicleId, value: JSON.stringify(event) })),
    });
    await producer.send({
      topic: config.kafkaTopic,
      messages: [{ key: vehicleId, value: JSON.stringify(events[1]) }],
    });
    await vi.waitFor(
      async () => {
        const alerts = await db.alert.findMany({
          where: {
            vehicleId: (await db.vehicle.findUniqueOrThrow({ where: { vin: 'FPNO0101000000000' } }))
              .id,
            alertType: 'ENGINE_OVERHEATING',
          },
        });
        expect(alerts).toHaveLength(1);
      },
      { timeout: 15_000 },
    );
    const normal = [
      makeEvent(randomUUID(), 20, 90),
      makeEvent(randomUUID(), 30, 90),
      makeEvent(randomUUID(), 40, 90),
    ];
    await producer.send({
      topic: config.kafkaTopic,
      messages: normal.map((event) => ({ key: vehicleId, value: JSON.stringify(event) })),
    });
    await vi.waitFor(
      async () => {
        const vehicle = await db.vehicle.findUniqueOrThrow({ where: { vin: 'FPNO0101000000000' } });
        const alert = await db.alert.findFirstOrThrow({
          where: { vehicleId: vehicle.id, alertType: 'ENGINE_OVERHEATING' },
        });
        expect(alert.status).toBe('RESOLVED');
      },
      { timeout: 15_000 },
    );
    await consumer.stop();
    await producer.disconnect();
    await run.catch(() => undefined);
    await db.$disconnect();
  }, 40_000);
});
