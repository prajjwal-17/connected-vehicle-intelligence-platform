import { describe, expect, it, vi } from 'vitest';
import { randomUUID } from 'node:crypto';
import { Kafka } from 'kafkajs';
import pino from 'pino';
import { loadStreamProcessorConfig } from '../src/config.js';
import { AlertManager } from '../src/alerts.js';
import { StreamEventProcessor } from '../src/processor.js';
import { StreamProcessorConsumer } from '../src/consumer.js';
import { PrismaClient } from '@prisma/client';

const enabled = process.env.FLEETPULSE_PERFORMANCE === 'true';

describe.skipIf(!enabled)('bounded stream throughput smoke', () => {
  it('processes a measured 1,000-event batch without alert writes', async () => {
    const config = loadStreamProcessorConfig({ KAFKA_FROM_BEGINNING: 'false' });
    const groupId = `phase4-performance-${Date.now()}`;
    const logger = pino({ level: 'silent' });
    const db = new PrismaClient({ datasources: { db: { url: config.databaseUrl } } });
    const manager = new AlertManager(db, config.redisUrl, config.alertCooldownSeconds, logger);
    const processor = new StreamEventProcessor({ ...config, groupId }, manager, logger);
    const consumer = new StreamProcessorConsumer({ ...config, groupId }, processor, logger);
    const kafka = new Kafka({
      clientId: `phase4-performance-${Date.now()}`,
      brokers: config.kafkaBrokers,
    });
    const producer = kafka.producer({ allowAutoTopicCreation: false });
    const started = Date.now();
    const messages = Array.from({ length: 1_000 }, (_, index) => ({
      key: `performance-${index % 100}`,
      value: JSON.stringify({
        schemaVersion: '1.0',
        eventId: randomUUID(),
        vehicleId: `performance-${index % 100}`,
        vin: `FP${String(index % 100).padStart(15, '0')}`,
        timestamp: new Date(started + index).toISOString(),
        sequenceNumber: index,
        eventType: 'TELEMETRY',
        vehicleState: 'DRIVING',
        location: { latitude: 28, longitude: 77 },
        motion: { speedKph: 40, heading: 90 },
        ignitionOn: true,
        tripId: null,
        engine: { rpm: 1800, temperatureC: 85 },
        fuel: { levelPercent: 60 },
      }),
    }));
    const run = consumer.start();
    await new Promise((resolve) => setTimeout(resolve, 750));
    await producer.connect();
    const sendStarted = Date.now();
    await producer.send({ topic: config.kafkaTopic, messages });
    await vi.waitFor(() => expect(processor.metrics.streamEventsValid).toBe(1_000), {
      timeout: 20_000,
    });
    const elapsedMs = Date.now() - sendStarted;
    expect(elapsedMs).toBeGreaterThan(0);
    console.log(
      JSON.stringify({
        events: 1_000,
        elapsedMs,
        eventsPerSecond: Math.round(1000 / (elapsedMs / 1000)),
      }),
    );
    await consumer.stop();
    await producer.disconnect();
    await run.catch(() => undefined);
    await db.$disconnect();
  }, 30_000);
});
