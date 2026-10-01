import { describe, expect, it } from 'vitest';
import { parseTelemetryEvent } from '@fleetpulse/schemas';
import { buildKafkaMessage } from '../src/kafka.js';
import { idempotencyKey } from '../src/idempotency.js';
import { loadIngestionConfig } from '../src/config.js';
import { classifyIngestionError } from '../src/errors.js';

const event = parseTelemetryEvent({
  schemaVersion: '1.0',
  eventId: '11111111-1111-4111-8111-111111111111',
  vehicleId: 'vehicle-001',
  vin: 'FP000000000000001',
  timestamp: '2026-01-01T00:00:00.000Z',
  sequenceNumber: 7,
  eventType: 'TELEMETRY',
  vehicleState: 'DRIVING',
  location: { latitude: 28.6139, longitude: 77.209 },
  motion: { speedKph: 35, heading: 90 },
  ignitionOn: true,
  tripId: null,
  engine: { rpm: 1800, temperatureC: 85 },
  fuel: { levelPercent: 70 },
});

describe('Kafka ingestion boundaries', () => {
  it('uses vehicleId as the partition key and preserves the versioned payload', () => {
    const message = buildKafkaMessage(event);
    expect(message.key).toBe('vehicle-001');
    expect(JSON.parse(message.value)).toEqual(event);
    expect(message.headers).toEqual({ 'schema-version': '1.0', 'event-type': 'TELEMETRY' });
  });

  it('namespaces event idempotency keys', () => {
    expect(idempotencyKey(event.eventId)).toBe(
      'telemetry:idempotency:11111111-1111-4111-8111-111111111111',
    );
  });

  it('loads configurable Kafka and Redis defaults', () => {
    const config = loadIngestionConfig({});
    expect(config.kafkaTopic).toBe('vehicle.telemetry.v1');
    expect(config.kafkaGroupId).toBe('fleetpulse-ingestion');
    expect(config.redisUrl).toBe('redis://localhost:6379');
  });

  it('rejects malformed broker configuration', () => {
    expect(() => loadIngestionConfig({ KAFKA_BROKERS: '' })).toThrow();
  });

  it('classifies validation, transient, and permanent errors', () => {
    expect(classifyIngestionError(new SyntaxError('invalid JSON'))).toBe('validation');
    expect(classifyIngestionError(new Error('Redis connection timeout'))).toBe('transient');
    expect(classifyIngestionError(new Error('unsupported payload'))).toBe('permanent');
  });
});
