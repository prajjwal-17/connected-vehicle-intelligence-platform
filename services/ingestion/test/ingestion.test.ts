import { describe, expect, it, vi } from 'vitest';
import { parseTelemetryEvent } from '@fleetpulse/schemas';
import { buildKafkaMessage } from '../src/kafka.js';
import { idempotencyKey } from '../src/idempotency.js';
import { loadIngestionConfig } from '../src/config.js';
import { classifyIngestionError } from '../src/errors.js';
import {
  RedisIdempotencyStore,
  idempotencyKey,
  mapIdempotencyResults,
} from '../src/idempotency.js';
import { processClaimedEventsInOrder } from '../src/consumer.js';

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

function fakeRedis(execResult: unknown) {
  let open = false;
  const commands: string[] = [];
  const pipeline = {
    set: vi.fn((key: string) => {
      commands.push(key);
      return pipeline;
    }),
    exec: vi.fn(async () =>
      typeof execResult === 'function' ? (execResult as () => unknown)() : execResult,
    ),
  };
  const client = {
    get isOpen() {
      return open;
    },
    connect: vi.fn(async () => {
      open = true;
    }),
    multi: vi.fn(() => pipeline),
    quit: vi.fn(async () => undefined),
  };
  return { client, pipeline, commands };
}

describe('Redis idempotency pipelining', () => {
  it('claims all events and preserves result order', async () => {
    const fake = fakeRedis(['OK', 'OK', 'OK']);
    const store = new RedisIdempotencyStore('redis://test', 60, fake.client as never);
    await expect(store.claimMany(['a', 'b', 'c'])).resolves.toEqual([true, true, true]);
    expect(fake.commands).toEqual([idempotencyKey('a'), idempotencyKey('b'), idempotencyKey('c')]);
  });

  it('rejects duplicate events from Redis null replies', async () => {
    const fake = fakeRedis(['OK', null]);
    const store = new RedisIdempotencyStore('redis://test', 60, fake.client as never);
    await expect(store.claimMany(['a', 'b'])).resolves.toEqual([true, false]);
  });

  it('maps mixed new and duplicate results to the original events', () => {
    expect(mapIdempotencyResults(['OK', null, 'OK', null])).toEqual([true, false, true, false]);
  });

  it('surfaces a Redis pipeline failure without claiming success', async () => {
    const error = new Error('pipeline unavailable');
    const fake = fakeRedis(() => {
      throw error;
    });
    const store = new RedisIdempotencyStore('redis://test', 60, fake.client as never);
    await expect(store.claimMany(['a', 'b'])).rejects.toBe(error);
  });

  it('can retry a failed pipeline and preserve mapping', async () => {
    let attempt = 0;
    const fake = fakeRedis(() => {
      attempt += 1;
      if (attempt === 1) throw new Error('temporary Redis failure');
      return ['OK', null];
    });
    const store = new RedisIdempotencyStore('redis://test', 60, fake.client as never);
    await expect(store.claimMany(['a', 'b'])).rejects.toThrow('temporary Redis failure');
    await expect(store.claimMany(['a', 'b'])).resolves.toEqual([true, false]);
    expect(attempt).toBe(2);
  });

  it('processes claimed events sequentially and skips duplicates', async () => {
    const eventIds = [
      '11111111-1111-4111-8111-111111111111',
      '22222222-2222-4222-8222-222222222222',
      '33333333-3333-4333-8333-333333333333',
    ];
    const events = eventIds.map((eventId, index) =>
      parseTelemetryEvent({ ...event, eventId, sequenceNumber: index + 1 }),
    );
    const processed: string[] = [];
    await processClaimedEventsInOrder(events, [true, false, true], async (item) => {
      processed.push(item.eventId);
    });
    expect(processed).toEqual([eventIds[0], eventIds[2]]);
  });
});
