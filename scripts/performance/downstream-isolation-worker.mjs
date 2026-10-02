/* global clearInterval, performance, process, setInterval */

import { Kafka } from 'kafkajs';
import { parseTelemetryEvent } from '../../packages/schemas/dist/telemetry.js';

const topic = process.env.KAFKA_TOPIC;
const groupId = process.env.KAFKA_GROUP_ID;
const role = process.env.DOWNSTREAM_ROLE ?? 'downstream';
if (!topic || !groupId) throw new Error('KAFKA_TOPIC and KAFKA_GROUP_ID are required');

const kafka = new Kafka({
  clientId: `fleetpulse-isolation-${role}-${process.pid}`,
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
});
const consumer = kafka.consumer({ groupId, allowAutoTopicCreation: false });
const metrics = { role, received: 0, valid: 0, invalid: 0, parseMs: 0, errors: 0 };
let stopping = false;

const emit = () => process.stdout.write(`${JSON.stringify({ metrics })}\n`);
const timer = setInterval(emit, 500);
const stop = async () => {
  if (stopping) return;
  stopping = true;
  clearInterval(timer);
  await consumer.disconnect().catch(() => undefined);
  emit();
  process.exit(0);
};
process.once('SIGINT', () => void stop());
process.once('SIGTERM', () => void stop());

await consumer.connect();
await consumer.subscribe({ topic, fromBeginning: false });
process.stdout.write(`${JSON.stringify({ ready: true, role, groupId })}\n`);
await consumer.run({
  eachMessage: async ({ message }) => {
    metrics.received += 1;
    const started = performance.now();
    try {
      parseTelemetryEvent(JSON.parse(message.value?.toString() ?? ''));
      metrics.valid += 1;
    } catch {
      metrics.invalid += 1;
    } finally {
      metrics.parseMs += performance.now() - started;
    }
  },
});
