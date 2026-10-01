export { TelemetryIngestionConsumer, type IngestionMetrics } from './consumer.js';
export { KafkaEventSink } from './sink.js';
export { KafkaTelemetryProducer, buildKafkaMessage, ensureTelemetryTopic } from './kafka.js';
export { loadIngestionConfig, kafkaConfigFromSimulator } from './config.js';
export { RedisIdempotencyStore, idempotencyKey } from './idempotency.js';
export { classifyIngestionError, type IngestionErrorClass } from './errors.js';
