import type { EachMessagePayload } from 'kafkajs';
import type { Logger } from 'pino';
import { parseTelemetryEvent, type TelemetryEvent } from '@fleetpulse/schemas';
import { createConsumer, ensureTelemetryTopic, type KafkaRuntimeConfig } from './kafka.js';
import { RedisIdempotencyStore } from './idempotency.js';
import { classifyIngestionError } from './errors.js';

export type IngestionMetrics = {
  telemetryEventsConsumed: number;
  telemetryEventsProcessed: number;
  telemetryEventsDuplicate: number;
  telemetryEventsInvalid: number;
  telemetryEventsFailed: number;
  kafkaConsumerErrors: number;
  ingestionProcessingLatencyMs: number;
};

export type IngestionConsumerConfig = KafkaRuntimeConfig & {
  kafkaGroupId: string;
  kafkaFromBeginning: boolean;
  redisUrl: string;
  idempotencyTtlSeconds: number;
};

export type EventProcessor = (event: TelemetryEvent) => Promise<void>;

export class TelemetryIngestionConsumer {
  readonly metrics: IngestionMetrics = {
    telemetryEventsConsumed: 0,
    telemetryEventsProcessed: 0,
    telemetryEventsDuplicate: 0,
    telemetryEventsInvalid: 0,
    telemetryEventsFailed: 0,
    kafkaConsumerErrors: 0,
    ingestionProcessingLatencyMs: 0,
  };
  private readonly runtime;
  private readonly idempotency: RedisIdempotencyStore;
  private running = false;

  constructor(
    private readonly config: IngestionConsumerConfig,
    private readonly logger: Logger<never, boolean>,
    private readonly processEvent: EventProcessor = async () => undefined,
  ) {
    this.runtime = createConsumer(config);
    this.idempotency = new RedisIdempotencyStore(config.redisUrl, config.idempotencyTtlSeconds);
  }

  async start(): Promise<void> {
    const admin = this.runtime.kafka.admin();
    await ensureTelemetryTopic(admin, this.config, this.logger);
    await admin.disconnect();
    await this.idempotency.connect();
    await this.runtime.consumer.connect();
    await this.runtime.consumer.subscribe({
      topic: this.config.kafkaTopic,
      fromBeginning: this.config.kafkaFromBeginning,
    });
    this.runtime.consumer.on(this.runtime.consumer.events.CRASH, ({ payload }) => {
      this.metrics.kafkaConsumerErrors += 1;
      this.logger.error({ err: payload.error }, 'kafka consumer crashed');
    });
    this.running = true;
    await this.runtime.consumer.run({ eachMessage: (payload) => this.handleMessage(payload) });
  }

  private async handleMessage({ message, partition }: EachMessagePayload): Promise<void> {
    this.metrics.telemetryEventsConsumed += 1;
    let event: TelemetryEvent;
    try {
      event = parseTelemetryEvent(JSON.parse(message.value?.toString() ?? ''));
    } catch (error) {
      this.metrics.telemetryEventsInvalid += 1;
      this.logger.warn(
        {
          err: error,
          errorClass: classifyIngestionError(error),
          partition,
          offset: message.offset,
        },
        'invalid telemetry rejected',
      );
      return;
    }

    const isNew = await this.idempotency.claim(event.eventId);
    if (!isNew) {
      this.metrics.telemetryEventsDuplicate += 1;
      this.logger.info({ eventId: event.eventId }, 'duplicate telemetry ignored');
      return;
    }

    const startedAt = performance.now();
    try {
      await this.processEvent(event);
      this.metrics.telemetryEventsProcessed += 1;
      this.metrics.ingestionProcessingLatencyMs = performance.now() - startedAt;
    } catch (error) {
      await this.idempotency.release(event.eventId);
      this.metrics.telemetryEventsFailed += 1;
      this.logger.error(
        { err: error, errorClass: classifyIngestionError(error), eventId: event.eventId },
        'telemetry processing failed',
      );
      throw error;
    }
  }

  async stop(): Promise<void> {
    if (!this.running) return;
    await this.runtime.consumer.disconnect();
    await this.idempotency.disconnect();
    this.running = false;
    this.logger.info({ ...this.metrics }, 'ingestion consumer stopped');
  }
}
