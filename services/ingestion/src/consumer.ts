import type { EachBatchPayload } from 'kafkajs';
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
  schemaValidationMs: number;
  idempotencyLatencyMs: number;
  processingLatencyMs: number;
  totalEventLatencyMs: number;
  redisCommands: number;
  maxConcurrentHandlers: number;
  activeHandlers: number;
};

export type IngestionConsumerConfig = KafkaRuntimeConfig & {
  kafkaGroupId: string;
  kafkaFromBeginning: boolean;
  redisUrl: string;
  idempotencyTtlSeconds: number;
};

export type EventProcessor = (event: TelemetryEvent) => Promise<void>;

export async function processClaimedEventsInOrder(
  events: TelemetryEvent[],
  claimed: boolean[],
  processEvent: EventProcessor,
): Promise<void> {
  for (let index = 0; index < events.length; index += 1)
    if (claimed[index]) await processEvent(events[index]);
}

export class TelemetryIngestionConsumer {
  readonly metrics: IngestionMetrics = {
    telemetryEventsConsumed: 0,
    telemetryEventsProcessed: 0,
    telemetryEventsDuplicate: 0,
    telemetryEventsInvalid: 0,
    telemetryEventsFailed: 0,
    kafkaConsumerErrors: 0,
    ingestionProcessingLatencyMs: 0,
    schemaValidationMs: 0,
    idempotencyLatencyMs: 0,
    processingLatencyMs: 0,
    totalEventLatencyMs: 0,
    redisCommands: 0,
    maxConcurrentHandlers: 0,
    activeHandlers: 0,
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
    await this.runtime.consumer.run({
      eachBatchAutoResolve: false,
      eachBatch: (payload) => this.handleBatch(payload),
    });
  }

  private async handleBatch({ batch, resolveOffset, heartbeat }: EachBatchPayload): Promise<void> {
    const receivedAt = performance.now();
    const validMessages: typeof batch.messages = [];
    const events: TelemetryEvent[] = [];
    const invalidMessages: typeof batch.messages = [];
    for (const message of batch.messages) {
      this.metrics.telemetryEventsConsumed += 1;
      try {
        const validationStarted = performance.now();
        const event = parseTelemetryEvent(JSON.parse(message.value?.toString() ?? ''));
        this.metrics.schemaValidationMs += performance.now() - validationStarted;
        validMessages.push(message);
        events.push(event);
      } catch (error) {
        this.metrics.telemetryEventsInvalid += 1;
        this.logger.warn(
          {
            err: error,
            errorClass: classifyIngestionError(error),
            partition: batch.partition,
            offset: message.offset,
          },
          'invalid telemetry rejected',
        );
        invalidMessages.push(message);
      }
    }

    const idempotencyStarted = performance.now();
    this.metrics.redisCommands += events.length;
    const claimed = await this.idempotency.claimMany(events.map((event) => event.eventId));
    this.metrics.idempotencyLatencyMs += performance.now() - idempotencyStarted;

    this.metrics.activeHandlers += 1;
    this.metrics.maxConcurrentHandlers = Math.max(
      this.metrics.maxConcurrentHandlers,
      this.metrics.activeHandlers,
    );
    try {
      for (let index = 0; index < events.length; index += 1) {
        const message = validMessages[index];
        const event = events[index];
        if (!claimed[index]) {
          this.metrics.telemetryEventsDuplicate += 1;
          this.logger.info({ eventId: event.eventId }, 'duplicate telemetry ignored');
          resolveOffset(message.offset);
          continue;
        }
        const startedAt = performance.now();
        try {
          await this.processEvent(event);
          this.metrics.processingLatencyMs += performance.now() - startedAt;
          this.metrics.telemetryEventsProcessed += 1;
          this.metrics.ingestionProcessingLatencyMs = performance.now() - startedAt;
          this.metrics.totalEventLatencyMs += performance.now() - receivedAt;
          resolveOffset(message.offset);
        } catch (error) {
          await this.idempotency.release(event.eventId);
          this.metrics.redisCommands += 1;
          this.metrics.telemetryEventsFailed += 1;
          this.logger.error(
            { err: error, errorClass: classifyIngestionError(error), eventId: event.eventId },
            'telemetry processing failed',
          );
          throw error;
        }
        await heartbeat();
      }
      for (const message of invalidMessages) resolveOffset(message.offset);
    } finally {
      this.metrics.activeHandlers -= 1;
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
