import { Kafka, type Consumer, type EachBatchPayload } from 'kafkajs';
import type { Logger } from 'pino';
import type { StreamProcessorConfig } from './config.js';
import { StreamEventProcessor } from './processor.js';
import { StreamRedisIdempotencyStore } from './idempotency.js';

export async function processStreamEventsInOrder<T extends { eventId: string }>(
  events: T[],
  claimed: boolean[],
  processEvent: (event: T, index: number) => Promise<void>,
): Promise<void> {
  for (let index = 0; index < events.length; index += 1)
    if (claimed[index]) await processEvent(events[index], index);
}

export class StreamProcessorConsumer {
  private readonly consumer: Consumer;
  private readonly idempotency: StreamRedisIdempotencyStore;
  private running = false;
  readonly metrics = {
    messagesReceived: 0,
    decodeMs: 0,
    redisCommands: 0,
    redisLatencyMs: 0,
    redisErrors: 0,
  };

  constructor(
    private readonly config: StreamProcessorConfig,
    private readonly processor: StreamEventProcessor,
    private readonly logger: Logger<never, boolean>,
    idempotency = new StreamRedisIdempotencyStore(config.redisUrl, config.stateTtlSeconds),
  ) {
    const kafka = new Kafka({
      clientId: 'fleetpulse-stream-processor',
      brokers: config.kafkaBrokers,
      retry: { retries: 8, initialRetryTime: 100, maxRetryTime: 3_000 },
    });
    this.consumer = kafka.consumer({ groupId: config.groupId, allowAutoTopicCreation: false });
    this.idempotency = idempotency;
  }

  async start(): Promise<void> {
    await this.idempotency.connect();
    await this.consumer.connect();
    await this.consumer.subscribe({
      topic: this.config.kafkaTopic,
      fromBeginning: this.config.fromBeginning,
    });
    this.running = true;
    await this.consumer.run({
      eachBatchAutoResolve: false,
      eachBatch: (payload) => this.handleBatch(payload),
    });
  }

  private async handleBatch({ batch, resolveOffset, heartbeat }: EachBatchPayload): Promise<void> {
    const parsedMessages: typeof batch.messages = [];
    const events: Array<Record<string, unknown> & { eventId: string }> = [];
    const invalidMessages: typeof batch.messages = [];

    for (const message of batch.messages) {
      this.metrics.messagesReceived += 1;
      const raw = message.value?.toString();
      if (!raw) {
        invalidMessages.push(message);
        continue;
      }
      try {
        const started = performance.now();
        const decoded = JSON.parse(raw) as Record<string, unknown>;
        this.metrics.decodeMs += performance.now() - started;
        if (typeof decoded.eventId !== 'string') throw new Error('stream eventId is missing');
        parsedMessages.push(message);
        events.push(decoded as Record<string, unknown> & { eventId: string });
      } catch {
        invalidMessages.push(message);
      }
    }

    const idempotencyStarted = performance.now();
    let claimed: boolean[];
    try {
      claimed = await this.idempotency.claimMany(events.map((event) => event.eventId));
      this.metrics.redisCommands += events.length;
      this.metrics.redisLatencyMs += performance.now() - idempotencyStarted;
    } catch (error) {
      this.metrics.redisErrors += 1;
      this.metrics.redisCommands += events.length;
      this.metrics.redisLatencyMs += performance.now() - idempotencyStarted;
      throw error;
    }

    const claimedEvents = events.filter((_, index) => claimed[index]);
    await this.processor.processBatch(
      claimedEvents,
      async (index) => {
        const originalIndex = events.findIndex((event) => event === claimedEvents[index]);
        resolveOffset(parsedMessages[originalIndex].offset);
        await heartbeat();
      },
      async (index) => {
        const releaseStarted = performance.now();
        await this.idempotency.release(claimedEvents[index].eventId);
        this.metrics.redisCommands += 1;
        this.metrics.redisLatencyMs += performance.now() - releaseStarted;
      },
    );

    for (let index = 0; index < events.length; index += 1)
      if (!claimed[index]) resolveOffset(parsedMessages[index].offset);
    for (const message of invalidMessages) resolveOffset(message.offset);
  }

  async stop(): Promise<void> {
    if (!this.running) return;
    await this.consumer.disconnect();
    await this.idempotency.disconnect();
    this.running = false;
    this.logger.info('stream processor consumer stopped');
  }

  profile() {
    return { consumer: this.metrics, processor: this.processor.metrics };
  }
}
