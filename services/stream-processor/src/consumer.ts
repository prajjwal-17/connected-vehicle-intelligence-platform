import { Kafka, type Consumer } from 'kafkajs';
import { createClient, type RedisClientType } from 'redis';
import type { Logger } from 'pino';
import type { StreamProcessorConfig } from './config.js';
import { StreamEventProcessor } from './processor.js';

export class StreamProcessorConsumer {
  private readonly consumer: Consumer;
  private readonly redis: RedisClientType;
  private running = false;
  constructor(
    private readonly config: StreamProcessorConfig,
    private readonly processor: StreamEventProcessor,
    private readonly logger: Logger<never, boolean>,
  ) {
    const kafka = new Kafka({
      clientId: 'fleetpulse-stream-processor',
      brokers: config.kafkaBrokers,
      retry: { retries: 8, initialRetryTime: 100, maxRetryTime: 3_000 },
    });
    this.consumer = kafka.consumer({ groupId: config.groupId, allowAutoTopicCreation: false });
    this.redis = createClient({ url: config.redisUrl });
  }

  async start(): Promise<void> {
    await this.redis.connect();
    await this.consumer.connect();
    await this.consumer.subscribe({
      topic: this.config.kafkaTopic,
      fromBeginning: this.config.fromBeginning,
    });
    this.running = true;
    await this.consumer.run({
      eachMessage: async ({ message }) => {
        const raw = message.value?.toString();
        if (!raw) return;
        let eventId: string | undefined;
        try {
          eventId = JSON.parse(raw).eventId as string;
        } catch {
          /* validator records malformed payload */
        }
        if (eventId) {
          const claimed = await this.redis.set(`stream:idempotency:${eventId}`, '1', {
            NX: true,
            EX: this.config.stateTtlSeconds,
          });
          if (claimed !== 'OK') return;
        }
        try {
          await this.processor.process(JSON.parse(raw));
        } catch (error) {
          if (eventId) await this.redis.del(`stream:idempotency:${eventId}`);
          throw error;
        }
      },
    });
  }

  async stop(): Promise<void> {
    if (!this.running) return;
    await this.consumer.disconnect();
    if (this.redis.isOpen) await this.redis.quit();
    this.running = false;
    this.logger.info('stream processor consumer stopped');
  }
}
