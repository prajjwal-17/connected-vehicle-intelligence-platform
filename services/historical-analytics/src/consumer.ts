import { Kafka } from 'kafkajs';
import type { Logger } from 'pino';
import { parseTelemetryEvent, type TelemetryEvent } from '@fleetpulse/schemas';
import type { HistoricalConfig } from './config.js';
import { ClickHouseStore } from './store.js';

export class HistoricalConsumer {
  private consumer?: ReturnType<Kafka['consumer']>;
  constructor(
    private readonly config: HistoricalConfig,
    private readonly store: ClickHouseStore,
    private readonly logger: Logger<never, boolean>,
  ) {}
  async start(): Promise<void> {
    await this.store.initialize();
    const kafka = new Kafka({
      clientId: 'fleetpulse-historical-analytics',
      brokers: this.config.brokers,
    });
    this.consumer = kafka.consumer({ groupId: this.config.group, allowAutoTopicCreation: false });
    await this.consumer.connect();
    await this.consumer.subscribe({
      topic: this.config.topic,
      fromBeginning: this.config.fromBeginning,
    });
    await this.consumer.run({
      eachBatchAutoResolve: false,
      eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
        const events: TelemetryEvent[] = [];
        for (const message of batch.messages) {
          try {
            events.push(parseTelemetryEvent(JSON.parse(message.value?.toString() ?? '')));
          } catch (error) {
            this.logger.warn({ err: error }, 'historical telemetry rejected');
            resolveOffset(message.offset);
          }
        }
        await this.store.insertBatch(events);
        for (const message of batch.messages) resolveOffset(message.offset);
        await heartbeat();
      },
    });
  }
  async stop(): Promise<void> {
    if (this.consumer) await this.consumer.disconnect();
  }
}
